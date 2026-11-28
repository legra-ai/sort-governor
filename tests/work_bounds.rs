//! Deterministic work bounds for spilling sorts and replay cursors.
//!
//! Wall-clock ceilings (`spill_throughput.rs`) are noisy; these tests bound
//! the *work* a sort does — descriptor-scoped operations, bytes moved, merge
//! passes and open readers — by formulas over the plan and the row count.
//! The regression they guard against paid one open/seek/close per row:
//! 20,000 rows of ~600 B took 45 s while every functional test stayed green.

use futures_util::StreamExt;
use sort_governor::{
    ReplayStep,
    ReplayWriter,
    SortPlan,
    SortSession,
    WorkStats,
};

/// Payload bytes per row, like small serialized triples plus keys.
const PAYLOAD_BYTES: usize = 600;
/// Upper bound on per-frame overhead: 12-byte header plus CBOR framing of
/// the key, ordinal and string length.
const FRAME_OVERHEAD_BYTES: u64 = 64;
/// The planner's minimum run buffer, where the regression hurt most.
const MIN_RUN_BUFFER_BYTES: usize = 1 << 20;
/// Session byte cap: leaves the full 1 MiB I/O chunk ceiling available.
const SESSION_LIMIT_BYTES: u64 = 64 << 20;
/// Rows per operation the batched engine must at least sustain overall.
const ROWS_PER_OP: u64 = 100;
/// Operations one run may cost beyond its chunks (dir, create, finish, open).
const OPS_PER_RUN: u64 = 4;
/// Fixed operations per sort (cleanup and the final merge setup).
const FIXED_OPS: u64 = 16;

/// One spilling sort over `rows` distinct keys pushed in reverse order.
struct Workload {
    rows: u64,
    duplicates: bool,
    dedup: bool,
    plan: SortPlan,
}

impl Workload {
    fn new(rows: u64, run_buffer_bytes: usize, max_fan_in: u32) -> Self {
        Self {
            rows,
            duplicates: false,
            dedup: false,
            plan: SortPlan::External {
                run_buffer_bytes,
                max_fan_in,
            },
        }
    }

    /// Duplicate every tenth key and collapse equal keys in the output.
    fn deduplicated(mut self, dedup: bool) -> Self {
        self.duplicates = true;
        self.dedup = dedup;
        self
    }

    fn pushes(&self) -> u64 {
        self.rows + if self.duplicates { self.rows / 10 } else { 0 }
    }

    fn payload(key: u64) -> String {
        let letter = char::from(b'a' + u8::try_from(key % 26).expect("letter"));
        letter.to_string().repeat(PAYLOAD_BYTES)
    }

    /// Push, drain and close; return the work the whole sort recorded.
    async fn run(&self) -> WorkStats {
        let root = tempfile::tempdir().expect("temp root");
        let mut session = SortSession::with_temp_dir(self.plan, root.path(), self.dedup)
            .with_memory_limit_bytes(SESSION_LIMIT_BYTES);
        let retained = PAYLOAD_BYTES + std::mem::size_of::<(u64, String)>();
        for key in (0..self.rows).rev() {
            session
                .push_with_size(key, Self::payload(key), retained)
                .await
                .expect("push");
            if self.duplicates && key % 10 == 0 {
                session
                    .push_with_size(key, Self::payload(key + 1), retained)
                    .await
                    .expect("push duplicate");
            }
        }
        let mut output = session.finish().await.expect("finish");
        let mut drained = 0u64;
        while let Some(value) = output.next().await {
            assert_eq!(value.expect("row").len(), PAYLOAD_BYTES);
            drained += 1;
        }
        let expected = if self.dedup { self.rows } else { self.pushes() };
        assert_eq!(drained, expected);
        output.close().await.expect("close");
        let stats = output.work_stats();
        println!("{} rows: {stats:?}", self.rows);
        stats
    }

    fn fan_in(&self) -> u64 {
        match self.plan {
            SortPlan::External { max_fan_in, .. } => u64::from(max_fan_in),
            SortPlan::InMemory => unreachable!("workloads spill"),
        }
    }
}

/// Bounds derived from one sort's recorded work.
trait WorkBounds {
    /// Spilled runs left on the binary-carry frontier handed to the cascade.
    fn frontier(&self) -> u64;
    /// Smallest `p` with `fan_in^p >= frontier`.
    fn frontier_depth(&self, fan_in: u64) -> u64;
    /// Every byte written is read back once by its merge (times the passes
    /// as slack); each refill re-reads at most one frame tail.
    fn assert_reads_bounded(&self);
}

impl WorkBounds for WorkStats {
    fn frontier(&self) -> u64 {
        u64::from(self.runs_spilled.count_ones())
    }

    fn frontier_depth(&self, fan_in: u64) -> u64 {
        let mut depth = 0;
        let mut covered = 1u64;
        while covered < self.frontier() {
            covered = covered.saturating_mul(fan_in);
            depth += 1;
        }
        depth
    }

    fn assert_reads_bounded(&self) {
        let tails = self.perform_ops * (PAYLOAD_BYTES as u64 + FRAME_OVERHEAD_BYTES);
        let bound = self.bytes_written * (self.merge_passes + 1) + tails;
        assert!(self.bytes_read <= bound, "{self:?} read bound {bound}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn twenty_thousand_rows_at_minimum_runs_pay_chunks_not_rows() {
    let workload = Workload::new(20_000, MIN_RUN_BUFFER_BYTES, 64).deduplicated(true);
    let stats = workload.run().await;
    let bound = workload.pushes() / ROWS_PER_OP + OPS_PER_RUN * stats.runs_spilled + FIXED_OPS;
    assert!(stats.runs_spilled > 1, "{stats:?} must spill several runs");
    assert!(stats.perform_ops <= bound, "{stats:?} op bound {bound}");
    // Carries rewrite each original run at most once per frontier level.
    let levels = u64::from(u64::BITS - stats.runs_spilled.leading_zeros());
    let written = workload.pushes() * (PAYLOAD_BYTES as u64 + FRAME_OVERHEAD_BYTES) * (levels + 1);
    assert!(
        stats.bytes_written <= written,
        "{stats:?} write bound {written}"
    );
    stats.assert_reads_bounded();
    assert!(stats.max_open_merge_readers <= workload.fan_in());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operations_per_row_do_not_grow_with_input_size() {
    let mut per_row = Vec::new();
    for rows in [2_000, 20_000, 50_000] {
        let workload = Workload::new(rows, MIN_RUN_BUFFER_BYTES, 64);
        let stats = workload.run().await;
        // bounded: three sizes.
        per_row.push((rows, stats.perform_ops, stats.runs_spilled));
    }
    for &(rows, ops, runs) in &per_row {
        let bound = rows / ROWS_PER_OP + OPS_PER_RUN * runs + FIXED_OPS;
        assert!(ops <= bound, "{rows} rows: {ops} ops, bound {bound}");
    }
    // Ops per row stay flat: carries add one frontier level per doubling
    // of the run count, amortized over a whole chunk, so 25x the rows may
    // raise the per-row rate by at most half. Per-row I/O paid >= 1 op per
    // row per level, so its rate tripled across these sizes.
    let (small_rows, small_ops, _) = per_row[0];
    let (large_rows, large_ops, _) = per_row[2];
    assert!(
        2 * large_ops * small_rows <= 3 * small_ops * large_rows,
        "ops per row grew: {per_row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_fan_in_cascades_in_logarithmic_passes() {
    // 64 KiB runs of ~100 rows: 3,100 rows spill 31+ runs, a frontier of
    // at least five, so fan-in two needs more than one cascade pass.
    let workload = Workload::new(3_100, 64 << 10, 2);
    let stats = workload.run().await;
    let fan_in = workload.fan_in();
    let frontier = stats.frontier();
    assert!(frontier > fan_in * fan_in, "{stats:?} must force a cascade");
    // The cascade folds until at most `fan_in` runs remain for streaming.
    let passes = stats.frontier_depth(fan_in) - 1;
    assert_eq!(stats.merge_passes, passes, "{stats:?}");
    assert!(stats.merge_groups <= frontier, "{stats:?}");
    assert!(stats.max_open_merge_readers <= fan_in, "{stats:?}");
    let carries = stats.runs_spilled - frontier;
    assert_eq!(stats.carry_merges, carries, "{stats:?}");
    stats.assert_reads_bounded();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deduplication_does_not_amplify_work() {
    let plain = Workload::new(20_000, MIN_RUN_BUFFER_BYTES, 64)
        .deduplicated(false)
        .run()
        .await;
    let collapsed = Workload::new(20_000, MIN_RUN_BUFFER_BYTES, 64)
        .deduplicated(true)
        .run()
        .await;
    assert_eq!(collapsed.runs_spilled, plain.runs_spilled);
    assert!(
        collapsed.perform_ops <= plain.perform_ops,
        "{collapsed:?} vs {plain:?}"
    );
    assert!(
        collapsed.bytes_read <= plain.bytes_read,
        "{collapsed:?} vs {plain:?}"
    );
}

#[tokio::test]
async fn replay_cursor_reads_about_one_frame_per_row() {
    const ROWS: u64 = 2_000;
    let root = tempfile::tempdir().expect("temp root");
    let mut writer = ReplayWriter::<String>::with_temp_dir(root.path(), SESSION_LIMIT_BYTES);
    for key in 0..ROWS {
        writer
            .append_with_size(Workload::payload(key), PAYLOAD_BYTES)
            .await
            .expect("append");
    }
    let replay = writer.seal().await.expect("seal");
    let sealed = replay.work_stats();
    let mut cursor = replay.cursor();
    let mut stepped = 0u64;
    while let ReplayStep::Item(value) = cursor.next_committed().await.expect("step") {
        assert_eq!(value.len(), PAYLOAD_BYTES);
        stepped += 1;
    }
    assert_eq!(stepped, ROWS);
    let stats = replay.work_stats();
    println!("replay: sealed {sealed:?} read {stats:?}");
    let read = stats.bytes_read - sealed.bytes_read;
    let ops = stats.perform_ops - sealed.perform_ops;
    // Each step reads its header, then its frame: the file once plus one
    // header per row, never a read-ahead chunk.
    let frame_bound = sealed.bytes_written + ROWS * FRAME_OVERHEAD_BYTES;
    assert!(
        read <= frame_bound,
        "{read} bytes read, bound {frame_bound}"
    );
    assert!(ops <= 2 * ROWS, "{ops} ops for {ROWS} steps");
    drop(cursor);
    replay.close().await.expect("close");
}

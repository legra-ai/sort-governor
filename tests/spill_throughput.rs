//! Regression: spilled rows must not each pay a file open/seek/close.
//!
//! Streamed callers open sorts with an unknown size (`SortSpec::new(0, 0)`).
//! When every frame reopened its run file and such sorts spilled at the 1 MiB
//! minimum run buffer, 20,000 rows of ~600 B took ~40 s in debug builds.
//! Payloads are CBOR text (like serialized terms); a `Vec<u8>` would encode
//! as 600 CBOR integers and measure the codec instead of run I/O.

use std::sync::Arc;
use std::time::{
    Duration,
    Instant,
};

use futures_util::StreamExt;
use sort_governor::{
    MemoryPressure,
    SortPlan,
    SortPlanner,
    SortSession,
    SortSpec,
    SorterConfig,
    SorterHandle,
    SorterSnapshot,
    StaticPressure,
};

/// Distinct keys pushed through each spilling session.
const ROWS: u32 = 20_000;
/// Payload bytes per row, matching small serialized triples plus keys.
const PAYLOAD_BYTES: usize = 600;
/// Debug-build ceiling for the planned unknown-size path.
const PLANNED_CEILING: Duration = Duration::from_secs(1);
/// Debug-build ceiling for many 1 MiB runs and their carry merges. The
/// per-row reopen regression took ~40 s; batched chunks take ~1.5 s.
const MINIMUM_RUN_CEILING: Duration = Duration::from_secs(5);
/// Session byte cap for the standalone sessions.
const SESSION_LIMIT_BYTES: u64 = 64 << 20;

/// Push `ROWS` keys in reverse order (every tenth duplicated), drain the
/// deduplicated output and return the elapsed time.
async fn sort_rows(mut session: SortSession<u32, String>) -> Duration {
    let started = Instant::now();
    for key in (0..ROWS).rev() {
        let letter = char::from(b'a' + u8::try_from(key % 26).expect("letter"));
        let payload = letter.to_string().repeat(PAYLOAD_BYTES);
        let retained = payload.capacity() + std::mem::size_of::<(u32, String)>();
        session
            .push_with_size(key, payload, retained)
            .await
            .expect("push");
        if key % 10 == 0 {
            session
                .push_with_size(key, "z".repeat(PAYLOAD_BYTES), retained)
                .await
                .expect("push duplicate");
        }
    }
    let mut output = session.finish().await.expect("finish");
    let mut rows = 0u32;
    while let Some(value) = output.next().await {
        assert_eq!(value.expect("row").len(), PAYLOAD_BYTES);
        rows += 1;
    }
    assert_eq!(rows, ROWS, "dedup keeps one row per key");
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_size_spill_of_twenty_thousand_rows_stays_well_under_a_second() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .expect("temp root");
    let snapshot = SorterSnapshot::new(1 << 30, 0, 256, 0);
    let plan = SortPlanner::plan(&SortSpec::new(0, 0), &snapshot, &SorterConfig::default());
    assert!(plan.is_external(), "unknown-size specs spill");
    let session = SortSession::with_temp_dir(plan, root.path(), true)
        .with_memory_limit_bytes(SESSION_LIMIT_BYTES);
    let elapsed = sort_rows(session).await;
    assert!(
        elapsed < PLANNED_CEILING,
        "{ROWS} rows took {elapsed:?}, ceiling {PLANNED_CEILING:?}"
    );
    root.remove().await.expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minimum_run_buffer_spills_do_not_pay_file_io_per_row() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .expect("temp root");
    let plan = SortPlan::External {
        run_buffer_bytes: 1 << 20,
        max_fan_in: 64,
    };
    let session = SortSession::with_temp_dir(plan, root.path(), true)
        .with_memory_limit_bytes(SESSION_LIMIT_BYTES);
    let elapsed = sort_rows(session).await;
    assert!(
        elapsed < MINIMUM_RUN_CEILING,
        "{ROWS} rows over 1 MiB runs took {elapsed:?}, ceiling {MINIMUM_RUN_CEILING:?}"
    );
    root.remove().await.expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_unknown_size_spill_releases_every_charge_when_output_drops() {
    let pressure: Arc<dyn MemoryPressure> = Arc::new(StaticPressure::new(256 << 20, 0));
    let scratch = tempfile::tempdir().expect("scratch directory");
    let sorter = SorterHandle::spawn(
        SorterConfig::from_fd_limit(256),
        160,
        pressure,
        scratch.path().to_path_buf(),
    );
    let lease = sorter
        .submit(SortSpec::new(0, 0).labelled("spill-throughput"))
        .await
        .expect("lease");
    let elapsed = sort_rows(lease.into_session(true)).await;
    assert!(
        elapsed < PLANNED_CEILING,
        "governed {ROWS} rows took {elapsed:?}, ceiling {PLANNED_CEILING:?}"
    );
    // Output cleanup runs on drop; wait (bounded) for its charges to return.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let used = sorter.stats().await.expect("stats").memory_bytes_used;
        if used == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{used} bytes still charged");
        tokio::task::yield_now().await;
    }
}

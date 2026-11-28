//! Work bounds: chunked run I/O pays one operation per chunk, not per row.
//!
//! The regression these guard against reopened the run file for every
//! frame: 10,000 rows cost 10,000 operations each way. The bounds below are
//! derived from the bytes moved and the chunk size the budget grants.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::{
    RunReader,
    RunWriter,
};
use crate::engine::budget::MemoryBudget;
use crate::engine::operation::IoScope;
use crate::engine::row::RunRow;

/// Rows written and read back by each test.
const ROWS: u32 = 10_000;
/// Payload bytes per row.
const PAYLOAD_BYTES: usize = 600;
/// Session cap; leaves the full 1 MiB chunk ceiling available.
const LIMIT_BYTES: u64 = 64 << 20;
/// Fixed operations: create/open plus the final count rewrite.
const FIXED_OPS: u64 = 2;

struct Fixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    fds: Arc<Semaphore>,
    budget: Arc<MemoryBudget>,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        Self {
            path: dir.path().join("run.cbor"),
            _dir: dir,
            fds: Arc::new(Semaphore::new(2)),
            budget: MemoryBudget::new(LIMIT_BYTES),
        }
    }

    /// Minimum chunk a flush or fill carries: a full chunk less one frame,
    /// halved for headroom drift while rows are charged.
    fn min_chunk(&self) -> u64 {
        u64::try_from(self.budget.io_chunk_bytes(0) / 2).expect("chunk fits u64")
    }

    async fn write(&self, io: &Arc<IoScope>) {
        let mut writer = RunWriter::create(
            self.path.clone(),
            Arc::clone(&self.fds),
            Arc::clone(&self.budget),
            Arc::clone(io),
        )
        .await
        .expect("create");
        for key in 0..ROWS {
            writer
                .write_row(&RunRow::new(key, "r".repeat(PAYLOAD_BYTES)))
                .await
                .expect("write row");
        }
        writer.finish().await.expect("finish");
    }
}

#[tokio::test]
async fn write_row_pays_one_operation_per_chunk() {
    let fixture = Fixture::new();
    let io = Arc::new(IoScope::default());
    fixture.write(&io).await;
    let stats = io.work().snapshot();
    let rows = u64::from(ROWS);
    assert!(stats.bytes_written >= rows * PAYLOAD_BYTES as u64);
    let bound = stats.bytes_written.div_ceil(fixture.min_chunk()) + FIXED_OPS;
    assert!(
        stats.perform_ops <= bound,
        "{ROWS} rows / {} bytes took {} ops, bound {bound}",
        stats.bytes_written,
        stats.perform_ops
    );
}

#[tokio::test]
async fn next_row_pays_one_operation_per_chunk() {
    let fixture = Fixture::new();
    fixture.write(&Arc::new(IoScope::default())).await;
    let io = Arc::new(IoScope::default());
    let mut reader: RunReader<u32, String> = RunReader::open(
        &fixture.path,
        Arc::clone(&fixture.fds),
        Arc::clone(&fixture.budget),
        Arc::clone(&io),
    )
    .await
    .expect("open");
    let mut rows = 0u32;
    while let Some(row) = reader.next_row().await.expect("next row") {
        assert_eq!(row.key, rows);
        rows += 1;
    }
    assert_eq!(rows, ROWS);
    let stats = io.work().snapshot();
    let bound = stats.bytes_read.div_ceil(fixture.min_chunk()) + FIXED_OPS;
    assert!(
        stats.perform_ops <= bound,
        "{ROWS} rows / {} bytes took {} ops, bound {bound}",
        stats.bytes_read,
        stats.perform_ops
    );
}

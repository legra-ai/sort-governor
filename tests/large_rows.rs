//! Rows larger than the run buffer must still be admitted under a small
//! governed pool, spilling one run per row without exceeding the limit.

use std::sync::Arc;

use futures_util::StreamExt;
use sort_governor::{
    MemoryPressure,
    SortSpec,
    SorterConfig,
    SorterError,
    SorterHandle,
    StaticPressure,
};

const POOL_BYTES: u64 = 1 << 20;
const ROWS: u32 = 128;
const PAYLOAD_BYTES: usize = 64 * 1024;
const RETAINED_BYTES: usize = 197_192;

#[tokio::test]
async fn rows_larger_than_the_run_buffer_spill_within_a_small_governed_pool()
-> Result<(), SorterError> {
    let pressure: Arc<dyn MemoryPressure> = Arc::new(StaticPressure::new(POOL_BYTES, 0));
    let scratch = tempfile::tempdir().expect("scratch directory");
    let sorter = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        pressure,
        scratch.path().to_path_buf(),
    );
    let lease = sorter.submit(SortSpec::new(0, 0)).await?;
    let mut session = lease.into_session::<u32, String>(false);
    for sequence in 0..ROWS {
        let key = sequence.wrapping_mul(37) % ROWS;
        let mut payload = format!("{key:08}");
        payload.push_str(&"x".repeat(PAYLOAD_BYTES - payload.len()));
        session.push_with_size(key, payload, RETAINED_BYTES).await?;
    }
    let mut files = std::fs::read_dir(session.scratch_dir()).expect("scratch has run files");
    assert!(files.next().is_some(), "rows spilled before finish");
    let mut stream = session.finish().await?;
    let mut previous = None;
    let mut count = 0_u32;
    while let Some(value) = stream.next().await {
        let value = value?;
        assert_eq!(value.len(), PAYLOAD_BYTES);
        let key: u32 = value[..8].parse().expect("key prefix");
        assert!(previous.is_none_or(|before| before < key), "key order");
        previous = Some(key);
        count += 1;
    }
    assert_eq!(count, ROWS);
    drop(stream);
    let stats = sorter.stats().await?;
    assert!(
        stats.memory_bytes_peak <= POOL_BYTES,
        "peak {} exceeds pool {POOL_BYTES}",
        stats.memory_bytes_peak
    );
    eprintln!("memory_bytes_peak = {}", stats.memory_bytes_peak);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while sorter.stats().await.expect("stats").memory_bytes_used != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("charges released after drop");
    Ok(())
}

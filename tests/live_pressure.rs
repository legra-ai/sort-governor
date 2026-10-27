//! New reservations must obey live pressure after admission.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use sort_governor::{
    MemoryPressure,
    SorterConfig,
    SorterError,
    SorterHandle,
};

struct Pressure(AtomicU64);
impl MemoryPressure for Pressure {
    fn effective_target_bytes(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
    fn resident_bytes(&self) -> u64 {
        0
    }
}

#[tokio::test]
async fn new_allocations_stop_when_live_memory_headroom_is_revoked() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let pressure = Arc::new(Pressure(AtomicU64::new(1024 * 1024)));
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        pressure.clone(),
        root.path().join("live"),
    );
    let mut writer = governor.replay_writer::<u32>().await.unwrap();
    writer.append_with_size(1, 4).await.unwrap();
    pressure.0.store(0, Ordering::Release);
    assert!(
        matches!(
            writer.append_with_size(2, 4).await,
            Err(SorterError::MemoryBudget { .. })
        ),
        "new retention must not use stale startup headroom"
    );
    assert!(matches!(
        writer.seal().await,
        Err(SorterError::SessionFailed)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while governor.stats().await.unwrap().memory_bytes_used != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    root.remove().await.unwrap();
}

//! Sealed replay preserves duplicates and independent bounded cursors.

use std::sync::Arc;

use sort_governor::{
    SorterConfig,
    SorterHandle,
    StaticPressure,
};

#[tokio::test]
async fn replay_cursors_checkpoint_without_holding_descriptors() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        Arc::new(StaticPressure::new(1024 * 1024, 0)),
        root.path().join("governed"),
    );
    let mut writer = governor.replay_writer::<u32>().await.unwrap();
    for value in [2, 2, 1, 3] {
        writer.append_with_size(value, 4).await.unwrap();
        assert_eq!(governor.stats().await.unwrap().fd_permits_available, 6);
    }
    let replay = writer.seal().await.unwrap();
    let mut first = replay.cursor();
    assert_eq!(first.next().await.unwrap(), Some(2));
    let mut second = first.clone();
    drop(replay);
    assert_eq!(first.next().await.unwrap(), Some(2));
    assert_eq!(second.next().await.unwrap(), Some(2));
    assert_eq!(first.next().await.unwrap(), Some(1));
    second.rewind();
    assert_eq!(second.next().await.unwrap(), Some(2));
    assert_eq!(first.next().await.unwrap(), Some(3));
    assert_eq!(first.next().await.unwrap(), None);
    assert_eq!(first.next().await.unwrap(), None);
    assert_eq!(governor.stats().await.unwrap().fd_permits_available, 6);
    drop((first, second));
    root.remove().await.unwrap();
}

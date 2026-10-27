//! Sealed replay preserves duplicates and independent bounded cursors.

use std::sync::Arc;

use futures_util::StreamExt;
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

#[tokio::test]
async fn replay_growth_keeps_resident_reservations_constant() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        Arc::new(StaticPressure::new(512 * 1024, 0)),
        root.path().join("bytes"),
    );
    let mut writer = governor.replay_writer::<Vec<u8>>().await.unwrap();
    let mut resident = None;
    for index in 0..1024 {
        let value = vec![u8::try_from(index % 251).unwrap(); 2048];
        writer
            .append_with_size(value, 2048 + std::mem::size_of::<Vec<u8>>())
            .await
            .unwrap();
        let stats = governor.stats().await.unwrap();
        assert!(stats.memory_bytes_peak <= stats.memory_bytes_limit);
        assert_eq!(
            *resident.get_or_insert(stats.memory_bytes_used),
            stats.memory_bytes_used
        );
    }
    let replay = writer.seal().await.unwrap();
    let mut cursor = replay.cursor();
    for index in 0..1024 {
        let row = cursor.next().await.unwrap().unwrap();
        assert_eq!(row.len(), 2048);
        assert!(
            row.iter()
                .all(|byte| *byte == u8::try_from(index % 251).unwrap())
        );
        assert_eq!(
            governor.stats().await.unwrap().memory_bytes_used,
            resident.unwrap()
        );
    }
    assert!(cursor.next().await.unwrap().is_none());
    drop((cursor, replay));
    assert_eq!(governor.stats().await.unwrap().memory_bytes_used, 0);
    root.remove().await.unwrap();
}

#[tokio::test]
async fn nested_sorted_and_replayed_inputs_share_one_real_descriptor() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        1,
        Arc::new(StaticPressure::new(1024 * 1024, 0)),
        root.path().join("one-fd"),
    );
    assert_eq!(governor.stats().await.unwrap().fd_permits_total, 1);
    let mut outer = governor
        .submit(sort_governor::SortSpec::new(0, 0))
        .await
        .unwrap()
        .into_session::<u8, Vec<u8>>(false);
    let mut inner = governor
        .submit(sort_governor::SortSpec::new(0, 0))
        .await
        .unwrap()
        .into_session::<u8, Vec<u8>>(false);
    let mut replay = governor.replay_writer::<u8>().await.unwrap();
    for key in (0..31u8).rev() {
        inner
            .push_with_size(key, vec![key; 16384], 16416)
            .await
            .unwrap();
        replay.append_with_size(key, 1).await.unwrap();
    }
    let replay = replay.seal().await.unwrap();
    let mut checkpoint = replay.cursor();
    let mut sorted = inner.finish().await.unwrap();
    for key in 0..31u8 {
        assert_eq!(checkpoint.next().await.unwrap(), Some(30 - key));
        let value = sorted.next().await.unwrap().unwrap();
        assert!(value.iter().all(|byte| *byte == key));
        outer.push_with_size(key, value, 16416).await.unwrap();
        assert_eq!(governor.stats().await.unwrap().fd_permits_available, 1);
    }
    assert!(sorted.next().await.is_none());
    let mut output = outer.finish().await.unwrap();
    for key in 0..31u8 {
        assert!(
            output
                .next()
                .await
                .unwrap()
                .unwrap()
                .iter()
                .all(|byte| *byte == key)
        );
    }
    assert!(output.next().await.is_none());
    drop((checkpoint, replay, output, sorted));
    root.remove().await.unwrap();
}

//! Cancellation at the actual asynchronous filesystem boundary.

use std::future::Future;
use std::sync::mpsc;
use std::task::{
    Context,
    Waker,
};
use std::time::Duration;

use async_fs_io::DirectoryReader;
use tokio::sync::oneshot;

use super::ReplayWriter;

#[test]
fn cancelled_directory_creation_cannot_resurrect_scratch() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = async_fs_io::TempDir::create(std::env::temp_dir())
            .await
            .unwrap();
        let mut writer = ReplayWriter::<u32>::with_temp_dir(root.path(), 1024 * 1024);
        let path = writer.dir.clone();
        let scope = writer.cleanup.scope();
        let (ready, started) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        // This boundary fixture occupies the sole OS worker so cancellation
        // occurs while directory creation is queued, rather than after it.
        let blocker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            let _ = gate.recv();
        });
        started.await.unwrap();
        {
            let append = writer.append_with_size(7, 4);
            tokio::pin!(append);
            let mut context = Context::from_waker(Waker::noop());
            assert!(append.as_mut().poll(&mut context).is_pending());
        }
        drop(writer);
        release.send(()).unwrap();
        blocker.await.unwrap();
        scope.idle().await;
        // Drain previously queued OS work before checking absence.
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if DirectoryReader::open_if_exists(&path)
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancelled directory creation must not leave late scratch behind");
        root.remove().await.unwrap();
    });
}

#[tokio::test]
async fn oversized_append_poisons_the_writer_before_creating_scratch() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let mut writer = ReplayWriter::<Vec<u8>>::with_temp_dir(root.path(), 64 * 1024);
    let path = writer.dir.clone();
    let result = writer
        .append_with_size(
            vec![0; 128 * 1024],
            128 * 1024 + std::mem::size_of::<Vec<u8>>(),
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::SorterError::MemoryBudget { .. })
    ));
    assert!(
        DirectoryReader::open_if_exists(path)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        writer.seal().await,
        Err(crate::SorterError::SessionFailed)
    ));
    root.remove().await.unwrap();
}

#[tokio::test]
async fn corrupt_frame_lengths_are_rejected_before_allocation_and_do_not_advance() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let mut writer = ReplayWriter::<u32>::with_temp_dir(root.path(), 64 * 1024);
    writer.append_with_size(11, 4).await.unwrap();
    writer.append_with_size(22, 4).await.unwrap();
    let replay = writer.seal().await.unwrap();
    let mut cursor = replay.cursor();
    assert_eq!(cursor.next().await.unwrap(), Some(11));
    let checkpoint = cursor.offset;
    let reserved = cursor.inner.budget.used();
    let mut file = async_fs_io::AsyncFile::open_write_at_end(&cursor.inner.path)
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(checkpoint))
        .await
        .unwrap();
    file.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
    file.flush().await.unwrap();
    drop(file);
    assert!(matches!(
        cursor.next().await,
        Err(crate::SorterError::MemoryBudget { .. } | crate::SorterError::RunLimit)
    ));
    assert_eq!(cursor.offset, checkpoint);
    assert_eq!(cursor.inner.budget.used(), reserved);
    assert!(cursor.inner.budget.peak() <= cursor.inner.budget.limit());
    assert_eq!(cursor.inner.fds.available_permits(), 2);
    cursor.rewind();
    assert_eq!(cursor.next().await.unwrap(), Some(11));
    drop((cursor, replay));
    root.remove().await.unwrap();
}

#[test]
fn pending_cleanup_keeps_its_memory_reservation_until_filesystem_completion() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = async_fs_io::TempDir::create(std::env::temp_dir())
            .await
            .unwrap();
        let governor = crate::SorterHandle::spawn(
            crate::SorterConfig::default(),
            6,
            std::sync::Arc::new(crate::StaticPressure::new(1024 * 1024, 0)),
            root.path().join("cleanup"),
        );
        // bounded: exactly eight independently owned replay scratch directories.
        let mut replays = Vec::with_capacity(8);
        for value in 0..8u32 {
            let mut writer = governor.replay_writer::<u32>().await.unwrap();
            writer.append_with_size(value, 4).await.unwrap();
            replays.push(writer.seal().await.unwrap());
        }
        let reserved = governor.stats().await.unwrap().memory_bytes_used;
        assert!(reserved > 0);
        let (ready, started) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        let os_worker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            let _ = gate.recv();
        });
        started.await.unwrap();
        drop(replays);
        tokio::task::yield_now().await;
        assert_eq!(
            governor.stats().await.unwrap().memory_bytes_used,
            reserved,
            "queued cleanup paths and worker state remain retained until OS completion"
        );
        release.send(()).unwrap();
        os_worker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while governor.stats().await.unwrap().memory_bytes_used != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        root.remove().await.unwrap();
    });
}

#[test]
fn deferred_directory_cleanup_obeys_the_same_descriptor_budget() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = async_fs_io::TempDir::create(std::env::temp_dir())
            .await
            .unwrap();
        let mut writer = ReplayWriter::<u32>::with_temp_dir(root.path(), 64 * 1024);
        writer.append_with_size(3, 4).await.unwrap();
        let replay = writer.seal().await.unwrap();
        let directory = replay.inner.path.parent().unwrap().to_path_buf();
        let budget = std::sync::Arc::clone(&replay.inner.budget);
        let reserved = std::sync::Arc::clone(&replay.inner.fds)
            .acquire_many_owned(2)
            .await
            .unwrap();
        let (ready, started) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        let os_worker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            let _ = gate.recv();
        });
        started.await.unwrap();
        drop(replay);
        tokio::task::yield_now().await;
        release.send(()).unwrap();
        os_worker.await.unwrap();
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(
            DirectoryReader::open_if_exists(&directory)
                .await
                .unwrap()
                .is_some(),
            "cleanup cannot open/remove directory state without a shared file permit"
        );
        assert!(budget.used() > 0);
        drop(reserved);
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.used() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        root.remove().await.unwrap();
    });
}

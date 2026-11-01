//! Abort joins the original scratch owner's work instead of sealing failure.

use std::future::Future;
use std::sync::{
    Arc,
    mpsc,
};
use std::task::{
    Context,
    Waker,
};

use async_fs_io::DirectoryReader;
use tokio::sync::oneshot;

use super::ReplayWriter;
use crate::SorterError;

#[tokio::test]
async fn abort_failed_append_removes_existing_scratch_and_is_repeatable() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let mut writer = ReplayWriter::<u32>::with_temp_dir(root.path(), 64 * 1024);
    writer.append_with_size(7, 4).await.unwrap();
    let path = writer.inner.dir.clone();
    let budget = Arc::clone(&writer.inner.budget);
    assert!(matches!(
        writer.append_with_size(8, 128 * 1024).await,
        Err(SorterError::MemoryBudget { .. })
    ));
    assert!(
        DirectoryReader::open_if_exists(&path)
            .await
            .unwrap()
            .is_some()
    );
    writer.abort().await.unwrap();
    assert!(
        DirectoryReader::open_if_exists(&path)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(budget.used(), 0);
    writer.abort().await.unwrap();
    assert!(matches!(
        writer.append_with_size(9, 4).await,
        Err(SorterError::SessionFailed)
    ));
    assert!(matches!(
        writer.seal().await,
        Err(SorterError::SessionFailed)
    ));
    root.remove().await.unwrap();
}

#[tokio::test]
async fn abort_invalidates_prefix_readers_but_requires_their_release() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let mut writer = ReplayWriter::<u32>::with_temp_dir(root.path(), 64 * 1024);
    writer.append_with_size(7, 4).await.unwrap();
    let mut cursor = writer.prefix_cursor();
    let path = writer.inner.dir.clone();
    assert!(matches!(
        writer.abort().await,
        Err(SorterError::ReplayInUse)
    ));
    assert!(matches!(
        cursor.next().await,
        Err(SorterError::ReplaySourceFailed)
    ));
    assert!(
        DirectoryReader::open_if_exists(&path)
            .await
            .unwrap()
            .is_some()
    );
    drop(cursor);
    writer.abort().await.unwrap();
    assert!(
        DirectoryReader::open_if_exists(&path)
            .await
            .unwrap()
            .is_none()
    );
    root.remove().await.unwrap();
}

#[test]
fn abort_joins_cancelled_creation_and_survives_its_own_cancellation() {
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
        let path = writer.inner.dir.clone();
        let budget = Arc::clone(&writer.inner.budget);
        let fds = Arc::clone(&writer.inner.fds);
        let (ready, started) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        // Audited boundary fixture: queue actual filesystem creation behind
        // the sole OS worker, then cancel its owning append future.
        let blocker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            gate.recv().unwrap();
        });
        started.await.unwrap();
        {
            let append = writer.append_with_size(7, 4);
            tokio::pin!(append);
            assert!(
                append
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        {
            let abort = writer.abort();
            tokio::pin!(abort);
            assert!(
                abort
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert!(budget.used() > 0, "pending cleanup retains its escrow");
        release.send(()).unwrap();
        blocker.await.unwrap();
        writer.abort().await.unwrap();
        assert!(
            DirectoryReader::open_if_exists(&path)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(budget.used(), 0);
        assert_eq!(fds.available_permits(), 2);
        writer.abort().await.unwrap();
        root.remove().await.unwrap();
    });
}

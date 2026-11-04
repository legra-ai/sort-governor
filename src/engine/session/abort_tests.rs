//! Explicit cleanup retains the original owner across cancellation.

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
use futures_util::StreamExt;
use tokio::sync::oneshot;

use super::SortSession;
use crate::{
    SortPlan,
    SorterError,
};

#[tokio::test]
async fn session_abort_releases_rows_and_invalidates_publication() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let mut session =
        SortSession::<u32, u32>::with_temp_dir(SortPlan::InMemory, root.path(), false);
    session.push(1, 7).await.unwrap();
    let budget = Arc::clone(&session.budget);
    assert!(budget.used() > 0);
    session.abort().await.unwrap();
    assert_eq!(budget.used(), 0);
    session.abort().await.unwrap();
    assert!(matches!(
        session.finish().await,
        Err(SorterError::SessionFailed)
    ));
    root.remove().await.unwrap();
}

#[tokio::test]
async fn output_close_releases_unconsumed_rows_and_scratch() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    for spill in [false, true] {
        let mut session =
            SortSession::<u32, u32>::with_temp_dir(SortPlan::InMemory, root.path(), false);
        session.push(1, 7).await.unwrap();
        session.push(2, 8).await.unwrap();
        if spill {
            session.spill().await.unwrap();
        }
        let path = session.dir.clone();
        let budget = Arc::clone(&session.budget);
        let mut output = session.finish().await.unwrap();
        assert_eq!(output.next().await.unwrap().unwrap(), 7);
        output.close().await.unwrap();
        assert_eq!(budget.used(), 0);
        assert!(output.next().await.is_none());
        output.abort().await.unwrap();
        assert!(
            DirectoryReader::open_if_exists(path)
                .await
                .unwrap()
                .is_none()
        );
    }
    root.remove().await.unwrap();
}

#[tokio::test]
async fn failed_finish_keeps_original_scratch_until_abort() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let lease = Arc::new(());
    let mut session =
        SortSession::<u32, u32>::with_temp_dir(SortPlan::InMemory, root.path(), false)
            .hold_resource(Box::new(Arc::clone(&lease)));
    session.push(1, 7).await.unwrap();
    session.spill().await.unwrap();
    let run = session.spills.iter().flatten().next().unwrap().clone();
    async_fs_io::remove_if_exists(run).await.unwrap();
    let budget = Arc::clone(&session.budget);
    let path = session.dir.clone();
    assert!(session.finish().await.is_err());
    assert_eq!(Arc::strong_count(&lease), 2);
    session.abort().await.unwrap();
    assert_eq!(Arc::strong_count(&lease), 1);
    assert_eq!(budget.used(), 0);
    assert!(
        DirectoryReader::open_if_exists(path)
            .await
            .unwrap()
            .is_none()
    );
    root.remove().await.unwrap();
}

#[test]
fn cancelled_finish_and_output_close_retain_pending_filesystem_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = async_fs_io::TempDir::create(std::env::temp_dir())
            .await
            .unwrap();
        for output_phase in [false, true] {
            let lease = Arc::new(());
            let mut session =
                SortSession::<u32, u32>::with_temp_dir(SortPlan::InMemory, root.path(), false)
                    .hold_resource(Box::new(Arc::clone(&lease)));
            session.push(1, 7).await.unwrap();
            session.push(2, 8).await.unwrap();
            session.spill().await.unwrap();
            let budget = Arc::clone(&session.budget);
            let fds = Arc::clone(&session.fds);
            let path = session.dir.clone();
            let mut output = if output_phase {
                Some(session.finish().await.unwrap())
            } else {
                None
            };
            let (ready, started) = oneshot::channel();
            let (release, gate) = mpsc::channel();
            // Audited boundary fixture: queue actual I/O behind the sole OS worker.
            let blocker = tokio::task::spawn_blocking(move || {
                ready.send(()).unwrap();
                gate.recv().unwrap();
            });
            started.await.unwrap();
            if let Some(output) = &mut output {
                {
                    let next = output.next();
                    tokio::pin!(next);
                    assert!(
                        next.as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
                {
                    let close = output.close();
                    tokio::pin!(close);
                    assert!(
                        close
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
            } else {
                {
                    let finish = session.finish();
                    tokio::pin!(finish);
                    assert!(
                        finish
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
                {
                    let abort = session.abort();
                    tokio::pin!(abort);
                    assert!(
                        abort
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
            }
            assert!(budget.used() > 0);
            assert_eq!(Arc::strong_count(&lease), 2);
            release.send(()).unwrap();
            blocker.await.unwrap();
            if let Some(output) = &mut output {
                output.abort().await.unwrap();
            } else {
                session.abort().await.unwrap();
            }
            assert_eq!(Arc::strong_count(&lease), 1);
            assert_eq!(budget.used(), 0);
            assert_eq!(fds.available_permits(), 2);
            assert!(
                DirectoryReader::open_if_exists(path)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        root.remove().await.unwrap();
    });
}

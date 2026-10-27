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

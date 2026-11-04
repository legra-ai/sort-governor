//! Explicit cleanup retains the original owner across cancellation.

use std::sync::Arc;

use async_fs_io::DirectoryReader;
use futures_util::StreamExt;

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

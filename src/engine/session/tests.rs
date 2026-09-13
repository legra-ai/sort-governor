//! Spill metadata bounds, ordinal exhaustion, and stable merge semantics.

use async_fs_io::DirectoryReader;
use futures_util::StreamExt;

use super::SortSession;
use crate::plan::SortPlan;

#[tokio::test]
async fn spill_frontier_stays_bounded_and_preserves_first_equal_row() {
    for max_fan_in in [2, 3, 7] {
        for dedup in [false, true] {
            let mut session = SortSession::with_temp_dir(
                SortPlan::External {
                    run_buffer_bytes: 1,
                    max_fan_in,
                },
                &std::env::temp_dir(),
                dedup,
            );
            for sequence in 0..257u32 {
                session
                    .push_with_size(sequence % 7, sequence, 1)
                    .await
                    .expect("push");
                assert!(
                    session.spills.len() <= 64,
                    "spill metadata grows with input"
                );
                let Some(mut files) = DirectoryReader::open_if_exists(session.scratch_dir())
                    .await
                    .expect("open scratch")
                else {
                    continue;
                };
                let mut count = 0;
                while files.next().await.expect("next file").is_some() {
                    count += 1;
                    assert!(count <= 64, "spill files grow with input before finish");
                }
            }
            let mut output = session.finish().await.expect("finish");
            for key in 0..7u32 {
                for sequence in (key..257).step_by(7) {
                    assert_eq!(output.next().await.expect("row").expect("value"), sequence);
                    if dedup {
                        break;
                    }
                }
            }
            assert!(output.next().await.is_none());
        }
    }
}

#[tokio::test]
async fn exhausted_spill_ordinal_returns_error_before_creating_a_file() {
    let mut session = SortSession::with_temp_dir(
        SortPlan::External {
            run_buffer_bytes: 1,
            max_fan_in: 2,
        },
        &std::env::temp_dir(),
        false,
    );
    session.next_run = !0;
    session
        .push_with_size(0u32, 0u32, 1)
        .await
        .expect("first row");
    assert!(
        session.push_with_size(1, 1, 1).await.is_err(),
        "ordinal must not wrap"
    );
    assert!(
        DirectoryReader::open_if_exists(session.scratch_dir())
            .await
            .expect("inspect scratch")
            .is_none()
    );
}

#[tokio::test]
async fn failed_spill_cannot_be_retried_or_finished() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .expect("temp dir");
    let dir = root.path().join("blocked");
    drop(
        async_fs_io::AsyncFile::create(&dir)
            .await
            .expect("block scratch"),
    );
    let mut session = SortSession::new(
        SortPlan::External {
            run_buffer_bytes: 1,
            max_fan_in: 2,
        },
        dir.clone(),
        false,
    );
    session
        .push_with_size(0u32, 0u32, 1)
        .await
        .expect("first row");
    assert!(session.push_with_size(1, 1, 1).await.is_err());
    async_fs_io::remove_if_exists(&dir)
        .await
        .expect("remove blocker");
    assert!(
        session.push_with_size(2, 2, 1).await.is_err(),
        "failed session must stay failed"
    );
    assert!(
        session.finish().await.is_err(),
        "failed session cannot yield partial results"
    );
    root.remove().await.expect("cleanup");
}

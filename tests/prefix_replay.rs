//! Demand-driven committed-prefix replay, without a producer thread.

use std::future::Future;
use std::sync::{
    Arc,
    mpsc,
};
use std::task::{
    Context,
    Waker,
};
use std::time::Duration;

use sort_governor::{
    ReplayStep,
    SorterConfig,
    SorterError,
    SorterHandle,
    StaticPressure,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn committed_prefix_checkpoints_distinguish_more_input_from_final_eof() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        Arc::new(StaticPressure::new(1024 * 1024, 0)),
        root.path().to_path_buf(),
    );
    let mut writer = governor.replay_writer::<u32>().await.unwrap();
    let mut cursor = writer.prefix_cursor();
    assert_eq!(
        cursor.next_committed().await.unwrap(),
        ReplayStep::AwaitingAppend
    );
    assert_eq!(governor.stats().await.unwrap().memory_bytes_used, 0);
    writer.append_with_size(4, 4).await.unwrap();
    assert_eq!(cursor.next_committed().await.unwrap(), ReplayStep::Item(4));
    let mut checkpoint = cursor.clone();
    for value in [4, 1] {
        writer.append_with_size(value, 4).await.unwrap();
    }
    for reader in [&mut cursor, &mut checkpoint] {
        assert_eq!(reader.next_committed().await.unwrap(), ReplayStep::Item(4));
        assert_eq!(reader.next_committed().await.unwrap(), ReplayStep::Item(1));
        assert_eq!(
            reader.next_committed().await.unwrap(),
            ReplayStep::AwaitingAppend
        );
    }
    let replay = writer.seal().await.unwrap();
    assert_eq!(
        cursor.next_committed().await.unwrap(),
        ReplayStep::Exhausted
    );
    assert_eq!(
        checkpoint.next_committed().await.unwrap(),
        ReplayStep::Exhausted
    );
    checkpoint.rewind();
    assert_eq!(
        checkpoint.next_committed().await.unwrap(),
        ReplayStep::Item(4)
    );
    drop((cursor, checkpoint));
    replay.close().await.unwrap();
    assert_eq!(governor.stats().await.unwrap().memory_bytes_used, 0);
    root.remove().await.unwrap();
}

#[tokio::test]
async fn caller_can_emit_first_witness_after_pulling_only_one_input_row() {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .unwrap();
    let governor = SorterHandle::spawn(
        SorterConfig::default(),
        6,
        Arc::new(StaticPressure::new(1024 * 1024, 0)),
        root.path().to_path_buf(),
    );
    let mut writer = governor.replay_writer::<u32>().await.unwrap();
    let mut cursor = writer.prefix_cursor();
    let mut input = 0..10_000u32;
    let mut pulled = 0;
    loop {
        match cursor.next_committed().await.unwrap() {
            ReplayStep::AwaitingAppend => {
                let value = input.next().unwrap();
                pulled += 1;
                writer.append_with_size(value, 4).await.unwrap();
            }
            ReplayStep::Item(value) => {
                assert_eq!(value, 0);
                break;
            }
            ReplayStep::Exhausted => panic!("unsealed input cannot claim final absence"),
        }
    }
    assert_eq!(pulled, 1, "first witness must not drain remaining relation");
    drop((cursor, writer));
    tokio::time::timeout(Duration::from_secs(2), async {
        while governor.stats().await.unwrap().memory_bytes_used != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    root.remove().await.unwrap();
}

#[test]
fn canceled_append_poisoning_never_publishes_the_inflight_tail() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = async_fs_io::TempDir::create(std::env::temp_dir())
            .await
            .unwrap();
        let governor = SorterHandle::spawn(
            SorterConfig::default(),
            6,
            Arc::new(StaticPressure::new(1024 * 1024, 0)),
            root.path().to_path_buf(),
        );
        let mut writer = governor.replay_writer::<u32>().await.unwrap();
        writer.append_with_size(1, 4).await.unwrap();
        let mut cursor = writer.prefix_cursor();
        assert_eq!(cursor.next_committed().await.unwrap(), ReplayStep::Item(1));
        let (ready, started) = oneshot::channel();
        let (release, gate) = mpsc::channel();
        let os_worker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            let _ = gate.recv();
        });
        started.await.unwrap();
        {
            let append = writer.append_with_size(2, 4);
            tokio::pin!(append);
            let mut context = Context::from_waker(Waker::noop());
            assert!(append.as_mut().poll(&mut context).is_pending());
        }
        assert!(matches!(
            cursor.next_committed().await,
            Err(SorterError::ReplaySourceFailed)
        ));
        assert!(matches!(
            writer.seal().await,
            Err(SorterError::SessionFailed)
        ));
        release.send(()).unwrap();
        os_worker.await.unwrap();
        drop(cursor);
        tokio::time::timeout(Duration::from_secs(2), async {
            while governor.stats().await.unwrap().memory_bytes_used != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(governor.stats().await.unwrap().fd_permits_available, 6);
        root.remove().await.unwrap();
    });
}

//! Engine behaviour: in-memory and external paths, the bounded multi-pass
//! cascade (the EMFILE regression), dedup, and empty input.

use std::path::PathBuf;

use futures_util::StreamExt;
use tempfile::TempDir;

use crate::engine::run::RunReader;
use crate::engine::session::SortSession;
use crate::error::SorterError;
use crate::plan::SortPlan;

fn scratch() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let sort_dir = dir.path().join("sort");
    (dir, sort_dir)
}

async fn drain(
    mut stream: crate::engine::merge::ValueStream<u32>,
) -> Result<Vec<u32>, SorterError> {
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Push `keys` (value == key) into a fresh session and collect the result.
async fn run(plan: SortPlan, dedup: bool, keys: &[u32]) -> Vec<u32> {
    let (_guard, dir) = scratch();
    let mut session: SortSession<u32, u32> = SortSession::new(plan, dir, dedup);
    for &k in keys {
        // A fixed 100-byte estimate against a 1-byte run buffer forces one
        // run per row on the external path.
        session.push_with_size(k, k, 100).await.expect("push");
    }
    drain(session.finish().await.expect("finish"))
        .await
        .expect("drain")
}

#[tokio::test]
async fn in_memory_sorts_ascending() {
    let out = run(SortPlan::InMemory, false, &[5, 3, 9, 1, 4, 1, 8]).await;
    assert_eq!(out, vec![1, 1, 3, 4, 5, 8, 9]);
}

#[tokio::test]
async fn external_without_spill_sorts_ascending() {
    // A generous run buffer keeps everything in one in-memory run.
    let plan = SortPlan::External {
        run_buffer_bytes: 1 << 20,
        max_fan_in: 4,
    };
    let out = run(plan, false, &[7, 2, 7, 0, 5]).await;
    assert_eq!(out, vec![0, 2, 5, 7, 7]);
}

#[tokio::test]
async fn external_cascade_many_runs_small_fan_in() {
    // 200 rows, one run each, merged three-at-a-time: several cascade passes
    // with never more than three readers open. This is the EMFILE regression.
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 3,
    };
    let keys: Vec<u32> = (0..200u32).rev().collect();
    let out = run(plan, false, &keys).await;
    let expected: Vec<u32> = (0..200u32).collect();
    assert_eq!(out, expected);
}

#[tokio::test]
async fn external_cascade_fan_in_two_is_stable() {
    // fan_in = 2 forces the deepest cascade; duplicates must survive in order.
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 2,
    };
    let keys: Vec<u32> = (0..64u32).map(|i| (i * 7) % 13).collect();
    let mut expected = keys.clone();
    expected.sort_unstable();
    let out = run(plan, false, &keys).await;
    assert_eq!(out, expected);
}

#[tokio::test]
async fn dedup_collapses_equal_keys_across_runs() {
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 3,
    };
    let keys = [4u32, 1, 4, 1, 2, 2, 2, 0, 4];
    let out = run(plan, true, &keys).await;
    assert_eq!(out, vec![0, 1, 2, 4]);
}

#[tokio::test]
async fn dedup_in_memory_collapses_equal_keys() {
    let out = run(SortPlan::InMemory, true, &[3, 3, 1, 1, 1, 2]).await;
    assert_eq!(out, vec![1, 2, 3]);
}

#[tokio::test]
async fn empty_input_yields_empty_stream() {
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 3,
    };
    assert_eq!(run(plan, false, &[]).await, Vec::<u32>::new());
}

#[tokio::test]
async fn scratch_dir_is_removed_once_the_stream_is_consumed() {
    let (_guard, dir) = scratch();
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 2,
    };
    let mut session: SortSession<u32, u32> = SortSession::new(plan, dir.clone(), false);
    for k in [3u32, 1, 2] {
        session.push_with_size(k, k, 100).await.expect("push");
    }
    assert!(dir.is_dir(), "spills must have created the scratch dir");
    let out = drain(session.finish().await.expect("finish"))
        .await
        .expect("drain");
    assert_eq!(out, vec![1, 2, 3]);
    assert!(
        !dir.exists(),
        "scratch dir must be gone once the stream ends"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scratch_dir_is_removed_when_the_stream_is_dropped_early() {
    let (_guard, dir) = scratch();
    let plan = SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in: 2,
    };
    let mut session: SortSession<u32, u32> = SortSession::new(plan, dir.clone(), false);
    for k in (0..8u32).rev() {
        session.push_with_size(k, k, 100).await.expect("push");
    }
    let mut stream = session.finish().await.expect("finish");
    let first = stream.next().await.expect("one item").expect("ok");
    assert_eq!(first, 0);
    drop(stream);
    // Cleanup is spawned onto the runtime on drop; give it a bounded window.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while dir.exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !dir.exists(),
        "scratch dir must be removed after an early drop"
    );
}

#[tokio::test]
async fn in_memory_plan_never_touches_the_scratch_dir() {
    let (_guard, dir) = scratch();
    let mut session: SortSession<u32, u32> =
        SortSession::new(SortPlan::InMemory, dir.clone(), false);
    for k in [2u32, 1] {
        session.push(k, k).await.expect("push");
    }
    let out = drain(session.finish().await.expect("finish"))
        .await
        .expect("drain");
    assert_eq!(out, vec![1, 2]);
    assert!(
        !dir.exists(),
        "an in-memory sort must not create a scratch dir"
    );
}

#[tokio::test]
async fn corrupt_run_row_surfaces_a_decode_error() {
    let (_guard, dir) = scratch();
    async_fs_io::ensure_dir(&dir).await.expect("dir");
    let path = dir.join("bad.cbor");
    let mut file = async_fs_io::AsyncFile::create(&path).await.expect("create");
    // One row claimed, three bytes of garbage that is not a CBOR `RunRow`.
    file.write_all(&1u64.to_le_bytes()).await.expect("count");
    file.write_all(&3u32.to_le_bytes()).await.expect("len");
    file.write_all(&[0xff, 0xff, 0xff]).await.expect("body");
    file.flush().await.expect("flush");
    drop(file);

    let mut reader: RunReader<u32, u32> = RunReader::open(&path).await.expect("open");
    let err = reader
        .next_row()
        .await
        .expect_err("garbage must not decode");
    assert!(matches!(err, SorterError::Decode(_)), "got {err:?}");
}

#[tokio::test]
async fn truncated_run_surfaces_an_io_error() {
    let (_guard, dir) = scratch();
    async_fs_io::ensure_dir(&dir).await.expect("dir");
    let path = dir.join("short.cbor");
    let mut file = async_fs_io::AsyncFile::create(&path).await.expect("create");
    // One row claimed, a 64-byte frame promised, but the file ends early.
    file.write_all(&1u64.to_le_bytes()).await.expect("count");
    file.write_all(&64u32.to_le_bytes()).await.expect("len");
    file.write_all(&[0u8; 8]).await.expect("partial body");
    file.flush().await.expect("flush");
    drop(file);

    let mut reader: RunReader<u32, u32> = RunReader::open(&path).await.expect("open");
    let err = reader
        .next_row()
        .await
        .expect_err("a truncated run must fail");
    assert!(matches!(err, SorterError::Io(_)), "got {err:?}");
}

//! Spill metadata bounds, ordinal exhaustion, and stable merge semantics.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use async_fs_io::DirectoryReader;
use futures_util::StreamExt;

use super::SortSession;
use crate::engine::budget::MemoryBudget;
use crate::plan::SortPlan;
use crate::service::MemoryPressure;

struct LivePressure(AtomicU64);
impl MemoryPressure for LivePressure {
    fn effective_target_bytes(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
    fn resident_bytes(&self) -> u64 {
        0
    }
}

#[tokio::test]
async fn full_retained_run_spills_before_the_next_row_arrives() {
    let pressure = Arc::new(LivePressure(AtomicU64::new(1024 * 1024)));
    let mut session = SortSession::<u32, Vec<u8>>::with_temp_dir(
        SortPlan::InMemory,
        &std::env::temp_dir(),
        false,
    )
    .with_memory_budget(MemoryBudget::governed(1024 * 1024, pressure.clone()));
    // bounded: 32 real 4096-byte retained rows fill the exact 128-KiB run
    // threshold. Empty lengths keep frames small without faking heap weights.
    for key in 0..32 {
        let value = Vec::with_capacity(4096 - std::mem::size_of::<(u32, Vec<u8>)>());
        let bytes = value.capacity() + std::mem::size_of::<(u32, Vec<u8>)>();
        assert_eq!(bytes, 4096);
        session
            .push_with_size(key, value, bytes)
            .await
            .expect("small row fits");
    }
    pressure.0.store(240 * 1024, Ordering::Release);
    let next = Vec::with_capacity(64 * 1024);
    let next_bytes = next.capacity() + std::mem::size_of::<(u32, Vec<u8>)>();
    session
        .push_with_size(32, next, next_bytes)
        .await
        .expect("full previous run must not deny the next row");
    let budget = Arc::clone(&session.budget);
    let mut output = session.finish().await.expect("finish bounded merge");
    for _ in 0..33 {
        assert!(
            output
                .next()
                .await
                .expect("complete output")
                .expect("bounded row")
                .is_empty()
        );
    }
    assert!(output.next().await.is_none());
    assert!(budget.peak() <= 1024 * 1024);
}

#[tokio::test]
async fn spill_frontier_stays_bounded_and_preserves_first_equal_row() {
    for rows in [255, 257] {
        for max_fan_in in [2, 3, 7] {
            for dedup in [false, true] {
                assert_bounded_sort(rows, max_fan_in, dedup).await;
            }
        }
    }
}

async fn assert_bounded_sort(rows: u32, max_fan_in: u32, dedup: bool) {
    let mut session = SortSession::with_temp_dir(
        SortPlan::External {
            run_buffer_bytes: 1,
            max_fan_in,
        },
        &std::env::temp_dir(),
        dedup,
    );
    for sequence in 0..rows {
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
        for sequence in (key..rows).step_by(7) {
            assert_eq!(output.next().await.expect("row").expect("value"), sequence);
            if dedup {
                break;
            }
        }
    }
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn exhausted_spill_ordinal_returns_error_before_creating_a_file() {
    let mut session = SortSession::with_temp_dir(
        SortPlan::External {
            run_buffer_bytes: 2 * std::mem::size_of::<crate::engine::row::RunRow<u32, u32>>(),
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
            run_buffer_bytes: 2 * std::mem::size_of::<crate::engine::row::RunRow<u32, u32>>(),
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

#[tokio::test]
async fn underestimated_in_memory_input_spills_at_the_runtime_byte_limit() {
    let mut session = SortSession::with_temp_dir(SortPlan::InMemory, &std::env::temp_dir(), false);
    // bounded: 144 fixed 64KiB records exceed the 8MiB adaptive threshold.
    for key in (0..144u32).rev() {
        session
            .push_with_size(
                key,
                vec![u8::try_from(key).unwrap(); 64 * 1024],
                64 * 1024 + 64,
            )
            .await
            .unwrap();
    }
    assert!(
        session.next_run > 0,
        "an underestimated plan must spill actual bytes"
    );
    let mut stream = session.finish().await.unwrap();
    for key in 0..144u32 {
        let value = stream.next().await.unwrap().unwrap();
        assert_eq!(value.len(), 64 * 1024);
        assert!(value.iter().all(|byte| *byte == u8::try_from(key).unwrap()));
    }
    assert!(stream.next().await.is_none());
}

//! End-to-end governor behaviour: planning through the handle, sorting
//! through a lease, and fd-permit accounting on grant and release.

use std::sync::Arc;

use futures_util::StreamExt;
use tempfile::TempDir;

use crate::config::SorterConfig;
use crate::service::handle::SorterHandle;
use crate::service::pressure::StaticPressure;
use crate::spec::SortSpec;

const MIB: u64 = 1024 * 1024;

fn spawn(fd_budget: u32, target: u64, resident: u64) -> (TempDir, SorterHandle) {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = SorterHandle::spawn(
        SorterConfig::default(),
        fd_budget,
        Arc::new(StaticPressure::new(target, resident)),
        dir.path().join("sorts"),
    );
    (dir, handle)
}

async fn drain(mut stream: crate::engine::ValueStream<u32>) -> Vec<u32> {
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.expect("stream item"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_sort_through_handle() {
    let (_dir, handle) = spawn(64, 1 << 30, 0);
    let lease = handle
        .submit(SortSpec::new(1_000, MIB))
        .await
        .expect("submit");
    assert!(lease.plan().is_in_memory());

    let mut session = lease.into_session::<u32, u32>(false);
    for k in [5u32, 1, 4, 2, 3] {
        session.push(k, k).await.expect("push");
    }
    let out = drain(session.finish().await.expect("finish")).await;
    assert_eq!(out, vec![1, 2, 3, 4, 5]);

    let stats = handle.stats().await.expect("stats");
    assert_eq!(stats.in_memory_sorts, 1);
    assert_eq!(stats.external_sorts, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_sort_through_handle_spills_and_merges() {
    // est_bytes above the 8 MiB ceiling forces the external path; pushing
    // > 1 MiB of rows forces real spills and a merge through the lease.
    let (_dir, handle) = spawn(128, 8 * MIB, 0);
    let lease = handle
        .submit(SortSpec::new(3_000, 64 * MIB))
        .await
        .expect("submit");
    assert!(lease.plan().is_external());

    let mut session = lease.into_session::<u32, u32>(false);
    let keys: Vec<u32> = (0..3_000u32).rev().collect();
    for k in &keys {
        session.push_with_size(*k, *k, 1024).await.expect("push");
    }
    let out = drain(session.finish().await.expect("finish")).await;
    assert_eq!(out, (0..3_000u32).collect::<Vec<_>>());

    let stats = handle.stats().await.expect("stats");
    assert_eq!(stats.external_sorts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_lease_holds_and_releases_fd_permits() {
    let (_dir, handle) = spawn(128, 8 * MIB, 0);
    let lease = handle
        .submit(SortSpec::new(1_000_000, 64 * MIB))
        .await
        .expect("submit");
    assert!(lease.plan().is_external());

    let busy = handle.stats().await.expect("stats");
    assert_eq!(busy.fd_permits_total, 128);
    assert_eq!(busy.active_external_sorts, 1);
    // Plentiful fds with no concurrency -> the full configured fan-in (64).
    assert_eq!(busy.fd_permits_available, 128 - 64);

    drop(lease);
    let free = handle.stats().await.expect("stats");
    assert_eq!(free.active_external_sorts, 0);
    assert_eq!(free.fd_permits_available, 128);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_fd_budget_backpressures_the_next_external_sort() {
    // A 64-permit budget with the default 64 fan-in: the first external sort
    // takes every permit, so the second must wait until the first releases.
    let (_dir, handle) = spawn(64, 8 * MIB, 0);
    let first = handle
        .submit(SortSpec::new(1_000_000, 64 * MIB))
        .await
        .expect("first submit");
    assert!(first.plan().is_external());
    let stats = handle.stats().await.expect("stats");
    assert_eq!(stats.fd_permits_available, 0);

    let second = handle.submit(SortSpec::new(1_000_000, 64 * MIB));
    let pending = tokio::time::timeout(std::time::Duration::from_millis(200), second).await;
    assert!(
        pending.is_err(),
        "second sort must block while permits are held"
    );

    drop(first);
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle.submit(SortSpec::new(1_000_000, 64 * MIB)),
    )
    .await
    .expect("second sort must be admitted once permits return")
    .expect("submit");
    assert!(second.plan().is_external());
}

#[test]
fn handle_reports_gone_once_the_governor_has_stopped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let build = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    };
    let governor_runtime = build();
    let handle = governor_runtime.block_on(async {
        SorterHandle::spawn(
            SorterConfig::default(),
            8,
            Arc::new(StaticPressure::new(1 << 30, 0)),
            dir.path().join("sorts"),
        )
    });
    // Tearing the runtime down aborts the actor task while a handle survives.
    drop(governor_runtime);
    let err = build()
        .block_on(handle.submit(SortSpec::new(10, 10)))
        .expect_err("a stopped governor must refuse admission");
    assert!(
        matches!(err, crate::error::SorterError::Gone),
        "got {err:?}"
    );
}

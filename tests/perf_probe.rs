//! Throwaway perf probe (perf/explore only): `PROBE_ROWS=200000 cargo test
//! --release --test perf_probe -- --ignored --nocapture`.

use std::time::Instant;

use futures_util::StreamExt;
use sort_governor::{
    SortPlan,
    SortPlanner,
    SortSession,
    SortSpec,
    SorterConfig,
    SorterSnapshot,
};

const PAYLOAD_BYTES: usize = 600;

async fn run(label: &str, plan: SortPlan, rows: u32) {
    let root = async_fs_io::TempDir::create(std::env::temp_dir())
        .await
        .expect("root");
    let mut session = SortSession::<u32, String>::with_temp_dir(plan, root.path(), true)
        .with_memory_limit_bytes(64 << 20);
    let t0 = Instant::now();
    for key in (0..rows).rev() {
        let letter = char::from(b'a' + u8::try_from(key % 26).expect("letter"));
        let payload = letter.to_string().repeat(PAYLOAD_BYTES);
        let retained = payload.capacity() + std::mem::size_of::<(u32, String)>();
        session
            .push_with_size(key, payload, retained)
            .await
            .expect("push");
        if key % 10 == 0 {
            session
                .push_with_size(key, "z".repeat(PAYLOAD_BYTES), retained)
                .await
                .expect("dup");
        }
    }
    let t1 = Instant::now();
    let mut output = session.finish().await.expect("finish");
    let t2 = Instant::now();
    let mut n = 0u32;
    while let Some(v) = output.next().await {
        v.expect("row");
        n += 1;
    }
    let t3 = Instant::now();
    assert_eq!(n, rows);
    println!(
        "PROBE {label} rows={rows} push={:?} finish={:?} drain={:?} total={:?}",
        t1 - t0,
        t2 - t1,
        t3 - t2,
        t3 - t0
    );
    drop(output);
    root.remove().await.expect("rm");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "perf probe"]
async fn probe() {
    let rows: u32 = std::env::var("PROBE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let snapshot = SorterSnapshot::new(1 << 30, 0, 256, 0);
    let planned = SortPlanner::plan(&SortSpec::new(0, 0), &snapshot, &SorterConfig::default());
    run("planned", planned, rows).await;
    run(
        "1mib",
        SortPlan::External {
            run_buffer_bytes: 1 << 20,
            max_fan_in: 64,
        },
        rows,
    )
    .await;
}

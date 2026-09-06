//! Public-API integration test: a governed sort streams its values back in
//! key order, from the shipped crate.

use std::sync::Arc;

use futures_util::StreamExt;
use sort_governor::{
    MemoryPressure,
    SortSpec,
    SorterConfig,
    SorterError,
    SorterHandle,
    StaticPressure,
};

#[tokio::test]
async fn governed_sort_streams_values_in_key_order() -> Result<(), SorterError> {
    let pressure: Arc<dyn MemoryPressure> = Arc::new(StaticPressure::new(1 << 30, 0));
    let scratch = tempfile::tempdir().expect("scratch directory");
    let sorter = SorterHandle::spawn(
        SorterConfig::from_fd_limit(256),
        160,
        pressure,
        scratch.path().to_path_buf(),
    );
    let lease = sorter
        .submit(SortSpec::new(5, 5 * 16).labelled("public-api"))
        .await?;
    let mut session = lease.into_session::<u32, String>(false);
    for key in [5_u32, 1, 4, 2, 3] {
        session.push(key, format!("row {key}")).await?;
    }
    let mut stream = session.finish().await?;
    let mut ordered = Vec::new();
    while let Some(value) = stream.next().await {
        ordered.push(value?);
    }
    assert_eq!(ordered, ["row 1", "row 2", "row 3", "row 4", "row 5"]);
    Ok(())
}

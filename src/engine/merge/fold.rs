//! Fold many runs into at most `width` runs with adjacent group merges.

use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::merge_group_to_file;
use crate::engine::budget::RunResources;
use crate::error::SorterError;

/// Where one fold pass writes the merge of one adjacent group.
pub(crate) enum FoldNames<'a> {
    /// Final cascade passes inside the scratch directory.
    Cascade(&'a Path),
    /// Intermediate passes of one carry, beside its output run.
    Carry(&'a Path),
}

impl FoldNames<'_> {
    fn path(&self, pass: u32, group: usize) -> PathBuf {
        match self {
            Self::Cascade(dir) => dir.join(format!("merge-{pass:03}-{group:05}.cbor")),
            Self::Carry(out) => out.with_extension(format!("fold-{pass:03}-{group:05}.cbor")),
        }
    }
}

/// Merge adjacent groups of `width` runs, oldest first so equal keys keep
/// their order, until at most `width` runs remain. Spent inputs are deleted.
pub(crate) async fn fold_to_width<K, V>(
    mut runs: Vec<PathBuf>,
    width: usize,
    names: FoldNames<'_>,
    resources: &RunResources,
) -> Result<Vec<PathBuf>, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    let mut pass = 0u32;
    while runs.len() > width {
        // bounded: callers hand over at most 64 frontier paths.
        resources.io.work().merge_pass();
        let mut next = Vec::new();
        for (group_idx, group) in runs.chunks(width).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            resources.io.work().merge_group();
            next.push(
                merge_group_to_file::<K, V>(
                    group,
                    names.path(pass, group_idx),
                    false,
                    Arc::clone(&resources.fds),
                    Arc::clone(&resources.memory),
                    Arc::clone(&resources.io),
                )
                .await?,
            );
        }
        runs = next;
        pass += 1;
    }
    Ok(runs)
}

/// Merge one frontier carry into `out`, first folding it to `width` runs
/// when the memory budget cannot hold every carry source open at once.
pub(crate) async fn merge_carry_to_file<K, V>(
    paths: &[PathBuf],
    out: PathBuf,
    width: usize,
    resources: &RunResources,
) -> Result<PathBuf, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    // bounded: one carry spans at most `MAX_CARRY_FAN_IN` paths.
    let folded =
        fold_to_width::<K, V>(paths.to_vec(), width, FoldNames::Carry(&out), resources).await?;
    merge_group_to_file::<K, V>(
        &folded,
        out,
        false,
        Arc::clone(&resources.fds),
        Arc::clone(&resources.memory),
        Arc::clone(&resources.io),
    )
    .await
}

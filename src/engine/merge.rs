//! Bounded, async, cascade k-way merge over spilled runs.
//!
//! At most `fan_in` run files are open at once. When the run count exceeds
//! `fan_in`, runs are merged in passes — each pass folds groups of `fan_in`
//! runs into one — until `≤ fan_in` remain, then the final merge streams
//! values out lazily. Memory and file descriptors stay
//! `O(fan_in)` for any number of runs, so the process descriptor table can
//! never be exhausted (the defect that crashed the old unbounded merge).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::{
    Path,
    PathBuf,
};
use std::pin::Pin;

use futures_util::{
    Stream,
    stream,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::engine::row::RunRow;
use crate::engine::run::{
    RunReader,
    RunWriter,
};
use crate::error::SorterError;

/// A boxed value stream — the merge's public output shape.
pub(crate) type ValueStream<V> = Pin<Box<dyn Stream<Item = Result<V, SorterError>> + Send>>;

/// One merge input: a run reader with its current head row buffered so the
/// heap can compare front keys without consuming them.
struct MergeSource<K, V> {
    head: Option<RunRow<K, V>>,
    reader: RunReader<K, V>,
}

impl<K, V> MergeSource<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Open a run and buffer its first row. Returns `None` for an empty run.
    async fn open(path: &Path) -> Result<Option<Self>, SorterError> {
        let mut reader = RunReader::open(path).await?;
        match reader.next_row().await? {
            Some(head) => Ok(Some(Self {
                head: Some(head),
                reader,
            })),
            None => Ok(None),
        }
    }

    fn key(&self) -> Option<&K> {
        self.head.as_ref().map(|row| &row.key)
    }

    /// Return the buffered head and refill it from the run.
    async fn take_head(&mut self) -> Result<RunRow<K, V>, SorterError> {
        let row = self.head.take().expect("take_head on exhausted source");
        self.head = self.reader.next_row().await?;
        Ok(row)
    }
}

/// Heap key: smallest `(key, idx)` first under `Reverse`. Derived ordering
/// compares `key` then `idx`, giving a deterministic tiebreak across runs.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct HeapEntry<K> {
    key: K,
    idx: usize,
}

/// The running k-way merge over a fixed set of sources.
struct MergeState<K, V> {
    sources: Vec<MergeSource<K, V>>,
    heap: BinaryHeap<Reverse<HeapEntry<K>>>,
    dedup: bool,
    last_key: Option<K>,
}

impl<K, V> MergeState<K, V>
where
    K: Ord + Clone + DeserializeOwned,
    V: DeserializeOwned,
{
    fn new(sources: Vec<MergeSource<K, V>>, dedup: bool) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, source) in sources.iter().enumerate() {
            if let Some(key) = source.key() {
                heap.push(Reverse(HeapEntry {
                    key: key.clone(),
                    idx,
                }));
            }
        }
        Self {
            sources,
            heap,
            dedup,
            last_key: None,
        }
    }

    /// Emit the next row in key order, collapsing equal keys when `dedup`.
    async fn next_row(&mut self) -> Result<Option<RunRow<K, V>>, SorterError> {
        loop {
            let Some(Reverse(entry)) = self.heap.pop() else {
                return Ok(None);
            };
            let idx = entry.idx;
            let row = self.sources[idx].take_head().await?;
            if let Some(key) = self.sources[idx].key() {
                self.heap.push(Reverse(HeapEntry {
                    key: key.clone(),
                    idx,
                }));
            }
            if self.dedup {
                if self.last_key.as_ref() == Some(&row.key) {
                    continue;
                }
                self.last_key = Some(row.key.clone());
            }
            return Ok(Some(row));
        }
    }
}

/// Removes the sort's scratch directory asynchronously when the stream ends
/// or is dropped.
struct AsyncCleanupGuard {
    dir: Option<PathBuf>,
}

impl AsyncCleanupGuard {
    async fn cleanup(&mut self) -> Result<(), SorterError> {
        let Some(dir) = self.dir.take() else {
            return Ok(());
        };
        async_fs_io::remove_dir_all(&dir).await.map_err(Into::into)
    }
}

impl Drop for AsyncCleanupGuard {
    fn drop(&mut self) {
        let Some(dir) = self.dir.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                dir = %dir.display(),
                "sorter scratch cleanup was dropped outside a Tokio runtime"
            );
            return;
        };
        handle.spawn(async move {
            if let Err(err) = async_fs_io::remove_dir_all(&dir).await {
                tracing::error!(dir = %dir.display(), error = %err, "sorter scratch cleanup failed");
            }
        });
    }
}

/// Merge a group of runs into one new run file, deleting the inputs.
async fn merge_group_to_file<K, V>(
    paths: &[PathBuf],
    out: PathBuf,
    dedup: bool,
) -> Result<PathBuf, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    let mut sources: Vec<MergeSource<K, V>> = Vec::with_capacity(paths.len());
    for path in paths {
        if let Some(source) = MergeSource::open(path).await? {
            sources.push(source);
        }
    }
    let mut state = MergeState::new(sources, dedup);
    let mut writer = RunWriter::create(out).await?;
    while let Some(row) = state.next_row().await? {
        writer.write_row(&row).await?;
    }
    let merged = writer.finish().await?;
    drop(state);
    for path in paths {
        async_fs_io::remove_if_exists(path).await?;
    }
    Ok(merged)
}

/// Fold `runs` down to `≤ fan_in` files, then stream the final merge.
///
/// `hold` is an opaque resource (typically the admission lease) kept alive
/// for exactly as long as the output stream, so the fd permits backing this
/// sort are released only once its results are fully consumed or dropped.
pub(crate) async fn cascade_and_stream<K, V>(
    mut runs: Vec<PathBuf>,
    fan_in: usize,
    dir: PathBuf,
    dedup: bool,
    hold: Option<Box<dyn Send>>,
) -> Result<ValueStream<V>, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
    V: Serialize + DeserializeOwned + Send + 'static,
{
    let guard = AsyncCleanupGuard {
        dir: Some(dir.clone()),
    };
    let mut pass = 0u32;
    while runs.len() > fan_in {
        let mut next = Vec::new();
        for (group_idx, group) in runs.chunks(fan_in).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let out = dir.join(format!("merge-{pass:03}-{group_idx:05}.cbor"));
            next.push(merge_group_to_file::<K, V>(group, out, false).await?);
        }
        runs = next;
        pass += 1;
    }
    let mut sources: Vec<MergeSource<K, V>> = Vec::with_capacity(runs.len());
    for path in &runs {
        if let Some(source) = MergeSource::open(path).await? {
            sources.push(source);
        }
    }
    let state = MergeState::new(sources, dedup);
    Ok(Box::pin(value_stream(state, guard, hold)))
}

/// What the value stream holds until it is fully consumed or dropped: the
/// scratch-directory guard and the opaque admission lease.
struct StreamHold {
    scratch: AsyncCleanupGuard,
    _resource: Option<Box<dyn Send>>,
}

impl StreamHold {
    async fn cleanup(&mut self) -> Result<(), SorterError> {
        self.scratch.cleanup().await
    }
}

/// Drive a [`MergeState`] as a lazy value stream, keeping its scratch
/// directory and resource lease alive for the stream's lifetime.
fn value_stream<K, V>(
    state: MergeState<K, V>,
    scratch: AsyncCleanupGuard,
    resource: Option<Box<dyn Send>>,
) -> ValueStream<V>
where
    K: Ord + Clone + DeserializeOwned + Send + 'static,
    V: DeserializeOwned + Send + 'static,
{
    let hold = StreamHold {
        scratch,
        _resource: resource,
    };
    Box::pin(stream::unfold(
        (Some(state), hold, false),
        |(maybe, mut hold, cleanup_reported)| async move {
            let Some(mut state) = maybe else {
                if cleanup_reported {
                    return None;
                }
                return match hold.cleanup().await {
                    Ok(()) => None,
                    Err(err) => Some((Err(err), (None, hold, true))),
                };
            };
            match state.next_row().await {
                Ok(Some(row)) => Some((Ok(row.value), (Some(state), hold, false))),
                Ok(None) => match hold.cleanup().await {
                    Ok(()) => None,
                    Err(err) => Some((Err(err), (None, hold, true))),
                },
                Err(err) => Some((Err(err), (None, hold, false))),
            }
        },
    ))
}

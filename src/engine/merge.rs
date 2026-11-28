//! Bounded, async, cascade k-way merge over spilled runs.
//!
//! At most `fan_in` run files are open at once. When the run count exceeds
//! `fan_in`, runs are merged in passes — each pass folds groups of `fan_in`
//! runs into one — until `≤ fan_in` remain, then the final merge streams
//! values out lazily. The session compacts runs during ingestion so this
//! final cascade receives at most 64 paths. Readers and head rows stay
//! bounded by `fan_in`; file merges also hold one output writer.

use std::path::{
    Path,
    PathBuf,
};
use std::pin::Pin;
use std::sync::Arc;

use futures_util::{
    Stream,
    stream,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use self::heap::SourceHeap;
use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
    RunResources,
};
use crate::engine::operation::IoScope;
use crate::engine::row::RunRow;
use crate::engine::run::{
    RunReader,
    RunWriter,
};
use crate::engine::work::OpenReader;
use crate::error::SorterError;

mod heap;

/// A boxed value stream — the merge's public output shape.
pub(crate) type ValueStream<V> = Pin<Box<dyn Stream<Item = Result<V, SorterError>> + Send>>;

/// One merge input: a run reader with its current head row buffered so the
/// heap can compare front keys without consuming them.
struct MergeSource<K, V> {
    head: Option<RunRow<K, V>>,
    reader: RunReader<K, V>,
    _open: OpenReader,
}

impl<K, V> MergeSource<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    /// Open a run and buffer its first row. Returns `None` for an empty run.
    async fn open(
        path: &Path,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        io: Arc<IoScope>,
    ) -> Result<Option<Self>, SorterError> {
        let open = OpenReader::new(&io);
        let mut reader = RunReader::open(path, fds, budget, io).await?;
        match reader.next_row().await? {
            Some(head) => Ok(Some(Self {
                head: Some(head),
                reader,
                _open: open,
            })),
            None => Ok(None),
        }
    }

    fn key(&self) -> Option<&K> {
        self.head.as_ref().map(|row| &row.key)
    }

    /// Read the next head after the previous one left this source.
    async fn refill(&mut self) -> Result<(), SorterError> {
        debug_assert!(self.head.is_none(), "refill over a buffered head");
        self.head = self.reader.next_row().await?;
        Ok(())
    }
}

/// The running k-way merge over a fixed set of sources.
struct MergeState<K, V> {
    sources: Vec<MergeSource<K, V>>,
    heap: SourceHeap,
    /// Source whose head was emitted last; it is refilled on the next call,
    /// after the caller has consumed and released the emitted row.
    refill: Option<usize>,
    dedup: bool,
    last_key: Option<(K, MemoryCharge)>,
    budget: Arc<MemoryBudget>,
    _arrays: MemoryCharge,
}

impl<K, V> MergeState<K, V>
where
    K: Ord + Clone + DeserializeOwned,
    V: DeserializeOwned,
{
    fn new(capacity: usize, dedup: bool, budget: Arc<MemoryBudget>) -> Result<Self, SorterError> {
        let source_bytes = capacity
            .checked_mul(std::mem::size_of::<MergeSource<K, V>>())
            .ok_or(SorterError::RunLimit)?;
        let heap_bytes = capacity
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or(SorterError::RunLimit)?;
        let arrays = budget.reserve(
            source_bytes
                .checked_add(heap_bytes)
                .ok_or(SorterError::RunLimit)?,
        )?;
        let sources = Vec::with_capacity(capacity);
        let heap = SourceHeap::with_capacity(capacity);
        assert_eq!(
            sources.capacity(),
            capacity,
            "exact source capacity reservation"
        );
        Ok(Self {
            sources,
            heap,
            refill: None,
            dedup,
            last_key: None,
            budget,
            _arrays: arrays,
        })
    }

    async fn open(
        paths: &[PathBuf],
        dedup: bool,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        io: Arc<IoScope>,
    ) -> Result<Self, SorterError> {
        let mut state = Self::new(paths.len(), dedup, Arc::clone(&budget))?;
        for path in paths {
            if let Some(source) = MergeSource::<K, V>::open(
                path,
                Arc::clone(&fds),
                Arc::clone(&budget),
                Arc::clone(&io),
            )
            .await?
            {
                state.sources.push(source);
                state.enqueue(state.sources.len() - 1);
            }
        }
        Ok(state)
    }

    /// Order sources by `(head key, index)`: equal keys keep run order.
    fn head_less(sources: &[MergeSource<K, V>], left: usize, right: usize) -> bool {
        match (sources[left].key(), sources[right].key()) {
            (Some(a), Some(b)) => (a, left) < (b, right),
            _ => unreachable!("only sources with a buffered head are queued"),
        }
    }

    fn enqueue(&mut self, idx: usize) {
        if self.sources[idx].head.is_some() {
            let sources = &self.sources;
            self.heap
                .push(idx, |left, right| Self::head_less(sources, left, right));
        }
    }

    /// Emit the next row in key order, collapsing equal keys when `dedup`.
    ///
    /// The emitted row's source is refilled lazily on the following call, so
    /// the caller's row and its successor are never charged at once.
    async fn next_row(&mut self) -> Result<Option<RunRow<K, V>>, SorterError> {
        loop {
            if let Some(idx) = self.refill.take() {
                self.sources[idx].refill().await?;
                self.enqueue(idx);
            }
            let sources = &self.sources;
            let Some(idx) = self
                .heap
                .pop(|left, right| Self::head_less(sources, left, right))
            else {
                return Ok(None);
            };
            let row = self.sources[idx]
                .head
                .take()
                .expect("queued source has a head");
            self.refill = Some(idx);
            if self.dedup {
                if self
                    .last_key
                    .as_ref()
                    .is_some_and(|(key, _)| key == &row.key)
                {
                    continue;
                }
                let charge = self.budget.reserve(row.retained_bytes)?;
                self.last_key = Some((row.key.clone(), charge));
            }
            return Ok(Some(row));
        }
    }
}

/// Merge a group of runs into one new run file, deleting the inputs.
pub(crate) async fn merge_group_to_file<K, V>(
    paths: &[PathBuf],
    out: PathBuf,
    dedup: bool,
    fds: Arc<Semaphore>,
    budget: Arc<MemoryBudget>,
    io: Arc<IoScope>,
) -> Result<PathBuf, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    let mut state = MergeState::<K, V>::open(
        paths,
        dedup,
        Arc::clone(&fds),
        Arc::clone(&budget),
        Arc::clone(&io),
    )
    .await?;
    let mut writer =
        RunWriter::create(out, Arc::clone(&fds), Arc::clone(&budget), Arc::clone(&io)).await?;
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
/// The session retains cleanup ownership throughout this awaited setup.
pub(crate) async fn cascade_and_stream<K, V>(
    mut runs: Vec<PathBuf>,
    fan_in: usize,
    dir: PathBuf,
    dedup: bool,
    resources: RunResources,
) -> Result<ValueStream<V>, SorterError>
where
    K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
    V: Serialize + DeserializeOwned + Send + 'static,
{
    let RunResources {
        fds,
        memory: budget,
        io,
    } = resources;
    let mut pass = 0u32;
    while runs.len() > fan_in {
        // bounded: the session hands over at most 64 frontier paths.
        io.work().merge_pass();
        let mut next = Vec::new();
        for (group_idx, group) in runs.chunks(fan_in).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            io.work().merge_group();
            let out = dir.join(format!("merge-{pass:03}-{group_idx:05}.cbor"));
            next.push(
                merge_group_to_file::<K, V>(
                    group,
                    out,
                    false,
                    Arc::clone(&fds),
                    Arc::clone(&budget),
                    Arc::clone(&io),
                )
                .await?,
            );
        }
        runs = next;
        pass += 1;
    }
    let state = MergeState::<K, V>::open(&runs, dedup, fds, budget, io).await?;
    Ok(value_stream(state))
}

/// Drive merge rows; the public output owner holds cleanup separately.
fn value_stream<K, V>(state: MergeState<K, V>) -> ValueStream<V>
where
    K: Ord + Clone + DeserializeOwned + Send + 'static,
    V: DeserializeOwned + Send + 'static,
{
    Box::pin(stream::unfold(Some(state), |maybe| async move {
        let mut state = maybe?;
        match state.next_row().await {
            Ok(Some(row)) => Some((Ok(row.value), Some(state))),
            Ok(None) => None,
            Err(error) => Some((Err(error), None)),
        }
    }))
}

#[cfg(test)]
mod tests;

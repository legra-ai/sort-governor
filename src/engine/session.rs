//! A single sort in flight: rows pushed in, sorted values streamed out. The
//! [`SortPlan`] chosen by the planner decides whether pushes stay in memory
//! or spill; the caller pushes and finishes without knowing which.

use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use futures_util::stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
    RunResources,
};
use crate::engine::cleanup::AsyncCleanupGuard;
use crate::engine::merge::{
    ValueStream,
    cascade_and_stream,
    merge_group_to_file,
};
use crate::engine::row::RunRow;
use crate::engine::run::RunWriter;
use crate::error::SorterError;
use crate::plan::SortPlan;

mod buffer;

#[cfg(test)]
mod tests;

/// An in-progress sort. Generic over the ordering key `K` and payload `V`.
pub struct SortSession<K, V> {
    plan: SortPlan,
    fds: Arc<Semaphore>,
    budget: Arc<MemoryBudget>,
    buffer_charge: Option<MemoryCharge>,
    metadata_reserved: bool,
    next_row: u64,
    dir: PathBuf,
    dedup: bool,
    buffer: Vec<RunRow<K, V>>,
    buffer_bytes: usize,
    // bounded: one run per bit in the checked u64 spill count. Level n
    // contains 2^n consecutive original runs; higher levels are older.
    spills: [Option<PathBuf>; u64::BITS as usize],
    next_run: u64,
    failed: bool,
    hold: Option<Box<dyn Send>>,
    /// Armed on the first spill; removes the scratch directory when the
    /// session is dropped before [`SortSession::finish`], and is handed to
    /// the output stream at `finish` so cleanup then follows the results.
    cleanup: AsyncCleanupGuard,
}

impl<K, V> SortSession<K, V>
where
    K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
    V: Serialize + DeserializeOwned + Send + 'static,
{
    /// Open a session for `plan`, spilling (if it spills at all) under the
    /// unique scratch directory `dir`. The directory is created lazily on
    /// the first spill, so a purely in-memory sort never touches disk.
    #[must_use]
    pub fn new(plan: SortPlan, dir: PathBuf, dedup: bool) -> Self {
        let fds = Arc::new(Semaphore::new(match plan {
            SortPlan::InMemory => 2,
            SortPlan::External { max_fan_in, .. } => max_fan_in.max(2) as usize,
        }));
        let mut cleanup = AsyncCleanupGuard::disarmed();
        cleanup.set_fd_budget(Arc::clone(&fds));
        Self {
            plan,
            budget: MemoryBudget::new(256 * 1024 * 1024),
            buffer_charge: None,
            metadata_reserved: false,
            next_row: 0,
            fds,
            dir,
            dedup,
            buffer: Vec::new(),
            buffer_bytes: 0,
            spills: std::array::from_fn(|_| None),
            next_run: 0,
            failed: false,
            hold: None,
            cleanup,
        }
    }

    /// Open an ungoverned session under a freshly-minted unique directory
    /// beneath `temp_root`. For tests and callers that run without a
    /// governor; the engine stays bounded and async regardless.
    #[must_use]
    pub fn with_temp_dir(plan: SortPlan, temp_root: &Path, dedup: bool) -> Self {
        let dir = temp_root.join(format!("sort-{}", uuid::Uuid::new_v4()));
        Self::new(plan, dir, dedup)
    }

    /// Attach an opaque resource (typically the admission lease) to be held
    /// alive for as long as this sort's output stream. File permits are
    /// acquired separately for actual I/O and are never retained while idle.
    #[must_use]
    pub fn hold_resource(mut self, resource: Box<dyn Send>) -> Self {
        self.hold = Some(resource);
        self
    }

    pub(crate) fn with_fd_budget(mut self, fds: Arc<Semaphore>) -> Self {
        self.cleanup.set_fd_budget(Arc::clone(&fds));
        self.fds = fds;
        self
    }

    pub(crate) fn with_memory_budget(mut self, budget: Arc<MemoryBudget>) -> Self {
        self.budget = budget;
        self
    }

    /// Set an explicit local byte cap before pushing rows. Process-governed
    /// sessions already share a cap and should keep their injected budget.
    ///
    /// # Panics
    /// Panics for a process-governed session or after any row/metadata
    /// admission.
    #[must_use]
    pub fn with_memory_limit_bytes(mut self, bytes: u64) -> Self {
        assert!(
            !self.budget.is_governed(),
            "cannot replace the process-governed memory budget"
        );
        assert!(
            self.next_row == 0 && !self.metadata_reserved,
            "memory cap is configured before ingestion"
        );
        self.budget = MemoryBudget::new(bytes);
        self
    }

    /// The unique scratch directory this sort spills into (created lazily on
    /// the first spill, removed when the output stream is dropped).
    #[must_use]
    pub fn scratch_dir(&self) -> &Path {
        &self.dir
    }

    /// Push one row with its retained key/value bytes, including owned heap
    /// data. Spills the current run first when adding this row would
    /// overflow the plan's run buffer.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError`] if spilling the current run fails. A failed
    /// or cancelled spill invalidates the session; subsequent pushes and
    /// [`Self::finish`] fail instead of yielding incomplete results.
    pub async fn push_with_size(
        &mut self,
        key: K,
        value: V,
        estimated_bytes: usize,
    ) -> Result<(), SorterError> {
        self.ensure_usable()?;
        self.reserve_metadata()?;
        let bytes = estimated_bytes.max(std::mem::size_of::<RunRow<K, V>>());
        let charge = self.budget.reserve(bytes)?;
        if !self.buffer.is_empty()
            && self
                .buffer_bytes
                .checked_add(bytes)
                .is_none_or(|sum| sum > self.buffer_limit())
        {
            self.spill().await?;
        }
        self.grow_buffer()?;
        let mut row = RunRow::new(key, value);
        row.ordinal = self.next_row;
        self.next_row = self.next_row.checked_add(1).ok_or(SorterError::RunLimit)?;
        row.retained_bytes = bytes;
        row.charge = Some(charge);
        self.buffer.push(row);
        self.buffer_bytes = self
            .buffer_bytes
            .checked_add(bytes)
            .ok_or(SorterError::RunLimit)?;
        if self.buffer_bytes >= self.buffer_limit() {
            self.spill().await?;
        }
        Ok(())
    }

    /// Push one row whose key and value contain no owned heap allocations.
    /// Heap-owning rows must use [`Self::push_with_size`] with their retained
    /// size.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError`] if spilling the current run fails.
    pub async fn push(&mut self, key: K, value: V) -> Result<(), SorterError> {
        let estimate = std::mem::size_of::<(K, V)>().max(1);
        self.push_with_size(key, value, estimate).await
    }

    /// Finish the sort, returning a stream of values in key order.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError`] if a final spill or the merge setup fails.
    pub async fn finish(mut self) -> Result<ValueStream<V>, SorterError> {
        self.ensure_usable()?;
        self.buffer.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.ordinal.cmp(&right.ordinal))
        });
        let fan_in = match self.plan {
            SortPlan::InMemory if self.next_run == 0 => return Ok(self.in_memory_stream()),
            SortPlan::InMemory => 2,
            SortPlan::External { .. } if self.next_run == 0 => {
                return Ok(self.in_memory_stream());
            }
            SortPlan::External { max_fan_in, .. } => max_fan_in as usize,
        };
        if !self.buffer.is_empty() {
            self.spill().await?;
        }
        let hold: Option<Box<dyn Send>> = self.hold.take();
        let guard = std::mem::take(&mut self.cleanup);
        // bounded: at most 64 frontier entries. Reverse levels to preserve
        // chronological run order, including the first row among equal keys.
        let spills = self
            .spills
            .iter_mut()
            .rev()
            .filter_map(Option::take)
            .collect();
        let dir = self.dir.clone();
        let io = guard.scope();
        cascade_and_stream::<K, V>(
            spills,
            fan_in.max(2),
            dir,
            guard,
            self.dedup,
            hold,
            RunResources::new(Arc::clone(&self.fds), Arc::clone(&self.budget), io),
        )
        .await
    }

    /// Stream the sorted in-memory buffer, collapsing equal keys when
    /// `dedup`. Used when nothing spilled — the buffer is bounded by the
    /// plan, so materializing its values here is safe. The resource hold
    /// rides the stream so its lease releases only when the stream ends.
    fn in_memory_stream(&mut self) -> ValueStream<V> {
        let rows = std::mem::take(&mut self.buffer).into_iter();
        let hold = self.hold.take();
        let allocation = self.buffer_charge.take();
        let metadata = self.cleanup.take_reservation();
        let dedup = self.dedup;
        Box::pin(stream::unfold(
            (rows, None, hold, allocation, metadata),
            move |(mut rows, mut last, hold, allocation, metadata)| async move {
                for row in rows.by_ref() {
                    if dedup && last.as_ref().is_some_and(|(key, _)| key == &row.key) {
                        continue;
                    }
                    if dedup {
                        last = Some((row.key, row.charge));
                    }
                    return Some((Ok(row.value), (rows, last, hold, allocation, metadata)));
                }
                None
            },
        ))
    }

    /// Sort and write the current buffer to a new run file.
    async fn spill(&mut self) -> Result<(), SorterError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        // Leave this set on errors and when the caller cancels this future:
        // a partially merged frontier cannot be used again.
        self.failed = true;
        let ordinal = self.next_run;
        self.next_run = ordinal.checked_add(1).ok_or(SorterError::RunLimit)?;
        self.buffer.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.ordinal.cmp(&right.ordinal))
        });
        self.cleanup.arm(self.dir.clone());
        let directory = self.dir.clone();
        crate::engine::operation::perform(
            Arc::clone(&self.fds),
            self.cleanup.scope(),
            Arc::clone(&self.budget),
            directory.capacity(),
            async move {
                async_fs_io::ensure_dir(&directory).await?;
                Ok(())
            },
        )
        .await?;
        let path = self.dir.join(format!("run-{ordinal:020}.cbor"));
        let mut writer = RunWriter::create(
            path,
            Arc::clone(&self.fds),
            Arc::clone(&self.budget),
            self.cleanup.scope(),
        )
        .await?;
        for row in &self.buffer {
            writer.write_row(row).await?;
        }
        let mut run = writer.finish().await?;
        self.buffer = Vec::new();
        self.buffer_charge = None;
        self.buffer_bytes = 0;
        for (level, slot) in self.spills.iter_mut().enumerate() {
            let Some(older) = slot.take() else {
                *slot = Some(run);
                self.failed = false;
                return Ok(());
            };
            let output = self
                .dir
                .join(format!("carry-{ordinal:020}-{level:02}.cbor"));
            // bounded: two adjacent runs, older first for stable equal keys.
            run = merge_group_to_file::<K, V>(
                &[older, run],
                output,
                false,
                Arc::clone(&self.fds),
                Arc::clone(&self.budget),
                self.cleanup.scope(),
            )
            .await?;
        }
        unreachable!("a checked u64 spill count always fits in 64 frontier levels")
    }

    fn ensure_usable(&self) -> Result<(), SorterError> {
        if self.failed {
            return Err(SorterError::SessionFailed);
        }
        Ok(())
    }
}

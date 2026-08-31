//! A single sort in flight: rows pushed in, sorted values streamed out. The
//! [`SortPlan`] chosen by the planner decides whether pushes stay in memory
//! or spill; the caller pushes and finishes without knowing which.

use std::path::{
    Path,
    PathBuf,
};

use futures_util::stream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::engine::cleanup::AsyncCleanupGuard;
use crate::engine::merge::{
    ValueStream,
    cascade_and_stream,
};
use crate::engine::row::RunRow;
use crate::engine::run::RunWriter;
use crate::error::SorterError;
use crate::plan::SortPlan;

/// An in-progress sort. Generic over the ordering key `K` and payload `V`.
pub struct SortSession<K, V> {
    plan: SortPlan,
    dir: PathBuf,
    dedup: bool,
    buffer: Vec<RunRow<K, V>>,
    buffer_bytes: usize,
    spills: Vec<PathBuf>,
    next_run: u32,
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
        Self {
            plan,
            dir,
            dedup,
            buffer: Vec::new(),
            buffer_bytes: 0,
            spills: Vec::new(),
            next_run: 0,
            hold: None,
            cleanup: AsyncCleanupGuard::disarmed(),
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
    /// alive for as long as this sort's output stream — so the fd permits
    /// backing the sort are released only once its results are consumed.
    #[must_use]
    pub fn hold_resource(mut self, resource: Box<dyn Send>) -> Self {
        self.hold = Some(resource);
        self
    }

    /// The unique scratch directory this sort spills into (created lazily on
    /// the first spill, removed when the output stream is dropped).
    #[must_use]
    pub fn scratch_dir(&self) -> &Path {
        &self.dir
    }

    /// Push one row, sized with a caller estimate. Spills the current run
    /// first when adding this row would overflow the plan's run buffer.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError`] if spilling the current run fails.
    pub async fn push_with_size(
        &mut self,
        key: K,
        value: V,
        estimated_bytes: usize,
    ) -> Result<(), SorterError> {
        let bytes = estimated_bytes.max(1);
        if let SortPlan::External {
            run_buffer_bytes, ..
        } = self.plan
            && !self.buffer.is_empty()
            && self.buffer_bytes.saturating_add(bytes) > run_buffer_bytes
        {
            self.spill().await?;
        }
        self.buffer.push(RunRow::new(key, value));
        self.buffer_bytes = self.buffer_bytes.saturating_add(bytes);
        Ok(())
    }

    /// Push one row using a conservative fixed size estimate.
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
        self.buffer.sort_by(|left, right| left.key.cmp(&right.key));
        let fan_in = match self.plan {
            SortPlan::InMemory => return Ok(self.in_memory_stream()),
            SortPlan::External { .. } if self.spills.is_empty() => {
                return Ok(self.in_memory_stream());
            }
            SortPlan::External { max_fan_in, .. } => max_fan_in as usize,
        };
        if !self.buffer.is_empty() {
            self.spill().await?;
        }
        let hold = self.hold.take();
        let guard = std::mem::take(&mut self.cleanup);
        let spills = std::mem::take(&mut self.spills);
        let dir = self.dir.clone();
        cascade_and_stream::<K, V>(spills, fan_in.max(2), dir, guard, self.dedup, hold).await
    }

    /// Stream the sorted in-memory buffer, collapsing equal keys when
    /// `dedup`. Used when nothing spilled — the buffer is bounded by the
    /// plan, so materializing its values here is safe. The resource hold
    /// rides the stream so its lease releases only when the stream ends.
    fn in_memory_stream(&mut self) -> ValueStream<V> {
        let rows = std::mem::take(&mut self.buffer);
        let mut values = Vec::with_capacity(rows.len());
        let mut last: Option<K> = None;
        for row in rows {
            if self.dedup {
                if last.as_ref() == Some(&row.key) {
                    continue;
                }
                last = Some(row.key.clone());
            }
            values.push(row.value);
        }
        let hold = self.hold.take();
        Box::pin(stream::unfold(
            (values.into_iter(), hold),
            |(mut values, hold)| async move { values.next().map(|value| (Ok(value), (values, hold))) },
        ))
    }

    /// Sort and write the current buffer to a new run file.
    async fn spill(&mut self) -> Result<(), SorterError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_by(|left, right| left.key.cmp(&right.key));
        async_fs_io::ensure_dir(&self.dir).await?;
        self.cleanup.arm(self.dir.clone());
        let path = self.dir.join(format!("run-{:06}.cbor", self.next_run));
        self.next_run += 1;
        let mut writer = RunWriter::create(path).await?;
        for row in &self.buffer {
            writer.write_row(row).await?;
        }
        self.spills.push(writer.finish().await?);
        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(())
    }
}

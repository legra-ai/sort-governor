//! Sequential replay using the sort engine's run format, budgets and cleanup.

use std::marker::PhantomData;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
};
use crate::engine::cleanup::AsyncCleanupGuard;
use crate::engine::row::RunRow;
use crate::engine::run::{
    RunReader,
    RunWriter,
};
use crate::error::SorterError;
use crate::service::SortLease;

/// A sequential, bounded append sink for replayable equal-key groups or rows.
/// Files and codec buffers are not retained between append operations.
pub struct ReplayWriter<V> {
    dir: PathBuf,
    writer: Option<RunWriter>,
    fds: Arc<Semaphore>,
    budget: Arc<MemoryBudget>,
    metadata: Option<MemoryCharge>,
    cleanup: AsyncCleanupGuard,
    lease: Option<SortLease>,
    rows: u64,
    failed: bool,
    marker: PhantomData<fn() -> V>,
}

impl<V: Serialize + DeserializeOwned + Send + 'static> ReplayWriter<V> {
    pub(crate) fn new(
        dir: PathBuf,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        lease: Option<SortLease>,
    ) -> Self {
        Self {
            dir,
            writer: None,
            fds,
            budget,
            metadata: None,
            cleanup: AsyncCleanupGuard::disarmed(),
            lease,
            rows: 0,
            failed: false,
            marker: PhantomData,
        }
    }

    /// Open the same replay engine without a process governor. All allocations
    /// still share the explicit local byte cap; actual I/O uses two permits.
    #[must_use]
    pub fn with_temp_dir(root: &Path, max_memory_bytes: u64) -> Self {
        Self::new(
            root.join(format!("replay-{}", uuid::Uuid::new_v4())),
            Arc::new(Semaphore::new(2)),
            MemoryBudget::new(max_memory_bytes),
            None,
        )
    }

    async fn prepare(&mut self) -> Result<(), SorterError> {
        if self.writer.is_some() {
            return Ok(());
        }
        let metadata_bytes = self
            .dir
            .capacity()
            .checked_add(128)
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<Self>() + std::mem::size_of::<ReplayInner>())
            })
            .ok_or(SorterError::RunLimit)?;
        self.metadata = Some(self.budget.reserve(metadata_bytes)?);
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
        self.writer = Some(
            RunWriter::create(
                self.dir.join("rows.cbor"),
                Arc::clone(&self.fds),
                Arc::clone(&self.budget),
                self.cleanup.scope(),
            )
            .await?,
        );
        Ok(())
    }

    /// Append one row, declaring its full retained heap and inline size.
    ///
    /// The caller must include owned capacities and nested payloads; shared
    /// backing may be conservatively overcounted. Serialized frame allocations
    /// are measured and admitted separately. Ownership transfers out again when
    /// a cursor returns a value; the consumer must account for retained values.
    ///
    /// # Errors
    /// Rejects allocations exceeding the shared byte cap, I/O/codec failures,
    /// and a writer invalidated by an earlier failed or cancelled append.
    ///
    /// # Panics
    /// Panics if the internal prepared-writer invariant is violated.
    pub async fn append_with_size(
        &mut self,
        value: V,
        retained_bytes: usize,
    ) -> Result<(), SorterError> {
        if self.failed {
            return Err(SorterError::SessionFailed);
        }
        self.failed = true;
        let mut row = RunRow::new((), value);
        row.retained_bytes = retained_bytes.max(std::mem::size_of::<RunRow<(), V>>());
        row.charge = Some(self.budget.reserve(row.retained_bytes)?);
        row.ordinal = self.rows;
        self.prepare().await?;
        self.writer
            .as_mut()
            .expect("prepared replay writer")
            .write_row(&row)
            .await?;
        self.rows = self.rows.checked_add(1).ok_or(SorterError::RunLimit)?;
        self.failed = false;
        Ok(())
    }

    /// Seal the append sink and transfer scratch ownership to replay readers.
    ///
    /// # Errors
    /// Returns budget/I/O errors or rejects a failed or cancelled writer.
    ///
    /// # Panics
    /// Panics if the internal prepared-writer invariant is violated.
    pub async fn seal(mut self) -> Result<Replay<V>, SorterError> {
        if self.failed {
            return Err(SorterError::SessionFailed);
        }
        self.prepare().await?;
        let path = self
            .writer
            .take()
            .expect("prepared replay writer")
            .finish()
            .await?;
        Ok(Replay {
            inner: Arc::new(ReplayInner {
                path,
                rows: self.rows,
                fds: self.fds,
                budget: self.budget,
                _metadata: self.metadata,
                cleanup: self.cleanup,
                _lease: self.lease,
            }),
            marker: PhantomData,
        })
    }
}

struct ReplayInner {
    path: PathBuf,
    rows: u64,
    fds: Arc<Semaphore>,
    budget: Arc<MemoryBudget>,
    _metadata: Option<MemoryCharge>,
    cleanup: AsyncCleanupGuard,
    _lease: Option<SortLease>,
}

/// Immutable replay storage. Cursors share scratch ownership without copying
/// rows.
pub struct Replay<V> {
    inner: Arc<ReplayInner>,
    marker: PhantomData<fn() -> V>,
}
impl<V> Replay<V> {
    /// Start an independent cursor. Cursor construction and cloning allocate no
    /// row buffers and hold no descriptors; active reads reserve their bytes.
    #[must_use]
    pub fn cursor(&self) -> ReplayCursor<V> {
        ReplayCursor {
            inner: Arc::clone(&self.inner),
            offset: 8,
            remaining: self.inner.rows,
            marker: PhantomData,
        }
    }
}

/// A cheap checkpoint into immutable replay storage. Cloning preserves the
/// current offset; advancing one cursor never changes another cursor.
pub struct ReplayCursor<V> {
    inner: Arc<ReplayInner>,
    offset: u64,
    remaining: u64,
    marker: PhantomData<fn() -> V>,
}
impl<V> Clone for ReplayCursor<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            offset: self.offset,
            remaining: self.remaining,
            marker: PhantomData,
        }
    }
}
impl<V: DeserializeOwned> ReplayCursor<V> {
    /// Return the next owned value; the cursor retains neither its allocation
    /// nor a file descriptor. Cancellation leaves the checkpoint unchanged.
    ///
    /// # Errors
    /// Propagates budget, I/O and decode errors without advancing past the row.
    pub async fn next(&mut self) -> Result<Option<V>, SorterError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let _metadata = self
            .inner
            .budget
            .reserve(self.inner.path.capacity() + std::mem::size_of::<RunReader<(), V>>())?;
        let mut reader: RunReader<(), V> = RunReader::from_position(
            &self.inner.path,
            self.offset,
            self.remaining,
            Arc::clone(&self.inner.fds),
            Arc::clone(&self.inner.budget),
            self.inner.cleanup.scope(),
        );
        let row = reader.next_row().await?;
        (self.offset, self.remaining) = reader.position();
        Ok(row.map(|row| row.value))
    }

    /// Return to the beginning without opening a file or retaining row data.
    pub fn rewind(&mut self) {
        self.offset = 8;
        self.remaining = self.inner.rows;
    }
}

#[cfg(test)]
mod tests;

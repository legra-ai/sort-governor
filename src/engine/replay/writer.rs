//! One sequential publisher of complete replay frames.

use std::marker::PhantomData;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use super::cursor::ReplayCursor;
use super::storage::{
    Replay,
    ReplayInner,
};
use crate::engine::budget::MemoryBudget;
use crate::engine::operation::perform;
use crate::engine::row::RunRow;
use crate::engine::run::RunWriter;
use crate::error::SorterError;
use crate::service::SortLease;

/// Bounded append sink. Complete prefixes are readable before the source ends.
pub struct ReplayWriter<V> {
    pub(super) inner: Option<Arc<ReplayInner>>,
    writer: Option<RunWriter>,
    rows: u64,
    marker: PhantomData<fn() -> V>,
}
impl<V> Drop for ReplayWriter<V> {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.publication.fail();
        }
    }
}

impl<V: Serialize + DeserializeOwned + Send + 'static> ReplayWriter<V> {
    pub(crate) fn new(
        dir: PathBuf,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        lease: Option<SortLease>,
    ) -> Self {
        Self {
            inner: Some(ReplayInner::new(dir, fds, budget, lease)),
            writer: None,
            rows: 0,
            marker: PhantomData,
        }
    }

    /// Use the same engine without a process governor, under an explicit local
    /// byte cap and two actual-I/O permits.
    #[must_use]
    pub fn with_temp_dir(root: &Path, max_memory_bytes: u64) -> Self {
        Self::new(
            root.join(format!("replay-{}", uuid::Uuid::new_v4())),
            Arc::new(Semaphore::new(2)),
            MemoryBudget::new(max_memory_bytes),
            None,
        )
    }

    /// Read complete committed prefixes without sealing or pulling more input.
    /// Cursor construction performs no I/O and allocates no row buffers.
    ///
    /// # Errors
    /// Rejects a failed, aborted or already sealed writer.
    pub fn prefix_cursor(&self) -> Result<ReplayCursor<V>, SorterError> {
        let inner = self.active()?;
        inner.publication.snapshot()?;
        Ok(ReplayCursor::new(inner))
    }

    fn active(&self) -> Result<Arc<ReplayInner>, SorterError> {
        self.inner
            .as_ref()
            .map(Arc::clone)
            .ok_or(SorterError::SessionFailed)
    }

    async fn prepare(&mut self) -> Result<(), SorterError> {
        if self.writer.is_some() {
            return Ok(());
        }
        let inner = self.active()?;
        let bytes = inner
            .dir
            .capacity()
            .checked_add(128)
            .and_then(|size| size.checked_mul(4))
            .and_then(|size| {
                size.checked_add(
                    std::mem::size_of::<Self>() + std::mem::size_of::<ReplayInner>() + 4096,
                )
            })
            .ok_or(SorterError::RunLimit)?;
        inner.arm(inner.budget.reserve(bytes)?);
        let directory = inner.dir.clone();
        perform(
            Arc::clone(&inner.fds),
            Arc::clone(&inner.scope),
            Arc::clone(&inner.budget),
            directory.capacity(),
            async move {
                async_fs_io::ensure_dir(directory).await?;
                Ok(())
            },
        )
        .await?;
        self.writer = Some(
            RunWriter::create(
                inner.path.clone(),
                Arc::clone(&inner.fds),
                Arc::clone(&inner.budget),
                Arc::clone(&inner.scope),
            )
            .await?,
        );
        Ok(())
    }

    /// Append a value with its retained inline/heap/capacity bytes. Serialized
    /// frame and I/O allocations are admitted separately. The watermark
    /// advances only after the complete frame is written and flushed.
    /// Cancellation or an error invalidates further publication and wakes
    /// all prefix readers.
    ///
    /// # Errors
    /// Returns budget, codec or I/O failures, or rejects an invalid writer.
    ///
    /// # Panics
    /// Panics if internal prepared-writer or scratch-owner invariants fail.
    pub async fn append_with_size(
        &mut self,
        value: V,
        retained_bytes: usize,
    ) -> Result<(), SorterError> {
        let inner = self.active()?;
        let operation = inner.publication.begin()?;
        let next = self.rows.checked_add(1).ok_or(SorterError::RunLimit)?;
        let mut row = RunRow::new((), value);
        row.retained_bytes = retained_bytes.max(std::mem::size_of::<RunRow<(), V>>());
        row.charge = Some(inner.budget.reserve(row.retained_bytes)?);
        row.ordinal = self.rows;
        self.prepare().await?;
        self.writer
            .as_mut()
            .expect("prepared replay writer")
            .write_row(&row)
            .await?;
        self.rows = next;
        inner.publication.publish(next)?;
        operation.complete();
        Ok(())
    }

    /// Invalidate publication and await removal of this writer's scratch.
    ///
    /// Drop every prefix cursor before calling this method. Failed or cancelled
    /// appends remain abortable; abort never attempts to publish a final EOF.
    /// Repeated successful calls are harmless. If the abort future is
    /// cancelled, the original cleanup owner remains in this writer so a
    /// later call joins outstanding filesystem work before returning.
    ///
    /// # Errors
    /// Returns [`SorterError::ReplayInUse`] while prefix cursors retain
    /// storage, or propagates cleanup failures. Readers are invalidated
    /// even when they prevent exclusive cleanup. Drop them and retry on the
    /// same writer.
    ///
    /// # Panics
    /// Panics if the internal scratch-owner mutex is poisoned.
    pub async fn abort(&mut self) -> Result<(), SorterError> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        inner.publication.fail();
        self.writer = None;
        let inner = Arc::get_mut(inner).ok_or(SorterError::ReplayInUse)?;
        inner
            .cleanup
            .get_mut()
            .expect("replay scratch owner poisoned")
            .cleanup()
            .await
    }

    /// Publish final EOF after flushing the complete input. Existing prefix
    /// cursors observe the same final watermark and keep their positions.
    /// Success transfers the original scratch owner to the returned replay.
    /// Failure or cancellation leaves it here for [`Self::abort`]; no cleanup
    /// owner is consumed before publication succeeds.
    ///
    /// # Errors
    /// Returns budget/I/O failures or rejects a failed/cancelled writer.
    ///
    /// # Panics
    /// Panics if internal prepared-writer or scratch-owner invariants fail.
    pub async fn seal(&mut self) -> Result<Replay<V>, SorterError> {
        let inner = self.active()?;
        let operation = inner.publication.begin()?;
        self.prepare().await?;
        self.writer
            .take()
            .expect("prepared replay writer")
            .finish()
            .await?;
        inner.publication.seal()?;
        operation.complete();
        Ok(Replay {
            inner: self.inner.take().expect("active replay owner"),
            marker: PhantomData,
        })
    }
}

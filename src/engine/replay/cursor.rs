//! Cursor checkpoints distinguish provisional prefix EOF from final absence.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::storage::ReplayInner;
use crate::engine::run::RunReader;
use crate::error::SorterError;

/// One demand-driven read from a complete replay prefix.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplayStep<V> {
    /// One fully written value, transferred into the consumer's ownership.
    Item(V),
    /// The committed prefix ended; the caller must drive more upstream input.
    AwaitingAppend,
    /// The writer sealed and every committed value has been consumed.
    Exhausted,
}

/// Cheap independent checkpoint. No file or decoded buffer remains between
/// pulls.
pub struct ReplayCursor<V> {
    pub(super) inner: Arc<ReplayInner>,
    pub(super) offset: u64,
    position: u64,
    marker: PhantomData<fn() -> V>,
}
impl<V> Clone for ReplayCursor<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            offset: self.offset,
            position: self.position,
            marker: PhantomData,
        }
    }
}
impl<V> ReplayCursor<V> {
    pub(super) fn new(inner: Arc<ReplayInner>) -> Self {
        Self {
            inner,
            offset: 8,
            position: 0,
            marker: PhantomData,
        }
    }

    /// Return to the beginning without opening a file or copying rows.
    pub fn rewind(&mut self) {
        self.offset = 8;
        self.position = 0;
    }
}
impl<V: DeserializeOwned> ReplayCursor<V> {
    /// Read one complete prefix value, provisional EOF or sealed final EOF.
    /// `AwaitingAppend` leaves the checkpoint in place; future appends become
    /// visible on the next call without replaying already-consumed records.
    ///
    /// # Errors
    /// Propagates budget/I/O/decode failures and rejects failed, cancelled or
    /// dropped unsealed publishers. Failed reads never advance the checkpoint.
    pub async fn next_committed(&mut self) -> Result<ReplayStep<V>, SorterError> {
        let (published, sealed) = self.inner.publication.snapshot()?;
        if self.position >= published {
            return Ok(if sealed {
                ReplayStep::Exhausted
            } else {
                ReplayStep::AwaitingAppend
            });
        }
        let _metadata = self
            .inner
            .budget
            .reserve(self.inner.path.capacity() + std::mem::size_of::<RunReader<(), V>>())?;
        let mut reader: RunReader<(), V> = RunReader::from_position(
            &self.inner.path,
            self.offset,
            1,
            Arc::clone(&self.inner.fds),
            Arc::clone(&self.inner.budget),
            Arc::clone(&self.inner.scope),
        );
        let row = reader
            .next_row()
            .await?
            .ok_or_else(|| SorterError::Decode("committed replay row is missing".into()))?;
        self.inner.publication.snapshot()?;
        let next = self.position.checked_add(1).ok_or(SorterError::RunLimit)?;
        self.offset = reader.position().0;
        self.position = next;
        Ok(ReplayStep::Item(row.value))
    }

    /// Await the next value or sealed EOF. Consumers that own the upstream
    /// producer should use `next_committed` to drive it on `AwaitingAppend`.
    ///
    /// # Errors
    /// Propagates read/budget failures and invalid publisher errors.
    pub async fn next(&mut self) -> Result<Option<V>, SorterError> {
        loop {
            let publication = Arc::clone(&self.inner.publication);
            let changed = publication.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.next_committed().await? {
                ReplayStep::Item(value) => return Ok(Some(value)),
                ReplayStep::Exhausted => return Ok(None),
                ReplayStep::AwaitingAppend => changed.await,
            }
        }
    }
}

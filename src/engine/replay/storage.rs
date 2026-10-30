//! Shared scratch ownership for live-prefix and sealed replay readers.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{
    Arc,
    Mutex,
};

use tokio::sync::Semaphore;

use super::cursor::ReplayCursor;
use super::publication::Publication;
use crate::engine::budget::{
    MemoryBudget,
    MemoryCharge,
};
use crate::engine::cleanup::AsyncCleanupGuard;
use crate::engine::operation::IoScope;
use crate::error::SorterError;
use crate::service::SortLease;

pub(super) struct ReplayInner {
    pub(super) dir: PathBuf,
    pub(super) path: PathBuf,
    pub(super) fds: Arc<Semaphore>,
    pub(super) budget: Arc<MemoryBudget>,
    pub(super) scope: Arc<IoScope>,
    pub(super) publication: Arc<Publication>,
    cleanup: Mutex<AsyncCleanupGuard>,
    _lease: Option<SortLease>,
}

impl ReplayInner {
    pub(super) fn new(
        dir: PathBuf,
        fds: Arc<Semaphore>,
        budget: Arc<MemoryBudget>,
        lease: Option<SortLease>,
    ) -> Arc<Self> {
        let mut cleanup = AsyncCleanupGuard::disarmed();
        cleanup.set_fd_budget(Arc::clone(&fds));
        let scope = cleanup.scope();
        Arc::new(Self {
            path: dir.join("rows.cbor"),
            dir,
            fds,
            budget,
            scope,
            publication: Publication::new(),
            cleanup: Mutex::new(cleanup),
            _lease: lease,
        })
    }

    pub(super) fn arm(&self, reservation: MemoryCharge) {
        // The mutex protects setup only. Filesystem work never holds it.
        let mut cleanup = self.cleanup.lock().expect("replay scratch owner poisoned");
        cleanup.retain(reservation);
        cleanup.arm(self.dir.clone());
    }
}

/// Sealed immutable replay. Cursors share storage without copying rows.
pub struct Replay<V> {
    pub(super) inner: Arc<ReplayInner>,
    pub(super) marker: PhantomData<fn() -> V>,
}
impl<V> Replay<V> {
    /// Start an independent reader without opening files or buffering rows.
    #[must_use]
    pub fn cursor(&self) -> ReplayCursor<V> {
        ReplayCursor::new(Arc::clone(&self.inner))
    }

    /// Finish cleanup in-band after dropping every cursor/checkpoint.
    ///
    /// # Errors
    /// Returns [`SorterError::ReplayInUse`] while readers still own storage,
    /// or propagates filesystem cleanup errors. Existing readers keep
    /// ownership.
    ///
    /// # Panics
    /// Panics if the internal scratch-owner mutex is poisoned.
    pub async fn close(self) -> Result<(), SorterError> {
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            return Err(SorterError::ReplayInUse);
        };
        let mut cleanup = inner
            .cleanup
            .into_inner()
            .expect("replay scratch owner poisoned");
        cleanup.cleanup().await
    }
}

//! Scratch-directory removal tied to object lifetime.
//!
//! The guard is *disarmed* until a sort first spills, *armed* while spill
//! files exist, and travels with whichever object currently owns the scratch
//! directory's lifecycle: the [`crate::SortSession`] until
//! [`crate::SortSession::finish`], then the output value stream. Cleanup is
//! awaited in-band where possible and spawned onto the runtime on drop, so
//! run files never outlive the sort that produced them.

use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::budget::MemoryCharge;
use crate::engine::operation::IoScope;
use crate::error::SorterError;

/// Removes a sort's scratch directory when cleaned up or dropped.
#[derive(Debug, Default)]
pub(crate) struct AsyncCleanupGuard {
    dir: Option<PathBuf>,
    scope: Arc<IoScope>,
    reservation: Option<MemoryCharge>,
}

impl AsyncCleanupGuard {
    /// A guard that owns no directory yet (nothing has spilled).
    pub(crate) fn disarmed() -> Self {
        Self::default()
    }

    pub(crate) fn scope(&self) -> Arc<IoScope> {
        Arc::clone(&self.scope)
    }

    pub(crate) fn retain(&mut self, reservation: MemoryCharge) {
        assert!(
            self.reservation.is_none(),
            "scratch reservation is assigned once"
        );
        self.reservation = Some(reservation);
    }

    pub(crate) fn take_reservation(&mut self) -> Option<MemoryCharge> {
        self.reservation.take()
    }

    /// Point the guard at the scratch directory it must remove. Arming an
    /// already armed guard for the same sort is a no-op by construction —
    /// each sort has exactly one scratch directory.
    pub(crate) fn arm(&mut self, dir: PathBuf) {
        self.dir.get_or_insert(dir);
    }

    /// Remove the directory now, in-band. Idempotent: the guard disarms
    /// itself so a later drop does nothing.
    pub(crate) async fn cleanup(&mut self) -> Result<(), SorterError> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        self.scope.idle().await;
        async_fs_io::remove_dir_all(dir).await?;
        self.dir = None;
        self.reservation = None;
        Ok(())
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
                "sort scratch cleanup was dropped outside a Tokio runtime"
            );
            return;
        };
        let scope = Arc::clone(&self.scope);
        let reservation = self.reservation.take();
        handle.spawn(async move {
            let _reservation = reservation;
            scope.idle().await;
            if let Err(err) = async_fs_io::remove_dir_all(&dir).await {
                tracing::error!(dir = %dir.display(), error = %err, "sort scratch cleanup failed");
            }
        });
    }
}

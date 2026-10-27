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

use tokio::sync::Semaphore;

use crate::engine::budget::MemoryCharge;
use crate::engine::operation::{
    IoScope,
    perform_reserved,
};
use crate::error::SorterError;

/// Removes a sort's scratch directory when cleaned up or dropped.
#[derive(Debug, Default)]
pub(crate) struct AsyncCleanupGuard {
    dir: Option<PathBuf>,
    scope: Arc<IoScope>,
    reservation: Option<Arc<MemoryCharge>>,
    fds: Option<Arc<Semaphore>>,
}

impl AsyncCleanupGuard {
    /// A guard that owns no directory yet (nothing has spilled).
    pub(crate) fn disarmed() -> Self {
        Self::default()
    }

    pub(crate) fn set_fd_budget(&mut self, fds: Arc<Semaphore>) {
        assert!(
            self.dir.is_none(),
            "descriptor budget is assigned before scratch creation"
        );
        self.fds = Some(fds);
    }

    pub(crate) fn scope(&self) -> Arc<IoScope> {
        Arc::clone(&self.scope)
    }

    pub(crate) fn retain(&mut self, reservation: MemoryCharge) {
        assert!(
            self.reservation.is_none(),
            "scratch reservation is assigned once"
        );
        self.reservation = Some(Arc::new(reservation));
    }

    pub(crate) fn take_reservation(&mut self) -> Option<Arc<MemoryCharge>> {
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
        let fds = Arc::clone(
            self.fds
                .as_ref()
                .expect("armed scratch has a descriptor budget"),
        );
        let directory = dir.clone();
        let reservation = self.reservation.clone();
        perform_reserved(fds, Arc::clone(&self.scope), async move {
            let _reservation = reservation;
            async_fs_io::remove_dir_all(directory).await?;
            Ok(())
        })
        .await?;
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
        let fds = Arc::clone(
            self.fds
                .as_ref()
                .expect("armed scratch has a descriptor budget"),
        );
        handle.spawn(async move {
            scope.idle().await;
            let directory = dir.clone();
            let operation_reservation = reservation.clone();
            let result = perform_reserved(fds, scope, async move {
                let _reservation = operation_reservation;
                async_fs_io::remove_dir_all(directory).await?;
                Ok(())
            })
            .await;
            if let Err(err) = result {
                tracing::error!(dir = %dir.display(), error = %err, "sort scratch cleanup failed");
            }
            drop(reservation);
        });
    }
}

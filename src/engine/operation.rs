//! Ownership of inherently non-cancellable filesystem operations.
//!
//! Tokio filesystem futures can outlive their caller in the OS worker pool.
//! Keep descriptor and frame reservations until that single operation finishes;
//! a scratch guard waits only for operations belonging to its own directory.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

use tokio::sync::{
    Notify,
    Semaphore,
};

use crate::engine::budget::MemoryBudget;
use crate::error::SorterError;

#[derive(Debug, Default)]
pub(crate) struct IoScope {
    pending: AtomicUsize,
    changed: Notify,
}

impl IoScope {
    fn start(self: &Arc<Self>) -> IoGuard {
        self.pending.fetch_add(1, Ordering::AcqRel);
        IoGuard(Arc::clone(self))
    }

    pub(crate) async fn idle(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pending.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct IoGuard(Arc<IoScope>);
impl Drop for IoGuard {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.changed.notify_waiters();
        }
    }
}

pub(crate) async fn perform<T, F>(
    fds: Arc<Semaphore>,
    scope: Arc<IoScope>,
    memory: Arc<MemoryBudget>,
    path_bytes: usize,
    operation: F,
) -> Result<T, SorterError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, SorterError>> + Send + 'static,
{
    // bounded: one worker future and path copies per admitted descriptor;
    // fixed task bookkeeping is independent of relation size.
    let bytes = std::mem::size_of::<F>()
        .checked_mul(2)
        .and_then(|size| {
            path_bytes
                .checked_mul(3)
                .and_then(|paths| size.checked_add(paths))
        })
        .and_then(|size| size.checked_add(1024))
        .ok_or(SorterError::RunLimit)?;
    let metadata = memory.reserve(bytes)?;
    perform_reserved(fds, scope, async move {
        let result = operation.await;
        drop(metadata);
        result
    })
    .await
}

/// Run cleanup from its already-held metadata escrow, even when live
/// headroom is zero. File ownership still follows the same operation scope.
pub(crate) async fn perform_reserved<T, F>(
    fds: Arc<Semaphore>,
    scope: Arc<IoScope>,
    operation: F,
) -> Result<T, SorterError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, SorterError>> + Send + 'static,
{
    let guard = scope.start();
    let permit = fds.acquire_owned().await.map_err(|_| SorterError::Gone)?;
    // bounded: one owned filesystem job per actual descriptor permit.
    tokio::spawn(async move {
        let result = operation.await;
        drop(permit);
        drop(guard);
        result
    })
    .await
    .map_err(|error| SorterError::Worker(error.to_string()))?
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn cancelled_caller_keeps_operation_reservations_until_the_os_job_finishes() {
        let fds = Arc::new(Semaphore::new(1));
        let scope = Arc::new(IoScope::default());
        let memory = MemoryBudget::new(16384);
        let (started, ready) = oneshot::channel();
        let (release, done) = oneshot::channel();
        let task = tokio::spawn(perform(
            Arc::clone(&fds),
            Arc::clone(&scope),
            Arc::clone(&memory),
            0,
            async move {
                started.send(()).unwrap();
                done.await.unwrap();
                Ok(())
            },
        ));
        ready.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            fds.available_permits(),
            0,
            "the OS job still owns its descriptor"
        );
        assert_eq!(scope.pending.load(Ordering::Acquire), 1);
        assert!(memory.used() > 0);
        release.send(()).unwrap();
        scope.idle().await;
        assert_eq!(fds.available_permits(), 1);
        assert_eq!(memory.used(), 0);
    }
}

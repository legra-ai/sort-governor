//! Complete-frame watermark and the single writer's terminal state.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU8,
    AtomicU64,
    Ordering,
};

use tokio::sync::Notify;

use crate::error::SorterError;

const WRITING: u8 = 0;
const SEALED: u8 = 1;
const FAILED: u8 = 2;

pub(super) struct Publication {
    rows: AtomicU64,
    state: AtomicU8,
    pub(super) changed: Notify,
}

impl Publication {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            rows: AtomicU64::new(0),
            state: AtomicU8::new(WRITING),
            changed: Notify::new(),
        })
    }

    pub(super) fn begin(self: &Arc<Self>) -> Result<PublicationGuard, SorterError> {
        if self.state.load(Ordering::Acquire) != WRITING {
            return Err(SorterError::SessionFailed);
        }
        Ok(PublicationGuard {
            publication: Arc::clone(self),
            armed: true,
        })
    }

    pub(super) fn publish(&self, rows: u64) -> Result<(), SorterError> {
        if self.state.load(Ordering::Acquire) != WRITING {
            return Err(SorterError::SessionFailed);
        }
        self.rows.store(rows, Ordering::Release);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(super) fn seal(&self) -> Result<(), SorterError> {
        self.state
            .compare_exchange(WRITING, SEALED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| SorterError::SessionFailed)?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(super) fn fail(&self) {
        if self
            .state
            .compare_exchange(WRITING, FAILED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.changed.notify_waiters();
        }
    }

    pub(super) fn snapshot(&self) -> Result<(u64, bool), SorterError> {
        let state = self.state.load(Ordering::Acquire);
        if state == FAILED {
            return Err(SorterError::ReplaySourceFailed);
        }
        Ok((self.rows.load(Ordering::Acquire), state == SEALED))
    }
}

pub(super) struct PublicationGuard {
    publication: Arc<Publication>,
    armed: bool,
}
impl PublicationGuard {
    pub(super) fn complete(mut self) {
        self.armed = false;
    }
}
impl Drop for PublicationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.publication.fail();
        }
    }
}

//! Checked aggregate reservations for retained rows and codec allocations.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use crate::error::SorterError;

#[derive(Debug)]
pub(crate) struct MemoryBudget {
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
}

impl MemoryBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    pub(crate) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<MemoryCharge, SorterError> {
        let bytes = u64::try_from(bytes).map_err(|_| SorterError::RunLimit)?;
        let mut previous = self.used();
        loop {
            let Some(next) = previous
                .checked_add(bytes)
                .filter(|next| *next <= self.limit)
            else {
                return Err(SorterError::MemoryBudget {
                    requested: bytes,
                    available: self.limit.saturating_sub(previous),
                });
            };
            match self.used.compare_exchange_weak(
                previous,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(MemoryCharge {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(current) => previous = current,
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryCharge {
    budget: Arc<MemoryBudget>,
    bytes: u64,
}

impl Drop for MemoryCharge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Shared descriptor and memory accounting passed through merge operations.
pub(crate) struct RunResources {
    pub(crate) fds: Arc<tokio::sync::Semaphore>,
    pub(crate) memory: Arc<MemoryBudget>,
}
impl RunResources {
    pub(crate) fn new(fds: Arc<tokio::sync::Semaphore>, memory: Arc<MemoryBudget>) -> Self {
        Self { fds, memory }
    }
}

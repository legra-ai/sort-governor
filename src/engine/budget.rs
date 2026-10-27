//! Checked aggregate reservations for retained rows and codec allocations.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use crate::error::SorterError;
use crate::service::MemoryPressure;

pub(crate) struct MemoryBudget {
    limit: u64,
    pressure: Option<Arc<dyn MemoryPressure>>,
    used: AtomicU64,
    peak: AtomicU64,
}

impl MemoryBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            pressure: None,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub(crate) fn governed(limit: u64, pressure: Arc<dyn MemoryPressure>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            pressure: Some(pressure),
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub(crate) fn is_governed(&self) -> bool {
        self.pressure.is_some()
    }

    pub(crate) fn limit(&self) -> u64 {
        self.pressure.as_ref().map_or(self.limit, |pressure| {
            self.limit.min(
                pressure
                    .effective_target_bytes()
                    .saturating_sub(pressure.resident_bytes()),
            )
        })
    }
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    pub(crate) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    pub(crate) fn reserve_frame(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<MemoryCharge, SorterError> {
        // bounded: our exact-sized frame plus at most two frame lengths for
        // Tokio's private file buffer capacity, with small framing reads.
        // Files are reopened per operation, so no historical capacity remains.
        let working = bytes
            .checked_mul(3)
            .and_then(|size| size.checked_add(32))
            .ok_or(SorterError::RunLimit)?;
        self.reserve(working)
    }

    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<MemoryCharge, SorterError> {
        let bytes = u64::try_from(bytes).map_err(|_| SorterError::RunLimit)?;
        let mut previous = self.used();
        loop {
            let limit = self.limit();
            let Some(next) = previous.checked_add(bytes).filter(|next| *next <= limit) else {
                return Err(SorterError::MemoryBudget {
                    requested: bytes,
                    available: limit.saturating_sub(previous),
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
    pub(crate) io: Arc<crate::engine::operation::IoScope>,
}
impl RunResources {
    pub(crate) fn new(
        fds: Arc<tokio::sync::Semaphore>,
        memory: Arc<MemoryBudget>,
        io: Arc<crate::engine::operation::IoScope>,
    ) -> Self {
        Self { fds, memory, io }
    }
}

impl std::fmt::Debug for MemoryBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryBudget")
            .field("admission_limit", &self.limit())
            .field("used", &self.used())
            .finish()
    }
}

//! The memory-pressure seam the actor reads to build a planner snapshot.
//!
//! A process-wide memory governor (anything that samples RSS against a
//! target) implements [`MemoryPressure`]; keeping it behind a trait keeps
//! this crate free of any particular governor and lets tests drive the
//! planner with fixed readings.

/// A live source of process memory pressure.
pub trait MemoryPressure: Send + Sync + 'static {
    /// The memory governor's effective RSS target (target minus active
    /// leases).
    fn effective_target_bytes(&self) -> u64;
    /// Non-cache working-set bytes already resident.
    fn resident_bytes(&self) -> u64;
}

/// A fixed pressure reading — for tests and for processes that run without a
/// memory governor.
#[derive(Debug, Clone, Copy)]
pub struct StaticPressure {
    effective_target_bytes: u64,
    resident_bytes: u64,
}

impl StaticPressure {
    /// A pressure source that always reports the given target and residency.
    #[must_use]
    pub fn new(effective_target_bytes: u64, resident_bytes: u64) -> Self {
        Self {
            effective_target_bytes,
            resident_bytes,
        }
    }
}

impl MemoryPressure for StaticPressure {
    fn effective_target_bytes(&self) -> u64 {
        self.effective_target_bytes
    }

    fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }
}

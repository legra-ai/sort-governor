//! The live, moment-to-moment resource pressure the planner reads. The
//! actor assembles one of these from its [`crate::MemoryPressure`] source,
//! the global file-descriptor headroom, and the count of in-flight external
//! sorts — then the pure planner decides against it.

/// A point-in-time view of process-global sort pressure.
///
/// Holding both the memory and the file-descriptor pressure in one value is
/// the whole point: only a component that sees both can size a spill run
/// buffer *against* the fan-in instead of letting the two budgets fight
/// (a small run buffer chosen for memory's sake manufactures tens of
/// thousands of runs and exhausts the fd table).
#[derive(Debug, Clone, Copy)]
pub struct SorterSnapshot {
    effective_target_bytes: u64,
    resident_bytes: u64,
    fd_headroom: u32,
    active_external_sorts: u32,
}

impl SorterSnapshot {
    /// Capture current pressure.
    ///
    /// - `effective_target_bytes` — the memory governor's effective RSS target
    ///   (target minus active leases).
    /// - `resident_bytes` — non-cache working-set bytes already resident.
    /// - `fd_headroom` — usable sort file descriptors available right now,
    ///   already net of the safety margin (the actor sizes its fd semaphore to
    ///   `soft_limit - margin`, so this is its available-permit count).
    /// - `active_external_sorts` — external sorts currently holding fds.
    #[must_use]
    pub fn new(
        effective_target_bytes: u64,
        resident_bytes: u64,
        fd_headroom: u32,
        active_external_sorts: u32,
    ) -> Self {
        Self {
            effective_target_bytes,
            resident_bytes,
            fd_headroom,
            active_external_sorts,
        }
    }

    /// The memory governor's effective RSS target.
    #[must_use]
    pub fn effective_target_bytes(&self) -> u64 {
        self.effective_target_bytes
    }

    /// Non-cache working-set bytes already resident.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    /// Open file descriptors still available before the process limit.
    #[must_use]
    pub fn fd_headroom(&self) -> u32 {
        self.fd_headroom
    }

    /// External sorts currently holding file descriptors.
    #[must_use]
    pub fn active_external_sorts(&self) -> u32 {
        self.active_external_sorts
    }

    /// Memory available to a new sort right now: the effective target less
    /// what is already resident, saturating at zero.
    #[must_use]
    pub fn available_memory_bytes(&self) -> u64 {
        self.effective_target_bytes
            .saturating_sub(self.resident_bytes)
    }
}

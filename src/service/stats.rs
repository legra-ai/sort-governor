//! A point-in-time snapshot of the Sorter's governance counters.

/// Observability counters reported by [`crate::SorterHandle::stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SorterStats {
    /// Sorts admitted on the in-memory path since startup.
    pub in_memory_sorts: u64,
    /// Sorts admitted on the external (spilling) path since startup.
    pub external_sorts: u64,
    /// External sorts currently admitted (holding or awaiting fd permits).
    pub active_external_sorts: u32,
    /// File-descriptor permits currently available to new external sorts.
    pub fd_permits_available: u32,
    /// Total file-descriptor permits the Sorter rations.
    pub fd_permits_total: u32,
}

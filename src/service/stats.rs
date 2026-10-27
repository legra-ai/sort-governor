//! A point-in-time snapshot of the Sorter's governance counters.

/// Observability counters reported by [`crate::SorterHandle::stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SorterStats {
    /// Sorts admitted on the in-memory path since startup.
    pub in_memory_sorts: u64,
    /// Sorts admitted on the external (spilling) path since startup.
    pub external_sorts: u64,
    /// External sorts currently admitted, including their live output streams.
    pub active_external_sorts: u32,
    /// File-descriptor permits currently available to actual file operations.
    pub fd_permits_available: u32,
    /// Total file-descriptor permits the Sorter rations.
    pub fd_permits_total: u32,
    /// Retained row and codec bytes currently reserved by the engine.
    pub memory_bytes_used: u64,
    /// Highest simultaneously reserved byte count since governor creation.
    pub memory_bytes_peak: u64,
    /// Hard aggregate reservation ceiling shared by every admitted session.
    pub memory_bytes_limit: u64,
}

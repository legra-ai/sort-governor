//! The decision the planner produces.

/// How a sort will be executed, chosen by [`crate::SortPlanner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortPlan {
    /// Sort entirely in memory — a `Vec` sort that never opens a file
    /// descriptor. Chosen for small sorts when the budget has headroom.
    InMemory,
    /// Spill to disk and merge. `run_buffer_bytes` bounds the in-memory run
    /// before each spill; `max_fan_in` bounds the number of run files open
    /// at once during the (possibly cascaded) merge, so the process file
    /// descriptor table can never be exhausted.
    External {
        /// In-memory run size before a spill, in bytes.
        run_buffer_bytes: usize,
        /// Maximum run files open simultaneously during the merge.
        max_fan_in: u32,
    },
}

impl SortPlan {
    /// Whether this plan spills to disk.
    #[must_use]
    pub fn is_external(&self) -> bool {
        matches!(self, SortPlan::External { .. })
    }

    /// Whether this plan stays entirely in memory.
    #[must_use]
    pub fn is_in_memory(&self) -> bool {
        matches!(self, SortPlan::InMemory)
    }
}

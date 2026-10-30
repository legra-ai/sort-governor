//! The decision the planner produces.

/// How a sort will be executed, chosen by [`crate::SortPlanner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortPlan {
    /// Start in memory for a small estimate. Actual retained-byte limits
    /// still apply: underestimated input spills through the same engine.
    InMemory,
    /// Spill to disk and merge. `run_buffer_bytes` bounds the in-memory run
    /// before each spill; `max_fan_in` bounds retained merge heads. Actual
    /// file opens additionally acquire shared operation-scoped permits.
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

    /// Whether this plan starts in memory, subject to runtime byte limits.
    #[must_use]
    pub fn is_in_memory(&self) -> bool {
        matches!(self, SortPlan::InMemory)
    }
}

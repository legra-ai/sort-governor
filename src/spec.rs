//! The caller's description of a sort job — what is being sorted and
//! roughly how big it is. The Sorter plans against this, never against the
//! rows themselves.

/// A request to sort a relation, sized but not yet executed.
///
/// `estimated_rows` / `estimated_bytes` are the planner's only view of the
/// input magnitude; they need not be exact, but a gross under-estimate
/// pushes a large sort onto the in-memory path and a gross over-estimate
/// wastes spill buffers. Callers derive them from cardinality estimates and
/// per-row width.
#[derive(Debug, Clone)]
pub struct SortSpec {
    estimated_rows: u64,
    estimated_bytes: u64,
    dedup: bool,
    label: &'static str,
}

impl SortSpec {
    /// Describe a sort of `estimated_rows` rows totalling roughly
    /// `estimated_bytes` of key + value bytes.
    #[must_use]
    pub fn new(estimated_rows: u64, estimated_bytes: u64) -> Self {
        Self {
            estimated_rows,
            estimated_bytes,
            dedup: false,
            label: "sort",
        }
    }

    /// Request that equal-key rows be collapsed to one during the merge.
    #[must_use]
    pub fn with_dedup(mut self, dedup: bool) -> Self {
        self.dedup = dedup;
        self
    }

    /// Attach a short static label used in tracing and statistics.
    #[must_use]
    pub fn labelled(mut self, label: &'static str) -> Self {
        self.label = label;
        self
    }

    /// Estimated number of rows to be sorted.
    #[must_use]
    pub fn estimated_rows(&self) -> u64 {
        self.estimated_rows
    }

    /// Estimated total key + value bytes to be sorted.
    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// Whether equal-key rows are collapsed during the merge.
    #[must_use]
    pub fn dedup(&self) -> bool {
        self.dedup
    }

    /// The short static label for tracing and stats.
    #[must_use]
    pub fn label(&self) -> &'static str {
        self.label
    }
}

//! How many runs one merge may open at once under the memory budget.

use crate::engine::budget::MemoryBudget;

/// Narrowest merge that still reduces the run count.
const MIN_MERGE_WIDTH: usize = 2;

/// Upper bound on what one open merge source keeps charged: its decoded head
/// row plus a read chunk covering that head's frame. Every run a session
/// writes, including merged runs, re-encodes rows the session admitted, so
/// the session-wide maxima bound every source of every merge.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HeadBound {
    row_bytes: usize,
    frame_bytes: usize,
}

impl HeadBound {
    /// Record one admitted row's retained charge.
    pub(crate) fn observe_row(&mut self, retained_bytes: usize) {
        self.row_bytes = self.row_bytes.max(retained_bytes);
    }

    /// Record the largest frame a finished run writer produced.
    pub(crate) fn observe_frame(&mut self, frame_bytes: usize) {
        self.frame_bytes = self.frame_bytes.max(frame_bytes);
    }

    fn source_bytes(self) -> u64 {
        u64::try_from(self.row_bytes.saturating_add(self.frame_bytes)).unwrap_or(u64::MAX)
    }

    /// Widest merge of up to `wanted` runs whose open sources fit half the
    /// current headroom; the other half covers the row in flight, its output
    /// chunk and the transient read buffers. Never below two, the narrowest
    /// merge that makes progress.
    pub(crate) fn merge_width(self, budget: &MemoryBudget, wanted: usize) -> usize {
        let per_source = self.source_bytes().saturating_mul(2).max(1);
        let fits = usize::try_from(budget.headroom() / per_source).unwrap_or(usize::MAX);
        fits.min(wanted).max(MIN_MERGE_WIDTH)
    }
}

#[cfg(test)]
mod tests {
    use super::HeadBound;
    use crate::engine::budget::MemoryBudget;

    #[test]
    fn large_heads_narrow_the_merge_and_small_heads_keep_the_plan_fan_in() {
        let budget = MemoryBudget::new(1 << 20);
        let mut large = HeadBound::default();
        large.observe_row(197_192);
        large.observe_frame(65_600);
        assert_eq!(large.merge_width(&budget, 16), 2);
        let mut small = HeadBound::default();
        small.observe_row(600);
        small.observe_frame(620);
        assert_eq!(small.merge_width(&budget, 16), 16);
        assert_eq!(HeadBound::default().merge_width(&budget, 1), 2);
    }
}

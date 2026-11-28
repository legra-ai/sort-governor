//! Min-heap of merge-source indices ordered by each source's buffered head.
//!
//! The heap stores no key copies: comparisons read the heads in place, so a
//! merge never holds a second, separately charged copy of every head key.

/// Binary min-heap over source indices with an external `less` order.
pub(super) struct SourceHeap {
    slots: Vec<usize>,
}

impl SourceHeap {
    /// Exact-capacity heap; the caller has already charged `capacity` slots.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        let slots = Vec::with_capacity(capacity);
        assert_eq!(
            slots.capacity(),
            capacity,
            "exact heap capacity reservation"
        );
        Self { slots }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub(super) fn push(&mut self, idx: usize, less: impl Fn(usize, usize) -> bool) {
        self.slots.push(idx);
        let mut child = self.slots.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !less(self.slots[child], self.slots[parent]) {
                break;
            }
            self.slots.swap(child, parent);
            child = parent;
        }
    }

    pub(super) fn pop(&mut self, less: impl Fn(usize, usize) -> bool) -> Option<usize> {
        let last = self.slots.len().checked_sub(1)?;
        self.slots.swap(0, last);
        let top = self.slots.pop();
        let mut parent = 0;
        loop {
            let left = 2 * parent + 1;
            let right = left + 1;
            let mut smallest = parent;
            if left < self.slots.len() && less(self.slots[left], self.slots[smallest]) {
                smallest = left;
            }
            if right < self.slots.len() && less(self.slots[right], self.slots[smallest]) {
                smallest = right;
            }
            if smallest == parent {
                return top;
            }
            self.slots.swap(parent, smallest);
            parent = smallest;
        }
    }
}

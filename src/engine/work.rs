//! Deterministic work counters for performance-regression tests.
//!
//! Wall-clock ceilings are noisy; these counters measure the work a sort
//! actually did (descriptor-scoped operations, bytes moved, runs and merge
//! passes) so tests can bound it by formulas over the plan and row count.
//! They are compiled only under the `work-stats` feature or `cfg(test)`;
//! otherwise every recording method is an empty inline no-op on a zero-sized
//! type, so default builds pay nothing.

use std::sync::Arc;
#[cfg(any(test, feature = "work-stats"))]
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

use crate::engine::operation::IoScope;

/// Copyable snapshot of one session's (or replay's) recorded work.
#[cfg(any(test, feature = "work-stats"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkStats {
    /// Descriptor-scoped filesystem operations (`perform`/`perform_reserved`).
    pub perform_ops: u64,
    /// Bytes appended or rewritten in run files.
    pub bytes_written: u64,
    /// Bytes read back from run files.
    pub bytes_read: u64,
    /// Original runs spilled from the session buffer.
    pub runs_spilled: u64,
    /// Final-cascade passes that folded runs before the streaming merge.
    pub merge_passes: u64,
    /// Multi-run groups merged to a file by those passes.
    pub merge_groups: u64,
    /// Pairwise carry compactions of the session spill frontier.
    pub carry_merges: u64,
    /// Peak number of merge-source readers open at the same time.
    pub max_open_merge_readers: u64,
}

/// Live atomic counters owned by one [`crate::engine::operation::IoScope`].
#[cfg(any(test, feature = "work-stats"))]
#[derive(Debug, Default)]
pub(crate) struct WorkCounters {
    perform_ops: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    runs_spilled: AtomicU64,
    merge_passes: AtomicU64,
    merge_groups: AtomicU64,
    carry_merges: AtomicU64,
    open_merge_readers: AtomicU64,
    max_open_merge_readers: AtomicU64,
}

/// Zero-sized stand-in: every recording method compiles to nothing.
#[cfg(not(any(test, feature = "work-stats")))]
#[derive(Debug, Default)]
pub(crate) struct WorkCounters;

/// Held by an open merge source; dropping it closes the reader's slot.
#[cfg(any(test, feature = "work-stats"))]
pub(crate) struct OpenReader(Arc<IoScope>);

/// Zero-sized stand-in for the open-reader token.
#[cfg(not(any(test, feature = "work-stats")))]
pub(crate) struct OpenReader;

impl OpenReader {
    pub(crate) fn new(scope: &Arc<IoScope>) -> Self {
        scope.work().reader_opened();
        #[cfg(any(test, feature = "work-stats"))]
        return Self(Arc::clone(scope));
        #[cfg(not(any(test, feature = "work-stats")))]
        return Self;
    }
}

#[cfg(any(test, feature = "work-stats"))]
impl Drop for OpenReader {
    fn drop(&mut self) {
        self.0
            .work()
            .open_merge_readers
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(any(test, feature = "work-stats"))]
impl WorkCounters {
    fn add(counter: &AtomicU64, amount: usize) {
        counter.fetch_add(u64::try_from(amount).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn perform_op(&self) {
        Self::add(&self.perform_ops, 1);
    }

    pub(crate) fn wrote(&self, bytes: usize) {
        Self::add(&self.bytes_written, bytes);
    }

    pub(crate) fn read(&self, bytes: usize) {
        Self::add(&self.bytes_read, bytes);
    }

    pub(crate) fn run_spilled(&self) {
        Self::add(&self.runs_spilled, 1);
    }

    pub(crate) fn merge_pass(&self) {
        Self::add(&self.merge_passes, 1);
    }

    pub(crate) fn merge_group(&self) {
        Self::add(&self.merge_groups, 1);
    }

    pub(crate) fn carry_merge(&self) {
        Self::add(&self.carry_merges, 1);
    }

    pub(crate) fn reader_opened(&self) {
        let open = self.open_merge_readers.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_open_merge_readers
            .fetch_max(open, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> WorkStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        WorkStats {
            perform_ops: load(&self.perform_ops),
            bytes_written: load(&self.bytes_written),
            bytes_read: load(&self.bytes_read),
            runs_spilled: load(&self.runs_spilled),
            merge_passes: load(&self.merge_passes),
            merge_groups: load(&self.merge_groups),
            carry_merges: load(&self.carry_merges),
            max_open_merge_readers: load(&self.max_open_merge_readers),
        }
    }
}

// The no-op methods keep the recording API identical in both builds.
#[cfg(not(any(test, feature = "work-stats")))]
#[allow(clippy::unused_self)]
impl WorkCounters {
    #[inline]
    pub(crate) fn perform_op(&self) {}

    #[inline]
    pub(crate) fn wrote(&self, _bytes: usize) {}

    #[inline]
    pub(crate) fn read(&self, _bytes: usize) {}

    #[inline]
    pub(crate) fn run_spilled(&self) {}

    #[inline]
    pub(crate) fn merge_pass(&self) {}

    #[inline]
    pub(crate) fn merge_group(&self) {}

    #[inline]
    pub(crate) fn carry_merge(&self) {}

    #[inline]
    pub(crate) fn reader_opened(&self) {}
}

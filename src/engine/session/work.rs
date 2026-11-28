//! Work-counter access for performance-regression tests.

use super::SortSession;
use crate::engine::work::WorkStats;

impl<K, V> SortSession<K, V> {
    /// Work recorded so far by this session's filesystem operations. After
    /// [`SortSession::finish`] the counters travel with the output; read
    /// them from [`crate::SortOutput::work_stats`].
    #[doc(hidden)]
    #[must_use]
    pub fn work_stats(&self) -> WorkStats {
        self.cleanup.scope().work().snapshot()
    }
}

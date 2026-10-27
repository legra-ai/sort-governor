//! An admitted sort sharing the governor's operation-scoped descriptor budget.

use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU32,
    Ordering,
};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::engine::SortSession;
use crate::plan::SortPlan;

/// Decrements the active-sort counter when its lease is dropped.
#[derive(Debug)]
struct ActiveGuard {
    active: Arc<AtomicU32>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An admitted sort: a plan, a private scratch path and a shared I/O budget.
///
/// Admission never reserves idle descriptors. Every engine file operation
/// acquires a permit and closes its file before returning. This permits nested
/// sorts to make progress while their parent's input is being produced.
#[derive(Debug)]
pub struct SortLease {
    plan: SortPlan,
    scratch_dir: PathBuf,
    fds: Arc<Semaphore>,
    _active: Option<ActiveGuard>,
}

impl SortLease {
    /// An in-memory lease: no fd permits, no concurrency slot.
    pub(crate) fn in_memory(plan: SortPlan, scratch_dir: PathBuf, fds: Arc<Semaphore>) -> Self {
        Self {
            plan,
            scratch_dir,
            fds,
            _active: None,
        }
    }

    /// An external lease sharing the descriptor budget and tracking activity.
    pub(crate) fn external(
        plan: SortPlan,
        scratch_dir: PathBuf,
        fds: Arc<Semaphore>,
        active: Arc<AtomicU32>,
    ) -> Self {
        Self {
            plan,
            scratch_dir,
            fds,
            _active: Some(ActiveGuard { active }),
        }
    }

    /// The plan chosen for this sort.
    #[must_use]
    pub fn plan(&self) -> SortPlan {
        self.plan
    }

    /// The private scratch directory this sort may spill into.
    #[must_use]
    pub fn scratch_dir(&self) -> &Path {
        &self.scratch_dir
    }

    /// Open the sort session bound to this lease, consuming the lease into
    /// the session so activity and resource accounting follow its output
    /// stream.
    #[must_use]
    pub fn into_session<K, V>(self, dedup: bool) -> SortSession<K, V>
    where
        K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
        V: Serialize + DeserializeOwned + Send + 'static,
    {
        let plan = self.plan;
        let dir = self.scratch_dir.clone();
        SortSession::new(plan, dir, dedup)
            .with_fd_budget(Arc::clone(&self.fds))
            .hold_resource(Box::new(self))
    }
}

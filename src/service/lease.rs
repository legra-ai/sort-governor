//! An admitted sort and the global resources it holds.

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
use tokio::sync::OwnedSemaphorePermit;

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

/// An admitted sort: the chosen [`SortPlan`], a private scratch directory,
/// and the global resource permits held for the sort's lifetime.
///
/// Dropping the lease returns the file-descriptor permits and the
/// concurrency slot. **Keep the lease alive until the value stream from
/// [`SortSession::finish`] is fully consumed** — dropping it early returns
/// the fd budget while the merge may still be reading run files.
#[derive(Debug)]
pub struct SortLease {
    plan: SortPlan,
    scratch_dir: PathBuf,
    _fd_permit: Option<OwnedSemaphorePermit>,
    _active: Option<ActiveGuard>,
}

impl SortLease {
    /// An in-memory lease: no fd permits, no concurrency slot.
    pub(crate) fn in_memory(plan: SortPlan, scratch_dir: PathBuf) -> Self {
        Self {
            plan,
            scratch_dir,
            _fd_permit: None,
            _active: None,
        }
    }

    /// An external lease holding `fd_permit` file-descriptor permits and a
    /// slot in the active-sort count.
    pub(crate) fn external(
        plan: SortPlan,
        scratch_dir: PathBuf,
        fd_permit: OwnedSemaphorePermit,
        active: Arc<AtomicU32>,
    ) -> Self {
        Self {
            plan,
            scratch_dir,
            _fd_permit: Some(fd_permit),
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
    /// the session so its fd permits live for as long as the sort's output
    /// stream — there is no way to drop the lease early by accident.
    #[must_use]
    pub fn into_session<K, V>(self, dedup: bool) -> SortSession<K, V>
    where
        K: Ord + Clone + Serialize + DeserializeOwned + Send + 'static,
        V: Serialize + DeserializeOwned + Send + 'static,
    {
        let plan = self.plan;
        let dir = self.scratch_dir.clone();
        SortSession::new(plan, dir, dedup).hold_resource(Box::new(self))
    }
}

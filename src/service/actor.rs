//! The Sorter governor actor. It is the only place the global sort budgets
//! are reconciled: it builds a [`SorterSnapshot`] from current pressure,
//! plans each sort, reserves its file descriptors, and hands back a
//! [`SortLease`]. Rows never reach it — it governs, it does not funnel.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU32,
    Ordering,
};

use tokio::sync::{
    Semaphore,
    mpsc,
    oneshot,
};

use crate::config::SorterConfig;
use crate::error::SorterError;
use crate::plan::{
    SortPlan,
    SortPlanner,
};
use crate::service::command::SorterCommand;
use crate::service::lease::SortLease;
use crate::service::pressure::MemoryPressure;
use crate::service::stats::SorterStats;
use crate::snapshot::SorterSnapshot;
use crate::spec::SortSpec;

/// The single sort governor for the process.
pub(crate) struct SorterActor {
    config: SorterConfig,
    fd_budget: Arc<Semaphore>,
    fd_total: u32,
    active: Arc<AtomicU32>,
    pressure: Arc<dyn MemoryPressure>,
    scratch_root: PathBuf,
    next_id: u64,
    in_memory_count: u64,
    external_count: u64,
}

impl SorterActor {
    /// Build an actor rationing `fd_budget` usable descriptors (floored at 2,
    /// the minimum a merge needs).
    pub(crate) fn new(
        config: SorterConfig,
        fd_budget: u32,
        pressure: Arc<dyn MemoryPressure>,
        scratch_root: PathBuf,
    ) -> Self {
        let fd_total = fd_budget.max(2);
        Self {
            config,
            fd_budget: Arc::new(Semaphore::new(fd_total as usize)),
            fd_total,
            active: Arc::new(AtomicU32::new(0)),
            pressure,
            scratch_root,
            next_id: 0,
            in_memory_count: 0,
            external_count: 0,
        }
    }

    /// Run the command loop until every handle is dropped.
    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<SorterCommand>) {
        while let Some(command) = rx.recv().await {
            match command {
                SorterCommand::Submit { spec, reply } => self.admit(&spec, reply),
                SorterCommand::Stats { reply } => {
                    let _ = reply.send(self.stats());
                }
            }
        }
    }

    fn admit(&mut self, spec: &SortSpec, reply: oneshot::Sender<Result<SortLease, SorterError>>) {
        let snapshot = self.snapshot();
        let plan = SortPlanner::plan(spec, &snapshot, &self.config);
        let id = self.next_id;
        self.next_id += 1;
        let dir = self.scratch_root.join(format!("sort-{id:020}"));
        match plan {
            SortPlan::InMemory => {
                self.in_memory_count += 1;
                let _ = reply.send(Ok(SortLease::in_memory(plan, dir)));
            }
            SortPlan::External { max_fan_in, .. } => {
                self.external_count += 1;
                self.active.fetch_add(1, Ordering::Relaxed);
                let sem = Arc::clone(&self.fd_budget);
                let active = Arc::clone(&self.active);
                // Acquire off the actor loop so a fully-committed fd budget
                // backpressures this one sort without stalling the governor.
                tokio::spawn(async move {
                    let Ok(permit) = sem.acquire_many_owned(max_fan_in).await else {
                        active.fetch_sub(1, Ordering::Relaxed);
                        let _ = reply.send(Err(SorterError::Gone));
                        return;
                    };
                    let _ = reply.send(Ok(SortLease::external(plan, dir, permit, active)));
                });
            }
        }
    }

    /// Permits not currently held by an admitted external sort. The semaphore
    /// was sized from a `u32`, so the count always fits; saturate rather than
    /// truncate if that invariant were ever broken.
    fn available_fd_permits(&self) -> u32 {
        u32::try_from(self.fd_budget.available_permits()).unwrap_or(u32::MAX)
    }

    fn snapshot(&self) -> SorterSnapshot {
        SorterSnapshot::new(
            self.pressure.effective_target_bytes(),
            self.pressure.resident_bytes(),
            self.available_fd_permits(),
            self.active.load(Ordering::Relaxed),
        )
    }

    fn stats(&self) -> SorterStats {
        SorterStats {
            in_memory_sorts: self.in_memory_count,
            external_sorts: self.external_count,
            active_external_sorts: self.active.load(Ordering::Relaxed),
            fd_permits_available: self.available_fd_permits(),
            fd_permits_total: self.fd_total,
        }
    }
}

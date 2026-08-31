//! The cheap, cloneable client side of the Sorter — the only way callers
//! reach the governor.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{
    mpsc,
    oneshot,
};

use crate::config::SorterConfig;
use crate::error::SorterError;
use crate::service::actor::SorterActor;
use crate::service::command::SorterCommand;
use crate::service::lease::SortLease;
use crate::service::pressure::MemoryPressure;
use crate::service::stats::SorterStats;
use crate::spec::SortSpec;

/// Bounded depth of the admission queue.
const COMMAND_CHANNEL_CAPACITY: usize = 256;

/// A handle to the process Sorter. Clone freely; all clones address the one
/// governor actor.
#[derive(Clone)]
pub struct SorterHandle {
    tx: mpsc::Sender<SorterCommand>,
}

impl SorterHandle {
    /// Spawn the governor and return a handle to it. `fd_budget` is the
    /// number of usable file descriptors the Sorter may ration (the process
    /// soft limit minus a safety margin); sorts spill under unique
    /// directories beneath `scratch_root`.
    #[must_use]
    pub fn spawn(
        config: SorterConfig,
        fd_budget: u32,
        pressure: Arc<dyn MemoryPressure>,
        scratch_root: PathBuf,
    ) -> Self {
        let (tx, rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let actor = SorterActor::new(config, fd_budget, pressure, scratch_root);
        tokio::spawn(actor.run(rx));
        Self { tx }
    }

    /// Admit a sort: the governor plans it, reserves its resources, and
    /// returns a lease. The returned lease must be held until the sort's
    /// value stream is fully consumed.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError::Gone`] if the governor is no longer running.
    pub async fn submit(&self, spec: SortSpec) -> Result<SortLease, SorterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SorterCommand::Submit { spec, reply })
            .await
            .map_err(|_| SorterError::Gone)?;
        rx.await.map_err(|_| SorterError::Gone)?
    }

    /// Read the governor's current counters.
    ///
    /// # Errors
    ///
    /// Returns [`SorterError::Gone`] if the governor is no longer running.
    pub async fn stats(&self) -> Result<SorterStats, SorterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SorterCommand::Stats { reply })
            .await
            .map_err(|_| SorterError::Gone)?;
        rx.await.map_err(|_| SorterError::Gone)
    }
}

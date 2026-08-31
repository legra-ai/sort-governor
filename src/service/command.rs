//! The admission tickets that ride the actor's bounded queue. Rows never
//! cross this channel — only requests to admit a sort or report stats.

use tokio::sync::oneshot;

use crate::error::SorterError;
use crate::service::lease::SortLease;
use crate::service::stats::SorterStats;
use crate::spec::SortSpec;

/// A message to the Sorter actor.
pub(crate) enum SorterCommand {
    /// Admit a sort: plan it, reserve its resources, and return a lease.
    Submit {
        /// The sort's size description.
        spec: SortSpec,
        /// Where the admitted lease (or admission error) is delivered.
        reply: oneshot::Sender<Result<SortLease, SorterError>>,
    },
    /// Report the current governance counters.
    Stats {
        /// Where the stats snapshot is delivered.
        reply: oneshot::Sender<SorterStats>,
    },
}

//! The Sorter service: the governor actor, its handle, the admission lease,
//! and the pressure seam it plans against.

mod actor;
mod command;
mod handle;
mod lease;
mod pressure;
mod stats;

#[cfg(test)]
mod tests;

pub use handle::SorterHandle;
pub use lease::SortLease;
pub use pressure::{
    MemoryPressure,
    StaticPressure,
};
pub use stats::SorterStats;

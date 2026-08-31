#![doc = include_str!("../README.md")]

mod config;
mod engine;
mod error;
mod plan;
mod service;
mod snapshot;
mod spec;

pub use config::SorterConfig;
pub use engine::SortSession;
pub use error::SorterError;
pub use plan::{
    SortPlan,
    SortPlanner,
};
pub use service::{
    MemoryPressure,
    SortLease,
    SorterHandle,
    SorterStats,
    StaticPressure,
};
pub use snapshot::SorterSnapshot;
pub use spec::SortSpec;

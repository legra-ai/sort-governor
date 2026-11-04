//! The Sorter's execution engine: a [`session::SortSession`] that buffers,
//! spills, and merges according to a [`crate::SortPlan`], backed by a
//! bounded async cascade merge over `async-fs-io`.

pub(crate) mod budget;
mod cleanup;
mod codec;
mod merge;
mod operation;
mod output;
mod replay;
mod row;
mod run;
mod session;

#[cfg(test)]
mod tests;

pub use output::SortOutput;
pub use replay::{
    Replay,
    ReplayCursor,
    ReplayStep,
    ReplayWriter,
};
pub use session::SortSession;

//! The Sorter's execution engine: a [`session::SortSession`] that buffers,
//! spills, and merges according to a [`crate::SortPlan`], backed by a
//! bounded async cascade merge over `async-fs-io`.

mod merge;
mod row;
mod run;
mod session;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use merge::ValueStream;
pub use session::SortSession;

//! The sort plan and the pure planner that chooses it.

mod kind;
mod planner;

#[cfg(test)]
mod tests;

pub use kind::SortPlan;
pub use planner::SortPlanner;

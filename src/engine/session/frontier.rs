//! The spill frontier: spilled runs not yet merged, oldest first.
//!
//! Each entry covers consecutive original runs and carries its merge level.
//! When the newest `fan_in` entries share a level they merge into one run
//! one level up, so every byte is rewritten about `log_fan_in(runs)` times
//! instead of `log_2(runs)`. Levels never increase toward the newest entry,
//! and carries only merge adjacent entries oldest first, so equal keys keep
//! their chronological order.

use std::path::PathBuf;

use crate::plan::SortPlan;

/// Most runs the frontier retains; one more forces merging the newest ones.
pub(super) const MAX_FRONTIER_RUNS: usize = 64;

/// Widest carry merge. Each open reader holds one I/O chunk charged to the
/// memory budget; a fan-in of 64 exhausted a 64 MiB session budget, and 16
/// measured fastest.
pub(super) const MAX_CARRY_FAN_IN: usize = 16;

/// Narrowest merge that still reduces the run count.
const MIN_CARRY_FAN_IN: usize = 2;

/// One frontier entry: a run file and how many carry levels built it.
struct FrontierRun {
    path: PathBuf,
    level: u32,
}

/// Adjacent frontier runs, oldest first, taken out to merge into one.
pub(super) struct Carry {
    paths: Vec<PathBuf>,
    level: u32,
}

impl Carry {
    /// The runs to merge, oldest first.
    pub(super) fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    /// The level of the run these merge into; the input paths are spent.
    fn into_level(self) -> u32 {
        self.level
    }
}

/// The bounded, ordered set of spilled runs awaiting the final merge.
pub(super) struct SpillFrontier {
    // bounded: at most `MAX_FRONTIER_RUNS + 1` entries, oldest first.
    runs: Vec<FrontierRun>,
    fan_in: usize,
}

impl SpillFrontier {
    /// An empty frontier carrying `min(plan fan-in, MAX_CARRY_FAN_IN)` runs
    /// at a time.
    pub(super) fn new(plan: SortPlan) -> Self {
        let fan_in = match plan {
            SortPlan::InMemory => MIN_CARRY_FAN_IN,
            SortPlan::External { max_fan_in, .. } => {
                usize::try_from(max_fan_in).unwrap_or(MAX_CARRY_FAN_IN)
            }
        };
        Self {
            runs: Vec::new(),
            fan_in: fan_in.clamp(MIN_CARRY_FAN_IN, MAX_CARRY_FAN_IN),
        }
    }

    /// Runs currently retained.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    /// The oldest retained run.
    #[cfg(test)]
    pub(super) fn oldest(&self) -> Option<&PathBuf> {
        self.runs.first().map(|run| &run.path)
    }

    /// Append a freshly spilled run as the newest entry.
    pub(super) fn push(&mut self, path: PathBuf) {
        if self.runs.capacity() == 0 {
            self.runs.reserve_exact(MAX_FRONTIER_RUNS + 1);
        }
        self.runs.push(FrontierRun { path, level: 0 });
    }

    /// Take the next carry to merge, if the frontier needs one: the newest
    /// `fan_in` runs when they share a level, or the newest runs when the
    /// frontier exceeds [`MAX_FRONTIER_RUNS`].
    pub(super) fn next_carry(&mut self) -> Option<Carry> {
        let len = self.runs.len();
        let level = self.runs.last()?.level;
        let full = len >= self.fan_in
            && self.runs[len - self.fan_in..]
                .iter()
                .all(|run| run.level == level);
        let take = if full {
            self.fan_in
        } else if len > MAX_FRONTIER_RUNS {
            self.fan_in.min(len)
        } else {
            return None;
        };
        // bounded: at most `fan_in` adjacent runs, oldest first.
        let group: Vec<FrontierRun> = self.runs.drain(len - take..).collect();
        // Levels never increase toward the end, so the oldest is the highest.
        let level = group[0].level + u32::from(full);
        let paths = group.into_iter().map(|run| run.path).collect();
        Some(Carry { paths, level })
    }

    /// Record `merged` as the newest entry in place of `carry`'s runs.
    pub(super) fn settle(&mut self, carry: Carry, merged: PathBuf) {
        self.runs.push(FrontierRun {
            path: merged,
            level: carry.into_level(),
        });
    }

    /// Hand every retained run to the final merge, oldest first.
    pub(super) fn into_paths(self) -> Vec<PathBuf> {
        // bounded: at most `MAX_FRONTIER_RUNS` entries.
        self.runs.into_iter().map(|run| run.path).collect()
    }
}

#[cfg(test)]
mod tests;

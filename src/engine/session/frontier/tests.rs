//! Frontier carry shape: width, bound, and chronological order.

use std::path::PathBuf;

use super::{
    MAX_CARRY_FAN_IN,
    MAX_FRONTIER_RUNS,
    SpillFrontier,
};
use crate::plan::SortPlan;

fn external(max_fan_in: u32) -> SortPlan {
    SortPlan::External {
        run_buffer_bytes: 1,
        max_fan_in,
    }
}

/// Simulates spills; a merged run is named by the runs it covers, so the
/// name records chronological order.
struct Spiller {
    frontier: SpillFrontier,
    next: u64,
    widest: usize,
    peak: usize,
}

impl Spiller {
    fn new(plan: SortPlan) -> Self {
        Self {
            frontier: SpillFrontier::new(plan),
            next: 0,
            widest: 0,
            peak: 0,
        }
    }

    fn spill(&mut self) {
        self.frontier.push(PathBuf::from(format!("{}", self.next)));
        self.next += 1;
        self.peak = self.peak.max(self.frontier.len());
        while let Some(carry) = self.frontier.next_carry() {
            self.widest = self.widest.max(carry.paths().len());
            let first = carry.paths()[0]
                .to_str()
                .unwrap()
                .split('-')
                .next()
                .unwrap();
            let last = carry.paths()[carry.paths().len() - 1].to_str().unwrap();
            let last = last.rsplit('-').next().unwrap();
            let merged = PathBuf::from(format!("{first}-{last}"));
            self.frontier.settle(carry, merged);
        }
    }

    /// Every retained run, as covered original-run ranges, must tile
    /// `0..next` in order.
    fn assert_chronological(self) {
        let mut expected = 0u64;
        for path in self.frontier.into_paths() {
            let name = path.to_str().unwrap().to_owned();
            let mut bounds = name.split('-').map(|n| n.parse::<u64>().unwrap());
            let first = bounds.next().unwrap();
            let last = bounds.next().unwrap_or(first);
            assert_eq!(first, expected, "runs out of order: {name}");
            expected = last + 1;
        }
        assert_eq!(expected, self.next);
    }
}

#[test]
fn carry_width_is_the_plan_fan_in_capped_at_sixteen() {
    for (max_fan_in, width) in [(2, 2), (3, 3), (7, 7), (16, 16), (64, 16), (u32::MAX, 16)] {
        let mut spiller = Spiller::new(external(max_fan_in));
        for _ in 0..1_000 {
            spiller.spill();
        }
        assert_eq!(spiller.widest, width, "max_fan_in {max_fan_in}");
        assert!(spiller.widest <= MAX_CARRY_FAN_IN);
        spiller.assert_chronological();
    }
    let mut in_memory = Spiller::new(SortPlan::InMemory);
    for _ in 0..100 {
        in_memory.spill();
    }
    assert_eq!(in_memory.widest, 2);
}

#[test]
fn frontier_never_exceeds_sixty_four_runs() {
    // Fan-in 16 keeps up to 15 runs per level: past four full levels the
    // frontier would pass 64 without forced merges.
    let mut spiller = Spiller::new(external(64));
    for _ in 0..400_000 {
        spiller.spill();
        assert!(spiller.frontier.len() <= MAX_FRONTIER_RUNS);
    }
    assert!(
        spiller.peak > MAX_FRONTIER_RUNS,
        "forced merges must engage"
    );
    spiller.assert_chronological();
}

#[test]
fn carries_merge_only_adjacent_runs_oldest_first() {
    for max_fan_in in [2, 3, 5, 16, 64] {
        let mut spiller = Spiller::new(external(max_fan_in));
        for _ in 0..4_099 {
            spiller.spill();
        }
        spiller.assert_chronological();
    }
}

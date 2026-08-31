//! Exhaustive unit coverage for the pure planner — every branch of the
//! in-memory-vs-external decision and the fan-in / run-buffer rationing.

use crate::config::SorterConfig;
use crate::plan::kind::SortPlan;
use crate::plan::planner::SortPlanner;
use crate::snapshot::SorterSnapshot;
use crate::spec::SortSpec;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

fn plan(spec: &SortSpec, snap: &SorterSnapshot) -> SortPlan {
    SortPlanner::plan(spec, snap, &SorterConfig::default())
}

#[test]
fn small_sort_with_headroom_stays_in_memory() {
    let spec = SortSpec::new(1_000, MIB);
    let snap = SorterSnapshot::new(GIB, 0, 1_000, 0);
    assert_eq!(plan(&spec, &snap), SortPlan::InMemory);
}

#[test]
fn small_sort_without_headroom_must_spill() {
    // Under the 8 MiB ceiling, but only 2 MiB of memory is available.
    let spec = SortSpec::new(10_000, 4 * MIB);
    let snap = SorterSnapshot::new(2 * MIB, 0, 1_000, 0);
    assert!(plan(&spec, &snap).is_external());
}

#[test]
fn large_sort_with_plentiful_fds_uses_full_fan_in() {
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(4 * GIB, 0, 1_000, 0);
    let cfg = SorterConfig::default();
    match SortPlanner::plan(&spec, &snap, &cfg) {
        SortPlan::External {
            run_buffer_bytes,
            max_fan_in,
        } => {
            assert_eq!(max_fan_in, cfg.max_fan_in());
            let buf = run_buffer_bytes as u64;
            assert!(buf >= cfg.min_run_buffer_bytes() && buf <= cfg.max_run_buffer_bytes());
        }
        SortPlan::InMemory => panic!("a 10 GiB sort must spill"),
    }
}

#[test]
fn fd_pressure_shrinks_fan_in_and_raises_run_buffer() {
    // Only 4 usable fds -> tiny fan-in, so the planner spends memory (max run
    // buffer) to keep the pass depth bounded.
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(2 * GIB, 0, 4, 0);
    let cfg = SorterConfig::default();
    match SortPlanner::plan(&spec, &snap, &cfg) {
        SortPlan::External {
            run_buffer_bytes,
            max_fan_in,
        } => {
            assert_eq!(max_fan_in, 4);
            assert_eq!(run_buffer_bytes as u64, cfg.max_run_buffer_bytes());
        }
        SortPlan::InMemory => panic!("must spill"),
    }
}

#[test]
fn concurrent_sorts_divide_the_fd_budget() {
    // 1000 usable fds shared across 16 sorts (15 active + this one) -> 62.
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(4 * GIB, 0, 1_000, 15);
    match plan(&spec, &snap) {
        SortPlan::External { max_fan_in, .. } => assert_eq!(max_fan_in, 62),
        SortPlan::InMemory => panic!("must spill"),
    }
}

#[test]
fn fan_in_never_drops_below_two() {
    // Exactly one usable fd still yields a workable two-way merge.
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(4 * GIB, 0, 1, 0);
    match plan(&spec, &snap) {
        SortPlan::External { max_fan_in, .. } => assert_eq!(max_fan_in, 2),
        SortPlan::InMemory => panic!("must spill"),
    }
}

#[test]
fn run_buffer_clamps_to_the_floor() {
    // Forced external but tiny: the two-pass target is well under the floor.
    let spec = SortSpec::new(10_000, 4 * MIB);
    let snap = SorterSnapshot::new(2 * MIB, 0, 1_000, 0);
    let cfg = SorterConfig::default();
    match SortPlanner::plan(&spec, &snap, &cfg) {
        SortPlan::External {
            run_buffer_bytes, ..
        } => assert_eq!(run_buffer_bytes as u64, cfg.min_run_buffer_bytes()),
        SortPlan::InMemory => panic!("must spill"),
    }
}

#[test]
fn run_buffer_is_capped_by_available_memory() {
    // Small fan-in wants a 256 MiB buffer, but only 50 MiB is available.
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(50 * MIB, 0, 4, 0);
    let cfg = SorterConfig::default();
    match SortPlanner::plan(&spec, &snap, &cfg) {
        SortPlan::External {
            run_buffer_bytes,
            max_fan_in,
        } => {
            assert_eq!(max_fan_in, 4);
            assert_eq!(run_buffer_bytes as u64, 50 * MIB);
        }
        SortPlan::InMemory => panic!("must spill"),
    }
}

#[test]
fn from_fd_limit_caps_fan_in_below_the_limit() {
    // A 256-fd process: (256 - 96) / 8 = 20.
    let cfg = SorterConfig::from_fd_limit(256);
    assert_eq!(cfg.max_fan_in(), 20);
    let spec = SortSpec::new(1_000_000_000, 10 * GIB);
    let snap = SorterSnapshot::new(4 * GIB, 0, 1_000, 0);
    match SortPlanner::plan(&spec, &snap, &cfg) {
        SortPlan::External { max_fan_in, .. } => assert!(max_fan_in <= 20),
        SortPlan::InMemory => panic!("must spill"),
    }
}

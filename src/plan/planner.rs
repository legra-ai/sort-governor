//! The pure in-memory-vs-external decision. No I/O, no state —
//! given a [`SortSpec`], a [`SorterSnapshot`], and a [`SorterConfig`] it
//! returns a [`SortPlan`]. This is the heart of the Sorter and is the only
//! place the two budgets (memory and file descriptors) are reconciled.

use crate::config::SorterConfig;
use crate::plan::kind::SortPlan;
use crate::snapshot::SorterSnapshot;
use crate::spec::SortSpec;

/// Chooses how a sort runs. Stateless; every input is explicit so the
/// decision is exhaustively unit-testable.
pub struct SortPlanner;

impl SortPlanner {
    /// Decide whether `spec` sorts in memory or spills, and with what run
    /// buffer and fan-in if it spills.
    ///
    /// A sort stays in memory only when it both fits under the configured
    /// ceiling *and* the budget has the headroom to hold it. Otherwise it
    /// spills, with the fan-in rationed from the global file-descriptor
    /// headroom (so concurrent sorts cannot collectively exhaust the fd
    /// table) and the run buffer sized *against* that fan-in to keep the
    /// merge-pass depth bounded.
    #[must_use]
    pub fn plan(spec: &SortSpec, snap: &SorterSnapshot, cfg: &SorterConfig) -> SortPlan {
        let available = snap.available_memory_bytes();
        if spec.estimated_bytes() <= cfg.in_memory_ceiling_bytes()
            && available >= spec.estimated_bytes()
        {
            return SortPlan::InMemory;
        }
        let max_fan_in = Self::ration_fan_in(snap, cfg);
        let run_buffer_bytes = Self::size_run_buffer(spec, snap, cfg, max_fan_in);
        SortPlan::External {
            run_buffer_bytes,
            max_fan_in,
        }
    }

    /// Share the available file descriptors across the sorts in flight,
    /// floored at 2 (a merge needs at least two inputs) and capped at the
    /// configured ceiling. `fd_headroom` is already net of the safety
    /// margin (the actor carves the margin out when it sizes the fd
    /// semaphore), so the margin is not subtracted again here.
    fn ration_fan_in(snap: &SorterSnapshot, cfg: &SorterConfig) -> u32 {
        let share = snap.fd_headroom() / snap.active_external_sorts().saturating_add(1);
        share.clamp(2, cfg.max_fan_in())
    }

    /// Pick a run buffer so the run count stays within `max_fan_in²` — at
    /// most a two-pass cascade — without requesting more memory than is
    /// available. Under fd pressure (small fan-in) this raises the buffer
    /// rather than letting the run count explode; the cascade merge remains
    /// correct for any run count, so this only bounds pass depth.
    fn size_run_buffer(
        spec: &SortSpec,
        snap: &SorterSnapshot,
        cfg: &SorterConfig,
        max_fan_in: u32,
    ) -> usize {
        let fan = u64::from(max_fan_in);
        let two_pass_target = spec
            .estimated_bytes()
            .div_ceil(fan.saturating_mul(fan).max(1));
        let mem_cap = snap
            .available_memory_bytes()
            .max(cfg.min_run_buffer_bytes());
        let buffer = two_pass_target
            .clamp(cfg.min_run_buffer_bytes(), cfg.max_run_buffer_bytes())
            .min(mem_cap);
        usize::try_from(buffer).unwrap_or(usize::MAX)
    }
}

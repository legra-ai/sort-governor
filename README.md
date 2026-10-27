# sort-governor

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![CI][ci-badge]][ci-url]
[![License][license-badge]][license-url]
[![Downloads][downloads-badge]][downloads-url]

A process-wide governor for bounded asynchronous sorting.

Every sort in a process shares one `SorterHandle`. The governor plans from
estimates, but actual retained rows and codec allocations are also reserved
against a shared byte ceiling. Unknown and underestimated inputs spill. File
permits cover actual operations and are released before a consumer pauses, so
nested sorts and replay readers can make progress under the same descriptor cap.

## Why a governor?

Constructing an external sort ad hoc at every call site produces four
independent failures once sorts run concurrently or inputs get large:

1. **Unbounded merge fan-in.** Opening one reader per spilled run exhausts
   the process descriptor table (`EMFILE`) as soon as run count exceeds the
   soft limit.
2. **Two budgets that fight.** A memory limit that shrinks the run buffer
   inflates the run count — and therefore the descriptor count. Only a
   component that sees *both* budgets can size a run buffer *against* the
   fan-in instead of letting one knob sabotage the other.
3. **Blocking I/O on the async runtime.** A synchronous reader driven from
   an async stream stalls the executor's worker threads.
4. **No global admission.** Even with every sort individually bounded, `N`
   concurrent sorts collectively exhaust the process.

One actor owns the shared budgets; row data stays in the calling task. The
engine uses fully asynchronous [`async-fs-io`](https://crates.io/crates/async-fs-io)
operations and a bounded merge frontier. Sixty-four optional path slots compact
adjacent runs during ingestion. Readers retain positions and checked row counts,
rather than open files, between pulls.

## Usage

```rust
use std::sync::Arc;

use futures_util::StreamExt;
use sort_governor::{MemoryPressure, SortSpec, SorterConfig, SorterError, SorterHandle, StaticPressure};

#[tokio::main]
async fn main() -> Result<(), SorterError> {
    // Wire in your process memory governor here; a fixed reading works for
    // processes without one.
    let pressure: Arc<dyn MemoryPressure> = Arc::new(StaticPressure::new(1 << 30, 0));
    let scratch_root = std::env::temp_dir().join("sort-governor-example");

    // 256 usable descriptors, of which the governor may ration 160.
    let sorter = SorterHandle::spawn(SorterConfig::from_fd_limit(256), 160, pressure, scratch_root);

    // Admission shares the process budget; it reserves no idle descriptors.
    let lease = sorter.submit(SortSpec::new(5, 5 * 16).labelled("example")).await?;
    let mut session = lease.into_session::<u32, String>(false);
    for key in [5_u32, 1, 4, 2, 3] {
        let value = format!("row {key}");
        let retained = std::mem::size_of::<(u32, String)>() + value.capacity();
        session.push_with_size(key, value, retained).await?;
    }

    // Values stream out in key order; the lease and any scratch directory are
    // released when the stream is fully consumed or dropped.
    let mut stream = session.finish().await?;
    for expected in 1..=5 {
        assert_eq!(stream.next().await.expect("one result")?, format!("row {expected}"));
    }
    assert!(stream.next().await.is_none());
    Ok(())
}
```

## How it decides

| Type              | Role                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| `SortSpec`        | The caller's estimate: rows, bytes, dedup, label. The planner never sees the rows themselves.          |
| `SorterConfig`    | Planner caps: in-memory threshold, maximum head fan-in and run-buffer sizes.    |
| `MemoryPressure`  | Live memory readings (effective target, resident bytes). Implement it over your memory governor.       |
| `SorterSnapshot`  | One point-in-time view of memory *and* descriptor pressure — the input the planner reconciles.         |
| `SortPlanner`     | Pure and stateless: `(spec, snapshot, config) → SortPlan`. Exhaustively unit-testable.                 |
| `SortPlan`        | `InMemory`, or `External { run_buffer_bytes, max_fan_in }`.                                            |
| `SorterHandle`    | Cloneable client of the one governor actor; `submit` returns a lease, `stats` reports counters.        |
| `SortLease`       | The admitted sort: its plan, private scratch directory, shared resource budgets and activity lifetime.  |
| `SortSession`     | Push rows, `finish` into a `Stream` of values in key order. Spills and merges without caller involvement. |

A nonzero estimate below the in-memory threshold starts without disk I/O.
A zero-byte estimate means unknown size and selects a spill-capable plan.
Actual retained-byte thresholds apply to both plans: underestimation cannot
leave an unbounded in-memory relation. Stable ordinal tie-breaking permits an
in-place unstable sort without allocating another run-sized sorting buffer.

Every new reservation uses the smaller of the configured maximum run buffer
and the pressure source's currently observed available target. Existing owned
rows are not magically reclaimed when the target shrinks; new allocations fail
until enough reservations are released. Deferred cleanup keeps its metadata
reservation until its filesystem work completes, bounding the cleanup backlog
with the same byte budget. Scratch identities are unique across governor
lifetimes even when callers share a temporary root.

`push_with_size` and `append_with_size` require the caller's actual retained
key/value size, including owned capacities and nested data. Shared backing may
be conservatively overcounted. `push` is for values without owned heap data.
The engine separately charges row-vector capacities, cloned merge keys, bounded
frontier/path metadata, measured CBOR frames, file-buffer working space and
in-flight operation bookkeeping. A record that cannot fit fails with a typed
budget error before its frame allocation. Returned values transfer to their
consumer, which must account for any values it retains. Reservations are not a
measurement of allocator bookkeeping, runtime stacks, RSS or consumer memory.

### Replaying rows

`sorter.replay_writer::<V>().await?` opens a sequential append sink using the
same run codec, scratch lifecycle and shared budgets. Append with
`writer.append_with_size(value, retained_bytes).await?`, then
`writer.seal().await?` to obtain immutable `Replay<V>`. A replay cursor's
`next().await?` yields one owned value. Cloning a cursor checkpoints its current
position; `rewind()` returns it to the beginning. Duplicate rows and insertion
order are preserved. Cursors share storage ownership, allocate no row buffers
when cloned, and retain no descriptor or decoded row between calls. Drop all
cursors and call `replay.close().await?` when a group/relation is finished to
complete cleanup in-band before constructing the next group. Closing while a
cursor still owns storage returns a typed error; early-drop cleanup preserves
that cursor's ownership.

The standalone `SortSession::with_temp_dir` and
`ReplayWriter::with_temp_dir(root, byte_cap)` use this same engine with local
budgets. Aggregate process governance requires sharing one `SorterHandle`.

## Guarantees

- **Bounded retention.** Input magnitude does not grow the run frontier, merge
  fan-in or replay cursor state. Byte reservations cover live engine data and
  fail rather than silently exceed the shared cap.
- **Bounded descriptors.** Every actual file operation acquires one shared
  permit. No admitted session or paused output holds an idle descriptor;
  nested sort/replay combinations work even with a one-descriptor budget.
- **Asynchronous throughout.** All spill and merge I/O is awaited over
  `async-fs-io`; no blocking calls run on Tokio worker threads.
- **Deterministic order.** Equal keys are emitted in run order; with dedup,
  the first row of each equal-key group survives.
- **Self-cleaning.** Scratch is armed before asynchronous directory creation,
  owned through output/replay lifetimes and removed on completion or drop.
  A cancelled caller leaves only its bounded, inherently non-cancellable OS
  operation running; that operation retains its permits and frame reservations.
  Cleanup waits for this scratch directory's operations before removing it.
- **Fail fast.** I/O, encode, and decode failures surface as typed
  `SorterError` variants; there are no silent fallbacks. Run-counter exhaustion
  fails before creating another file. A failed or cancelled spill invalidates
  the session so later calls cannot produce partial results.

Row keys and values must implement `serde::Serialize` and
`serde::de::DeserializeOwned`; spilled runs are framed CBOR.

## License

Licensed under either of:

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE));
- MIT License ([`LICENSE-MIT`](LICENSE-MIT)).

## Links

[crates-badge]: https://img.shields.io/crates/v/sort-governor.svg
[crates-url]: https://crates.io/crates/sort-governor
[docs-badge]: https://docs.rs/sort-governor/badge.svg
[docs-url]: https://docs.rs/sort-governor
[ci-badge]: https://github.com/legra-ai/sort-governor/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/legra-ai/sort-governor/actions/workflows/ci.yml
[license-badge]: https://img.shields.io/crates/l/sort-governor.svg
[license-url]: https://github.com/legra-ai/sort-governor/blob/main/LICENSE-APACHE
[downloads-badge]: https://img.shields.io/crates/d/sort-governor.svg
[downloads-url]: https://crates.io/crates/sort-governor

# sort-governor

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![CI][ci-badge]][ci-url]
[![License][license-badge]][license-url]
[![Downloads][downloads-badge]][downloads-url]

A process-wide governor for bounded asynchronous sorting.

Every sort in a process — small or huge, in memory or spilling to disk —
is admitted by one `SorterHandle`. The governor decides **in-memory versus
external** for each sort from its estimated size and the *live* memory and
file-descriptor pressure, rations the **process-global** sort resources
(open descriptors, spill memory, concurrent external sorts), and runs a
**bounded, fully asynchronous cascade merge** whose memory and descriptor
footprint stay `O(fan_in)` for any input size.

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

`sort-governor` addresses all four: one pure planner reconciles memory and
descriptor pressure, one actor rations permits, every run reader holds
exactly one descriptor and reads one row at a time over
[`async-fs-io`](https://crates.io/crates/async-fs-io), and the cascade
merge folds runs in passes so at most `fan_in` files are ever open.

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

    // Describe the sort; the governor plans it and hands back a lease that
    // holds the sort's resource permits.
    let lease = sorter.submit(SortSpec::new(5, 5 * 16).labelled("example")).await?;
    let mut session = lease.into_session::<u32, String>(false);
    for key in [5_u32, 1, 4, 2, 3] {
        session.push(key, format!("row {key}")).await?;
    }

    // Values stream out in key order; the lease and any scratch directory are
    // released when the stream is fully consumed or dropped.
    let mut stream = session.finish().await?;
    let mut ordered = Vec::new();
    while let Some(value) = stream.next().await {
        ordered.push(value?);
    }
    assert_eq!(ordered[0], "row 1");
    assert_eq!(ordered[4], "row 5");
    Ok(())
}
```

## How it decides

| Type              | Role                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| `SortSpec`        | The caller's estimate: rows, bytes, dedup, label. The planner never sees the rows themselves.          |
| `SorterConfig`    | Process-lifetime caps: in-memory ceiling, max fan-in, concurrent external sorts, run-buffer bounds.    |
| `MemoryPressure`  | Live memory readings (effective target, resident bytes). Implement it over your memory governor.       |
| `SorterSnapshot`  | One point-in-time view of memory *and* descriptor pressure — the input the planner reconciles.         |
| `SortPlanner`     | Pure and stateless: `(spec, snapshot, config) → SortPlan`. Exhaustively unit-testable.                 |
| `SortPlan`        | `InMemory`, or `External { run_buffer_bytes, max_fan_in }`.                                            |
| `SorterHandle`    | Cloneable client of the one governor actor; `submit` returns a lease, `stats` reports counters.        |
| `SortLease`       | The admitted sort: its plan, a private scratch directory, and the descriptor and concurrency permits.  |
| `SortSession`     | Push rows, `finish` into a `Stream` of values in key order. Spills and merges without caller involvement. |

A sort stays in memory only when its estimate fits under the configured
ceiling *and* the available memory can hold it. Otherwise the planner shares
the descriptor headroom across the sorts in flight (floored at two — a merge
needs two inputs), then sizes the run buffer so the run count stays within
`fan_in²` — at most a two-pass cascade — without exceeding available memory.
Under descriptor pressure it deliberately spends memory to keep pass depth
bounded rather than letting the run count explode.

## Guarantees

- **Bounded memory.** A session buffers at most one run; the merge holds one
  head row per open reader. Nothing materialises the whole input.
- **Bounded descriptors.** At most `max_fan_in` run files are open per sort,
  and the governor's semaphore caps the total across sorts.
- **Asynchronous throughout.** All spill and merge I/O is awaited over
  `async-fs-io`; no blocking calls run on Tokio worker threads.
- **Deterministic order.** Equal keys are emitted in run order; with dedup,
  the first row of each equal-key group survives.
- **Self-cleaning.** The scratch directory is created lazily on the first
  spill and removed when the output stream ends or is dropped — and also when
  a spilled session is dropped before `finish()` (an error-path bail-out
  cannot leak run files).
- **Fail fast.** I/O, encode, and decode failures surface as typed
  `SorterError` variants; there are no silent fallbacks.

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

# Access Performance Contract

## Purpose and Scope

Native measurement executable owned by the [Framework package](../../DESIGN.md).
It verifies the DatabaseAccess performance acceptance matrix without entering a
production library product. Children: none. The fixed Native lane uses Swift
6.4.0 release with macOS SDK identity recorded by its harness. Ordinary/Embedded
runtime regression remains in the existing adapter and Client runtime gates;
these Native measurements are not portable performance claims.

## Responsibilities and Boundaries

The fixture owns data, output checks, benchmark scheduling and observations.
It consumes canonical Client invocation and real Framework SQLite execution.
Its loopback transport is a bounded Wire endpoint, not a SQL server or an
authorization substitute. The SQLite branch enables original security policy,
real monotonic deadlines and the same per-call budget for direct and named use.
No parser, query executor, retry policy or storage implementation is replaced.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Framework](../../DESIGN.md) | parent | Optional fixture composition | Owns executable graph | No production product dependency |
| [Common access](../../../database-client/Sources/DatabaseAccess/DESIGN.md) | depends on | Lookup, immutable query construction and execution | Owns acceptance matrix | Original result semantics remain fixed |
| [Embedded](../../Sources/DatabaseEmbedded/DESIGN.md) | depends on | Borrowed scope with original authority | Real SQLite path | Borrowed shutdown preserves the container |
| [Observation](../AccessPerformanceProbe/DESIGN.md) | depends on | Clock, heap/RSS and allocator observations | Native process observer | Allocator calls are not Swift slab suballocations |

## Architecture

```text
lookup / construction / execution matrix
    -> direct connection or named handle
        -> Client + canonical Wire loopback
        -> Framework + SQLite + original policies/budgets
    -> identical row consumption -> outside-timer output checks
    -> JSON measurements and exact input provenance
```

## Contracts and Invariants

- Lookup runs at concurrency 1/4/8. Construction and execution run at output
  limits 1/100/10000 and concurrency 1/4/8. Both connections explicitly allow 8. The fixture budget allows 100000
  cumulative intermediate rows for the 10000-row scan/sort/projection stages,
  128MiB intermediate bytes, 10 million work units and a 60-second deadline;
  the output-row limit matches the scenario. These are fixture inputs, not
  production defaults.
- Every comparison uses identical query definitions, source data, budget and
  output consumption. Aliases are resolved before execution timing; lookup and
  construction are separate measured phases.
- A warm round precedes alternating direct/named rounds. Raw per-call latency
  and batch wall time are retained; throughput derives from complete batches.
  Construction/lookup report each worker loop duration divided by its operation
  count separately from batch wall time per concurrent operation.
  Outputs are counted/summed during consumption and checked after timing ends.
- Memory observation uses a separate run which holds all concurrent final
  results at their ownership boundary before consumption. Record baseline and
  retained heap/block counts, process heap/RSS high-water values, allocator
  reservations and successful allocator API calls/requested bytes. Report
  scoped deltas separately from cumulative process high-water values.
- Native allocator observation is process-local and never installed globally.
  Its calibration records observation overhead. Time and allocation runs use
  equal observation settings within each direct/named comparison.
- No payload-sized additional retention, unbounded queue or extra encoding is
  accepted without investigation. No universal latency threshold is invented.

## Runtime Flows

Open one isolated SQLite container and explicit authorized context, seed its
owned table, open direct and named scopes, warm each scenario, measure the
matrix, check every result, then drain scopes and await container shutdown.
Each benchmark process owns its complete setup and teardown.

## State, Ownership, and Lifecycle

Task-group results are immutable Sendable values. Each iterator has exclusive
progress. The directory owns only borrowed logical scopes. SQLite owns its
in-process state. The observation library lives for the benchmark process and
retains no database buffers or allocation pointers.

## Failure, Concurrency, and Constraints

Wrong output, missing observer in allocation mode, failed native memory query,
invalid timing, capacity rejection and incomplete matrix are failures. The
harness uses external build/run timeouts and exact completion/matrix checks.
Heavy unrelated compilation must finish before formal measurements.

## Verification and Change Impact

The probe self-checks successful allocation observation and snapshots. A short
representative execution establishes cost before the full matrix. The full run
requires all scenarios and identical results; fixture diagnostics and raw JSON
are retained. The final package graph and runtime gates must name the committed
Client revision. Source or measurement-setting changes invalidate that run.


### Native acceptance recorded on 2026-10-25

The URL graph pins Client `89f03891f9d6700e7eb6d10fd04aaa1a50bf0d5f`.
Swift 6.4.0 release, macOS SDK 27.0 and SQLite/ScalarIndexes produced 84 path
scenarios (42 direct/named comparisons), five retained rounds per scenario:
420 time records and 420 independent allocation records, all output checks
passed. Raw logs, complete comparisons and compiler/input hashes are in
[the final evidence](../../../.codex-tools/access-shutdown-20261025/s02-url-final/provenance.json).
The Client endpoint is an in-process Wire loopback; these figures do not
measure HTTP, network latency or a remote SQL server.

| 10000 output rows | Direct p50 / p95 ms | Named p50 / p95 ms | Direct / named queries per second |
|---|---:|---:|---:|
| Client, concurrency 1 | 10.026 / 10.084 | 10.104 / 10.369 | 99.68 / 98.90 |
| Client, concurrency 8 | 12.567 / 13.376 | 12.624 / 13.512 | 618.00 / 610.31 |
| SQLite, concurrency 1 | 362.207 / 367.081 | 360.687 / 364.604 | 2.76 / 2.77 |
| SQLite, concurrency 8 | 1593.533 / 2841.823 | 1586.799 / 2815.011 | 2.82 / 2.84 |

Across row counts, named Client results retain an additional 448 bytes per
concurrent result; named SQLite results have zero additional retained heap.
The extra allocator API calls per execution are approximately 2 and 1,
respectively, independent of row count. Process-cumulative maximum RSS is
74.22MiB for the observation run. Allocator heap high-water is unavailable on
this allocator and is explicitly null; it is not reported as a zero peak.
Requested bytes count cumulative allocator requests, not live or copied bytes.

Lookup and construction timings are worker-loop means per operation, with
separate batch-amortized costs; their p95 values are not individual-call
latency quantiles. At concurrency 8, local named lookup averages a median
347ns versus 253.5ns for a cached reference. Bound construction averages a
median 4469ns versus 248.5ns for pure IR (batch-amortized 599.125ns versus
40.125ns), without additional allocator calls. Retaining an already-built
immutable query avoids this repeated construction cost. The bound-reference
path carries database identity through EntitySource/QueryReference; profiling
ARC, dispatch and optimizer visibility is needed before assigning that CPU
cost to one cause or changing those implementations.

SQLite throughput plateaus near 2.8 queries/second when concurrency rises
from 1 to 8, while queue-inclusive latency grows. The 10000-row execution
observes about 1.89 million allocation API calls per query in both paths.
These are execution/backend profiling targets rather than facade payload
retention. No queue, duplicate codec or payload-sized additional retained
buffer was introduced by the named facade. This task records these existing
performance limits without expanding into query-engine optimization.

Observation calibration records 20000 malloc/free iterations: disabled
516000ns, enabled 497000ns. This short diagnostic is not evidence that
instrumentation is faster or free. All reported response timings come from
the separate process without the injected allocation observer.

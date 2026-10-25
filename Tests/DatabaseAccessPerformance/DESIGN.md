# Access Performance Contract

## Purpose and Scope

Native measurement executable owned by the [Framework package](../../DESIGN.md).
It verifies the DatabaseAccess performance acceptance matrix without entering a
production library product. Children: none. The executable supports the default
single-root configuration only; MultiBase builds compile an explicit unavailable
entry point and do not claim a performance result. The fixed Native lane uses Swift
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


### RX25 matched refactoring measurements (2026-10-25)

Final source is `ee1af9dd`; the archived baseline has the original `a7bcf834`
production path. Both run the unchanged 10000-row fixture, SQLite in-memory
single-connection configuration, input/budget/output checks and URL pins under
Swift 6.4.0 release. Fresh adjacent baseline/final runs each passed all 84 path
scenarios: 420 timing and 420 separate allocation records. The allocator observer
passed its 40000-call, four-thread self-check. Complete data and input provenance:
[matched comparisons](../../../.codex-tools/sql-execution-refactor-20261025/matched-comparisons.json).

| SQLite execution, 10000 output rows | Before | Final | Change |
|---|---:|---:|---:|
| Direct allocator calls/query, concurrency 1 | 1883702 | 1760780 | -6.53% |
| Direct requested allocation bytes/query | 133827552 | 126439601 | -5.52% |
| Named allocator calls/query, concurrency 1 | 1883703 | 1760780 | -6.53% |
| Named requested allocation bytes/query | 133826984 | 126438937 | -5.52% |
| Retained heap, concurrency 1 | 524912 bytes | 524912 bytes | Equal |
| Retained heap, concurrency 8 | 4198784 bytes | 4198784 bytes | Equal |
| Process-cumulative peak RSS, complete observation run | 72.89 MiB | 73.03 MiB | +0.19% |

Requested bytes are cumulative allocator requests, not copied bytes or live heap.
Allocator heap high-water remains unavailable. One intermediate run increased
requested bytes by12.6%; its positional-root VM startup was corrected before
acceptance, with an exact-overlay-budget regression and the final decrease above.

| Raw SQLite timing, direct path | Before p50/p95 ms | Final p50/p95 ms | Before/final queries per second |
|---|---:|---:|---:|
| Concurrency 1 | 369.194 / 374.557 | 564.934 / 629.453 | 2.708 / 1.770 |
| Concurrency 8 | 1653.548 / 2910.124 | 2074.562 / 3744.716 | 2.749 / 2.226 |

These elapsed values do not establish a causal speed improvement or regression:
the source-unchanged Client loopback control also slowed from10.079 to18.718ms
at concurrency1 and12.729 to22.456ms at concurrency8, with unchanged allocation
counts. An earlier timing pair moved in the opposite direction. All owned builds
were stopped before the final pair, but this shared desktop's elapsed-time
variation remains unresolved. Preserve the raw latency/throughput results;
accept the reproducible allocation reduction and equal retained memory without
claiming a latency gain. Dedicated-runner scheduling/power observations are
needed for a causal speed claim, rather than another uncontrolled repetition.

SQL admission overlap is separately proven by the controlled Native commit
barrier test. This fixture intentionally retains one physical SQLite connection;
its throughput does not measure a parallel backend pool. Client loopback is not
HTTP/server/network performance. Standard/Embedded four-case functional results
do not constitute portable performance or durable-storage measurements.

### SPEED25 latency and throughput acceptance

The RX25 completion judgement above is withdrawn: allocation reduction alone
does not satisfy execution-speed acceptance. The preserved numbers remain
historical measurements, not acceptance of the elapsed-time variation.

The SQL workload mode uses the same real SQLite engine, original security and
execution APIs, with independently selected physical input rows and worker count.
`ordered` exercises unique ordering, `ties` forces canonical fingerprint ties,
`computed` executes a nonmaterialized computed CTE projection, and `wide` adds
three lexical CTE levels and eight computed outputs. Output validation checks
all cells, row multiplicity and checksums; unordered SQL results do not assume
source order. The ordinary named/direct fixture remains available.

Before and after executables compile identical fixture bytes and URL pins with
the same release compiler. Sequential builds stop before measurement. The runner
alternates executable order per workload/size/concurrency block, preserves raw
per-call latency and batch throughput, and measures allocations separately from
uninstrumented timing. Sampling is a separate diagnostic run, never a timing
sample. Setup is excluded from query timing, and warmup precedes recorded rounds.
Process user+system CPU deltas are collected at the same observation boundaries;
they include the boundary observation overhead and separate CPU work from wall
time changes caused by competing host processes.

Acceptance requires lower paired representative execution latency and higher
throughput, with unchanged-result validation and no repeatable regression in
the tied-order or computed/wide workload. Record noisy or inconclusive cases;
do not dismiss adverse timing from a control alone. Allocation and requested-byte
results describe cumulative allocation traffic; retained heap and process peak
RSS remain separately reported and cannot establish a heap high-water bound.

Projection values/flags/leases, ORDER ordinals and optional fingerprints are
invocation-local exclusive buffers on Native, standard WASM and Embedded.
Their reservation and the unchanged request work meter use the same Mutex-backed
owners on every target. No new shared state, conditional Sendable conformance,
background task, authority source or cleanup path is introduced.

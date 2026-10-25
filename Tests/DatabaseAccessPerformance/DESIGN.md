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

### TAIL25 measurement contract

TAIL25 reopens the remaining cumulative-allocation and concurrent-p50 gaps.
Its measurement contract retains the same single-connection SQLite control.
That backend deliberately grants a FIFO native transaction lease through
terminal cleanup, so concurrent request latency includes queue position.
The framework does not remove or bypass this storage isolation boundary.

A cohort gate releases all measured requests from one monotonic origin. Raw
records retain request identity, dispatch time, query-ready time, consumption
completion and the original per-call duration. Reports preserve pooled p50/p95
and additionally compare cohort p50 and each completion rank; they never select
the favorable statistic or discard a recorded cohort. An identical-binary
calibration distinguishes pooled multilevel-distribution artifacts and host
noise from repeatable implementation regressions. Explicit file-backed WAL
connections are a separate parallel-capacity control, never a replacement for
the single-connection baseline. Generated files close after container shutdown.

Before/current/final executables use identical fixture bytes, pins, compiler,
physical rows, budgets and backend configuration. Allocation observations remain
separate from uninstrumented timing. Acceptance requires cumulative byte traffic
below both47df0b2a and a7bcf834 for computed/wide workloads, improved paired
small-CTE latency with its observed dispersion and completion-rank distributions
reported, and unchanged correctness/authority/budget/rollback/lifetime evidence.

### TAIL25 measured results, 2026-10-25

Framework source at `f0286841` was compared with `47df0b2a` (current baseline) and `a7bcf834` (pre-refactoring). All three executables use identical fixture/probe/header bytes, current URL pins and release Swift6.4.0 with SQLite/ScalarIndexes. These isolate Framework source changes; they are not historical whole-stack release comparisons. Each timing condition alternates AB/BA with9 measured cohorts per process after a warm cohort. Allocation uses five separate observed cohorts per condition. Raw pooled quantiles and every cohort remain in the evidence.

| Current baseline comparison | Rows / workers | p50 before → after ms | Initial p95 before → after ms | Cohort p50 MAD before → after ms | Throughput change | CPU/query change |
|---|---:|---:|---:|---:|---:|---:|
| computed | 100 / 4 | 10.198 → 9.829 | 16.474 → 15.953 | 0.197 → 0.160 | +4.07% | -2.45% |
| computed | 1000 / 4 | 96.989 → 93.025 | 155.393 → 147.819 | 1.123 → 0.859 | +5.37% | -4.40% |
| wide | 100 / 4 | 27.802 → 23.783 | 41.991 → 38.030 | 0.212 → 0.220 | +9.92% | -6.31% |
| wide | 1000 / 4 | 253.216 → 224.846 | 399.817 → 360.680 | 1.615 → 2.205 | +10.83% | -6.73% |
| computed | 1000 / 1 | 38.011 → 37.252 | 39.266 → 38.538 | 0.225 → 0.407 | +2.04% | -1.31% |
| wide | 1000 / 1 | 101.418 → 92.201 | 103.762 → 94.543 | 1.069 → 0.574 | +9.99% | -6.19% |
| computed | 10000 / 1 | 375.904 → 363.813 | 396.754 → 637.509 | 2.337 → 2.183 | +3.32% | -2.47% |
| ordered | 10000 / 1 | 308.523 → 307.226 | 318.518 → 315.337 | 2.401 → 3.313 | +0.42% | -0.41% |
| ties | 1000 / 1 | 39.267 → 37.174 | 39.746 → 38.220 | 0.427 → 0.272 | +5.63% | -5.20% |

The original two small concurrent computed-CTE conditions improve raw p50 by3.62%/4.09% and cohort MAD by19.01%/23.55%. All nine initial raw p50 values decrease, including the unchanged positional ORDER control whose0.42% difference is not evidence of a meaningful speed change. Dispersion does not decrease universally: the wide1000/four-worker condition has a larger MAD, while its p50 and p95 decrease. No claim of deterministic latency or universal tail improvement is made.

An identical-binary control for the two small conditions measures raw-p50 ratios1.0078/0.9872 and cohort-p50 ratios0.9852/0.9764. These are observed drift, not confidence intervals. Request origin/dispatch/ready/completion timestamps and completion rank remain in every uninstrumented record; the ready interval includes backend waiting plus execution and is not claimed as isolated storage wait.

Two initial p95 observations required one predeclared audit rather than favorable-sample selection. With18 single-worker observations, the empirical95th percentile is the maximum. The audit adds exactly three AB/BA cycles of nine cohorts to the10000-row memory condition and100-row/four-connection WAL condition, with identical-binary controls. Original and additional records are pooled in full:

| Pooled condition | Cohorts / responses per version | p50 before → after ms | p95 before → after ms | Maximum before → after ms |
|---|---:|---:|---:|---:|
| computed10000, memory/one connection | 72 / 72 | 384.870 → 364.938 | 414.054 → 383.353 | 541.994 → 637.509 |
| computed100, WAL/four connections | 72 / 288 | 7.438 → 7.064 | 8.341 → 8.377 | 10.777 → 13.195 |

The WAL small-condition pooled p95 is0.43% higher. Its identical-binary audit also varies substantially (p95 ratio0.7831); the large-memory identical-binary p95 ratio is1.0108. The initial637.509ms event remains recorded. This audit finds no reproducible code-driven tail regression and does not erase maximum latency or declare every p95 improved.

| Cumulative allocator API requested bytes/query | Rows / workers | Pre-refactor a7bcf834 | Before47df0b2a | After f0286841 |
|---|---:|---:|---:|---:|
| computed | 1000 / 1 | 82,864,823 | 86,396,883 | 79,819,883 |
| wide | 1000 / 1 | 113,800,033 | 162,044,079 | 109,428,079 |
| computed | 100 / 4 | 69,190,435 | 69,505,391 | 68,847,651 |
| computed | 1000 / 4 | 82,868,547 | 86,397,419 | 79,820,455 |
| wide | 100 / 4 | 72,747,673 | 77,573,367 | 72,311,803 |
| wide | 1000 / 4 | 113,803,797 | 162,044,675 | 109,428,615 |

All six byte-volume gates are below both references. For1000-row single-worker computed/wide queries the reduction from47df0b2a is7.61%/32.47%, and from a7bcf834 is3.67%/3.84%. These are cumulative successful allocator API requested bytes, including realloc, not live heap or RSS. Current-baseline retained-result snapshots stay equal at66,784/99,856 bytes. Observed process-cumulative peak RSS is27,131,904→27,213,824 and30,031,872→30,703,616 bytes; both are below the pre-refactor28,295,168/31,653,888. RSS includes startup, seeding and warmup and is not a per-query heap peak.

Attribution follows actual allocation stacks: full bound/evaluated payloads amplified all instruction capacity, and synchronous projected/numeric operand arrays repeated transport copies. A candidate projected batch owner additionally copied a192-byte instruction buffer on push. The final direct owner reduces that size-class count from128,052 to32,052 in the diagnostic wide workload; no extra copy remains beyond one admitted buffer per arithmetic root. The diagnostic histogram/stack library is separate from the original observer used for accepted allocation totals. The diagnostic allocation-size distribution separates the repeated small-buffer copies from infrequent large allocation classes; no async-frame ABI interpretation is required for the accepted byte-volume result.

Explicit file-backed WAL controls preserve the single-connection default. Final cohort p50 for one→four connections is10.646→7.081ms (computed100),94.619→59.362ms (computed1000), and224.601→132.887ms (wide1000). Queue position explains the single-connection staircase; the framework does not bypass transaction leases or change storage defaults. The initial small/four-connection before/after raw median is7.008→7.090ms, while cohort medians are7.071→7.081ms; these and the pooled audit remain visible rather than selecting a favorable run.

Native SQLite/AllRuntimeFeatures completed2650 tests (Engine1438, Database235, SQLite152, QueryAST825), with zero failures/skips/expected failures/runtime warnings. The2KiB arithmetic storage counterexample fails the payload-wide candidate and passes the final driver. Release Swift6.4.0 standard and Embedded WASM each execute four real groups on wasm32-unknown-wasip1/Node24.19.0, including nested CASE/extrema,100-row/eight-column projection, SQL/IR/block results, budget rollback, concurrent reads, sequence, reinitialization and close. Both builds have zero compiler warnings. Compiled source hashes, fixture bytes and URL pins match the committed implementation; no public invocation/result API, authority, meter, rollback or lifecycle contract changes.

[Raw evidence and measurement manifest](../../../.codex-tools/sql-scalar-tail-20261025/performance-verification.json) retains all original/audit samples, source/probe hashes, pins, compiler/SDK identities, result bundles, wasm artifacts and executable hashes. The owning public harnesses reproduce timing, cycles and explicit storage configurations. Native allocation/RSS observations make no WASM/Embedded memory-performance claim.

### SPEED25 measured results, 2026-10-25

Identical fixture sources, C probe/header and URL pins were compiled against
`a7bcf834`, `5ef46212` and the SPEED25 implementation with release Swift6.4.0.
The complete alternating comparison contains19 physical-input/workload/worker
conditions; three representative conditions also compare against `a7bcf834`.
Raw evidence and source hashes are retained in the workspace
`.codex-tools/sql-speedup-20261025/performance-verification.json` and its linked
`paired-full`, `paired-reference` and `paired-cte-recheck` directories.

| Warm Native SQLite workload, one worker | Before5ef p50 ms | SPEED25 p50 ms | Before / after queries per second |
|---|---:|---:|---:|
| Unique ORDER,10000 rows | 341.276 | 302.142 | 2.930 / 3.310 |
| Tied ORDER,10000 rows | 405.811 | 364.912 | 2.464 / 2.740 |
| Computed CTE,10000 rows | 420.854 | 384.016 | 2.376 / 2.604 |
| Three CTE levels / eight computed columns,1000 rows | 106.551 | 101.477 | 9.384 / 9.853 |

All19 conditions improve median batch throughput, process CPU per query and
individual-call p95. Individual-call p50 improves in17 of19 initial conditions.
Computed CTE at100/1000 rows with four workers initially increases p50 by5.6%/
0.7%; one targeted nine-round repetition improves both. Retain both recordings:
the pooled100-row individual-call p50 remains mixed across blocks, so universal
p50 improvement is not established. No further favorable-sample search is used.

Against `a7bcf834`, unique ORDER10000 rows improves364.493->301.911ms and
2.744->3.312queries/sec; computed CTE1000 rows improves44.611->38.661ms, and
the eight-column workload improves119.674->102.644ms. These are paired SQL-text
measurements, not comparisons against the earlier uncontrolled IR timing pair.

The four current-baseline allocation observations reduce allocator calls by
1.1%-8.2% and cumulative requested bytes by0.6%-3.8%, with equal retained heap.
Process peak RSS is separately recorded and is not a query-local heap maximum.
Compared with `a7bcf834`, cumulative requested bytes still increase4.26% in the
computed1000-row case and42.39% in the eight-column1000-row case, despite fewer
allocator calls, equal retained output heap and lower CPU/latency. This remaining
cost predates SPEED25; its cause was not separately profiled here. These results
do not claim every memory metric improves over the pre-refactoring implementation.
WASM/Embedded runtime regressions are separate functional evidence, not portable
performance measurements. No cold-start, HTTP-server or parallel SQLite-pool
speed claim is made.

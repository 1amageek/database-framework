# SQLWindows

## Purpose and Scope

Owns selected Q14/F13 partition, ordering, peer, frame and analytic execution.
The parent is [QueryExecution](../DESIGN.md). There are no children. This is
an internal Foundation-independent component; parent rows already passed
GROUP BY and HAVING. This document defines completion contracts, not evidence
that implementation has passed verification.

## Responsibilities and Boundaries

| Owner | Responsibility |
|---|---|
| Parent binder | Selected signatures, named-window resolution and copy rules, frame/type legality, clause/nesting restrictions |
| SQLWindows | Partition/order permutation, peers, ranking, offsets, frame membership/exclusion, frame-value selection |
| SQLAggregation | Ordinary aggregate semantics over the supplied frame |
| Parent relation evaluator | Post-group row contexts, selected expression evaluation, result caching and final projection |
| Existing scalar owners | Declared comparison/collation, numeric arithmetic, temporal interval arithmetic |

No runtime cell selects an overload. Source-order tie breaking determines
iteration only and never peer identity. Named-window resolution stays in the
binder. Aggregate DISTINCT/internal ORDER/ordered-set OVER are rejected there.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | CanonicalBoundWindow, ProjectedQueryValue, selected callback | Supplies post-group rows and selected declarations | Do not evaluate windows before grouping/HAVING |
| [SQLAggregation](../SQLAggregation/DESIGN.md) | depends on | SQLAggregateEvaluator over frame-local indices | Shared ordinary aggregate semantics | Binder rejects window-aggregate modifiers prohibited by PG18 |
| [SQLTemporal](../SQLTemporal/DESIGN.md) | depends on | Selected temporal comparison/arithmetic, operation environment | Calendar-correct temporal RANGE | No clock or timezone fallback |
| [DatabaseEngine](../../DESIGN.md) | ancestor | DatabaseWorkMeter, immutable retained leases | Request work and retained-memory authority | Same meter across every input/output claim |
| [Kit SQL](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | Q14/F13 semantic model | Window calls, frames and exclusions | Binding resolves syntax before execution |

## Architecture

```text
post-GROUP/HAVING row contexts -> selected partition/order evaluation
  -> admitted source-index permutation -> partition boundaries -> peer groups
  -> rank/offset or ROWS/RANGE/GROUPS frame + EXCLUDE
      -> selected first/last/nth child or shared SQLAggregateEvaluator
  -> admitted values indexed by original source order -> parent projection
```

## Contracts and Invariants

`SQLWindowEvaluator.evaluate(plan:rowCount:evaluate:direct:workMeter:temporalEnvironment:limits:)`
accepts a bound window, nonnegative row count, a nonescaping asynchronous
`(Int, CanonicalBoundExpression) -> DatabaseContext.ProjectedQueryValue` callback,
a nonescaping row-independent `direct(CanonicalBoundExpression)` callback,
the request meter/environment/value limits. `SQLWindowResults` retains output
cells plus an immutable payload lease. The callback sees original row indices
and the parent owns every row/group context through the call. No callback,
release authority or parent context escapes the returned owner.

ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST and NTILE use partition and
peer coordinates. LAG/LEAD evaluate offset and out-of-partition default in the
current row context; only the selected value/default is evaluated. Negative
LAG/LEAD offsets reverse direction; NULL offset returns NULL. NTH_VALUE and
NTILE require positive counts; NTILE initializes its count at the first non-NULL argument in partition order,
returning NULL until initialization, as PostgreSQL 18 does. NULL NTILE returns NULL and NULL NTH_VALUE
returns NULL. Frame offsets evaluate once via `direct`, including empty input. ROWS/GROUPS
reject NULL/negative values before iteration; RANGE rejects NULL before iteration
and negative offsets when its row-dependent predicate executes. These timing
rules are confirmed by the PostgreSQL 18.6 oracle.
FIRST/LAST/NTH_VALUE respect the admitted frame after exclusion. Empty frames
return NULL or the aggregate's own empty-input result.

Default frames use RANGE UNBOUNDED PRECEDING through CURRENT ROW, including
all peers; without ordering the entire partition is one peer group. ROWS uses
positions, GROUPS peer ordinals, RANGE selected numeric or temporal interval
thresholds. DESC reverses threshold direction; NULL current keys select their
NULL peers for offset boundaries. NO OTHERS, CURRENT ROW, GROUP and TIES apply
after bound membership. Collation and NULL order use selected scalar semantics.

## Runtime Flows

1. Evaluate row-independent frame offsets and enforce the preceding empty-input
   rules. Admit key/output/permutation/peer/frame capacities before allocating.
2. Evaluate each partition/order key once and retain its producer claims.
3. Sort indices with bounded iterative scratch, identify partitions and peers.
4. Evaluate each result, invoking SQLAggregateEvaluator with frame-local to
   original-row index mapping for aggregate OVER. For EXCLUDE NO OTHERS, cache
   input cells while the frame head stays fixed; reset the cache generation when
   the head moves. Any EXCLUDE restarts evaluation. FILTER is scheduled first,
   followed by argument ordinals, preserving duplicate/volatile occurrences
   without AST equality. This follows PostgreSQL nodeWindowAgg.c's observable
   restart behavior while sharing the same aggregate reduction kernel.
5. Retain output capacity/payload claims in one immutable owner; drop temporary
   keys and scratch claims before returning.

## State, Ownership, and Lifecycle

Partition/order/frame arrays are task-local and live within one evaluate call.
The aggregate callback cache owns its exact scheduled FILTER/argument slots and
row ordinals in one `Mutex<State>` on every target. A short lookup advances the
slot cursor, evaluation awaits outside the lock, then a short insertion publishes
the immutable cell. No I/O, callback or await occurs under the mutex. The cache
is scoped to one bound-window invocation and never escapes output; cached cells
retain independent immutable claims. There is no platform conditional, callback
retention, actor or unchecked Sendable owner. Immutable output cells retain referenced source
payload leases and their own destination reservation. Every imported lease is
checked against the identical work meter. Cancellation, deadline, work/memory
failure and callback errors propagate unchanged and release all temporary
claims. The parent owns source-row leases through window evaluation/projection.

## Failure, Concurrency, and Constraints

The numeric RANGE predicate uses Int128/widened decimal thresholds so SQL
integer limits do not overflow during range comparison. Time RANGE uses only
interval microseconds without midnight wrapping; interval/timestamp RANGE uses
selected temporal invariants and calendar arithmetic.

| Logical state | Native / WASM / Embedded storage | Isolation | Read / mutation | Release |
|---|---|---|---|---|
| Aggregate selected-input cache | `Mutex<State>` with admitted optional-cell/generation arrays | Same mutex on every target | `next` / `store` / `beginFrame` | Invocation owner; cells independently retain immutable claims |

Every comparison, sort, peer scan, frame scan and selected result charges the
request work meter and checks cancellation/deadline. All multiplication and
index arithmetic are checked or bounded by admitted array counts. Invalid
selected descriptors and counts produce typed expression errors. Memory
budgets include array capacity, retained key payloads, peer coordinates, frame
indices and output cells; no unbounded standard-library sort scratch is used.

## Verification and Change Impact

Focused evaluator tests own ranking/peer identity, unsorted source remapping,
partitions, default peers, ROWS/RANGE/GROUPS and every EXCLUDE, frame emptiness,
DESC/NULL handling, temporal and numeric thresholds, lazy current-row offsets
and defaults, negative/NULL analytic offsets, shared aggregate FILTER/frame
behavior, cancellation, small memory/work limits and release after success or
failure. Aggregate callback-counter tests distinguish fixed-head prefix reuse,
moving-head/exclusion restart and duplicate argument slots. Parent integration tests own post-group/HAVING ordering and public SQL
execution; binder tests own named/copy/invalid-frame pre-read rejection. A
selected aggregate-kernel change requires frame differential checks. A binder
frame/type change requires this component's semantic tests; parent context or
retention changes require parent integration tests. Native/WASM/Embedded use
the same immutable output and task-local algorithm; target evidence is scoped
to the actually executed lane.

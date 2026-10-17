# SQLAggregation

## Purpose and Scope

This QueryExecution child owns selected SQL F11/F12 aggregate transitions and
finalization. Parent: [QueryExecution](../DESIGN.md). There are no child designs.
Grouping sets, GROUPING bits, row visibility, window partitions and frames,
source access authority and result promotion remain with their respective owners.

## Responsibilities and Boundaries

`SQLAggregateEvaluator` consumes an immutable bound aggregate and scoped async
expression callbacks. It owns once-only FILTER, aggregate argument evaluation,
semantic DISTINCT, aggregate-local ordering, transition and finalization. The
caller owns the group/frame row sequence and maps relative row ordinals through
its own retained sequence. The evaluator never stores either callback, captures
session authority in output, or accesses storage.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | Selected expressions, identity/comparison, ProjectedQueryValue, work meter | Owns binding and integration dispatch | Binder must normalize SQL aggregates once to `.aggregate(.sql(call))`. |
| [SQLArray](../SQLArray/DESIGN.md) | depends on | SQLArrayStorage.makeArray and admitted buffers | Constructs rich/nested output arrays | Array-input aggregation adds rank and preserves inner dimensions/lower bounds. |
| [SQLJSON](../SQLJSON/DESIGN.md) | depends on | SQLJSONPlan and SQLJSONMemory | Uses existing lexical JSON and semantic JSONB conversion | JSON object duplicate keys preserve input order; JSONB is last-value-wins and canonical. |
| [SQLTemporal](../SQLTemporal/DESIGN.md) | depends on | Interval precision, arithmetic, comparison | Reuses interval semantics | Opposite infinities fail; NULL and infinite transitions stay explicit. |

## Architecture

```text
retained group/frame owner
  -> evaluate(row ordinal, selected expression) / direct(selected expression)
    -> SQLAggregateEvaluator
      -> FILTER -> once-only arguments -> semantic DISTINCT -> selected ORDER
        -> transition -> finalization
          -> ProjectedQueryValue(value + immutable payload leases)
```

## Contracts and Invariants

The entry is `SQLAggregateEvaluator.evaluate(bound:rowCount:evaluate:direct:
temporalEnvironment:workMeter:limits:) async throws ->
DatabaseContext.ProjectedQueryValue`. Callback result and evaluator result share
that existing immutable carrier. `rowCount` is nonnegative and callback ordinals
are exactly `0..<rowCount`. Callbacks are nonescaping and execute in the current
task. Bound aggregate metadata supplies `arguments`, `directArguments`, `orderBy`,
`filter` and `withinGroup`; the kernel does not infer selected signatures from
runtime values or rebind the AST. Ordered-set direct arguments are evaluated
once per invocation, including empty input. Direct arguments are not per-row
aggregate arguments.

| Family | Required behavior |
|---|---|
| COUNT | Empty is bigint zero; expression NULLs skipped; star counts admitted rows. |
| SUM/AVG | Existing selected numeric accumulator; interval components and infinity counts retain temporal semantics. |
| MIN/MAX | Selected comparison; return the selected input carrier with its leases. |
| STRING_AGG | text/bytea; NULL value skipped; each later value uses its own delimiter, NULL delimiter contributes nothing. |
| ARRAY_AGG | Scalar NULLs retained; array inputs reject NULL/empty/mismatched shape; rank grows once and original inner lower bounds survive. |
| Boolean/bit | NULLs ignored; all-NULL yields NULL; integer width remains selected. |
| JSON/B | NULL values retained; object NULL keys fail; existing conversion and duplicate-key policy are reused. |
| Variance/stddev | Numeric/integer transitions remain widened exact decimal; no Double numeric variance approximation; sample needs N>1, population N>0. |
| Pair statistics | Only pairs with both operands non-NULL count; PostgreSQL Y,X argument order; regression degeneracies produce documented NULL/one results. |
| Percentiles/mode | NULL ordered inputs skipped; selected order semantics; scalar and fraction-array percentiles, bounds preserved, NULL fraction elements preserved; mode ties use first selected ordering. |

Except COUNT/regr_count, empty or meaningless reductions yield SQL NULL. Every
output is normalized to its bound result type. Aliases variance/stddev use their
sample form. Rich values keep immutable payload owners live; output may retain
value/lease owners only, never callback/producer/context/session/authority.

## Runtime Flows

For every input row, checkpoint/yield, evaluate FILTER once, discard FALSE/NULL
without evaluating arguments, evaluate selected arguments and ordering keys once,
then retain the accepted carrier. DISTINCT compares all aggregate arguments using
selected NULL-safe identity. Order keys consume already selected expressions;
an order expression equal to an argument reuses that evaluated occurrence.
Throwing, metered stable merge ordering uses admitted scratch. Ordered-set input
is the WITHIN GROUP selected expression. Finalization consumes prepared carriers
and never reevaluates arguments. Window callers invoke this same entry with the
frame member mapping.

## State, Ownership, and Lifecycle

All mutable transition state and scratch is invocation-confined. Widened decimal
owners are immutable; numeric accumulator and collection builders own their own
admitted buffers. Prepared values retain the callback carrier's leases. Each
container admits full layout and requested growth before allocation and reconciles
actual capacity before publication. Input payload with valid immutable leases is
shared, not copied; an unretained callback payload receives its own metered claim
before retention. Output construction overlaps admitted input/scratch until its
own immutable lease is established. Scratch is released on success, throw and
cancellation. No new shared mutable state or target-specific isolation is added.

## Failure, Concurrency, and Constraints

Selected type, numeric overflow/domain, invalid aggregate argument, invalid array
shape, NULL JSON key, work/row/byte limits, deadline and cancellation failures are
explicit. Existing typed causes propagate to the parent's occurrence wrapper;
no empty/synthetic success or fallback is permitted. QueryValueLimits supplies
intrinsic output bounds; DatabaseWorkMeter owns runtime bounds. Loops consume work
and checkpoint; async outer loops yield in the same task. No detached tasks,
thread stack changes, optimizer workaround or target-conditional synchronization
is introduced. Stable engine stack remains 8 MiB.

## Verification and Change Impact

The focused test owner is
`Tests/DatabaseEngineTests/SQLAggregateEvaluatorTests.swift`. Test the actual entry
with controlled selected callbacks and decisive value/lease/work assertions.

| Invariant | Decisive evidence |
|---|---|
| Modifier schedule | Rejected FILTER avoids failing/volatile arguments; accepted arguments and selected ORDER occur once; DISTINCT precedes transition. |
| General reducers | Width/type/NULL/empty matrix, text/bytea delimiter changes, rich and multidimensional arrays, JSON duplicate keys and NULL-key error. |
| Numeric/statistical fidelity | Widened cancellation fixtures beyond Int128 intermediate range; exact numeric variance/stddev scale; regression degeneracies. |
| Ordered-set | Scalar/array fractions 0/1/interior/NULL/invalid, preserved shape and lower bounds, interval interpolation and mode tie order. |
| Ownership/resources | Allocation rejection before growth; exact retained claims; cancel/deadline/work failure releases scratch; result stays readable after input owner leaves. |
| Integration | Parent public SQL paths exercise grouping, FILTER/order, aggregate OVER frames, empty descriptors, occurrence errors and output promotion. |

Native, ordinary WASM and Embedded WASM evidence must name the exact toolchain,
SDK and executed backend. A host-only pass does not prove target behavior.
Changes to bound descriptors require binder/root/window coordination; scalar
numeric/array/JSON/temporal owner changes require their focused regressions.

Behavioral reference: [PostgreSQL 18 aggregate functions](https://www.postgresql.org/docs/18/functions-aggregate.html),
[numeric transition/finalization source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/numeric.c),
[ordered-set source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/orderedsetaggs.c).

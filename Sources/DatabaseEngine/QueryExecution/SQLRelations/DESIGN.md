# SQLRelations

## Purpose and Scope

This planned QueryExecution child owns demand-driven SELECT production and
nonrecursive CTE execution-local materialization and recursive working-frontier production. It is not implementation or
verification evidence. Parent: [QueryExecution](../DESIGN.md#s08-frozen-relational-execution).
Children: none. Its implementation belongs in this directory; no package,
module, generic visitor framework or storage abstraction is introduced.

## Responsibilities and Boundaries

SQLRelations owns producer position, SELECT invocation identity, independent
consumer offsets, retained CTE prefixes and awaited terminal cleanup. The
parent owns scopes/types/occurrences, selected expressions/keys, row presence,
comparison, binding failures and final result composition. Read owns session,
transaction, schema/authority lease and result readiness. Physical fetch may
remain blocking; SQL projection and nested CTE output still obey demand.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md#s08-frozen-relational-execution) | parent / used by | Bound SELECT/source plan, selected evaluator, retained positional rows and meter | Composes this lifecycle into public SQL. | No projection/volatile work may be hidden in eager cache initialization. |
| [Read](../../Read/DESIGN.md) | depends on | Scoped transaction, physical cursor registration and authoritative close | Supplies already admitted read capabilities. | Do not open a session/transaction or widen authority here. |
| [Kit Query](../../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage) | depends on | NamedSubquery, Materialization, complete query operands and pagination | Supplies immutable query meaning. | Bound definition identity and invocation lifetime are runtime responsibilities. |
| [Kit SQL](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership) | depends on | Typed positional values/descriptors | Retained outputs may contain composites and arrays. | Descriptor identity does not retain operational producers. |

## Architecture

```text
one SELECT execution owner
  -> bound CTE declarations in lexical order
       -> immutable visible earlier/inherited cache handles
       -> one selected body producer per materialized definition
       -> immutable retained prefix chunks
  -> main SELECT producer
       -> consumer A(offset) / consumer B(offset)
            -> cached row or one serialized producer advance
  -> await close all owned producers/caches -> return collecting rows
```

### Frozen internal integration seams

These are internal integration contracts, not additional public package APIs.

| Seam | Contract |
|---|---|
| `CanonicalBoundSelect` CTE declarations | Binder supplies ordered immutable definition identities, selected body plans/output descriptors, partial output aliases, materialization hint and volatility. Definition identity is distinct from its textual name. It retains no runtime cache or mutable cursor. |
| `CanonicalQueryEvaluationContext` | Rows passes the existing transaction/session/meter, frozen outer row and immutable lexical CTE handles. Child execution reuses them; a new SELECT invocation creates new local caches while references to inherited definitions retain their declaring invocation's handles. |
| `SQLRelationProducer: Sendable` | Internal protocol requirements are immutable `schema: CanonicalRelationSchema`, `next() async throws -> CanonicalRelation?`, and `close() async throws`. A successful next returns exactly one retained source row with matching schema/presence and full payload leases; nil means true terminal EOF. Every operation is a protocol requirement, including on Embedded existentials. |
| `makeSelectProducer` and its returned producer | Each call constructs one invocation's local definitions and main producer. Its immutable SQLCTEEnvironment supplies lookup handles; the returned producer owns local caches through its cleanup path. Draining or stopping awaits close before returning through Read. Reusing a compiled plan for another correlated row constructs a new producer and local caches. |
| `SQLCTECache` | Serialized async owner of one definition in one invocation: producer state, schema, retained chunks, actual EOF/failure and consumer positions. Reuses existing retained builders, shared owners and bounded views; it neither edits a shared Array nor snapshots a growing Array repeatedly. |
| CTE consumer | One producer facade with an independent zero-based offset. Cached rows are replayed without evaluation; an offset at the frontier requests new output from the same body producer. Consumer close removes that consumer, not the shared body. Owner close ends the body even if consumers stopped early. |

Binder/selected-scalar work and Rows/producer work meet at the parent's bound
seams: whole-row descriptor/field slots/presence address; selected field ordinal
and composite comparison mode; source-key versus target-occurrence key; selected
pagination expressions; and ordered bound CTE declarations. SQLRelations
consumes these rather than resolving names, reselecting types or evaluating an
unbound AST. Rows exposes existing single-row selected evaluation and blocking
operator adapters internally; it does not duplicate their scalar algorithms.

## Contracts and Invariants

- Cache identity is the pair of lexical bound definition and declaring SELECT
  execution instance. Names, equal query values, compiled plan identity alone
  and equal correlated row contents are insufficient keys. Local shadowing and
  earlier-sibling visibility remain the binder's sealed decision.
- Unused definitions bind but do not start a producer. Explicit MATERIALIZED
  and default multiply referenced/volatile definitions share a cache. A
  side-effect-free default single-reference definition may be inlined; explicit
  NOT MATERIALIZED may inline only a nonvolatile body. A volatile body retains
  shared once-per-produced-row behavior even with NOT MATERIALIZED. Built-in
  volatility is bound explicitly, including clock_timestamp; no runtime ambient
  registry decides it.
- A consumer that reaches LIMIT1 may stop after one row while another consumer
  later grows the same prefix. Only actual producer EOF, including its own
  query limit, sets cache EOF. Empty cached prefix is not EOF. Rows before the
  frontier are never rerun, skipped with a fresh OFFSET query or recomputed to
  discover more rows.
- Demand propagates through projection/filter, derived queries, CTE consumers
  and join output. Sort/group/set operations may consume required complete
  input, but materializing raw physical input is distinct from evaluating all
  output target expressions. Blocking adapters must not eagerly execute
  projections they can leave to the requested output rows.
- Qualified primitive equijoins preserve bounded candidate lookup. The existing
  JOIN producer may index a non-lateral physical right table (including alias
  wrappers whose bound leaf has no CTE plan) for inner/left/right/full joins.
  Every ON conjunct must be a qualified cross-input equality with identical
  admitted primitive scalar declarations and no selected coercion. Other
  sources, including CTE/derived/VALUES right inputs and selected rich keys,
  retain demand-driven nested probing; indexing cannot force their body tail.
  The eligible physical right input is read once, while left demand, output
  projection and matching remain incremental. NULL keys are excluded from
  buckets but remain available for right/full unmatched output. Candidate
  buckets retain original right order, full ON validation remains selected,
  and exact metered key comparison resolves digest collisions.
- Candidate entries retain their source row and selected-key owners. Entry
  row/capacity and bucket/link storage are admitted before allocation or
  mutation on the same meter; all index state belongs to the JOIN gate and
  is cleared at terminal detach before awaited cleanup. Presence, duplicate
  multiplicity, left-then-right output and unmatched-right source order use
  the existing merge and matched-right paths. The Server qualified equality
  handler/scaling regressions distinguish this path from all-pairs replay.
- Projection values referenced by an ORDER target reuse that exact bound
  target occurrence and its leases; unrelated projection occurrences remain
  independent. Consumer demand and cache replay preserve that evaluation count.
- Protocol next/close calls are serialized across suspension, including actor
  reentrancy. An actor uses an explicit in-flight advance/close gate; a mutex
  never spans await. A failed or closing producer cannot publish another row.
- Incremental, JOIN, deferred and blocking materialized pulls cooperatively yield before
  their owned advance/factory and before publishing
  the result, retaining the serial gate across both boundaries. Resumption
  checks the same meter, cancellation and deadline. It remains in the same
  task and admitted session: scheduling cannot reset position, clock scope,
  transaction authority or cleanup responsibility. This bounds synchronous
  async-continuation accumulation on the supported WASM runtime.
  Producer and CTE-cache cleanup likewise yield around recursive child close
  after detaching terminal state. Cleanup does not add cancellation checks: all owned children
  must still close before the same-task scope returns or reports a failure.

### Positional column transfer

A streaming SELECT whose every selected expression is a bound local column transfers
normalized cells directly into its retained source-row output. It still invokes
the selected evaluator, including occurrence failures,
work/cancellation/deadline checks and normalization. The source row retains the
payload owners; this path avoids the intermediate public QueryRow container and
per-row public schema/lease-vector construction. Computed, composite-field,
outer-frame, aggregate and volatile targets continue through the selected projection owner.
Scratch cell storage is admitted before allocation and output storage before
publication, on the same invocation meter.

An alias reframe may reuse the immutable positional value buffer and the target
schema's immutable layout when the input mapping is dense identity and the
visible ordinals cover the complete physical vector in order. Sparse presence,
USING-hidden slots, reordered slots and outer frames use the existing checked
mapping path. Both paths retain the original row-container payload lease and
produce the same target addresses, annotations and version. No producer gate,
cooperative yield, CTE cache, demand boundary or cleanup lifetime is removed.

The SQLite positional CTE regression covers reordered/duplicate columns, NULL
from an outer join, aliases, filtering/offset and rich payloads after close.
Existing S08 producer tests remain the replay, failure, cancellation, admission
and concurrent-frontier witnesses. Native and portable behavior plus matched
optimized nested-depth/RSS measurements are required before task completion.

## Runtime Flows

```text
consumer next(offset)
  -> validate owner/meter/cancel/deadline
  -> offset < prefix.count: retain cached row; offset += 1
  -> offset == prefix.count and open:
       serialize one next -> admit row/chunk -> append -> advance consumer
  -> actual EOF: mark terminal; close producer; return nil
  -> failure: terminalize; await cleanup; propagate original + cleanup cause

SELECT success / early stop / failure / cancellation
  -> close main producer and local CTE producers, including suspended prefixes
  -> retain only demanded output value/schema/payload claims
  -> existing session invalidation and transaction closure
  -> existing Collecting-to-Ready admission
```

Owned pull advances and publication use the cooperative scheduling boundary
above. Native concurrency/cancellation witnesses and the exact default-stack
ordinary/Embedded runtime execute this path; a larger diagnostic VM stack is
not completion evidence.

Advancing clock/counting producer witnesses distinguish cache replay from
reexecution. MATERIALIZED with an outer LIMIT1 over a three-row body executes
one volatile output occurrence, not three followed by a one-row window.

## State, Ownership, and Lifecycle

| State/resource | Owner | Terminal requirement |
|---|---|---|
| notStarted / open / EOF / failed / closed | Definition cache / producer | Only one transition publishes each new prefix row; repeated close awaits the same cleanup. |
| Bound schema/metadata | Existing admitted plan and shared schema leases | Remains available with zero rows and after producer close. |
| Prefix chunks / cell payloads | Immutable shared retained owners on the exact parent meter | Cache and returned rows retain the claims they keep alive; final release frees them. |
| Consumer offset | Its consumer facade | Does not advance another reader or decide shared EOF. |
| Physical cursor | Existing Read-scoped KeyValueCursor | Await finish on early stop/error; Read's registry remains the final physical cleanup backstop. |
| Outer frame and inherited handles | Selected invocation environment | Retain values/leases through actual use without a second authority. |

Construct visible CTE environments in declaration order. A body's retained
factory/environment includes only the earlier/inherited handles admitted for
that body, never its own cache or the execution owner's aggregate registry.
Producer ownership and nonrecursive dependency handles therefore form an
acyclic graph. Completed output never retains cache/producer/session objects.
Partial initialization and close failure still release unobservable owners.

## Failure, Concurrency, and Constraints

Charge cache entry/metadata, consumer state, retained chunk/container growth,
all referenced payload owners and pending advance scratch before allocation.
Use DatabaseRetainedArrayBuilder admission and immutable shared transfer;
unique retained buffers may resumeBuilding only while unshared. Reconcile
actual capacity before publication. Check work/deadline/cancel at advances and
long blocking loops. Failed append never increments prefix count or a consumer
offset. Failed body/close does not become EOF, NULL or empty successful rows.

Existing KeyValueCursor.next/finish and Read registration provide physical
pull and awaited close. ItemStorage.consumeRetainedScan provides scoped
blocking decode; its ItemScanSequence has no public awaited finish and is not
an unchanged lifecycle solution. retainedCanonicalPage and QueryCursor are
completed-window / offset-rerun APIs, not suspended SQL producers. Do not
expand StorageKit to compensate for this missing SQL-layer producer. Adapt
existing admitted table fetch as blocking input first, with selected projection
performed on demand. Preserve authoritative secondary cleanup failures through
the parent operation's typed cleanup contract.

## Verification and Change Impact

The existing Engine formal `expression-values-aliases` group also executes a
physical qualified self-equijoin at 50 and 100 rows under one meter. Its 100k
work guard, less-than-3x scaling, typed work failure, zero final retention and
same-meter follow-up complement the Server handler/scaling regressions. The
existing producer/relation suites retain CTE-right demand, outer presence,
NULL keys, multiplicity, cancellation and awaited cleanup witnesses.

[Parent S08-E06/E07](../DESIGN.md#occurrences-resources-and-completion-evidence)
own the fixed completion IDs. DatabaseEngineTests/DatabaseTests execute unused
invalid/unused volatile CTEs; shadowing and partial/excess aliases; three-row
volatile default/MATERIALIZED/NOT MATERIALIZED sharing; outer LIMIT1 exactly
one call; two consumers stopping/resuming at different prefix positions; empty
and producer-owned-limit EOF; correlated repeated SELECT invocations; and
projection-alias versus independent-occurrence counts. A consumer LIMIT1 then
another needing three rows must obtain all three with exactly three body
calls. Exact/one-past row/byte/work claims, failure/cancel during frontier
growth, close racing advance, cleanup error, retained row after close and final
owner release are mandatory. Existing focused relational tests remain
regressions; declarations, cache-only tests and compile-only portable results
are not the demand/evaluation proof. Changes to the producer contract require
rechecking parent binding/evaluation and Read's closure ordering; physical
backend behavior stays within its existing owner.


## S10 Recursive Working Frontier

The [parent S10](../DESIGN.md#s10-frozen-recursive-and-function-execution)
consumes the [SQL C03/C04/C10 contract](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s10-frozen-recursion-and-table-functions).
The following selected internal seam is shared with the parent Rows binder.
It is planned behavior until the corresponding behavioral evidence exists.

| Seam | Selected contract |
|---|---|
| `SQLRecursiveRowProducer` | Receives final output schema, user target count, UNION quantifier, optional generated-column specifications, temporal environment, value limits, the existing meter, lazy seed factory and a step factory accepting `SQLRecursiveFrontier`. It emits exactly one matching retained row per pull. |
| `SQLRecursiveFrontier` | Immutable private retained row owner plus final self schema and meter. `consumer()` returns an independent cursor capturing only this frontier. It cannot reach the recursive producer, CTE cache, bound plan or session. |
| `SQLRecursiveGeneratedColumns.Search` | Selected user key ordinals, anonymous key-record descriptor, and breadth/depth mode. Binder supplies final generated output types. |
| `SQLRecursiveGeneratedColumns.Cycle` | Selected user key ordinals, anonymous key-record descriptor, selected comparable mark type and cast cycle/default constants. |
| Seed / term child output | Seed emits exactly the user targets. Term emits user targets followed by hidden parent SEARCH value when selected, then hidden parent CYCLE path when selected. Rows binds those hidden source slots before runtime and preserves the self source alias through joins. |
| Generated publication | Reuses the already evaluated user key slots. Appends SEARCH then CYCLE mark/path and discards hidden parent cells. The output schema contains only user and generated columns. |

```text
CTE cache -> recursive actor + serial gate
  -> lazy seed child -> generated row -> UNION visited -> accepted output
                                                    -> expandable next frontier
  -> seed EOF -> await close seed -> immutable frontier
  -> lazy step child(frontier consumer only)
       -> selected user targets + selected hidden parent metadata
       -> generated row -> UNION visited -> accepted output + next frontier
  -> step EOF -> await close step -> replace frontier -> next iteration
  -> empty frontier / early close / failure -> await child close -> release owners
```

Each iteration consumes the common work meter before child construction. Every
candidate, selected identity comparison, key/path membership and generated
construction also consumes that meter; there is no independent iteration cap.
UNION identity includes every final output slot, including generated values,
and uses the selected SQL equality/hash contract with NULL-safe row identity.
UNION ALL preserves multiplicity. The existing CTE cache remains the sole
multiple-consumer prefix owner, including early-LIMIT stop/resume behavior.

Depth SEARCH appends a selected key record to its parent's record array.
Breadth SEARCH emits an anonymous record containing bigint depth followed by
selected key cells, starting at zero. Anonymous fields are `f1` onward. CYCLE
appends the selected key record, uses NULL-safe record identity for membership,
and emits both repeated-key and ordinary rows. Only rows for which selected
SQL `mark <> cycleValue` evaluates true enter the expansion frontier; NULL or
equal default/cycle marks may therefore stop after the seed. Generated columns
remain visible to the recursive term through the immutable self schema.

Generated values use the existing flat selected value construction owner.
Copying the complete parent path into each independent output is required by
the existing flat immutable SQL value representation; it is admitted and
metered before construction, rather than represented by an operational parent
backpointer. Frontier retained-array capacity/row claims, visited identities,
path cells, generated descriptors and output claims are admitted before state
mutation/publication. Failed growth cannot publish the candidate.

| Logical state | Native / WASM / Embedded storage | Isolation and entry points | Release |
|---|---|---|---|
| phase / child / failure | Same recursive actor stored properties | Same serial gate around next/close across suspension | Detach terminal child, await close, then release owners |
| pending frontier / visited identity | Same actor-confined retained builder and SQL identity set | Mutated only while the gate is held | Clear at EOF/close/failure |
| current frontier | Same immutable retained owner | Independent frontier-only consumer actor | Child close precedes final frontier release |
| generated descriptors/constants | Same immutable selected configuration | Synchronous selected adapter inside gated advance | Output retains values/descriptors/claims only |

No target conditional removes synchronization, Sendable or owner retention.
Cleanup runs despite cancellation and awaits every owned child before releasing
frontier/visited/generated state. Secondary cleanup failures preserve the
original operation error. Cooperative yields around owned pulls and child close
retain the existing formal-target stack contract.

Lower `DatabaseEngineTests/S10RecursiveProducerTests` owns iteration order,
UNION/ALL, repeated NULL cycle keys, default/custom/NULL mark admission,
SEARCH metadata, multiple consumers, early stop, child-close failure,
cancellation and row/byte/work failure with zero final retention and same-meter
follow-up. Public recursive SQL binding, authorization, volatile target counts,
JOIN provenance and complete function composition remain parent test owners.


S11 source identity follows [QueryExecution's exact logical identity contract](../DESIGN.md#s11-exact-logical-source-identity). SQLCTEEnvironment consumer lookup and immutable replacement compare UTF-8 bytes, so two normalized-equivalent CTE names keep separate caches and factories. Producer state, lifetime and synchronization remain unchanged.

## S14 statement-owned returned relations

[QueryExecution's statement read scope](../DESIGN.md#s14-statement-read-and-mutation-projection-scope)
owns the prestatement source registry and private lexical output declarations.
SQLRelations consumes their immutable retained frames through the existing
producer protocol. A RETURNING reference receives an independent consumer offset
over the same completed relation; it cannot invoke the mutation again.

```text
SQLBlock eagerly completes one writable definition
    -> one retained RETURNING frame
        -> consumer A(offset) / consumer B(offset)
            -> existing selected join/filter/projection
```

Writable definitions execute exactly once even when unreferenced or consumed with
LIMIT 0. Their effect execution is SQLBlock's responsibility and never a CTE
producer factory. This exception does not alter ordinary SELECT CTE behavior:
unused SELECT definitions remain unstarted and requested prefixes remain lazy.
The existing mutable `SQLCTECache` is still actor/gate protected identically on
all targets. Completed returned relations need only immutable retained backing
plus the existing independently isolated consumer cursor; no mutable Array or
target-specific synchronization branch is introduced.

Base-source pulls in a mutation-bearing statement use the captured prestatement
frame. RETURNING pulls use their declaration's changed-image frame. Closing a
consumer never discards a sibling's frame, and all producer cleanup completes
before the statement's read session closes. Typed failure for an unreadable
non-RETURNING definition is distinct from an empty returned relation.

## Producer construction resumption boundary

Selected producer construction enters and resumes through cooperative same-task
boundaries, before any pull begins. Ordinary WASM can synchronously resume both
sides of an async call; a nested derived source otherwise accumulates the
`makeSelectProducer -> makeSourceProducer -> makeSelectProducer` construction
continuations despite bounded query depth and the separate pull boundaries.

```text
SELECT construction -> yield + request checkpoint
    -> construct child source in the same session/task
        -> yield + request checkpoint
            -> compose selected producer and transfer child ownership
```

The child is retained immediately after construction. Cancellation, deadline or
budget rejection at the resumption checkpoint closes that child before closing
its CTE caches; cleanup failure preserves the original error. Construction does
not start a new task/session, alter output demand, increase the VM stack, or
change query/fixture limits. The existing 30-level S08 derived-source fixture on
the ordinary/Embedded default-stack runtime is the behavioral regression owner.

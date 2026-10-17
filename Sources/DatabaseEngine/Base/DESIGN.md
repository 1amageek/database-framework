# Base Composition Execution

## Purpose and Scope

This component owns MultiBase-only Base lifecycle and read-only Composition
execution. Its parent is [DatabaseEngine](../DESIGN.md); it has no children.
The S01 change binds and carries positional query schemas across existing
member sessions, global merge, aggregate, DISTINCT and final materialization.

## Responsibilities and Boundaries

Composition owns member lease and federated snapshot coordination, global
row ordering, provenance and collecting-to-ready completion. QueryExecution
owns expression typing; Read owns session admission. GraphIndex owns RDF
identity qualification. Composition creates no second authority or transaction
to discover a member schema.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | MultiBase execution and lifetime | Provides schema and Base leases. | Default builds do not enable MultiBase. |
| [Read](../Read/DESIGN.md) | depends on | Admitted session and retained page | Binds and reads within the captured member transaction. | A page must retain the bound columns on zero rows. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Bound output columns, checked value normalization and canonical direct table page retention | Supplies static output signatures and the lower page-retention guarantee. | Schema cannot depend on a member's first row; preserve all admitted source and binding-frame owners. |
| [Query](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage) | depends on | Canonical quantifier, binary query operands, pagination and collation declarations | Supplies one immutable query grammar for every member and outer operation. | Wire carriage does not prove SQL execution or authorize a member read. |
| [SQL values](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership) | depends on | Composite descriptors, positional fields and complete-owner footprints | Supplies result/parameter values consumed by selected SQL comparison. | Representation equality is not SQL equality; S08 does not admit composite persistence. |
| [GraphIndex](../../GraphIndex/DESIGN.md) | used by | Feature-owned member execution and identity qualification | Uses the shared merger for SPARQL. | RDF columns retain primitive term semantics. |

## Architecture

```text
Composition lease + operation schema lease
    -> captured domain transactions + authorized member sessions
        -> bind every member output and require matching columns
            -> positional member pages
                -> merge / aggregate / distinct + provenance
                    -> bounded retained ready events
                        -> close every snapshot transaction and lease
                            -> began(columns) / row events
                                -> host persistence or CompositionQueryResponse
```

## Contracts and Invariants

Before provisioning a foreign-domain root or preparing a move to one, the
control transaction reads the durable SQL catalog without snapshot semantics.
If an SQL writer floor has been published and the current catalog owns
primary keys, constraints, or stored generated/identity obligations, the operation rejects `StorageError.unsupportedOperation` for the unavailable atomic transaction-domain capability before
creating lifecycle records or data-root side effects. [Schema](../Schema/DESIGN.md)
owns the integrity classification and writer-generation floor. This read
conflicts with concurrent catalog publication; removing all current obligations
allows foreign-domain placement again. No control key is read through a foreign
data transaction.


- Every member output is bound before the first row fetch or began event.
  Column order, type, nullability and labels must agree across members.
- Every continuation page must match that schema, including empty pages.
- Relational request parameters retain their references, payloads and explicit
  declarations until QueryExecution binds the complete query in an existing
  admitted member session. Its resolved immutable declarations feed every
  member schema/page, aggregate operand and cross-Base JOIN binding; splitting
  the query cannot restart contextual parameter inference. Feature-owned
  member executors retain their existing parameter contract.
- Output labels may duplicate. Slots, aggregate operands, identity and row
  alignment remain positional; dictionary projection is an explicit adapter.
- CompositionQueryMetadata carries columns. CompositionQueryResponse retains
  those columns with zero or more provenance-bearing rows, and its public
  initializer validates every row against the complete schema.
- Member schemas are bound within the existing admitted read sessions. Domain
  transactions, authorization, cancellation and lease release remain unchanged.
- Prepared rows, identity storage, aggregate destinations and the complete
  pending event buffer are admitted with the exact request meter before retention.
- External event callbacks run only after the snapshot vault has drained, every
  Domain transaction is terminal and member leases have finished (SPEC F-26).
  A producer close failure or cancellation publishes no metadata or row events.

## Runtime Flows

Source projection and local aggregate-operand projection each bind before
their respective fetches. Global aggregate results normalize to the bound
output type before positional row construction. SQL expressions use SQL type
binding; SPARQL output uses GraphIndex's term and aggregate type contract.
Parameter preflight uses the snapshot's captured member and exact request
meter before any row fetch. It opens no additional authority or transaction.
The Server adapter passes raw request parameters into planner options.

## State, Ownership, and Lifecycle

The snapshot owns member leases and domain transactions. Metadata owns one
immutable column array shared by cursor expectations and emitted output.
Cursors retain member pages until consumed; prepared row owners and DISTINCT
state retain their claims until merge completion or failure cleanup. The planner retains a bounded complete result before leaving the snapshot.
Only after authoritative close does it call an external consumer. Consumer
backpressure stops publication; it cannot interleave host control writes with
producer Domain transactions. Buffer capacity and metadata/provenance storage
have request claims; row payloads retain their existing producer claims once.
Consumer page size bounds delivery batches, not the complete Ready result.
Every pending output keeps its prepared-row and producer-page claims until
publication finishes; an insufficient request budget fails before any callback.
Aggregate page sizing consumes QueryExecution's canonical direct table page
retention guarantee: source, bound-addressing and projected lookahead frames
may coexist. The member page needs up to `3 * (pageSize + 1)` retained rows;
Composition adds one prepared row, so the admitted overlap is
`3 * (pageSize + 1) + 1`. Its minimum is seven rows, and the local page size is
`(maximumIntermediateRows - 1) / 3 - 1`. A maximum below seven fails with
`maximumIntermediateRows` at `aggregateInput` before any member page executes.
The planner preserves producer claims through prepared-row admission. Additional
operator-specific claims remain subject to the same request budget.
DISTINCT result aliases retain row and workspace provenance claims after
workspace removal until the last returned result owner is released. An opaque
workspace payload lease follows every derived DISTINCT origin into Ready
storage; Ready does not charge that same contributor backing array again.
Cross-Base JOIN owns its newly selected two-member contributor array, admits
its capacity before construction and propagates a distinct opaque payload
lease with each row event. It does not borrow the entire Composition member
array when the JOIN selects only a subset.
Final caller-owned response construction is the explicit materialization boundary.
Every production CompositionQueryRow carries an opaque producer payload lease.
Prepared member rows retain the member page's producer leases; preparation
admits destination structure and newly qualified RDF blank-node text only.
A retained event consumer admits its row and lease-handle storage before
retaining the event, and keeps that lease until final output transfer. Manually
constructed event rows have no producer lease and require full payload admission.

## Failure, Concurrency, and Constraints

Schema mismatch, row arity/type/nullability failure and unsupported operations
remain typed errors. The existing actor/mutex isolation and bounded merge
limits apply unchanged on all supported targets. No lock covers I/O or await.

## Verification and Change Impact

[CompositionQueryExecutorTests](../../../Tests/DatabaseEngineTests/CompositionQueryExecutorTests.swift)
owns aggregate, ordering, DISTINCT, zero-row schema, duplicate labels and
cross-Base behavior. Its publication-lifetime regression requires both Base
lease counts to be zero in metadata and row callbacks, positive retained claims
during delivery, and complete cleanup after consumer cancellation. Budget
zero/one-byte-short cases reject the first Ready buffer admission at result
materialization; invalid aggregate operands fail static type binding before
member fetching. Retained aggregate probe aliases keep their claims until all
probe and local copies are explicitly released.
Parameter regressions cover positional/named identity across paged members,
typed NULL projection, aggregate operands, cross-Base JOIN expressions and
whole-query ambiguity rejection before any event is published.
Server durable-paging fixtures must cover both rejection below complete-result
retention and successful bounded publication with the Server's page destinations
inside the same request budget. SQL aggregate assertions consume their bound
column types and values, including numeric SUM output.
[CompositionSPARQLAuthorizationTests](../../../Tests/GraphIndexTests/CompositionSPARQLAuthorizationTests.swift)
owns RDF primitive values, authorization and provenance. Changes to output
columns require member executors, retained page consumers, model decoding and
external operation adapters to use the same schema. Runtime backend evidence
must exercise MultiBase; a default-trait compile is insufficient.

### S04 selected SQL aggregate composition

[S04 QueryExecution](../QueryExecution/DESIGN.md#s04-frozen-selected-value-execution)
owns numeric overloads, displayScale and widened accumulator behavior. Base
consumes its QueryValue input/result and selected input type directly in
CompositionQueryPlanner. SUM/AVG must not call comparablePrimitive before
accumulation or rebuild displayScale after finalization. The same operation
meter pre-admits dynamic limb/math capacity; the existing fixed state claim
covers only fixed state. Clear/failure/cancel/final-output paths release the
accumulator exactly at its actual lifetime. S04-C09/C10/C12 require MultiBase
widened AVG/cancellation SUM, scale retention, budget failure and release.


### S06 Composition temporal inheritance

CompositionDataSource owns one container. Each public execution captures one
[Core-owned temporal environment](../Core/DESIGN.md#s06-clock-and-configuration-ownership)
before lease/read acquisition. Its snapshot, member sessions, join/aggregate
ReadExecutionContext copies and new transaction bindings retain the same
transaction/statement instants. CompositionDecisionTransaction relays its
writer's logical transaction instant into member read bindings. Member
selection never re-samples transaction time or chooses another zone provider.
Existing lease, authorization, domain, row/byte budget and closure guarantees
remain unchanged. An advancing clock across members and a derived read
counterexample verify the added immutable relay; portable MultiBase remains
part of the agreed S18 integrated lane.

S06's global Composition merger retains its primitive/numeric capability.
Its rich temporal ORDER BY and MIN/MAX branches currently fail explicitly;
member-local selected SQL uses the full S06 temporal owner. The S08 contract
below replaces global sorting's primitive-only branch after behavioral proof.
The S09 contract below replaces the retained rich MIN/MAX transition after
its focused behavioral and lifetime evidence is complete.

### S08 selected relational merge contract

This is the planned Base integration contract, not evidence that the current
merger implements it. QueryExecution owns SQL binding, coercion, equality and
ordering. Base owns composition of admitted member results, provenance,
global operator placement and the existing Collecting-to-Ready boundary.
The canonical Query declarations and SQL value contract linked above remain
the single authorities for their shapes and intrinsic invariants.

Confirmed migration inputs are
[CompositionQueryPlanner](CompositionQueryPlanner.swift)'s output-name lookup,
`comparablePrimitive`, `MergeOrdering` and fixed-count `EmissionWindow`, and
[CompositionDistinctWorkspace](CompositionDistinctWorkspace.swift)'s raw
QueryValue hash/equality. Those paths cannot establish selected SQL CHAR,
temporal, numeric, array or composite semantics. Existing raw-value behavior
remains valid only under its own language-qualified contract.

```text
canonical complete query + resolved parameters + one temporal environment
    -> existing snapshot and each authorized member session
        -> QueryExecution binds output, selected keys and operator semantics
            -> compatible retained member rows and typed key tuples
                -> global order / selected identity / set result / window
                    -> complete admitted Ready buffer
                        -> authoritative snapshot close -> publication
```

#### Binding and operator interfaces

| Boundary | Required input and guarantee |
|---|---|
| Member binding | Bind the complete query and parameters in admitted sessions before fetching its source rows. Require compatible output descriptors and selected ordering/identity semantics across all members, including empty members. The plan retains the admitted schema generation and exact operation meter. |
| Selected keys | Base forwards the complete query and retained source relation. QueryExecution resolves and consumes SQL alias/ordinal/expression keys, their source occurrences, selected types, NULL placement and collation. Projected-name lookup cannot reconstruct non-output keys. Alias/ordinal references consume the selected output value under the lower evaluation contract. |
| Member production | Preserve source layout, positional fields, outer-join presence and producer leases under the same meter. QueryExecution retains evaluated key tuples under the bound contract once per required row; comparisons do not re-evaluate expressions. Output metadata excludes internal ordering and identity keys. |
| Comparison and identity | Delegate SQL temporal, CHAR, numeric, array, composite and C/POSIX collation semantics to QueryExecution's selected SQL owner. Carry the operation's immutable temporal environment. Do not use Kit representation equality, raw FieldValue conversion or serialized bytes as SQL comparison. |
| Language qualification | Select SQL versus feature-owned/RDF comparison and identity with the admitted query plan. GraphIndex retains RDF term qualification, SPARQL order/unbound/error semantics and its existing raw primitive contract. The shared merger cannot infer language from a value's carrier or install global comparison state. |
| Global window | QueryExecution supplies the checked, evaluated pagination meaning. Base applies the global offset, row count and optional final peer extension after the required global quantifier/set/order operations. Consumer page size remains delivery batching rather than SQL LIMIT. |

SQL ORDER defaults are ASC NULLS LAST and DESC NULLS FIRST; an explicit NULLS
FIRST/LAST is final placement and is not reversed again. COLLATE consumes only
the bound C/POSIX capability. Unsupported or conflicting collation fails in
SQL binding even with no rows. The lower owner determines comparison
admissibility, composite NULL semantics and coercion before a member fetch;
Base does not redefine those rules. All-null present composite values remain
distinct from absent/typed-NULL values throughout retained rows and keys.

ORDER BY may use a bound expression, output alias or one-based ordinal under
the SQL resolution rules. A local ordered stream and the heap must use the
same selected key comparison. Fingerprint, Base identity and sequence may
provide a deterministic total tie-break only after all SQL keys compare equal.
WITH TIES peer membership compares only the complete SQL key tuple; that
deterministic tie-break is excluded. Offset is consumed first, the count then
selects rows, and the final selected peer group may extend the result. Zero
count returns no rows; WITH TIES requires ORDER BY even for an empty result.

SELECT ALL preserves the implicit bag union of authorized member inputs.
DISTINCT delegates its projected-row identity to the selected owner and
retains the existing contributor/provenance ownership. DISTINCT ON consumes
bound selected key identity and the SQL-validated ORDER prefix; the first row
in global SQL order is the representative. It is not a per-member DISTINCT
followed by concatenation. DISTINCT's ORDER restrictions and DISTINCT ON's
type/coercion/ordering restrictions are checked before fetching, rather than
disappearing on empty members. Identity state retains the selected value/key
owners and their claims until removal or retained-output transfer.

Binary set operands remain complete queries with their local ORDER/pagination,
quantifier and CTE scope. QueryExecution owns positional common-type coercion,
NULL equality and UNION/INTERSECT/EXCEPT ALL/DISTINCT multiplicity; Base supplies
only separately admitted member inputs and retained provenance-bearing rows.
The outer set envelope's ORDER and pagination apply to the complete set result.
An implicit union of member inputs does not justify independently executing a
set operator per member: operator placement must preserve the complete query's
coercion, multiplicity and local-versus-outer modifier meaning. A plan lacking
that proof fails explicitly before source execution instead of returning a
partial or member-local substitute.

S08 does not expand Composition's admitted source families. Its existing
Base-local table/JOIN and explicitly Base-qualified INNER JOIN boundaries
remain the admission scope. Derived, VALUES, CTE and set-source Composition
queries retain typed rejection before member source reads; preserving those
complete query declarations does not grant execution capability. Their
callable rejection branches retain incomplete markers until a separately
owned global placement and lineage implementation is verified.

Pagination expressions use QueryExecution's SQL signed-bigint coercion and
failure contract. Resolve types and parameter identity for the complete query;
evaluate each admitted count expression once per query invocation in its
correct correlation scope, not once per member, page or heap comparison.
Nonsubquery scalar count work uses the same snapshot, authority and meter.
Composition scalar count subqueries have no established global source scope;
their bound capability check rejects them before member source fetch. Their
QueryIR is preserved rather than rewritten to a first-member query. Source-row
references, aggregate/window use,
negative counts, overflow and fixed SQL counts above Int64.max fail explicitly.
NULL OFFSET means zero; absent/ALL/NULL LIMIT means no logical count bound.
The declaration distinctions remain intact. Non-SQL fixed UInt64 pagination
and feature language rejection retain their own contracts.

#### Retention, pushdown and closure

The admitted operation schema generation remains stable through source and
global work. DatabaseRetainedQueryColumns carries its metadata and binding
claims through destination adoption without retaining a session or executable
tree. Each source plan owns its source-execution lifetime; the global selected
plan retains its descriptors, expressions and evaluated keys through global
processing. A prepared heap head
retains its row, key payloads, descriptor owners and producer page claims.
Composite field and array-element views retain their complete value owner;
descriptor views retain their independent type owner. Base uses the lower
owner's measured footprints and attaches claims before exposing those views;
subtree size does not replace complete-owner retention. No composite value is
adapted through model persistence to make it comparable or publishable.

Admit key storage, heap replacement, selected identity/count tables, peer
lookahead, retained set operands and Ready output before their owned growth.
The same request meter charges key evaluation, comparison, collision checks,
coercion and peer detection, including cancellation/deadline checks. A large
WITH TIES group can exceed the request budget and must fail with its typed
resource error; LIMIT does not authorize unbounded peers. Existing page-size
overlap calculations remain lower-bound admission checks, not an exemption
for additional key, operator or complete-result retention.

Member ORDER, quantifier, set and pagination pushdown requires proof of the
same selected type/coercion, collation, NULL order, key evaluation, equality,
multiplicity, DISTINCT ON representative and WITH TIES peer/window meaning.
Local LIMIT/OFFSET generally cannot replace the global window. If equivalence
is unavailable, execute the canonical global operator with admitted retained
inputs; unavailable runtime capability fails explicitly. Do not silently use
primitive ordering or truncate members to satisfy a smaller budget.

Success, failure and cancellation preserve the existing snapshot drain and
lease order. No metadata/row callback occurs until every producing Domain
transaction and member lease closes. Partial keys, set state and peer buffers
release on failure; retained output aliases keep their producer/provenance
claims through final publication or final owner release. Consumer cancellation
releases the pending complete Ready buffer without reopening member sessions.
The feature executor's Sendable/isolation contract remains shared across native,
ordinary WASM and Embedded; language selection adds no mutable global registry.

The selected implementation retains each admitted member's original source
relation under its existing field authority, annotates it with Base-owned lineage,
and invokes QueryExecution's global pipeline through an admitted member session. Canonical duplicate
identity alone invokes the lineage merge callback. This preserves hidden ORDER
expressions and parameter evaluation without a second scalar interpreter. The
annotation is removed before publication; its admitted payload owner follows
every derived contributor list. Member-local joins retain their scoped relation
and source-presence state through the same pipeline; explicitly qualified joins continue through the canonical
two-input execution boundary under an admitted read session. Their pagination
is resolved once before either input is read; nested queries on a row with two
origins remain an explicit capability failure. An admitted global aggregate
has at most one output row and applies the same resolved count and offset.

Required scalar work dispatches by the source row's single-member lineage into
an admitted member session in the same live snapshot. The lower executor calls
the dispatcher only while evaluating required filter, ordering, DISTINCT ON or
projection expressions; output aliases reuse their cached selected value and
unused targets remain unevaluated. A nested scalar read therefore uses its
originating Base rather than the first member. The async borrow returns only
the selected value and payload leases; no context/session escapes to Ready.

| Logical state | Native / ordinary WASM / Embedded storage and isolation | Read / mutation / release |
|---|---|---|
| Snapshot vault and generation/lease counts | The same Mutex<State> and exactly-once token Mutex | Existing bound session access / admission, drain and finish APIs / authoritative snapshot close. |
| DISTINCT entries and Ready events | The same actor-owned arrays and reservations | Actor methods / insert, append and remove / final retained owner or publication completion. |
| Global window | Task-local state; existing feature DISTINCT callback uses the same Mutex<EmissionWindow> | Operator iteration / acceptsNextRow / invocation exit. |
| Selected lineage | Immutable member map plus one DatabaseIntermediateReservation, whose own Mutex protects accounting | Synchronous borrow / checked reservation growth and immutable byte destinations / final payload lease release. |

There is no Embedded-only storage, Sendable or synchronization branch in these
paths. Source inspection establishes this shared implementation; exact portable
compile/link/runtime evidence remains the S18 lane and is not claimed here.

#### S08 evidence and change impact

| ID | Required falsifiable evidence |
|---|---|
| S08-B01 | MultiBase SQL ordering across member/page boundaries preserves selected CHAR trailing-space equality, numeric display variants, finite/infinite temporal values, arrays and composites; C/POSIX, aliases, ordinals, non-output expressions, explicit/default NULL order and empty member descriptors agree with QueryExecution. Raw/RDF ordering and identity retain their separate behavior. |
| S08-B02 | Selected DISTINCT collapses SQL-equivalent representations while retaining contributor ownership; DISTINCT ON chooses the correct global representative with a validated ORDER prefix. Invalid prefix, nonprojected DISTINCT order and unsupported comparison/collation fail before member source fetch and before callbacks, including empty inputs. |
| S08-B03 | Complete binary set operands preserve local and outer windows, positional common types, NULL equality and ALL/DISTINCT multiplicity across member boundaries. A counterexample where per-member INTERSECT/EXCEPT loses a cross-member match must reject that operator distribution or produce the correct global result. |
| S08-B04 | Parameter/scalar count expressions are evaluated once per global invocation across multiple member pages. Signed-bigint boundaries, negatives, NULL/ALL, offset, zero count, and a final tied group spanning members/pages preserve SQL results; changing only fingerprint/Base/sequence does not split a peer group. |
| S08-B05 | Exact/one-past key, identity, set, peer and Ready row/byte budgets fail before growth/publication; cancellation/deadline/comparison failure frees partial claims and allows a subsequent valid operation. A retained composite/key/provenance alias keeps complete-owner claims after page/snapshot release, and its final release returns the meter to zero. |
| S08-B06 | Member schema and selected comparison mismatch fails before row fetch; metadata/row callbacks observe zero active member leases and terminal Domain transactions. Consumer cancellation and producer-close failure retain the established no-partial-publication behavior. |

Extend the existing CompositionQueryExecutorTests and selected SQL public-entry
integration tests for these contracts; parser/Wire tests or manually constructed
IR alone cannot establish SQL behavior. Existing publication, collision,
parameter and cancellation regressions remain evidence for their unchanged
assumptions. Changes to lower selected comparison, metadata, pagination or page
retention require this Base integration and its parent design to be rechecked;
GraphIndex consumers require the language-boundary regression. Server retained
paging must preserve the same complete-result budget and post-close publication.
All execution remains behind the non-default MultiBase trait; AllRuntimeFeatures
does not enable it. Native MultiBase behavior is S08 evidence; the final URL
dependency and ordinary-WASM/Embedded integrated matrix remains S18, with no
portable completion inferred from a native run.


### S09 selected aggregate and global placement contract

Base owns streaming Composition transitions only for plain COUNT, SUM, AVG,
MIN and MAX over admitted Base-local inputs. The SQL aggregate declaration,
selected overload and result/operand types remain QueryExecution contracts.
SQL aggregate calls with DISTINCT, FILTER, ordered inputs, GROUPING, HAVING,
GROUP BY/grouping sets or window evaluation execute once through the canonical
pipeline over the merged retained source relation. Member-local windows and
member-local grouping cannot substitute for that global operation. Feature
executors retain their existing explicit rejection of unsupported grouping.

```text
plain selected MIN/MAX operand + selected operand type
    -> compare through CanonicalSQLValueSemantics + captured temporal environment
        -> pre-admit complete replacement value/type footprint
            -> immutable QueryValue + type + request claim
                -> admitted final row -> release transition claim
complete grouping / aggregate modifiers / windows
    -> admitted merged source + lineage -> canonical global pipeline
        -> final retained rows + merged contributor lineage -> Ready
```

| State | Owner and lifetime | Admission and release |
|---|---|---|
| Extremum | One immutable Base transition containing the original QueryValue and selected operand type; retains neither prepared member row nor page. | Measure the complete value and selected descriptor owner and reserve before copying into the transition. Keep the old claim live until replacement admission succeeds; release once after replacement, failure, cancellation or final row promotion. |
| Numeric SUM/AVG | Existing DatabaseNumericAggregateAccumulator and its dynamic math claims. | Preserve existing widening, displayScale, budget and final overflow behavior. SQL algebra migration changes carriage only. |
| Global grouping/window input | Existing canonical relation builder and producer payload leases. | Admit every source/lineage destination before retention; lower canonical operators own their own grouping, aggregate and window state. |
| Global output lineage | Base lineage annotations and immutable contributor lease. | QueryExecution merges annotations for grouped rows. Empty scalar aggregate receives all admitted Composition contributors under Base-owned admission. No Domain/session escapes into Ready. |

Global result provenance does not select evaluation authority. A grouped result
may merge contributors from several Bases, and an empty group has no source
annotation. Group keys, aggregate transitions, analytic results and other
non-reading selected computations consume the global retained relation/context;
they do not select a member from that output contributor set. Required nested
source reads within an aggregate argument retain the underlying input row's
single-member lineage and dispatch through its already-admitted session. A
nested read at an inherently multi-origin or source-free group scope requires
an explicit authority contract and otherwise fails with a typed capability
error. No first-member or fabricated single-origin fallback is permitted.
The global output retains merged contributor provenance separately from any
single-row source authority used by a required nested read.

MIN/MAX comparison delegates numeric, CHAR, JSONB, array, composite and finite/
infinite temporal ordering to CanonicalSQLValueSemantics.compareValues using
options.readContext.temporalEnvironment. NULL operands do not replace state;
SQL-equivalent operands keep the first value and its original numeric displayScale.
JSON or other unsupported selected comparisons fail during binding even for
empty input. Raw feature-owned extrema retain their language-qualified primitive
contract and do not infer SQL semantics from the carrier.

The transition is task-local immutable Sendable state on Native, ordinary WASM
and Embedded. Its reservation uses the existing synchronized work-meter owner;
no target conditional weakens storage, isolation or Sendable contracts.

CompositionQueryExecutorTests owns rich MIN/MAX replacement across member/page
boundaries, exact numeric scale, temporal infinity, CHAR, JSONB/array/composite
comparison, all-NULL input and static comparison failure. Exact/one-byte-short
admission, work/cancellation failure, final alias retention/release and a subsequent
valid operation prove transition cleanup. Global grouping/window tests require
cross-member counterexamples that member-local execution cannot satisfy. Existing
SUM/AVG and Collecting-to-Ready lease/publication regressions remain required.
Native MultiBase runtime verification proves this scope only; S18 owns final URL
dependency and exact ordinary-WASM/Embedded compile/link/runtime integration.


### S10 source capability boundary

Recursive WITH and general table functions remain outside Composition's
Base-local relational placement contract. CompositionQueryPlanner rejects
these source families before member reads; it does not infer placement from
function arguments or grant a recursive working table member authority.
Single-Base canonical execution composes the QueryExecution S10 producers
within the existing admitted session. Existing global grouping/window and
member-provenance guarantees remain unchanged.

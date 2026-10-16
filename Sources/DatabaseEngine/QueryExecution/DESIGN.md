# QueryExecution

## Purpose and Scope

The QueryExecution component owns the DF-06F0 retained polymorphic aggregate,
the DF-06R retained regular-model fetch, and the only operations that may
transform retained values into canonical rows, index rows, or an explicit
final public output.

- Parent: [DatabaseEngine](../DESIGN.md).
- Child: [SQLPatternMatching](SQLPatternMatching/DESIGN.md), the bounded SQL pattern language implementation.
- Planned S01–S10 SQL execution obligations are recorded below; implementation
  completeness remains tracked by the workspace progress record.

## Responsibilities and Boundaries

QueryExecution couples retained primary-key sources, aggregate array storage, present/missing slot claims,
retained identifiers, decoded model reservations, runtime-derived type
metadata, and destination footprint admission. It rejects cross-meter
composition before constructing a destination value.

It consumes a Read-owned admission and never creates or widens authority. It
does not authorize storage access, resolve runtime registrations, decode
stored envelopes, or own specialized scoring/ranking/search policy. It does
not expose a general model or identifier borrow because those values are
copyable and could escape without their reservation.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Aggregate role in the retained flow | Defines cross-component assumptions and outputs. | Any new output must be an explicit destination boundary. |
| [DatabaseKit Query](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md) | depends on | `DataSource.usesSPARQLSemantics` | Supplies SELECT source language applicability for execution dispatch. | Classifies the top-level source; it does not prove executor availability or recursively classify descendants. |
| [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQL profile and typed semantic model | Supplies statements, values and ordered column contracts. | Type/equality changes invalidate every relational consumer. |
| [DatabaseTypes Bytes](../../../../database-types/Sources/DatabaseTypes/Bytes/DESIGN.md) | depends on | Immutable bounded bytes and exact retained capacity for `ByteString.copying` | Supplies the byte owner used by SQL BYTEA producers and Kit lexical conversion. | Exact output admission relies on the copying contract; Array-backed construction may retain excess capacity and must still use measured capacity. |
| [DatabaseMath](../../DatabaseMath/DESIGN.md) | depends on | Platform scalar math and bounded decimal formatting | Supplies selected float kernels and significant-digit cast formatting. | SQL domains, widths and typed numeric failures remain here. |
| [QueryAST](../../QueryAST/DESIGN.md) | depends on | Text-to-semantic syntax contract | Canonical execution receives semantic input from the entry layer. | This is a contract relationship, not permission for an engine-to-parser import. |
| [Core](../Core/DESIGN.md) | used by | Pre-admitted builder append | Produces present and missing entries. | Runtime metadata is validated before append. |
| [Read](../Read/DESIGN.md) | coordinates with | Exact session work meter | Supplies the meter to builder and destination. | Budget equality is not identity. |
| [SQLPatternMatching](SQLPatternMatching/DESIGN.md) | child | Immutable pattern programs and retained match/split results | Supplies SQL pattern algorithms without inheriting Graph XPath regex semantics. | Parent owns selected SQL types, NULL and public result adaptation. |

## Architecture

```text
Builder
  -> pre-admitted slot
      -> missing
      -> retained identifier + retained decoded model + type metadata
          -> noncopyable aggregate
              +-> append canonical source row
              +-> append index row with admitted annotations
              +-> borrow a retained vector through a noncopyable field view
              +-> consuming public-output promotion

retained primary-key source + Read admission
  -> async scoped key borrow
      -> bounded point fetch
          -> pre-admitted retained regular-model slot

Fusion sealed projection + retained primary-key source
  -> projection-bound Read admission
      -> the same bounded retained point-fetch implementation
```

## Contracts and Invariants

- SELECT execution dispatch consumes Kit's source language applicability
  contract through `DataSource.usesSPARQLSemantics`. Executor capability and
  Read admission remain Framework checks after that semantic classification.
- The aggregate is noncopyable and owns one retained array whose entries keep
  every identifier/model reservation alive.
- Builder append requires identifier, decoded model, and aggregate storage to
  share the same `DatabaseWorkMeter` object.
- Stored type code, runtime entity name, model entity, and retained identifier
  are validated as one entry invariant before append.
- A missing input is represented by a committed missing slot, preserving count
  and order.
- Regular retained fetch rejects a foreign source meter before authorization,
  storage, or destination allocation. Each key is borrowed asynchronously so
  its owner and reservation remain live until the point fetch completes.
- Regular retained fetch preserves source order, duplicates, missing `nil`
  slots, the sealed snapshot mode, and the session transaction.
- The regular complete-model entry point requires all entity fields. Fusion
  uses a separate projection-bound entry point whose field set is derived only
  from its sealed authorization evidence; QueryExecution consumes either as
  the same immutable Read-owned admission and never chooses the field set.
- Canonical retained primary-key owners do not conform to `Collection` and do
  not expose a raw `Tuple` subscript or return value.
- Destination builders must share the aggregate meter. Complete destination
  footprint and array growth are admitted before the row or annotation map is
  materialized.
- Package API does not expose general `withModel`, `withIdentifier`, entry,
  backing array, or reservation access. A borrowing closure around a copyable
  value is not an ownership boundary because the closure can copy it. A
  noncopyable field view may expose only synchronous element borrows; it cannot
  expose the model, copyable vector, or owner.
- Canonical and index conversion are purpose-specific operations whose
  destinations retain their own admitted representations.
- Public output promotion consumes the aggregate and is allowed only at a
  top-level API boundary. Intermediate feature paths cannot invoke it.

## Runtime Flows

```text
prepare slot claim
    -> producer constructs retained entry
        -> append commits claim to aggregate owner
            -> destination computes exact footprint
                -> destination pre-admits
                    -> materialize once into destination owner
```

If producer construction throws, the linear slot admission rolls back. If a
later entry fails, destruction of the unfinished builder releases all earlier
entries and the aggregate array claim.

## State, Ownership, and Lifecycle

| Value | Owner | Released or transferred when |
|---|---|---|
| Aggregate array/capacity | Noncopyable retained buffer | Aggregate is consumed or destroyed |
| Slot row claim | Array append admission, then retained buffer | Append is abandoned or aggregate ends |
| Identifier tuple/bytes | Retained primary key in entry | Entry ends |
| Regular fetch source key | Retained primary-key collection | Its async point fetch completes |
| Decoded model | Retained model entry | Entry ends or explicit final promotion consumes aggregate |
| Canonical/index row | Destination retained builder/owner | Downstream query owner ends |

## Failure, Concurrency, and Constraints

All mutation is task-local inside the noncopyable builder. Published aggregates
are immutable and `Sendable`. Footprint overflow, work-meter mismatch, invalid
entry metadata, destination budget failure, and materialization failure remain
typed failures; no partial destination row is committed.

## Verification and Change Impact

- [PolymorphicRetainedResourceContractTests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
  proves aggregate order, missing slots, later-failure release, and final
  release.
- Source audit must reject any general copyable model/identifier borrow or raw
  collection-returning intermediate API.
- Canonical-row tests prove direct retained-to-destination construction without
  an intermediate `QueryRow` array.
- Specialized index migration tests own annotation and field-comparison
  semantics; this component owns only noncopyable access, footprint-first
  append, and same-meter rejection.
- [RetainedRegularModelFetchContractTests](../../../Tests/DatabaseEngineTests/RetainedRegularModelFetchContractTests.swift)
  proves regular order, duplicates, missing slots, suspended source lifetime,
  authorization, failure, cancellation, and final release.

### Planned typed SQL relational execution

The exhaustive feature/profile contract is owned by
[DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
This component owns binding, planning, SQL evaluation, relational operators,
and retained relation production above its parent's operation lifecycle.
Syntax is owned by [QueryAST](../../QueryAST/DESIGN.md), and primitive value
invariants remain in DatabaseTypes. These are planned implementation
contracts; current dictionary-backed rows and incremental numeric aggregates
do not yet satisfy them.

```text
DatabaseKit statement + schema lease + parameters + admitted scope
    -> relation/name scope resolution
    -> type / typmod / collation / overload / volatility binding
    -> ordered output schema and physical plan
    -> retained positional relations
        -> join / filter / set operation / CTE
        -> grouping / aggregate / window / recursive frontier
        -> ordered projection / pagination
    -> collecting relation owner
    -> parent closes producing resources
    -> purpose-specific Ready relation
```

#### Binding and positional relation contracts

- Bind output columns before evaluating rows: ordered labels, stable column
  references, type, nullable metadata and typmod remain present for zero
  rows. Duplicate labels are valid output; name lookup can be ambiguous
  without data loss. Internal slots and output ordinals are never inferred
  from dictionary iteration, field labels, or the first row.
- Resolve aliases, qualified names, CTE visibility/shadowing, lateral and
  outer scopes, output alias and ordinal precedence before selecting a
  physical access path. Invalid or ambiguous references fail even on empty
  input and cannot disappear through pushdown.
- Bind unknown literals and parameters, operator/function overloads,
  assignment/explicit casts, common types, collation and SQL equality once
  into a typed plan. JOIN, GROUP, DISTINCT, set operations and SQL indexes
  use one equality/hash contract. Output nullability is metadata, not a
  guarantee that outer joins cannot create NULL.
- CASE, COALESCE and related conditional expressions evaluate only required
  branches, including their subqueries. Binding still checks all branch
  types. SQL three-valued logic, empty quantified relations, cardinality
  errors and NULL ordering follow the profile.
- Physical pushdown is legal only with proof of the same typed comparison,
  NULL order, collation, residual predicates and pagination meaning. A
  backend optimization never replaces canonical semantics silently.

#### Numeric, functions, and relational algorithms

SQL numeric remains based on DatabaseTypes' finite 38-digit intrinsic
representation. Framework selects SQL result types, typmod, precision,
scale, rounding and overflow semantics. Pure rounded primitive operations
are added without weakening the existing exact-operation contract. Numeric
display scale belongs to the DatabaseKit query value, not primitive identity.
Wider bounded workspace and exact limb accumulation allow intermediate sums
to exceed result width when the final AVG is representable. A double
conversion is not an implicit numeric fallback.

Function binding carries input/result type, NULL behavior, volatility and
resource effect. Transaction/statement clock values are supplied by the
operation owner; timezone/regex/collation capability follows the explicit
profile and injected data. Unsupported overloads fail before evaluation.

The accepted inventory includes bag-preserving set operators, DISTINCT ON,
grouping sets, FILTER, ordered aggregates, ordered-set statistics, windows
and named frames, nonrecursive/recursive CTEs with SEARCH/CYCLE, and lateral
table functions. Each must execute from SQL text through this typed path.
Recursive working/frontier tables and window peer/frame state are concrete
owned relations, not uncharged Arrays or repeated implicit materialization.

#### Resource ownership and failures

| Resource | Charge before | Terminal requirement |
|---|---|---|
| Bound plan/type slots | Allocation of nodes and metadata | Operation or reusable-plan owner releases reservation. |
| Join/hash/group/set table | Capacity growth and value retention | Partial table released on failure/cancel. |
| Sort/window/CTE relation | Materialization, sort workspace and retained payloads | Same exact meter through every consuming operator. |
| Recursive frontier/visited state | New row, path, cycle key, and next iteration | Depth/work/rows/bytes are bounded; abandoned frontier drains. |
| Numeric/regex/JSON workspace | Parse, expansion, match, or arithmetic work | Resource limit remains typed; no altered-semantics fallback. |
| Output and copied page | External materialization/copy | Result owner retains full staging claim; page copies are measured. |

Configuration owns operational defaults and the selection of portable
limits. A request can tighten but cannot exceed validated backend maxima.
Consumed work and deadlines are monotonic across rollback and retry;
retained claims follow actual lifetime. Pointer borrows are synchronous and
bounded, never escaping their retained owner. Actor/mutex isolation and
Sendable contracts are identical across native, WASM and Embedded targets.

#### Verification and change impact

The existing [SQLRowExecutionTests](../../../Tests/DatabaseTests/SQLRowExecutionTests.swift)
and retained-owner tests prove only their exercised current paths. New
profile cases compare real PostgreSQL with Framework execution, including
column metadata for empty relations, duplicate labels, alias/ordinal binding,
NULL and bag semantics, numeric type/rounding, lazy branches, window peers,
recursive cycles, resource boundary failure and retained-owner release.
Same SQL text must traverse parser, binding and production executor;
independent parser and IR tests cannot jointly replace that proof.

Type/collation/equality changes invalidate all comparison consumers and index
pushdown assumptions. Retained representation changes invalidate canonical
destinations and public entry/wire adaptation. Transaction or readiness
changes belong to the parent operation owner and require its verification.

### S01 frozen positional execution contract

This section is the implementation boundary for S01 only. It consumes the
frozen [Query](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md),
[SQL values and case IDs](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s01-frozen-value-and-persistence-interfaces),
[Model](../../../../database-kit/Sources/DatabaseKit/Model/DESIGN.md) and
[Schema](../../../../database-kit/Sources/DatabaseKit/Schema/DESIGN.md)
contracts. S01 changes carriage, declared persistence adaptation and typing
of already-admitted expressions; later grammar, arithmetic compatibility,
functions, DDL and block execution are not prerequisites for this boundary.
Names below are planned internal contracts unless already present in source.

#### Implementable representation and lookup map

| Declaration/operation | Exact responsibility and caller |
|---|---|
| `CanonicalRelationSlot` | Immutable column declaration, optional source qualifier and optional persisted field number. Slot index is its offset in the shared schema; persisted field number never doubles as output ordinal. |
| `CanonicalRelationSchema` | Ordered slots, ordered wildcard slot selection and name-to-matching-slot-index lookup. Owns immutable type/nullability declarations and scope visibility once for the relation. |
| `resolve(ColumnRef)` | Returns one slot or typed missing/ambiguous error. Qualified names can be ambiguous when a derived relation exposes duplicate labels. Zero local matches may resolve in an outer schema; multiple local matches never fall back. |
| `wildcardSlots(scope:)` | Returns bound slot indices in declared order. Ordinary JOIN concatenates left then right without renaming duplicate output labels. USING/NATURAL emits merged keys in USING/left-declaration order, then remaining left/right visible slots; qualified side slots remain addressable. |
| `CanonicalSourceRow` | Positional `[QueryValue]`, primitive annotations and version only. No per-row name/scope/flattened/coalescing value dictionaries. Row arity equals its relation schema slot count. |
| `CanonicalRelation` | Shared admitted schema plus retained source-row storage using the exact same meter. Alias creates a schema view, not per-row copies; derived/set alignment moves cells by position. |
| `CanonicalBoundExpression` | Resolved column/outer-slot references, output QueryColumnType/nullability, and checked operation signature for an existing Expression. Evaluation never infers type from a row. |
| `CanonicalQueryTypeBinder` | Consumes admitted entity metadata, parameters and relation scopes; binds projection/filter/join/group/order/aggregate/CASE/subquery before fetching source rows. S03 extends this same binder. |

The schema contains internal addressable slots separately from its visible
wildcard indices so USING/NATURAL can retain both qualified input keys while
emitting one unqualified merged key from only the key slots of that JOIN.
Visibility belongs to slot indices,
not a relation-wide set of labels: an ordinary chained JOIN preserves each
input's visible slots in left-to-right order, including a later same-name
column that makes unqualified lookup ambiguous. Layout cloning and NULL
extension preserve those indices; scoped wildcard lookup retains hidden keys.
A null-extended side creates the exact
number of primitive NULL cells, preserving schema and slot addressing; it
never calls Dictionary(uniqueKeysWithValues:). A projected derived relation
has exactly its projected slots; duplicate labels survive alias application.
A scoped row is always a row/schema pair. Schema references are retained
through grouping representatives and correlated subqueries without copying
label maps into each row.
LATERAL right-source binding receives the left schema followed by enclosing
schemas in both validation and execution. Canonical binding wraps expression
failures in CanonicalReadError. Scalar cardinality failure reports the row
count before testing the one-column contract.
Primitive numeric VALUES carriers normalize to their bound common type without
entering the rich-value evaluator. Exact and floating relation inputs use an
exact decimal common type only when checked conversion succeeds; arithmetic
overloads remain separately bound. DISTINCT and set identity compare an exact
SQL numeric payload through the primitive numeric identity owner, while general
rich numeric expression operators remain unsupported. Canonical rich values
retain display scale and SQL type metadata.
LEFT LATERAL marks the right output nullable before projection binding. Scalar
subquery projection overlays its enclosing frame before the nested await, with
the same admitted ownership used by other correlated expression evaluation.

Output QueryColumn numbers are consecutive one-based ordinals. Internal
source slots remain zero-based storage indices and do not become public
field identity. Output labels can be duplicate or empty. Scope names must
still be unique in one visible binding frame: permitting duplicate columns
is not permission to accept duplicate table aliases.

#### Existing-syntax output typing

The binder is total over currently executable literal, parameter, column,
CAST, arithmetic/comparison/logical, function, CASE/COALESCE/NULLIF,
aggregate and subquery forms. It produces the same schema on empty and
nonempty inputs. A bare NULL acquires a contextual type, or text at a final
SQL output with no stronger context; explicit typed NULL retains its CAST or
parameter declaration. S03 refines the earlier S02 literal-NULL rule: only a
bare untyped NULL may acquire an arbitrary valid resolved target. A typed NULL
retains its incoming declaration and must pass source-to-target signature
validation. Legal same-family temporal precision casts preserve NULL and the
target precision without invoking conversion or rounding; illegal typed chains
fail even for NULL. Builder leaves typed NULL CAST edges for this check.
Non-NULL temporal precision conversions still fail explicitly until S06
implements rounding. Invalid target descriptors fail even for NULL. Column metadata comes from admitted FieldSchema,
never persisted object shape. Outer joins widen nullability. Scalar
subqueries retain their sole declared output type and become nullable;
multiple output columns fail before execution and multiple rows fail at
execution. COUNT is nonnullable; other aggregate nullability includes empty
input. CASE checks all branch types while evaluating only its selected
branch.

Result signatures must describe actual evaluator results for every row.
Existing operations with value-dependent primitive representations must
normalize to their bound common result type through a checked explicit
conversion; they cannot pick a first-row type or publish a guessed type.
This normalization does not claim S04 PostgreSQL arithmetic/rounding parity.
Unsupported rich arithmetic/functions remain typed unsupported operations,
while rich literal/parameter/column projection and declared persistence work
in S01. Primitive-only non-SQL outputs use QueryColumnType.primitive,
including the array flag, rather than a fabricated SQL type. SQL-specific
binding is language-qualified; SPARQL term identity, unbound/error behavior,
and equality do not adopt SQL rules.

### S03 executable binding contract

This is the runtime authority for
[S03 semantics and case IDs](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s03-frozen-semantic-binding-boundary).
It extends the existing CanonicalQueryTypeBinder and CanonicalBoundExpression
path; it does not introduce a second binder or a duplicate SQL statement AST.
Current source returns only columns from query binding and executes original
expressions. That metadata-only path does not satisfy S03.

```text
QueryIR + supplied parameters + semantic occurrence arena/root
    + admitted schema, parent transaction, authority and exact meter
    -> ordered whole-query binding
        -> resolved frames/columns, base parameter types, available signatures
        -> bound children and executable coercions
        -> ordered output columns and admitted plan owner
    -> canonical relation operators consume those bound operations
        -> proven equivalent physical hints, otherwise canonical evaluation
    -> retained typed positional rows -> existing authoritative Ready boundary
```

| Owner | Assumption | Guarantee |
|---|---|---|
| Kit declarations | Structurally valid QueryIR, original parameter references/declarations, occurrence field/index meaning | Source-independent immutable semantics; no evaluator or source-text policy. |
| QueryExecution binder | Admitted schema and one explicit operation parameter state | Binds every CTE/body/source/ON/filter/projection/group/HAVING/order expression before its data read; returns operations actually consumed, including empty-source paths. |
| Existing scalar evaluator | Selected executable signature and explicitly lowered operands | Executes selected coercions/functions, preserves lazy branches, and returns values compatible with the bound signature. No second name/type inference. |
| Relation execution | Bound source/child plans and stable frame addresses | Uses the same resolved column address in JOIN, filter, grouped and ordinary projection, sorting, nested and lateral execution. |

Grouped projection retains the evaluated `QueryValue` through row admission;
it does not down-convert numeric/rich cells through a primitive scalar bridge.
Temporary cell storage uses its actual collection footprint, and the complete
output payload is admitted before temporary evaluation leases end. This keeps
both primitive declared carriers and canonical numeric metadata unchanged.
Ungrouped correlated references reject at their bound column occurrence and
include its qualified name; no representative-row value makes such a reference
legal.

| Database bridge | Parsed arena/root and optional QueryAST map | Resolves typed semantic failure addresses to local source positions without Engine importing QueryAST. |

The bound expression entry preserves the existing runtime failure boundary:
scalar evaluation and normalization failures become
`CanonicalReadError.expressionEvaluation` with the original typed cause and optional decisive occurrence.
The innermost selected failing node attaches the occurrence once; enclosing
expressions preserve it.
Resource, cancellation, aggregate and nested-read failures retain their owning
error types. This boundary does not evaluate additional children or change
CASE, COALESCE, AND or OR branch selection. The existing
`SchemaDeclaredIndexBehaviorTests.canonicalSelectUsesDatabaseExpressionSemantics`
assertion distinguishes a correctly wrapped division-by-zero failure from a
raw scalar-kernel error escaping canonical execution.

#### Binding state, execution and scope

One request-owned parameter state is passed explicitly through preparation,
canonical execution and nested binders. Explicit declarations and intrinsic
non-NULL payload types initialize it; contextual parameters follow the SQL
child's observed visitation order. Position/name aliases share one identity.
Every occurrence retains its resolved use and legal coercion, including
unresolved earlier occurrences that cannot be repaired by a later CAST.
Repeated execution of an already-bound child consumes established decisions
rather than recording fresh constraints. No TaskLocal, global registration,
or weakening of the sealed ReadExecutionContext identity is introduced.

CanonicalBoundExpression retains selected children/type/nullability and the
resolved column address. It may materialize an executable existing Expression
from those children, typed parameter/column literals and explicit coercions,
then reuse the existing scalar/aggregate/query-scoped evaluator. Rebuilding
from the original unbound expression, discarding children, or using a raw-AST
fallback is prohibited. Binding must not eagerly evaluate a CASE/COALESCE
branch or scalar subquery. Selected overloads, input conversion, result width,
NULL contract and runtime availability have one decision owner; an immutable
existing policy table is sufficient and no generic plugin/catalog service is
required.

A column address is a stable frame depth plus slot, or an equivalent shared
layout mapping used identically by binder and evaluator. Current binder
local-before-outer offsets cannot be applied to runtime inherited-before-local
overlay cells. Local ambiguity is terminal; a local qualifier shadows the
outer qualifier even when its requested column is absent. Public ordinals,
output labels, internal slots and frame depth remain separate meanings.
Outer frames retain payload leases across nested await and preserve existing
outerFrameLifetime ownership; NULL extension retains complete typed layout.

Bind all nonrecursive CTEs in order, including unused definitions. Only prior
siblings and inherited definitions are visible to a body; local definitions
shadow inherited names. Recursive execution stays explicitly unsupported
until S10. Derived, JOIN and LATERAL schemas bind before row iteration, so an
empty left source cannot hide an invalid right expression. GROUP/ORDER use
the SQL child's separate label/ordinal precedence, including identical-output
alias equivalence; their resolved expressions feed actual execution.

Selected coercions must run at their use sites, not only as final-cell
normalization. The same binding feeds filter, JOIN, grouping, projection,
sorting, conditional branches, CTE/derived/subquery and set alignment. Existing
working primitive operations remain available; future numeric/string/temporal/
array/relational kernels remain owned by S04–S10. Unavailable signatures fail
before the affected scan, even if NULL or zero rows would avoid evaluation.
Physical predicate/order/pagination hints require equivalence with the chosen
types, coercions, comparison and NULL behavior; otherwise use canonical
evaluation with the same bound operations.

SQL boolean and primitive Bool are identical scalar carriers. Common-type
selection may preserve either declaration; use-site coercion lowers their
representation-only change through the existing BOOLEAN identity cast,
preserving NULL and rejecting non-boolean or array operands.

SUM/AVG retain the existing exact numeric aggregate capability: the bound
input must have an admitted numeric type, and the aggregate's scoped-value
bridge converts canonical numeric to its ExactDecimal carrier before invoking
DatabaseNumericAggregateAccumulator. This existing bridge admits decimal
columns and numeric literals without enabling general rich-value arithmetic;
non-numeric inputs still fail during signature binding.

Source-column alignment retains the existing relational numeric identity
contract independently of scalar CAST admission. Mixed decimal/floating
VALUES, set inputs and JOIN USING columns select numeric metadata; the existing
source-carrier normalization converts exactly representable floating values
through RelationalValueIdentity into ExactDecimal and rejects unrepresentable
values explicitly. Scalar expressions still reject deferred exact/floating
coercions. Integer arithmetic result widths remain the existing bigint/uint64
contract until S04; SQL integer literal classification alone does not narrow
an arithmetic result.

#### Occurrence, resource and lifecycle proof

The operation receives Kit's arena and root explicitly. Child lookup uses
semantic field/index edges in that same arena; it never guesses IDs from a
second traversal or identifies nodes by Expression equality. Binding failures
preserve typed cause plus the decisive occurrence and its arena/root lifetime.
Alias/ordinal expansion and product SPARQL rewriting retain explicit origin
provenance. Direct IR input legitimately has no SQL source location.

Plan nodes, executable lowered expressions, signatures, parameter occurrence
state, scope lookup indexes and address storage are pre-admitted under the
existing work meter's bindingCandidate stage. Count/depth/overflow and retained
bytes are checked before growth, including conservative capacity bounds for
containers. Shared parent/edge storage is O(nodes + edges); repeated full
root-to-node path copying is not a permitted hot-path implementation. Plan
reservations and parameter/payload owners survive until execution releases
them; failure/cancellation releases partial owners. Existing row/schema claims,
authority, exact meter identity, transaction closure and publication guarantees
remain unchanged on native, WASM and Embedded.

Primitive-array parameter inference visits non-NULL elements once with one
element-kind accumulator; it creates no payload-sized filtered/type arrays
and rejects heterogeneous or nested primitive arrays. Contextual unknown
string conversion receives the exact operation work meter. Before copying
input, the binder reserves a conservative scratch allowance for eight UTF-8
conversion buffers (including terminators and String headers); decimal input
also reserves both existing parser split arrays with the retained-array
capacity policy and a checked UTF-8-count-plus-one element bound. String
allowances bound copied content, consistent with existing relational string
accounting; they do not claim observable allocator capacity. A synchronous
defer releases scratch on success or throw, and failed/cancelled admission
performs no conversion. Text identity and unsupported input types do not copy
input. Production binders always receive the operation meter; schema-only
common-type helpers and isolated unit binders may omit it.

CAST validates the bound child's source-to-target signature before inspecting
that child's statically NULL value. Supplied typed NULL parameters therefore
use the same legal precision/typmod identity lowering as NULL literals;
non-NULL deferred conversions remain rejected before source reads.

S03-C01–C12 require public SQL through actual production execution and focused
scope/type/resource failures. Binding rejection observes zero affected scans
and a subsequent valid query on the same container; correlated rich values
prove outer-frame retention. Scan/index comparison proves actual coercion
equivalence. Empty and nonempty inputs have identical declarations. Existing
S01 retained-owner and SPARQL/graph suites remain regressions; final full FDB
and published URL graph verification remain S18.

#### Admission and direct model conversion

```text
Read admitted schema lease + exact meter
  -> bind shared slots/name lookup/output columns
  -> measure and admit schema once
  -> borrow retained PersistedModel
  -> read admitted fields in schema slot order
  -> validate descriptor and measure QueryValue destination
  -> append(footprint: make:)
       -> decode declared rich envelope or wrap primitive
       -> create positional row directly
  -> retained relation -> collecting response -> existing Ready transition
```

Registry's regular-table path and retained-polymorphic source append use one
purpose-limited canonical-slot append operation. It borrows the model only
inside the existing retained entry, measures directly from its fields and
admitted descriptor, and builds the destination inside the admitted append
closure. It does not construct an intermediate QueryRow, field dictionary,
public model array, or permissive generic model borrow. Field ordering is
schema order, not model field iteration. Missing required fields fail;
optional absence maps to NULL only where the admitted schema permits it.
The sealed admitted field set selects source slots before decode and binding,
including regular tables, polymorphic members and index/Fusion candidates.
Unadmitted required fields are omitted from that source schema; they are never
filled with NULL. A Fusion candidate's internal routing identity is validated
from its retained model against the retained primary key independently of its
admitted output slots. A restricted identifier is never added to those slots
to satisfy internal matching. Fusion execution contract tests require the
authorized projection to succeed and the identifier projection to remain denied;
score-payload fixtures declare every payload field in their candidate schema.
Physical predicate/order pushdown cannot evaluate declared
rich storage envelopes as primitive objects. Unsupported rich operations
remain explicit failures on the canonical semantic path.

CanonicalRelationalFootprintMeter combines Kit's bounded value/storage
footprints with existing runtime layout/capacity measurement.
It accounts once for shared slots, labels, scope/name lookup and column
metadata; per row it accounts for value-array capacity, retained rich
payloads, annotations and version. Borrowing a primitive ByteString/lexical
JSON retains its owner and charge without materializing bytes. Typed-array
shape, JSONB flat backing storage and numeric display metadata are included.
Logical node, collection and depth limits reject growth before it occurs,
and known source-byte retention is pre-admitted. Framework selects a bounded
layout/capacity allowance before decoded destination assembly, then reconciles
actual whole-owner array capacities and UTF-8 content before publication.
Reconciliation failure releases the unpublished value and its claims. Array
counts are not a guarantee about allocator internals or total physical peak.
Exact/one-past tests distinguish logical pre-growth rejection from actual-owner
pre-publication reconciliation. Projection/group/derived operators share
schema ownership; new schema metadata receives its own admission. Rollback
cannot refund consumed work; destruction releases retained claims only when
their last owner ends.

#### Rich literal, parameter and persistence route

`Literal.queryValue(value, type:)` retains the QueryValue and explicit
QueryParameter.type after address replacement. A declared FieldSchema
queryType selects QueryFieldStorageAdapter.decode; absent metadata wraps the
unchanged FieldValue as primitive, even when object/bytes resemble an
envelope. Declared rich writes measure and admit the exact adapter envelope,
then encode before existing PersistedModel storage. Model insert/reload and
selected-field update use the same descriptor/adaptation. No read-time
shape inference, special object fallback or runtime metatype registry exists.

#### S01 acceptance ownership

The SQL child owns case meaning. This table assigns Framework evidence only;
Kit, Wire and transport owners retain their own stages of the same case.

| Cases | Framework test owner and concrete evidence |
|---|---|
| S01-C01–C03 | DatabaseTests SQLRowExecutionTests and DatabaseEngineTests positional relation tests: ordered duplicate projections, empty typed schema, JOIN/derived ambiguity, outer scope and null extension. |
| S01-C04–C08 | DatabaseTests generated rich-model SQL round-trip tests: literal and parameter projection, model insert/reload/update and nonlossy metadata. |
| S01-C09 | DatabaseEngineTests registry/schema adaptation tests: descriptor-based reads/defaults/update, malformed envelopes, no inference for primitive lookalikes. |
| S01-C10 | DatabaseTests CanonicalPaginationBoundaryTests plus server/client/CLI owners: first/continuation/zero-row schemas identical, no first-row reconstruction. |
| S01-C11 | Wire owns malformed/golden/lazy-frame cases; Framework bridge tests preserve typed cells/columns without eager all-page decode. |
| S01-C12 | DatabaseTests CanonicalSQLRetainedOwnershipTests and DatabaseEngineTests retained model/resource tests: exact/one-past schema/row admission, failure/cancel release, shared last-owner release. |
| S01-C13 | Existing primitive persistence, graph/SPARQL and MultiBase tests for affected shared paths; SQL comparator never replaces RDF identity. |

The file-backed SQLite lane traces S01-C02 and C04–C09 to
[`SQLiteStorageTests.richValuesSurviveSQLiteRestart()`](../../../Tests/SQLiteTests/SQLiteStorageTests.swift):
model insert/reload/update, authoritative shutdown/reopen, ordered rich SQL
projection, explicit rich AST literals and parameters, and identical typed
columns for empty results. It complements the focused in-memory and codec
fixtures with the actual SQLite persistence and restart boundary.

These tests must execute the real admitted backend path. Structural or
compile-only evidence cannot mark these cases complete. Existing SQL tests
that expect dictionary fields migrate to positional assertions, with separate
explicit ambiguity tests for name projection; removing an assertion is not
coverage migration.

#### S01 reservation terminal boundary

The existing consuming final-output contract remains: claims stay live
through pre-admitted construction, producing-resource closure and retained
Ready/page ownership. Retained results share both schema and rows until the
last retained owner is released. Explicit consuming public promotion moves
complete arrays and byte owners into caller-owned QueryResponse storage and
ends runtime reservations. No runtime token is inserted into QueryValue or
QueryColumn. Intermediate array extraction is not final output; in particular
server encoding/persistence completes under the retained owner's claims.
This is the precise Framework interpretation of S01-C12; Wire's encoded-frame
ownership is a separately verified terminal boundary.

#### S01 byte-retention provenance and measurement

Kit footprint counts describe representation and conservative per-reference
known retained capacity, not unique physical allocation. Framework does not
introduce a primitive owner-identity API or attempt universal alias detection.

A producer-known frame/model/source origin carries one shared payload lease
with its reservation. All positional aliases/projections retain that lease;
new destinations charge their new structures, not the source payload again.
Retaining the complete source owner truthfully retains and charges its full
capacity, including portions outside a visible byte slice. Partial projection
cannot release that claim while any surviving alias retains the owner.

#### Canonical direct table page retention

For a direct table page with visible size `p`, canonical execution admits up to
`p + 1` rows, including its lookahead. With no residual filter/reordering or
additional relation stage, three row containers may coexist: the canonical
source frame, an optional bound-addressing frame, and the projected result
frame. The direct page boundary therefore guarantees at most `3 * (p + 1)`
retained rows for these containers. Existing operator-specific claims remain
independently admitted.

`addressingBoundSource` builds a positional runtime frame when declared binding
addresses differ from the fetched layout. Its new row container retains the
original source reservation through producer payload leases. This is distinct
from applying a SQL relation alias, which remains a schema view without per-row
copies. Shared payload bytes are retained once; separately allocated row
containers keep their own row claims. The page consumer must preserve all live
frame owners and admit any additional prepared destination before retaining it.
[Base](../Base/DESIGN.md) consumes this guarantee for aggregate page sizing.

Independent raw parameter/value roots use a documented conservative bound:
each reference contributes its known retained capacity. Equal bytes and equal
capacity do not establish shared ownership. This sum is never labeled unique
physical memory. Unknown capacity is a typed resource-admission failure or
crosses an explicitly pre-admitted copying/detachment boundary; visible count
is never substituted for unknown retained capacity. Byte validation/work
consumption remains separate from allocation/reservation accounting.

S01-C12 distinguishes: a producer-known 4 MiB frame shared by two slices pays
one source lease plus new views/containers; arbitrary caller-supplied aliases
may conservatively pay both known capacities; unknown-capacity input fails
before unadmitted retention. Both cases retain their claims through Ready and
retained output, release after last owner or final consuming transfer, and
release partial destinations on failure/cancel. Wire's known frame admission
and its view/new-UTF8/node ledgers remain Wire-owned; Framework consumes its
explicit provenance rather than deducing it from QueryValue equality.

#### S01 scoped expression admission consistency

Primitive expression producers and their retained scoped values use the same
Framework structural-count formula over Kit value footprints. Prospective
scalar, string and array bounds include the same per-node and collection
allowances as the actual known-capacity measurement; changing the retained
representation cannot leave an older FieldValue-only maximum at the producer.
The scoped owner still rejects a producer exceeding its declared maximum and
releases its whole claim on producer or validation failure. Shared payload
provenance remains separate from independent derived-value admission.

VALUES metadata is admitted once in its shared positional layout. Its row
producer reserves slot-array capacity and value payloads, without charging
column labels as per-row dictionary entries. Exact/one-byte-short tests include
the operation's persistent parameter/selected-plan claims and that schema claim
before the append boundary. A raw-string fixture with no conversion scratch
can measure its persistent binding prefix through pure metadata binding; that
measurement does not replace the separate exact row-append footprint. Tiny work budgets may reject
mandatory schema construction even for zero rows; physical cursor bounds and
stable LIMIT 0 no-read behavior remain separate observable contracts.

### S04 frozen selected-value execution

Consume [S04 SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s04-frozen-numeric-and-conditional-semantics)
and [Decimal](../../../../database-types/Sources/DatabaseTypes/Decimal/DESIGN.md#s04-frozen-explicit-rounding-boundary).
This component owns SQL policy; no arithmetic policy moves into Kit or Types.
The existing CanonicalBoundExpression children/type/occurrence/subqueryBinding
are the selected signature. Binding coerces children to exact selected input
types and records the result type before source reads; evaluation never
re-resolves overloads from row values. Explicit, implicit and assignment cast
admission are separate decisions, including statically typed NULL.

Searched CASE children are conditions N, results N, then fallback. Simple CASE
children are the original typed operand once, N independently selected equality
nodes, results N, then fallback. Each equality node retains its selected left
coercion declaration and right match child. The runtime evaluates the original
operand once, coerces that saved value separately for the examined equality,
and evaluates only that equality's right child. A different WHEN never changes
another equality's input signature. Lowering retains the semantic simple-CASE
operand and match-value shape; selected execution consumes the bound equalities.
All match/result branches bind before reads; NULL equality is unknown and does
not select a result branch.

```text
bound children + selected input/result types
  -> lazy control/subquery owner evaluates required QueryValues
      -> scalar numeric evaluator (lossless inputs and displayScale)
      -> aggregate exact workspace (admit before growth)
          -> QueryValue -> selected declaration validation -> retained output
```

Planned internal shared entry:
`CanonicalSQLNumericEvaluator.evaluate(_: CanonicalBoundExpression,
operands: [QueryValue], workMeter: DatabaseWorkMeter) throws -> QueryValue`.
It handles selected numeric arithmetic/functions/casts and preserves
QueryNumericValue.displayScale. CanonicalRows calls it directly with evaluated
children, never through primitive literal materialization/demotion. Raw
DatabaseExpressionEvaluator exact/RDF contracts remain unchanged. Other
selected scalar operations continue through their existing kernel. Existing
explicit typed-text to unconstrained NUMERIC casts retain their admitted
lexical kernel and canonical decimal scale; they are distinct from S04
numeric-source casts. Already-admitted primitive Int8 and unsigned integer
carriers without a SQL kind use the existing checked Int128 value normalization
into canonical numeric before the selected CAST kernel. The complete UInt64
range is retained without an Int64 or floating intermediate; this bridge neither
selects an overload nor creates an unsigned SQL type. Declared source validation,
selected-target rounding, displayScale, final width and typed failures stay with
their existing owners. PostgreSQL lexical display-scale preservation and lexical
typmod conversion remain S05-owned. Typed-text to numeric-typmod signatures,
including declared NULL, fail during binding before NULL propagation;
unknown-string and typed-numeric literal normalization keep their S03 path. Binding
and final value normalization do not reconstruct numeric scale from canonical
ExactDecimal.scale. Ordinary scalar comparisons, BETWEEN, IN/NOT IN, conditional matching and
quantified reduction consume the same metered selected QueryValue comparison.
Numeric values contribute their exact decimal value for comparison without
converting result payloads or displayScale through the primitive evaluator.
BETWEEN and membership preserve SQL three-valued reduction and retain the
innermost failing occurrence. Raw primitive and RDF execution remain separate.
SQL source alignment remains the existing independently
owned checked exact identity contract; changing scalar common type does not
change VALUES/set source-carrier normalization.

Scale selection and overloads follow the retained REL_18_6 reference linked by
SQL. In particular divide/AVG use normalized base10000 leading weights/digits;
SQRT/EXP/LN/LOG/POWER match both decimal value and observable output scale.
Decimal transcendental computation uses bounded decimal workspace, with no
floating result fallback. Source estimates used only to select scale/work do
not authorize a floating calculation. Existing DatabaseMath Float/Double
wrappers may be extended for float signatures; SQL checks domains, finite
results, underflow and width. New internal numeric files remain in this
existing component; an independent module/component requires its own design
and parent link before creation.

The scalar evaluator delegates selected numeric SQRT/EXP/LN/LOG/LOG10/POWER
to internal `CanonicalSQLDecimalMath.evaluate(function: String,
operands: [QueryNumericValue], workMeter: DatabaseWorkMeter) throws ->
QueryNumericValue`. The caller supplies an already-selected numeric signature;
this entry does not resolve overloads. Its owner owns decimal computation and
operation-local workspace. Aggregate state remains independently owned by the
accumulator; neither owner reads or duplicates the other's internal state.

Integer POWER derives its working significant-digit target from the selected
final scale and estimated final decimal weight, plus the retained reference's
exponent-dependent error margin. Each multiplication selects its local fractional
scale from both operands' current normalized base10000 weights, bounded by the
product's actual fractional scale and zero. A fixed absolute intermediate scale
must not erase a nonzero tiny power before a negative exponent takes its
reciprocal. Positive powers round to the selected result scale at finalization;
negative powers take the reciprocal at that same selected scale. Weight estimates
select precision only: all products, rounding and reciprocal computation stay
decimal and retain the existing metered workspace ownership and typed failures.

#### S04 numeric workspace ownership

`CanonicalSQLDecimalWorkspaceValue` is an immutable copyable signed decimal
value. Its decimal digits and reservation share a last-owner lifetime through
an immutable storage owner; copying an accumulator never releases a live
claim. Methods return new owners and never mutate an existing value.

| Internal entry | Guarantee |
|---|---|
| `init(_: ExactDecimal, workMeter:)` | Admits coefficient limbs before construction. |
| `adding(_:)`, `subtracting(_:)`, `multiplied(by:)`, `remainder(dividingBy:)` | Exact widened signed result; no SQL38 admission. |
| `divided(by: UInt64, toScale: Int32, rounding: DecimalRoundingRule = .toNearestOrAwayFromZero)` | Rounded widened quotient for aggregate finalization. |
| `divided(by: Self, toScale: Int32, rounding:)`, `rounded(toScale:rounding:)` | Decimal-only bounded math intermediates. |
| `exactDecimal()` | Converts only at the final primitive boundary; throws `ExactDecimalError.numericOverflow` for unrepresentable coefficients/scales. |
| `leadingBase10000`, `decimalDigitCount`, `scale`, `sign`, `isZero`, `comparison(to:)` | Read-only normalized metadata for SQL-owned scale selection. |

The representation retains implicit decimal exponent and expands alignment
only after deriving capacity and admitting its complete retained/scratch claim.
Arbitrary Int32 scale gaps may fail the existing memory/work budget before
allocation; sparse success is not required. Allocation-size overflow is a
resource failure, never an arithmetic failure. Loops charge work and checkpoint
through the same meter. Final SQL38 admission and aggregate count/displayScale
remain consumer responsibilities. Mathematical domain failures belong to
`DatabaseExpressionEvaluationError`; primitive representation failures use
`ExactDecimalError`; cancellation/deadline/work/memory failures propagate.

The existing DatabaseNumericAggregateAccumulator migrates its SQL callers to
`init(inputType: QueryColumnType, workMeter: DatabaseWorkMeter)`,
`add(_: QueryValue)`, `sum()/average() -> QueryValue?`. It owns widened exact
signed limb accumulation, non-NULL count and maximum input displayScale, plus
selected floating width. It validates only the final rounded numeric width,
allowing cancellation sums and representable AVG through wider intermediates.
All-null/empty returns nil for the caller's already-bound typed NULL column.
`CanonicalQueryTypeBinder.numericAggregateInputType(_:)` owns single-operand
aggregate type selection for both binding and incremental reduction. Composition's
local operand projection can retain primitive column metadata even when its
aggregate result is bound by the canonical SQL binder. The accumulator records
that source declaration, validates its primitive carrier, and consumes the
binder's existing normalization into the selected SQL input type. This route
uses the same SQL SUM/AVG result contract for SQL text and direct SelectQuery IR;
selection is never inferred from row values. Selected SQL declarations retain
strict carrier validation and rich numeric displayScale. This boundary does not
change standalone primitive/RDF evaluation or SQL source-carrier alignment.
Existing non-SQL consumers, if any are found by the foundation migration,
retain their exact owner contract through a separate explicit path rather
than silently acquiring SQL promotion.

Variable limb/math workspace is operation-local and pre-admitted through the
same DatabaseWorkMeter before capacity growth. The owner charges actual
retained capacity and scratch, consumes work/checkpoints at bounded iteration
boundaries, and releases on success/failure/cancel/destruction. Operational
limits come from existing validated work/memory/deadline admission; do not add
unmeasured constants. Base's fixed aggregate-state reservation covers only
fixed container state, not dynamic limbs. Partial arithmetic never publishes
an output or converts resource/cancellation failure into numeric overflow.
Use typed arithmetic/domain failures and preserve meter failures and decisive
occurrences through CanonicalReadError; new domain categories extend the
existing error owner and its server mapping in the same batch.
The existing DatabaseExpressionEvaluationError owns numericDomain(operation:),
numericUnderflow and nonFiniteValue alongside divisionByZero/numericOverflow.
CanonicalReadError.expressionEvaluation carries these unchanged with the
optional decisive Kit occurrence; the [Read error contract](../Read/DESIGN.md#s04-decisive-scalar-failure-occurrence) owns that public shape. Server maps
these numeric categories to explicit invalid-request codes; S16 still owns
SQLSTATE and remote occurrence details. Selected accumulator methods use
untyped throws so work/deadline/cancellation failures are never converted into
its nested numeric Failure cases. The SQL SUM/AVG entry consumes QueryValue
before the primitive-only aggregate path; DISTINCT reduces canonical numeric identities in a pre-admitted operation-local
set while adding the original selected QueryValue, preserving its displayScale.
NULL inputs are skipped and empty/all-NULL output uses the bound typed NULL.

S04-C01–C12 assign binder tests to selected metadata/early rejection, scalar
kernel tests to exact values/scales/domains, aggregate tests to final-width and
NULL matrices, and actual Database SQL tests to text→binding→execution and
empty declarations. Resource tests prove exact/one-past/cancel/deadline and
last-owner release for scalar, ordinary aggregate and Base paths. Existing
raw exact-division, RDF, source alignment, index/scan equivalence, persistence
and retained-owner suites remain regressions. Two heavy build lanes maximum;
focused behavioral tests precede one converged affected/full package run.

### S05 selected string, pattern, binary and cast execution

Consume the [S05 SQL semantic authority](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s05-frozen-string-pattern-binary-and-lexical-conversion-semantics).
This contract supersedes the preceding S04 deferral of typed-text numeric
displayScale/typmods and the old primitive LIKE-only selected path.

```text
semantic expression + schema/parameter declarations
  -> binder: exact children/result/nullability/cast context/occurrences
      -> CanonicalRows: evaluated QueryValues and retained source owners
          +-> CanonicalSQLStringEvaluator: F04
          +-> CanonicalSQLBinaryEvaluator: F06 and BYTEA overloads
          +-> CanonicalSQLScalarCast / ScalarText: conversion and type output
          +-> SQLPatternMatching: immutable program + task-local iterator
              -> declared QueryValue validation + retained destination

compiled field queryType + INSERT/UPDATE expression
  -> assignment binding before source scan / any mutation
      -> same selected conversion contract, CastContext.assignment
          -> existing model adaptation and atomic mutation owner
```

| Internal owner/entry | Contract |
|---|---|
| CanonicalQueryTypeBinder / CanonicalBoundExpression | Static catalog selects overload, exact input/result types, nullability and CastContext. Binding validates all declarations including typed NULL before reading. Runtime does not reselect from payload values. |
| CanonicalSQLStringEvaluator.evaluate(bound, operands, workMeter) | F04 selected scalar inputs to DatabaseQueryScopedFieldValue. Strict functions propagate NULL; CONCAT variants use their explicit non-strict rules. |
| CanonicalSQLBinaryEvaluator.evaluate(bound, operands, workMeter) | F06 and selected BYTEA length/subsequence/concat to the same scoped primitive result. No duplicate binary algorithms in the string evaluator. |
| CanonicalSQLScalarCast.evaluateValue(value, from, to, context, workMeter) | Fixed/rich results as QueryValue, preserving NUMERIC displayScale and using shared Kit lexical constructors. |
| CanonicalSQLScalarCast.evaluatePayload(value, from, to, context, workMeter) | Text/CHAR/VARCHAR/BYTEA results as DatabaseQueryScopedFieldValue, preserving a reservation through parent publication. |
| CanonicalSQLScalarText.withText(of, type, preservingCharacterPadding, workMeter, body) | Synchronously lends a scoped Substring for admitted type-output. Formatted temporary owners are pre-admitted and live through body. No borrowed view escapes. |
| SQLPatternMatching | Child owns grammar, flags, search/capture priorities, replacement/split algorithms and operation-local scratch. Parent passes explicit escape/flags/operation and retains inputs/results under the same work meter. |
| CanonicalSQLPatternEvaluator | Child-local selected adapter for the frozen F05 scalar names, patternMatch and regex SUBSTRING; consumes bound metadata/operands/meter/value limits and returns QueryValue with an optional retained payload lease. It does not admit additional REGEXP names or public row composition. |
| SQL comparison/identity | A selected QueryColumnType accompanies SQL values at comparison and hash boundaries. CHAR ignores trailing U+0020 consistently in order/equality/group/DISTINCT/join/aggregate and conditional/quantified comparisons. Primitive/RDF byte identity is unchanged. |
| Index planning | Pushdown is permitted only if persisted key identity/order is equivalent to the selected SQL comparison. Otherwise use the canonical path; an index name is not evidence of semantic equivalence. |

Each variable payload output is measured with checked arithmetic before
allocation and constructed under DatabaseQueryScopedFieldValue.producing or an
equivalent existing retained destination builder. Text kernels use codepoint,
UTF-8 and byte views rather than grapheme arrays or intermediate String arrays.
BYTEA producers use the existing `ByteString.copying` exact-capacity contract
owned by DatabaseTypes Bytes. Their pre-admitted byte count therefore equals
the copied owner's retained capacity. This assumption does not apply to the
Array/COW constructors, slices or owners with unknown capacity. The observed
nested `encode(decode(...), ...)` three-byte output retaining sixteen bytes is
the regression that must reject an Array-backed copying implementation.
Measurement and writing passes both charge work/checkpoints. Immutable input
owners and claims survive every capture/slice; scratch ends with its operation.
Lease composition requires identical DatabaseWorkMeter objects. No global
mutable regex cache or target-specific synchronization weakening is introduced.
No new operational quota is guessed: existing work/memory/deadline and Kit
bounded value limits own admission. SQL descriptor maxima are intrinsic type
validation, not allocation permission.
Same-base CHAR common-type selection drops the typmod through a distinct
implicit CAST node retaining the original child's signature. Its typmod-free
char(nil) result borrows the physical input without length conversion. Explicit
and assignment bare CHAR still apply length one; retyping the original CAST
would incorrectly change that operation and truncate its result.

The selected comparison boundary includes extrema, CASE/NULLIF, quantified
comparisons, ordering and all relational hash keys; trimming one filter branch
alone does not satisfy T05. The Mutation component owns the real INSERT/UPDATE
assignment and existing UPDATE/DELETE predicate caller connection using
FieldSchema.queryType, including empty source binding and no mutation on failure.
Existing statement predicates consume this same selected scalar path, so
codepoint and C/POSIX behavior does not depend on whether a statement reads or
mutates. New DML forms, dynamic catalogs and RETURNING remain their planned sprints.

Semantic traversal includes both concatenate operands and subject/pattern/escape
pattern operands, with occurrence roles operand 0/1/2 respectively. Source
qualification and field authorization may not omit dynamic pattern/escape
columns. Fusion preflight visits these children through its existing admitted
stack; SQL/SPARQL rewriting preserves their positions through the existing
Mutex-owned occurrence ledger. This adds no shared state or isolation branch.

Raw DatabaseExpressionEvaluator pattern callers retain existing string/NULL
failures and consume the same SQL pattern child. Graph programmatic LIKE
migrates to case-sensitive LIKE with escape disabled; Graph export must carry
that case sensitivity. SPARQL regex keeps its separate XPath language. Graph
forms whose semantics cannot be guaranteed fail explicitly rather than being
silently translated.
Binding retains a CHAR declaration for the subject of a pattern operator,
and the child adapter requests padding-preserving type output for that operand.
Pattern/escape operands and text-declared regex function inputs retain ordinary
text coercion. CHAR comparison trimming is not a pattern-matching rule.

CAST lexical/length failures use the existing invalidCast contract. Function
domain failures use invalidFunctionArgument(function:reason:); invalid and
unsupported SQL patterns retain distinct typed failure categories and source
offsets through the expression/read error owner. Resource/deadline/cancellation
errors propagate unchanged. Server maps newly admitted failure categories;
SQLSTATE remains S16. Original input is charged before shared lexical parsing,
and output formatting never uses description or a default value fallback.

S05-C01–C10 assign constructor/model/Wire evidence to Kit, grammar/source and
serializer evidence to QueryAST, kernel/iterator/resource/identity evidence to
this component and child, assignment state evidence to Mutation, and public
SQL/persistence/transport failure evidence to their actual callers. Scalar
regex arrays use existing QueryArrayValue without an S07 prerequisite. Public
REGEXP_MATCHES/SPLIT_TO_TABLE composition is deliberately still a required S10
gate; S05 does not claim their end-to-end completion. One final comprehensive
review and concrete finding-limited recheck follow coherent implementation;
focused then affected/full lanes run against the frozen local task graph, with
the user-deferred URL/FDB/Graph Database portable matrix in S18.

#### S05 context-free selected scalar entry

Internal `DatabaseContext.evaluateCanonicalScalar(bound, on: CanonicalSourceRow,
workMeter)` consumes the Mutation owner's admitted positional frame and returns
`(value: QueryValue, leases: [DatabaseQueryPayloadLease])`. It invokes the same
selected recursive evaluator as SELECT, with no DatabaseContext or relation
execution context. The entry rejects any bound aggregate, subquery or relation
operation before evaluation, including unexamined conditional branches. It
never creates a context, target session or authority. SELECT's existing instance
wrapper supplies its existing context only to the relation/aggregate branches.
The Mutation owner retains returned leases through storage encoding and final
prepared-change admission. Nested casts, strict NULL, lazy conditionals,
selected signatures, innermost failures and the work meter therefore have one
implementation across both callers.

The selected CAST adapter dispatches only S05 scalar destinations. Primitive
destinations retain their declared width and identity; NUMERIC conversion back
to a primitive integer requires an integral, exactly representable value and
reports range overflow. SQL NUMERIC results retain their rich representation.
Explicit bare CHAR still executes its default length-one conversion when its
input has the same typmod-free CHAR declaration. Existing temporal CASTs remain
on the primitive evaluator path; this does not admit S06 precision conversion.

# QueryExecution

## Purpose and Scope

The QueryExecution component owns the DF-06F0 retained polymorphic aggregate,
the DF-06R retained regular-model fetch, and the only operations that may
transform retained values into canonical rows, index rows, or an explicit
final public output.

- Parent: [DatabaseEngine](../DESIGN.md).
- Child: [SQLPatternMatching](SQLPatternMatching/DESIGN.md), the bounded SQL pattern language implementation.
- Child: [SQLTemporal](SQLTemporal/DESIGN.md), the S06 temporal evaluation design; S06 contract and completion are tracked by the workspace progress record.
- Child: [SQLArray](SQLArray/DESIGN.md), selected array algorithms and bounded row cursors.
- Child: [SQLJSON](SQLJSON/DESIGN.md), lexical JSON and semantic JSONB algorithms and bounded row cursors.
- Child: [SQLRelations](SQLRelations/DESIGN.md), S08 demand-driven SELECT producers and execution-local CTE cache lifecycle.
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
| [SQLTemporal](SQLTemporal/DESIGN.md) | child | Temporal evaluation and explicit clock/zone environment | Owns T07/T08/F07 execution design. | Consumes the explicit operation-owned environment; no ambient clock/zone. |
| [SQLArray](SQLArray/DESIGN.md) | child | Selected array evaluate/cast/compare and noncopyable row cursor | Implements the array layer over existing selected scalar policy. | Parent owns types, lazy selection and every referenced input claim. |
| [SQLJSON](SQLJSON/DESIGN.md) | child | JSON conversion/extraction, JSONB compare/edit and noncopyable row cursor | Implements the JSON layer over the existing Kit owners. | Semantic identity is distinct from Kit representation hashing. |
| [SQLRelations](SQLRelations/DESIGN.md) | child | Demand-driven producer, independent CTE consumers and awaited close | Owns S08 producing state and retained prefixes. | Binding, selected comparison and resource readiness remain with this parent and Read. |

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
invariants remain in DatabaseTypes. These are staged implementation contracts; S01/S03 already provide positional
rows and executable binding. The frozen S08 section below owns the remaining
relation changes and does not claim their implementation or verification.

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

`bindProjection` binds ordered projection items into the existing bound SELECT
carrier with empty clauses. The relation binder calls that projection-only seam
before binding its own clauses, so nested scalar/ARRAY query callbacks do not
retain an unrelated full-SELECT clause frame. Full `bindSelect` reuses that same
projection implementation. Each phase retains its actual array-capacity claim
through the existing binding metadata owner; parameter finalization remains at
the full selected-plan boundary. Direct scalar, ARRAY, EXISTS and quantified
query expressions dispatch to the existing narrow subquery binders before
entering the unrelated scalar-operator switch, with the same located error and
parameter-finalization boundary.

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
values explicitly. `bindSourceValue` marks only a selected common-source cast
with `sourceAlignment`; its evaluator calls `CanonicalSQLScalarCast.alignSource`.
That seam preserves the existing exact primitive-to-numeric normalization and
otherwise executes the existing implicit conversion. In particular, common
CHAR without a typmod preserves physical padding. This marker is not an SQL
CAST context: explicit and assignment CAST retain their own rounding, typmods,
and bare-CHAR default length. The input owner and any returned payload lease
remain retained by the relation producer. Scalar expressions still reject deferred exact/floating
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

#### Binding-plan footprint traversal

The prospective binding-plan meter traverses expressions, sources and SELECT
statements with one operation-local LIFO work array. Configured valid query
depth must not consume one native call frame per syntax node. This traversal
does not change structural limits, the prospective 1024-byte allowance per
visited node, or the existing depth-first visit order and work-unit charges.
VALUES cells retain their existing byte-only allowance rather than becoming
additional expression visits.

The work array references the immutable input syntax; it creates no payload
copies or shared mutable state. Its container and capacity are admitted through
the same work meter at `bindingCandidate`, using `DatabaseRetainedArrayLayout`.
Growth is admitted before allocation and actual capacity is reconciled before
append. Popping an entry does not release retained capacity. One scoped
reservation releases all traversal storage on success, overflow, cancellation
or admission failure, before the caller reserves the final binding plan. This
temporary storage consumes bytes, not relation-row quota.

The Server `configuredStructuralLimitsReachSPARQLCompiler` behavioral test
owns the demonstrated valid-depth regression. Focused plan-budget/release
tests and exact ordinary/Embedded runtime cases verify the changed traversal;
unchanged collection kernels retain their existing evidence.

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

### S07 selected collections and relational identity

The semantic authority is [Kit S07-C01...C12](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s07-frozen-array-and-json-contract).
The lower algorithm/resource interfaces are owned by [SQLArray](SQLArray/DESIGN.md)
and [SQLJSON](SQLJSON/DESIGN.md); syntax and canonical emission remain
[QueryAST](../../QueryAST/DESIGN.md). This section owns their composition, not
a second semantic table. No new value carrier, persistence envelope, scalar
ROW, dependency package or ambient runtime service is introduced.

```text
QueryAST / Wire expressions and declared source schema
    -> CanonicalQueryTypeBinder: selected overload / coercions / capabilities
        -> CanonicalBoundExpression: selected children and occurrence edges
            -> Rows: evaluate selected children and retain producer leases
                +-> SQLArray / SQLJSON -> admitted value + destination lease
                +-> selected comparison -> metered child comparator
                +-> admitted canonical identity -> metered hash/probe table
            -> bound.normalize -> retained positional/result destination
```

#### Binding and selected execution

| Owner | Required behavior |
|---|---|
| Binder | Select array element common types across scalar leaves, contextual empty types and registered function/operator signatures before source reads. Distinguish unknown NULL overloads from typed NULL elements. Rank is value metadata. Existing numeric/character/temporal selection remains authoritative. |
| Binder capability | Reject equality/order on JSON, including arrays of JSON, before reading even an empty relation. Apply capability checks to comparisons, DISTINCT, GROUP, set equality, aggregate DISTINCT/min/max and ORDER BY as appropriate. JSON extraction/conversion does not require equality. |
| BoundExpression | Rebuild ARRAY, access groups, collection operators and function calls from selected children. Preserve omitted bounds, finalArrayIsVariadic and source occurrences. Never return the original uncoerced ARRAY tree as a substitute. |
| Rows | Dispatch selected ARRAY/access/F08 to SQLArray and JSON/F09/F10 to SQLJSON. Existing subtract/concatenate overloads dispatch by bound declarations. Compute one QueryValueLimits from operation structural limits and budget, and pass the same temporal environment/meter. |
| Scalar conversion | Selected rich casts delegate to the owning child; scalar cell casts reuse the current scalar caster. Rich text output delegates to the owning child rather than FieldValue.description. Pure unknown constant failures remain pre-read; context-dependent temporal conversion retains its existing runtime boundary. |
| Result placement | Combine every referenced child lease with the optional new destination lease before normalization and publication. A nil destination lease never authorizes releasing aliased input. Preserve innermost occurrence wrapping and lazy branch selection. |
| Language visitors | Walk every actual child under Kit's occurrence layout; preserve explicit variadic metadata during rebuilds. SQL-only forms reject through existing SPARQL/raw-expression capability boundaries instead of becoming opaque terminals or plausible success. |

VARIADIC expansion belongs to the selected registered signature. The final array
is not expanded by the parser, serializer or generic function materializer.
Fixed-plus-path arguments and homogeneous variadic builders retain selected
types, SQL NULL/empty distinctions and limits. Unknown names or unsupported
variadic signatures fail before reads. Existing zero-argument ordinary builders
remain distinct. Explicit scalar ROW and ARRAY(subquery) relation construction
are mandatory S08 work; general cursor invocation with FROM/LATERAL/ordinality
is mandatory S10 work. S07 implements and tests the bounded lower cursors, and
rejects these unimplemented public composition paths before reads with the
required incomplete-implementation markers. S08 replaces only its ROW and
ARRAY(subquery) gates under the frozen contract below; S10 relation-function
gates remain until their own behavior is implemented. These gates preserve
the fixed inventory rather than remove features from it.

#### Metered SQL identity

CanonicalSQLValueIdentity adds an internal prepared collection case for selected
array/JSONB values. Preparation measures and writes an unambiguous flat byte key
under the operation meter, admits the final ByteString before allocation and
retains its reservation plus the original typed value for ordering. Lengths,
kind tags and boundaries are explicit. Numeric keys omit displayScale and use
canonical ExactDecimal value; character and temporal cells use the existing
selected scalar identity, including CHAR padding and interval/TIMETZ identity.
Array keys include dimensions/bounds and cell NULL markers. JSONB keys preserve
canonical member order, scalar kinds and semantic numbers. JSON is never given
a fabricated equality key. Primitive/RDF identity and CanonicalRowFingerprint
remain unchanged.

The cached digest selects a bucket and does not establish equality. Rich keys
must not enter standard Set/Dictionary callbacks that perform unmetered token
scans. A task-local CanonicalSQLIdentitySet uses fixed-size digest lookup plus
explicit throwing, metered candidate equality. Its row-key and single-key
insert/contains paths retain complete key owners, reserve table growth before
allocation, and charge collision scans before exact comparison. GROUP's
existing open-addressing probe likewise becomes throwing/metered. Duplicate,
failure, cancellation and table destruction release claims at their actual
last-owner boundary. Forced digest collisions must still distinguish values.

Token byte ordering is not SQL ordering. GROUP tie ordering passes the explicit
meter/environment and uses the prepared original declarations with the same
SQLArray/SQLJSON comparator as ORDER BY. Stored-key pushdown and the existing
primitive hash-JOIN optimization exclude selected arrays/JSONB until their
key equivalence is proved; the normal selected nested JOIN remains executable.
S15 owns index equivalence. This selection does not reinterpret primitive
arrays, RDF values or physical stored key bytes.

#### Typed collection failures

QueryExecution owns one public SQLCollectionError, carried by the existing
DatabaseExpressionEvaluationError.collection case. Child kernels do not define
competing error enums. Exact cases are:

| Cases | Meaning |
|---|---|
| invalidArrayShape; arraySubscriptOutOfRange | Incompatible shape/rank or checked SQL subscript arithmetic failure. Ordinary missing/out-of-range reads remain NULL/empty under Kit semantics. |
| invalidArrayInput(offset: UInt64, reason: String) | Kit malformedArrayInput mapped with the original UTF-8 offset. |
| invalidArrayArgument(function: String); nullArraySubscript(function: String) | Selected rank/argument failure and the specified reached NULL start argument. |
| jsonArrayRootRequired(function: String); jsonObjectRootRequired(function: String); jsonContainerRootRequired(operation: String) | A function or mutation requires that root shape. Extraction's normal missing/wrong-shape NULL is separate. |
| oddJSONObjectArgumentCount(actual: Int); nullJSONObjectKey(argument: Int); invalidJSONObjectKey(argument: Int) | Object-builder arity and selected key constraints. |
| multidimensionalJSONMutationPath(rank: Int); nullJSONMutationPathElement(position: Int); invalidJSONMutationPathIndex(position: Int, text: String); existingJSONInsertKey(key: String) | Mutation-only path failures, raised only when the corresponding path step is reached. |

Argument/path positions in these causes are one-based SQL positions; lexical
UTF-8 offsets are zero-based. They are separate from outer source occurrences.

Other intrinsic QueryValueError, scalar, numeric, work-limit, cancellation,
deadline and temporal failures preserve their original typed cause. Internal
signature/ROW arity mismatches retain existing typeMismatch. Server maps the
collection wrapper to a nonretryable invalid-request error; SQLSTATE remains
S16. Neither malformed input nor failed admission becomes NULL, empty rows or
an unchanged JSONB result.

The existing expression failure algebra also carries queryValue(QueryValueError).
Binder and Rows attach occurrences through this case when an intrinsic value
failure crosses their boundary; direct intrinsic/child callers retain the
original QueryValueError. Work/cancellation failures are not value failures.
Server maps the intrinsic wrapper to INVALID_QUERY_VALUE, invalidRequest and
never retry, preserving the typed cause description through source wrappers.

#### S07 completion evidence and change impact

| Case owner | Decisive proof |
|---|---|
| S07-B01...B04 / Binder and Bound | Actual public parsing/binding, typed empty/nested ARRAY, every selected overload and VARIADIC mode, native-array access/VARIADIC canonical coercion with nullable source slots preserved, empty-relation capability failures and source occurrence. |
| S07-C01...C12 / children | Child designs' native and separately identified ordinary/Embedded runtime success, malformed/NULL/boundary/cancel/deadline/lifecycle evidence. |
| S07-I01...I04 / identity | Numeric display variants, CHAR/temporal array cells, dimensions/NULLs, JSONB semantic keys, forced collision, DISTINCT/GROUP/set/JOIN results and metered sorting. |
| S07-R01...R03 / result composition | Exact/one-past row/byte input/output/scratch budgets, retained full external owners, independent rows after cursor close, failure cleanup and a successful subsequent operation. |
| S07-P01...P03 / public consumers | SQL executed results and exact metadata, persisted native-array access/VARIADIC and rich-value roundtrip, canonical text roundtrip in the same environment and typed Server failures. |

Source owners are disjoint: Kit values/IR/Wire; QueryAST syntax/emission;
SQLArray; SQLJSON; Binder/Bound/scalar dispatch; selected identity helpers;
other language/security/Fusion visitors and public failure relay; Rows/public
integration. Each owner verifies its changed target and behavior after the
coherent source batch. One comprehensive review and finding-limited recheck
precede the single affected/full sprint lanes and local commits. Existing
user-deferred URL, FoundationDB and Graph-enabled portable integration remain
S18; their absence is never reported as S07 coverage.


### S08 frozen relational execution

**PLANNED; not implementation or verification evidence.** This section owns
executable binding, selected keys and positional relation composition. It
consumes [Query S08](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage),
[SQL composite values](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership)
and [Wire S08](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s08-relational-query-carriage)
as the lower authorities; their declarations, layouts and capacity APIs are
not redefined here. [SQLRelations](SQLRelations/DESIGN.md) owns the new producer
and CTE state machine. Existing S07 iterative admission is extended along the
new named edges; it is not replaced by recursion or a generic visitor.

Confirmed migration inputs are `CanonicalQueryTypeBinder`,
`CanonicalBoundExpression`, `CanonicalBoundSelect` and
`DatabaseContext+CanonicalRows`: schema binds before rows, source rows are
positional and retained, LATERAL and nested queries share parent transaction
and meter, and Collecting-to-Ready follows authoritative close. Remaining
source gates and eager materialization are not conformance evidence.
[Accepted PostgreSQL18.6 observations](../../../../.codex-tools/sql-s08-evidence/accepted-summary.json)
and [judgments](../../../../.codex-tools/sql-s08-evidence/judgments.json)
fix the observed dialect choices; the targeted NULL correction supersedes only
its identified ambiguous cells. Product behavior still requires the cases below.

```text
Kit query + schema/authority lease + parameters + occurrence arena
  -> Binder: complete scopes, descriptors, selected comparisons/keys/counts
  -> BoundSelect: output occurrences, source plan, CTE declarations
  -> SQLRelations: one invocation / demand / retained CTE prefixes
       -> selected scalar/composite kernels + blocking physical inputs
       -> retained positional rows and selected-key frames
  -> close every producer/cache -> existing Read/session/transaction close
  -> existing Collecting -> Ready -> public or retained result
```

#### Binding, descriptors and source presence

| Seam | Frozen responsibility |
|---|---|
| `CanonicalRelationSchema` / `CanonicalSourceLayout` | Retain ordered slots, exact names/types/nullability, visible wildcard indices, qualified side slots and lexical frame scopes. Add a bound source-presence address for each whole-row-visible source. Presence is not inferred from field NULLs. |
| `CanonicalSourceRow` | Carry positional cells and per-source presence with the same retained layout/payload owners. A produced source row is present even if every cell is NULL; outer NULL extension marks that source absent. Alias of a joined group exposes its own present row and hides constituent qualifiers; extending that alias marks the whole group absent. |
| Binder / `CanonicalBoundExpression` | Bind a whole-row descriptor and ordered field slots, named field ordinal, selected comparison mode, expansion context and collation. Retain each original occurrence independently. An ordinary column name resolves columns before whole-row fallback; explicit wholeRow resolves the exact visible source without that fallback. Local qualifier presence prevents a missing local field from falling through to the same outer qualifier. |
| Bound SELECT output | Expand mixed wildcards in place. Leading source/CTE column aliases replace only supplied positions; excess aliases and ambiguous references fail before reads. Partial aliases preserve remaining names/types, including zero rows. Constructor fields receive f1...fN; wholeRow and selected composite fields preserve their actual named descriptors. |
| VALUES / aliases | Bind every expression cell in row-major order and choose each column common type before evaluation. Evaluate each row with its admitted outer scope; ordinary derived sources cannot acquire left-sibling correlation, while LATERAL receives left then enclosing frames. Preserve values([]) and values([[]]) as distinct zero-column relations. |
| Join plan | Validate ON/USING once, select key coercions and exact equality, and retain qualified originals. USING emits its listed keys then remaining left/right visible slots; NATURAL derives shared keys from visible left declarations in their existing order. Empty common keys mean Cartesian product. Duplicate/missing keys fail statically. LEFT/RIGHT/FULL apply slot nullability and source presence together, including empty inputs. |

The existing IN-subquery binder computes and discards a common type and its
executor casts members toward the original operand type. Replace this path
with one selected comparison signature for both operands, including implicit
coercion legality; decimal members cannot be rounded to integers merely to
compare them.
Selected positional alignment, USING output and comparison conversion call
`CanonicalSQLScalarCast.coerce(_:from:to:temporalEnvironment:workMeter:limits:)`.
Its assumption is a previously admitted implicit source/target pair. It
executes the existing scalar, payload, array/composite and temporal kernels
under the caller's environment and meter, returning the value and optional
producer lease. Equal declarations borrow their input owner; callers retain
that owner and every returned conversion lease through publication. Result
normalization validates a selected result and never substitutes for conversion.
 Scalar subqueries bind arity before reads and retain zero-row
NULL / multiple-row cardinality failure. ROW-constructor comparison may bind
a multi-column subquery to its required row arity; ordinary scalar contexts
still require one output column. Conditional branches remain fully bound and
only required branches execute.

SQL schema-qualified relation execution remains S11's namespace/catalog gate;
S08 retains its explicit pre-read failure. Parsing a qualifier never selects a
Partition, Tenant or authority. General SRF/table-function composition and
recursive CTEs remain S10, grouping/windows S09, and persistence of composites
remains explicitly rejected by the lower value contract.

#### Selected composite and collection execution

- ROW construction produces the Kit composite with its bound ordered descriptor.
  Direct list fieldExpansion expands only in the contexts admitted by Query;
  scalar/function/order/nested positions consume one composite. An expansion
  evaluates its producing expression separately for each selected field, as
  required by the SQL semantics; it does not invent common-subexpression reuse.
- Whole-row absence produces primitive SQL NULL with its composite declaration.
  A present all-NULL composite remains a value. ROW(r.*) constructs a present
  anonymous record even when r is absent. Field selection from typed NULL yields
  typed NULL; duplicate field names are ambiguous and missing fields fail.
- Bind constructor comparisons separately from composite-value comparisons.
  Constructor equality examines all field pairs: a decisive unequal pair is
  false, otherwise a NULL comparison yields UNKNOWN. Constructor ordering stops
  at the first unequal or NULL deciding pair. Stored/whole composite comparison
  uses record semantics: corresponding NULLs equal, and a NULL field sorts
  greater than a non-NULL field. Arity, field comparison capability and selected
  field types are checked before reads, including nested composites and arrays.
- IS NULL is true for SQL NULL or a present composite with every direct field
  NULL; IS NOT NULL requires a present composite with every direct field
  non-NULL. Mixed fields make both false. IS DISTINCT FROM NULL tests whole-value
  presence; it does not collapse an all-NULL present row. Null-safe composite
  equality, set/group identity and record ordering use their selected SQL mode,
  not Kit representation equality or constructor UNKNOWN.
- SQLJSON consumes the real descriptor and selected fields through its existing
  convertRow kernel for ROW_TO_JSON/TO_JSON/TO_JSONB, including nested composite
  leaves. Preserve strict SQL NULL, named versus anonymous keys, duplicate-name
  JSON/JSONB rules and every input lease. SQLArray consumes the same flat Kit
  views; no primitive object or JSON intermediary represents a ROW.
- ARRAY(subquery) consumes exactly one bound column in query order. Scalar cells
  form one dimension; array-valued cells add outer lower bound 1 while preserving
  every inner dimension and lower bound. Inner rank, lengths and lower bounds
  must match; NULL or empty subarrays fail explicitly. Zero input produces a
  typed canonical empty array, not NULL. Six-rank/overflow/shape limits remain
  cumulative. Use one postorder QueryValueBuilder with the lower capacity seam;
  reserve construction, complete retained input and output before growth, then
  reconcile actual storage reports and attach destination leases.

#### Selected keys, sets and pagination

| Boundary | Frozen execution contract |
|---|---|
| Binary set node | Bind both complete query operands at that node, in their lexical scopes, before reads. Select arity/common types positionally; preserve left labels and each operand's local ORDER/pagination. UNION ALL concatenates bags; INTERSECT ALL emits min(left,right) occurrences and EXCEPT ALL max(left-right,0); DISTINCT emits at most one SQL-equal row. NULLs compare equal for multiplicity. Admit counted hash/probe state and exact collision comparisons under the same meter. |
| SQL key frame | Bind each ORDER/DISTINCT key to a source expression or one designated projection target occurrence. Save that target's selected value and leases once and reuse them for ordering/output; clock_timestamp AS x ORDER BY x must not execute twice. Distinct projection occurrences are never merged by expression equality. Unreferenced targets remain demand-evaluated after ordering/pagination; DISTINCT-required values evaluate before reduction. Prefix matching separately compares resolved bound source slots and selected operator/cast/collation structure; semantic prefix equivalence never changes the projectionIndex used for value caching, and independent volatile target occurrences are not equivalent. Duplicate output labels use this same selected equivalence check and select the first matching target without merging target evaluation. Nested subquery volatility is classified by the relation owner through `isVolatileSubquery`; pure subqueries retain exact QueryIR matching. Standalone binding without the relation classifier cannot equate independent nested plans; operational relation binding supplies both that classifier and its work meter. The comparison uses metered temporary pair storage and releases it before returning. |
| Names and ordinals | Preserve existing SQL ORDER output-name priority, GROUP input-name priority, ambiguity rules and bare positive one-based ordinal binding. An explicit cast constant is an expression, not an ordinal. Set-envelope ORDER binds only admitted output names/ordinals; local operand names do not leak. |
| SQL sorting | Omitted NULL order is ASC NULLS LAST / DESC NULLS FIRST. Explicit NULL order wins. C/POSIX use the selected binary text contract. Apply the SQL correction only to SQL selected keys; retain RDF/unbound and generic comparison semantics. Pushdown requires proven equivalent selected type, NULL order and collation. |
| Peers / DISTINCT ON | Peer equality uses only all declared ORDER keys, their selected types and collations. Continuation/row fingerprints and incidental deterministic tie breakers never enter peer equality. DISTINCT ON accepts a reordered or shorter matching initial DISTINCT-key prefix; introducing a non-DISTINCT key before an unmatched DISTINCT key is invalid. Keep the first row of each selected key group after ordering; without ORDER, no additional ordering guarantee is created. |
| QueryPagination | Bind SQL bigint coercion before reads; reject source-row refs, aggregates/windows, incompatible types, negatives and overflow. Evaluate admitted offset then limit once per query invocation with allowed correlation. NULL offset means zero; omitted/ALL/NULL limit is unlimited, except the accepted bare unknown NULL WITH TIES failure. Explicit NULL::bigint WITH TIES remains unlimited. Fixed UInt64 above Int64.max fails only in SQL; existing non-SQL unsigned domain is preserved. |
| FETCH WITH TIES | Require ORDER, return zero for count zero, apply offset first and retain every row peer with the final selected row under all ORDER keys. A probe row outside the group is not published or retained unnecessarily. Blocking key computation may read input, but output projection/CTE demand cannot fabricate extra volatile row evaluations. |
| Explicit physical access | An admitted single-table index hint preserves the same selected projection, DISTINCT ON, sorting and expression-pagination result as its scan equivalent. When physical pushdown cannot prove equivalence, retain the logical clauses and finalize admitted candidates through the selected pipeline. Resolve counts once before candidate reads; LIMIT zero still validates the requested path and metadata without scanning data. |

#### Occurrences, resources and completion evidence

Consume Query's exact named edges and canonical order: CTEs, projection,
source, filter, groupBy, having, DISTINCT ON, orderBy, offset, limit. VALUES row
and cell IDs, alias source, binary set query children and composite operand
edges remain distinct. Expanded targets retain explicit origins to their
original fieldExpansion occurrence; no structural-equality source search or
untyped metadata walk substitutes for provenance.
`bindAssignment` accepts either a raw expression with an explicit occurrence
or an already bound child. The bound-child form applies the selected
assignment cast without rebinding its tree or occurrence.
`bindOrderKey` accepts a key-edge occurrence: source expression binding uses
that edge, while a projection reference preserves the target expression's
original occurrence and stores the key edge separately on CanonicalBoundKey.
 Preserve the existing S07
bounded iterative prepass, full-owner measurement and shared-state contract on
Native, ordinary WASM and Embedded.

Source descent retains only its dispatch/binding prelude. Per-kind source
selection, CTE scope preparation, VALUES cells and post-source SELECT clause
finalization use separate private frames so recursive aliases/subqueries do
not carry unrelated branch temporaries. The existing
structural nesting limit, typed rejection and work/metadata admission remain
unchanged; the Native worker stack is not enlarged to admit valid queries.
Recursive source dispatch uses direct typed catches; its shared non-generic
failure decorator runs only when an error is thrown. It preserves the same
decisive source occurrence without retaining a generic result-bearing error
wrapper or capture frame on successful descent.
JOIN/SET descent binds its children before entering common-type/output
finalization. Projection binding uses the Binder's projection-only entry in a small prefix;
other clause binding and output/source-plan assembly run in separate frames
only after that prefix returns. Each expression-bearing clause isolates its
binding temporaries, and subquery callback environment construction completes
before descending into the child query. Group validation runs afterward. Scalar-subquery callbacks can reenter query
binding, so their complete active chain is included alongside source, JOIN,
SET and CTE chains in the Native depth witness and compiled-frame evidence.

Selected expression evaluation also owns a cooperative scheduling boundary:
it yields before descending into a selected node and before publishing that
node's value, checking the same request meter on each resume. The selected
body, conditional evaluation order, temporal environment and source context
remain in the same Task. The returned value and payload leases remain owned
through publication; failure releases them through the existing parent scope.
This bounds fast-path async continuations across child evaluation and successive
projection columns within one producer pull. The S05 UUID/bytea eight-target
public SELECT is the concrete ordinary-WASM regression witness; producer pull
scheduling alone does not bound this expression-level chain.

Bound descriptors/key frames, projection values, presence masks, multiset
counts, sort/peer state, producer state, retained CTE prefix, construction
scratch and root output are charged before allocation/growth on the exact
parent meter. Checked arithmetic and measured capacity reconciliation precede
publication. Long scans/probes/expansion/flat value walks checkpoint work and
cancellation. Typed intrinsic, binding, cardinality, shape, budget, deadline,
cancellation and cleanup errors preserve their decisive occurrence and cause;
no partial row, false EOF, synthetic NULL or old-layout fallback is success.

A selected JOIN reserves its merged payload's row credit and structure bytes
before allocating destination cells. The same layout/payload lease retains
that credit. The single-row publication container owns its separate retained
row credit; its later admission does not substitute for payload admission.

`CanonicalBindingMetadata` owns only the admitted descriptor-construction
reservation shared by binder copies. Its optional reservation is protected by
one common Mutex on every target; it caches no type/value, expression, source
plan, session or producer. Construction and common-type factories reserve
before allocation, reconcile observed type storage and retain that claim in
the resulting bound expression/select. This covers descriptors returned by
common-type selection before an expression exists. The SPI immutable
`DatabaseRetainedQueryColumns` couples ordered columns to their payload leases
when bound metadata leaves a binder. Public context/session boundQueryColumns
return that owner; consumers retain it until their destination adopts all
metadata claims. No raw-column compatibility API or operational plan owner is
retained in the returned metadata. S08-E07 and Base metadata tests require a
descriptor to remain charged across scope closure and final alias release.

Optional Base finalization dispatches row-dependent selected evaluation through
the original member's admitted context/session under the same retained
snapshot. Filter, ordering and demanded projection therefore preserve existing
correlated-subquery authority and data. Cached projection-target keys are
reused, while unneeded output targets stay unevaluated. The scoped callback
returns only a selected value and payload claims; final results retain no
dispatcher, context, session or snapshot. New count-subquery scopes lacking a
proved global interpretation are rejected before member reads, while admitted
scalar/parameter counts retain once-per-global-invocation evaluation. Existing
Base source capability gates remain explicit rather than choosing one member
as the meaning of a global query.

Existing Read admission, schema/authority leases, parent transaction and
Collecting-to-Ready remain unchanged. Producers close before the existing
session/transaction closure and Ready edge; last output owners retain only
value/schema/payload claims, never operational producers or authority.

| Fixed case ID | Owner and falsifiable completion evidence |
|---|---|
| S08-E01 / Q01-Q03 | DatabaseEngineTests + DatabaseTests: SQL text through binder/runtime preserves mixed-star positions, duplicate/zero-row descriptors, expression VALUES, partial aliases/group visibility, local/outer scope and invalid-name rejection on empty inputs. NATURAL z,a stays z,a; chained USING/NATURAL preserves visible-key ownership. RIGHT/FULL empty sides preserve merged/qualified keys, exact common types and presence. |
| S08-E02 / Q04-Q06/F09 | Bound success/failure tests + public SQL: constructor versus composite NULL equality/order, typed NULL/present all-NULL/absent row, field/expansion multiplicity, ROW JSON, zero/nested ROW and arity/type/capability failures. IN decimal comparison must not round a member to integer; scalar cardinality and lazy subquery branches retain typed failures. |
| S08-E03 / Q06/F08 | Public ARRAY(subquery): scalar NULL cells, typed empty input, one-column rejection, correlation, composite cells, accepted inner lower bounds/outer1, NULL/empty/mismatched subarrays and rank/overflow failures. Native and exact ordinary/Embedded witnesses exercise flat owners and post-root field/element lifetime. |
| S08-E04 / Q07-Q09 | Binary set bag counts/common types/local clauses; SQL NULL ordering/C/POSIX, alias/ordinal ambiguity, DISTINCT ON prefix and selected representative. Counting clock proves projection alias ordering evaluates its target once while independent target occurrences execute independently. Forced key collisions distinguish rows; SQL peers exclude fingerprints and RDF/generic regressions remain unchanged. |
| S08-E05 / Q10 | Parameter/expression/fixed/ALL/NULL/negative/overflow counts, once-only invocation evaluation, explicit NULL CAST distinction, offset/zero/complete final ties and exact metadata. Scan and admitted explicit-index execution have equivalent selected windows and DISTINCT ON; LIMIT zero does not scan. Non-SQL UInt64.max and typed SQL-language rejection regress without truncation. |
| S08-E06 / Q12 | SQLRelations' demand/cache cases below execute through public SQL: unused bodies bind but perform no work, lexical shadowing/partial aliases, MATERIALIZED LIMIT1 one volatile call, multiple consumers reuse/grow the same prefix without rerun or false EOF, and volatile NOT MATERIALIZED shares one computation. |
| S08-E07 / resources/lifecycle | All changed paths: exact/one-past row/byte/work/depth/capacity admission, retained source/full-owner inputs, failed/cancelled prefix growth and sort/set/composite construction; awaited close on success/failure, secondary cleanup errors, final-owner release and a successful later operation. No result crosses Ready early. |

Replace each existing S08 incomplete-implementation marker only with its actual
production path and corresponding success/failure evidence; retain the S10/S11
gates. These cases compose with Kit S08-V/QC and Wire S08-W proofs rather than
repeating intrinsic codec tests. Affected parent Database/Read and child
SQLArray/SQLJSON designs must consume these seams. Actual sprint verification,
local commits and deferred S18 integrated/URL/platform lanes remain owned by
the workspace progress record.

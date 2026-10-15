# QueryExecution

## Purpose and Scope

The QueryExecution component owns the DF-06F0 retained polymorphic aggregate,
the DF-06R retained regular-model fetch, and the only operations that may
transform retained values into canonical rows, index rows, or an explicit
final public output.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none in the current retained-fetch design.
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
| [QueryAST](../../QueryAST/DESIGN.md) | depends on | Text-to-semantic syntax contract | Canonical execution receives semantic input from the entry layer. | This is a contract relationship, not permission for an engine-to-parser import. |
| [Core](../Core/DESIGN.md) | used by | Pre-admitted builder append | Produces present and missing entries. | Runtime metadata is validated before append. |
| [Read](../Read/DESIGN.md) | coordinates with | Exact session work meter | Supplies the meter to builder and destination. | Budget equality is not identity. |

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
parameter declaration. A CAST of a literal NULL is a pure typed-NULL identity:
validate the target descriptor and retain its complete type, including temporal
precision 0...6, without invoking conversion or rounding. This is the same
contract as Builder constant normalization; the explicit NULL remains NULL,
not a synthetic result for an unsupported operand. Nonliteral or non-NULL
temporal precision conversions still fail explicitly until S06 implements
rounding. Invalid target descriptors fail even for NULL. Column metadata comes from admitted FieldSchema,
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
that schema claim before the append boundary. Tiny work budgets may reject
mandatory schema construction even for zero rows; physical cursor bounds and
stable LIMIT 0 no-read behavior remain separate observable contracts.

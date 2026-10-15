# Read

## Purpose and Scope

Owns schema/principal-bound sessions and operation admission, including the
DF-06F0 retained polymorphic entry gate and DF-06R regular retained-fetch
admission. Parent:
[DatabaseEngine](../DESIGN.md). Children: none in this scope.

## Responsibilities and Boundaries

Read owns authorization, schema lease, session scope, transaction, snapshot,
revocation, and meter identity. It creates the noncopyable admission consumed
by regular retained fetch. It does not resolve stored types, decode values, or
build rows. TaskLocal state and a caller's earlier temporal validation are not
authority.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit cursor contract](https://github.com/1amageek/storage-kit/blob/53e615faee772d2ee7c1a59311beaf8e8f0cf7c1/Sources/StorageKit/Storage/DESIGN.md) | depends on | State-owned `KeyValueCursor.validatingScope` | Supplies the admitted read boundary. | The pinned URL revision must provide post-validation before cursor readiness and finish-boundary resolution. |
| [DatabaseEngine](../DESIGN.md) | parent | Module operation lifecycle | Fixes session and transaction ownership. | Result declarations confer no authority. |
| [Registry](../Registry/DESIGN.md) | coordinates with | Admitted field query metadata | Supplies declared typed relation schema. | Do not infer rich values from persisted contents. |
| [DatabaseKit Query](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md) | depends on | QueryColumn, QueryColumnType and QueryValue | Supplies immutable semantic declarations. | Runtime reservations remain here and in QueryExecution. |
| [Core](../Core/DESIGN.md) | used by | Session transaction and admission | Supplies the admitted read boundary. | Core cannot widen authority. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Session meter | Supplies the admitted read boundary. | Identity, not equal budgets. |

## Architecture

```text
ReadPolicy -> sealed authorization -> ReadSession closure
  -> validate required list/fields -> nested admission -> Core/QueryExecution
```

## Contracts and Invariants

- The decision is made once and sealed to schema lease and principal.
- Admission is nested, non-forgeable, created only inside the session operation
  after exact coverage validation, and required by Core.
- Scan derives its meter from session scope and has no external meter input.
- Point fetch rejects retained keys from another meter before storage.
- Regular retained fetch derives its work meter from the session scope and
  requires complete entity-field authority, then seals entity, admitted
  fields, transaction, snapshot, and schema generation into one non-forgeable
  admission.
- Fusion candidate materialization uses a distinct session entry point. It
  derives the immutable field set from Fusion's explicit sealed authorization
  rather than accepting a caller-supplied field map, so projected reads do not
  widen authority or weaken the complete-model entry point.
- Per-entity authorization after decode supplements, not replaces,
  pre-storage list/field admission.
- A scoped range cursor uses the StorageKit state-owned validation contract;
  Read supplies the scope lease and does not wrap a second cursor state.

## Runtime Flows

```text
requirement -> validate coverage -> begin scoped closure/create admission
  -> async retained-key borrow -> QueryExecution call
  -> validate operation -> return retained result
```

## State, Ownership, and Lifecycle

The immutable session owns policy, lease, transaction, clocks, root, scope,
and meter. Admission cannot outlive or replace its closure/scope.

## Failure, Concurrency, and Constraints

Authorization and foreign-meter failures precede storage. Lower
failure/cancellation ends the operation before escape. Reads remain serial on
one transaction.

## Verification and Change Impact

[Authorization tests](../../../Tests/DatabaseEngineTests/ReadAuthorizationCapabilityTests.swift)
prove sealed denial; [retained regular fetch tests](../../../Tests/DatabaseEngineTests/RetainedRegularModelFetchContractTests.swift)
prove async source lifetime, order, missing slots, and foreign-meter rejection;
[retained polymorphic tests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
prove polymorphic point rejection. Scan meter provenance is a signature and
source audit. Admission changes invalidate QueryExecution, Fusion, and index-caller evidence.

### S01 frozen public result and projection boundary

These are implemented S01 API changes to the existing DatabaseReadModel and
QueryRowCodec owners; they are not new authority or a parallel result model.
The [QueryExecution S01 contract](../QueryExecution/DESIGN.md#s01-frozen-positional-execution-contract)
owns internal slots/binding and [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s01-falsifiable-case-ids)
owns S01-C01–C13 meanings.

| API | Frozen S01 contract |
|---|---|
| Framework `QueryRow.values: [QueryValue]` | Ordered cells only; annotations remain primitive and version unchanged. Remove stored name-keyed fields rather than keeping duplicate row representations. |
| Framework `QueryResponse.columns: [QueryColumn]` | Ordered immutable Kit declarations carried even when rows are empty. Columns have consecutive one-based numbers; each row matches arity and declared compatible type. |
| `QueryRow.namedValues(columns:) throws` | Explicit dictionary projection preserving QueryValue; duplicate names, arity or invalid declarations fail. No first/last-wins behavior. |
| `QueryRow.primitiveFields(columns:) throws` | Explicit primitive dictionary projection; additionally rejects rich cells whose metadata cannot be represented by FieldValue. No automatic metadata dropping. |
| `QueryRowCodec.decode(_:columns:as:) throws` | Explicit typed model projection; validates unique names and expected admitted field metadata, encodes rich cells with the matching declared adapter, then uses existing persisted-field reconstruction. |
| `QueryRowCodec.persistedModel(from:columns:entity:) throws` | Same explicit declared conversion for schema-driven destinations; incompatible value/type/missing required field is a typed error. |

Public construction validates columns/row arity/type with a throwing
initializer or explicit validated factory; there is no default inferred
schema from rows. QueryRow cannot independently claim a row schema: its
column contract is the response/relation's schema. Internal code cannot call
name projection merely to avoid migrating slot consumers. Typed convenience
`executeSQL(as:)` passes response.columns to the explicit codec, so duplicate
labels fail visibly instead of overwriting model fields. Primitive model
encoding remains FieldValue-based; rich values enter only through declared
Model/Schema adaptation.

```text
session-authorized collecting rows + shared typed columns
  -> producing scope closes transactions, sessions, cursors and leases
  -> existing purpose-specific Ready admission
      +-> retained result/page: schema + row claims stay owned
      `-> consuming final public materialization:
            complete typed columns + positional rows + byte owners
            -> caller ownership; runtime reservation ends
```

Runtime reservations live through destination admission and authoritative
closure. Retained Ready/server page storage owns row and shared schema claims
until its last retained owner ends or an explicit final transfer completes.
Public QueryResponse arrays are that explicit final materialization boundary:
complete arrays and primitive byte owners transfer to the caller, without
injecting runtime tokens into Kit values. Extracting or copying an
intermediate raw array is not final output, and server encoding/persistence
must finish while its retained owner and claims remain live. Empty output
also retains typed schema until the same boundary. The existing copyable
snapshot cancellation exception remains unchanged; only the Framework-owned
Collecting-to-Ready owner performs its applicable post-closure check.

S01-C01/C02 prove positional and zero-row API behavior; C04–C09 prove typed
model adaptation/rejection; C10 proves pagination schema; C12 proves both
retained last-owner lifetime and consuming final-public transfer; C13 proves
primitive/SPARQL regressions. Tests belong to DatabaseTests SQL row/retained
ownership suites and DatabaseEngineTests codec/read admission suites. Wire
owns its encoded-frame last-owner evidence; no test equates these two
ownership domains or treats final caller-owned output as a leaked runtime
reservation.

### S03 canonical parameter relay

Canonical execution and retained-page entry points carry immutable query
parameters to the [QueryExecution binding owner](../QueryExecution/DESIGN.md#s03-executable-binding-contract).
Session-bound output-metadata binding accepts the same parameters, so
Composition member admission and execution use the same declarations.
Session-bound parameter preflight returns resolved declarations under that
operation's exact work meter. The caller owns their retained lifetime;
metadata and parameter binding do not create another operational meter.
Fusion relays ordered relational inputs to QueryExecution's batch binding and
returns those selected plans to the same authorized execution. Its internal
row entry points forward the plan and frozen parameters without re-inferring
them or changing the session's authority, transaction or meter.
They also relay an optional immutable semantic occurrence arena and selected
root. Read neither interprets addresses nor retains SQL source bytes.
Read does not infer types or substitute references. SQL preparation may supply
the binding owner's resolved declarations; ordinary canonical calls may supply
the original declarations for operation-scoped binding. Omitted parameters
retain the existing empty-input behavior.

```text
session operation + immutable parameters
  -> QueryExecution binding and canonical execution
  -> operation validation -> response or retained page
```

Parameter data grants no authority. The same schema lease, principal,
transaction, scope and work-meter identity checks surround execution, and
failure/cancellation still ends the admitted operation before it escapes.
S03 public SQL and retained-page tests verify reference identity and typed
NULL declarations through these entry points; existing session authorization,
foreign-meter and retained-owner tests cover the unchanged admission boundary.

### S03 output-reference list authorization

List authorization uses the same immutable admitted schema as field admission.
Its ordering requirement resolves an output ordinal or alias to the projected
input expressions before collecting field names. Constant keys, including an
explicit constant CAST, require no input field. Direct table wildcard ordinals
use the entity declaration's existing field order. A composite wildcard already
requires all projected entity fields; its list requirement conservatively keeps
that complete per-entity field set. QueryExecution remains the sole owner of
ordinal range, ambiguity, coercion, evaluation and decisive occurrence failures.
Unresolved or invalid output references conservatively retain every admitted
entity field (or every candidate projection dependency), then proceed to the
binding owner. Read does not issue semantic range or ambiguity errors. Binding
still rejects the request before any affected source read, with its original
occurrence; conservative requirements never authorize a missing dependency.
The original limit and offset, sealed principal/schema lease, field authorization
plan and exact meter identity remain unchanged.

```text
logical ORDER key + admitted schema
  -> projection/input field requirements -> sealed list authorization
  -> existing bound ORDER execution
```

The existing public SQL ordinal test covers reordered rows, direct table
wildcards and explicit constant CAST keys; the source-position assertions keep
invalid references at the binding owner's original occurrences.

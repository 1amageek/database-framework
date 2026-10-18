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
- List ordering requirements resolve output aliases and ordinals through the
  original source bindings to physical entity fields. Whole-row keys include
  every source field; whole-row field selection includes its physical field.
  A bare source alias becomes a whole row only after ordinary column lookup
  fails. These ordering requirements remain separate from field-read coverage.
  A derived source retains its underlying physical dependencies in its own
  aliased output bindings, including when a JOIN also has physical-table
  siblings. Ordering a derived output conservatively requires those source
  dependencies; ordering an ordinary sibling field remains precise.
- A scoped range cursor uses the StorageKit state-owned validation contract;
  Read supplies the scope lease and does not wrap a second cursor state.

### SQL mutation session admission

The internal `withSQLMutationSession(context:transaction:workMeter:_:)` entry
borrows an already admitted writable DatabaseTransaction for mutation-owned
source, expression and RETURNING work. It requires the exact active transaction
object, context, principal, root and schema binding; MultiBase additionally
requires the binding's existing write admission. It does not require a second
Base read grant. Existing entity LIST/GET and physical field authorization still
apply to every demanded source and transition image; constraint-integrity reads
retain their separate Transaction contract.

The ordinary `withSession` entry continues to require read admission. A
SELECT main statement, including a WITH whose main body is SELECT, uses that
entry. SQLBlock and Mutation may select the mutation entry only when the main
statement is DML or TRUNCATE and its enclosing writable transaction is already
admitted. Ordinary SELECT definitions consumed by that mutation are internal
reads; independently requested SELECT steps retain ordinary read admission. Both entries share one private session
lifecycle, meter/root validation, checkpoint registration, scoped read-only
storage facade and awaited invalidation/cleanup. No returned value grants an
independent read session or extends the producing transaction's lifetime.

## Runtime Flows

```text
requirement -> validate coverage -> begin scoped closure/create admission
  -> async retained-key borrow -> QueryExecution call
  -> validate operation -> return retained result
```

## State, Ownership, and Lifecycle

The immutable session owns policy, lease, transaction, clocks, root, scope,
and meter. Admission cannot outlive or replace its closure/scope.

S08 [SQLRelations](../QueryExecution/SQLRelations/DESIGN.md) producers and CTE
prefix caches are invocation-owned consumers of this admission. They close
authoritatively before session invalidation and transaction closure. They
cannot acquire an independent snapshot, extend scope lifetime through a
retained output value, or reset the operation meter. Existing cursor registry
claims remain session-owned until invalidation; producer cleanup tests compare
the live-session entry baseline and require zero after the session closes.

Optional Base global finalization borrows an existing admitted member session
through `finalizeCompositionRelation`. The facade enforces identical meter,
active operation and sealed policy while QueryExecution applies selected
operators to retained inputs. It forwards this session's transaction and
temporal environment and validates scope again before returning. It neither
opens a second context nor grants new Base source capabilities. Base closes
the borrowed session/snapshot before exposing events; selected merge and
authorization tests own this boundary's success and denial evidence.

Bound output metadata crosses the session facade as QueryExecution's immutable
DatabaseRetainedQueryColumns, with its construction/storage payload leases.
The consumer keeps that owner through destination metadata admission. Read does
not return a naked composite descriptor after releasing its binding claim.
An empty polymorphic page constructed from this owner retains the same immutable
metadata and claims until its last page copy ends, even after the producing
binding scope and the caller's original metadata owner have ended.
Source-scoped Base evaluation borrows the original admitted member context and
session through an internal callback, then validates scope before returning
only the selected value/claims. No new context replaces the member identity.
When a global finalizer borrows another member, an explicit internal bridge
validates the parent operation, clears only its inherited operation-borrow
slot for the nested admission, and validates the parent again on return.
The nested member still establishes its own snapshot membership, Base lease,
execution binding, policy and active read scope. Ordinary implicit cross-scope
operation borrowing remains rejected. This prevents a foreign task-local lease
from being mistaken for the newly admitted member's authority.
Global count resolution happens once before source materialization, and its
resolved value is forwarded into finalization. LIMIT zero still binds metadata
and emits no source reads or output rows.

Fixed list pagination retains its existing bounded authorization. An expression
count has an explicit deferred admission state, retaining its declaration
identity and requiring unbounded-list permission before evaluating the count.
After authorization is sealed, QueryExecution evaluates its bigint window once
without widening that authority. A denied unbounded capability is a typed
pre-storage denial, not a silent nil/default interpretation or permission to
execute a count subquery. Field/list requirements of admitted nested counts
remain part of the original preflight.

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

### S04 decisive scalar failure occurrence

`CanonicalReadError.expressionEvaluation` retains the original
`DatabaseExpressionEvaluationError` and an optional Kit `QueryOccurrenceID`.
QueryExecution attaches its selected node's address only when an inner failure
has no address; enclosing expressions preserve an already decisive address.
Direct semantic IR may have no occurrence. Meter, cancellation, aggregate and
subquery failures retain their existing owners and are never converted into
scalar errors. Database maps a retained address through its own source map;
Read does not import parser ranges or retain source text.


### S06 inherited temporal environment

ReadExecutionContext carries an optional immutable SQLTemporalEnvironment until
the public operation resolves it. Removing an external page window or deriving
a subquery keeps the same environment. DatabaseReadSession retains the admitted
statement environment and logical transaction time; authorizedSession copies
them unchanged. The default non-SQL session uses its admitted transaction's
instant for the implicit statement rather than sampling a new time per member.

The [Core clock owner](../Core/DESIGN.md#s06-clock-and-configuration-ownership)
captures and propagates time; Read does not interpret zones or grant authority
from clock data. Scope validation, exact meter, schema, principal and storage
transaction checks remain mandatory. Session-bound execution uses the same
environment for selected values, nested reads and output formatting.

Tests prove environment identity through authorization attenuation, derived
sessions, page/subquery contexts, retry and failure cleanup on Native and the
applicable portable owner paths. Clock state is Sendable immutable data on all
targets, with no new shared mutable cache or conditional synchronization.


### S11 explicit SQL resolution carriage

**Implementation contract; not completion evidence.** Read adds a package-visible
immutable final reference owner `SQLReadResolution: Sendable` containing the
admitted SQLCatalog snapshot, captured read policy/schema lease, `SQLSearchPath`,
authenticated current-user identifier and generation identity.
`ReadExecutionContext` carries it explicitly as an optional package property.
The public default remains absent for typed QueryIR/SPARQL calls. SQL entries
obtain it through `DatabaseContext.makeSQLReadResolution(searchPath:workMeter:)`
inside the existing `withDataOperation` / `withOperationSchemaLease` scope,
never before that scope captures its generation. The factory uses the admitted
read policy; absence is never inferred from source syntax. Anonymous authorization supplies
no current-user schema. Resolution does not add authority.

```text
withDataOperation / withOperationSchemaLease
  -> admitted generation + principal + explicit SQL search path
     -> SQLReadResolution -> preflight/list/field requirements
     -> sealed physical authorization -> bound logical rows
```

Every context reconstruction, temporal-environment change, page-window removal,
prepared execution and Fusion batch retains the exact resolution owner.
The reference owner retains its original policy/lease, and Read validates the
actual execution session against that identity before work proceeds. A prepared
owner presented to a different generation fails with a typed mismatch before
authorization or reads; it never silently rebinds. A retained catalog cannot
replace the session's live operation/transaction lease. The catalog owner validates physical mapping, and
[QueryExecution](../QueryExecution/DESIGN.md#s11-frozen-catalog-bound-select-execution)
owns alignment and source-position failure mapping. The runtime fingerprint
includes the catalog digest; SQL continuation identity also distinguishes the
selected search path/resolved source so changing either cannot resume another
logical relation.

Field and list authorization consume the same resolved relation as binding.
Visible logical field names map to physical entity fields before security
policy evaluation, including wildcard, whole-row, aliases, ORDER aliases and
ordinals, derived sources, joins, index dependencies and Fusion inputs.
`DatabaseFieldReadAuthorizationPlan` and `SelectQuery` ordering requirements
must accept explicit resolution rather than separately looking up terminal
names. Existing complete-model and sealed subset contracts remain intact.
SQL namespace lookup, including privileged catalog inspection, never bypasses
entity/field security or grants access to a physical source.

The factory preadmits the reference owner, path/name bytes and retained metadata
before constructing them. `CanonicalQueryBindingEnvironment` stores only the
optional owner reference, preserving the S10 depth-30/8MiB stack contract. Every
new retained reference/map remains charged through its last consumer.

The normative [S11-C01–C12 acceptance matrix](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s11-frozen-logical-catalog-and-identity)
owns the completion IDs. C03/C06/C08/C11/C12 specifically require an execution
to keep its old mapping while a new generation is published, and require an
external prepared owner to reject a different session generation.

Focused authorization tests deny a renamed logical field's physical source
before I/O, distinguish same terminal names in separate namespaces, and verify
that qualified CTE-like names never suppress real physical authorization.
Prepared/continuation tests reject stale catalog/search-path reuse. Existing
physical typed QueryIR/SPARQL, meter, cancellation, lease and retained-output
fixtures remain required. Added state is immutable across Native/WASM/Embedded;
the existing Mutex/actor owners and shutdown paths remain unchanged.


S11 logical source qualifiers, aliases, field names, CTE keys and USING/NATURAL
column membership use the exact UTF-8 identity defined by
[QueryExecution's source identity contract](../QueryExecution/DESIGN.md#s11-exact-logical-source-identity).
`DatabaseFieldReadAuthorizationPlan` preserves that identity through source and
ordering dependency collection before lowering to physical requirements.
Normalized-equivalent logical names must not merge permissions, shadow a
source, or suppress a required physical field read.

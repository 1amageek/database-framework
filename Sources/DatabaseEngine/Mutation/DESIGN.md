# Entity Mutation

## Purpose and Scope

Mutation owns prepared entity changes, preconditions and their execution within
[DatabaseEngine](../DESIGN.md). Parent: DatabaseEngine. Children: none.

## Responsibilities and Boundaries

Preserve logical entity identity, declared primitive persistence fields,
preconditions, typed adaptation failures and request accounting. QueryExecution
owns canonical query values and the bounded value meter. Storage commit and
container lifetime remain with the transaction/container owners.

Statement assignment planning consumes the compiled `FieldSchema.queryType`,
or the field's primitive declaration when absent. QueryExecution owns binding,
cast admission and scalar conversion; Mutation supplies the target declaration
and `.assignment` context, then adapts the converted value at the existing
persisted-model boundary. Mutation does not implement text, numeric, byte or
UUID conversion algorithms.

S09 assignment preparation calls `CanonicalQueryTypeBinder.requireScalarPhase`
for every persisted target before selecting canonical or generic value adaptation.
It then uses `bindScalarAssignment` for canonical scalar conversion. The phase
check rejects aggregate, GROUPING and window expressions in the current scalar
scope before UPDATE scanning or INSERT conflict lookup, including an empty
source, and stops at a child SELECT boundary. QueryExecution owns that scalar-phase check and its typed failure;
Mutation supplies the persisted field declaration. This does not change the
existing statement forms or row-dependent scalar conversion behavior.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Transaction execution authority and prepared mutation scan handoff | Composes prepared changes and admits scan destinations. | Keep temporary payload owners through destination admission; success requires authoritative close. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | `CanonicalQueryTypeBinder.requireScalarPhase`, `bindScalarAssignment`, `DatabaseContext.evaluateCanonicalScalar`, bounded value footprints | Binds compiled target assignments and converts selected scalar values. | Pass `.assignment`; retain generated payload claims until the prepared change admits its destination. Synthetic query rows confer no mutation authority. |
| [Kit Model](../../../../database-kit/Sources/DatabaseKit/Model/DESIGN.md) | depends on | PersistableEncodingError | Preserves typed query-value causes. | Rich meaning requires a declared descriptor. |

## Architecture

```text
Declared model -> primitive persisted fields -> prepared mutation
    -> identity/precondition checks -> caller-owned transaction -> close

Compiled fields -> bind supplied INSERT cells / UPDATE assignments
    -> bind UPDATE / DELETE / conflict predicates as Boolean
    -> shared context-free selected scalar evaluation
    -> outer assignment CAST (assignment context)
    -> declared persistence encoding + model adaptation
    -> prepared change with temporary payload owner
    -> destination reservation -> release temporary owner
    -> execute all prepared changes in the caller's transaction
```

## Contracts and Invariants

### S12 Statement Effects and Explicit Temporal Environment

Mutation owns two distinct outcome scopes. Existing public
`DatabaseEntityMutationExecutor.execute` retains its transaction-net-effect
contract and the transaction-wide journal remains authoritative for final
validation, maintenance and checkpoint undo. The package-only
`executeStatementEffects` path reports only the current prepared statement's
distinct requested identities, whose uniqueness is already established by
preparation. It determines INSERT/UPDATE/DELETE meaning from the immediately
preceding state, applies those mutations through the existing transaction
owner, then obtains post-apply version digests only for those identities through Transaction's operation-scoped journal lookup. Computing this write outcome does not fetch rows through a user read capability or require GET permission. Derived
relationship work does not add SQL affected rows. Later journal coalescing or
rollback does not rewrite an earlier statement outcome.

The statement outcome records its own affected-row count once on the exact
request meter. INSERT(a); INSERT(b) therefore reports [1, 1] and consumes two
rows, while INSERT(a); DELETE(a) reports [1, 1] even though the transaction's
final net effect is empty. SQLBlock consumes this Mutation-owned result; it
never inspects journal history to infer statement effects.

`DatabaseEntityStatementMutationExecutor.execute` accepts an optional explicit
`SQLTemporalEnvironment` after its existing work-meter argument. Existing
callers that omit it keep the admitted transaction's prior environment
capture. SQLBlock supplies its immutable retry-stable statement environment
explicitly; Mutation passes that same value to assignment and predicate
evaluation without sampling a replacement statement clock. CLOCK_TIMESTAMP
continues to invoke the supplied live clock at each evaluated occurrence.

Statement-kind scratch and outcome storage use existing request-accounted
retained builders. There is no new shared mutation state, journal copy or
target-specific synchronization branch. Public SQLBlock tests own the two
per-statement count/budget counterexamples; its lifecycle suite owns explicit
temporal propagation through a real transaction retry. Existing ordinary
mutation lifecycle, precondition, net-effect and version tests remain required
regressions for the preserved public executor path.

Mutation footprint measurement consumes primitive named fields directly and
does not construct QueryRow or infer a query schema. A model encoding failure
with QueryValueError is preserved as
DatabaseEntityMutationError.invalidQueryValue(entity:cause:). Existing scalar,
field identity and compiled-schema errors retain their prior mappings.

The statement executor resolves and validates assignment targets before an
UPDATE source scan or INSERT existing-row lookup. INSERT supplied columns,
UPDATE assignments and conflict-update assignments use the same compiled target
declaration. Unknown or duplicate targets and invalid conversion signatures do
not depend on a matching source row. Value-dependent length/domain failures
occur when the corresponding assignment is evaluated; all changes are prepared
before any are applied. UPDATE, DELETE and conflict-update predicates bind as
Boolean expressions before scanning or existing-row lookup and use the same
selected scalar evaluator, including codepoint and C/POSIX text semantics.
NULL predicates do not select a row. Omitted-field defaults,
model adaptation, identity, preconditions and conflict policies remain owned by
their existing paths. The S09 implementation alone does not add INSERT SELECT, UPDATE FROM or
RETURNING; the S14 contract below owns their subsequent composition.

Explicit CAST inside an assignment retains its explicit context; only the outer
target-field conversion uses assignment context. CHAR/VARCHAR assignment must
not silently acquire explicit truncation semantics. The scalar owner defines
codepoint length, permitted trailing-space removal, padding and typed failure.
Declared rich values are encoded through `QueryFieldStorageAdapter` so numeric
display scale is preserved; primitive fields keep their declared model adapter.

## Runtime Flows

INSERT binds all supplied cell signatures before reading conflicting rows. Each
row then evaluates, converts and adapts its supplied fields before producing a
change. UPDATE binds its complete assignment list before scanning and evaluates
all right-hand sides against the original row. Conflict-update assignments are
and conflict predicates are also bound before the INSERT loop. The async scan
transform calls `DatabaseContext.evaluateCanonicalScalar(_:on:workMeter:)`
with an admitted positional row aligned to the compiled binding slots. This
context-free entry rejects relation/subquery/aggregate evaluation before I/O.
Neither a binding error nor a later
row-dependent conversion error may publish an earlier prepared change.

## State, Ownership, and Lifecycle

Prepared changes retain request claims through execution and transaction
completion. Measurement grants no transaction or publication authority.

A Mutation-owned prepared change couples `EntityMutationChange` with its
temporary scalar payload owner. The transaction scan consumes that internal
handoff and keeps it alive while measuring and admitting the destination change.
Borrowed selected-column results retain the admitted positional source-row
payload claims through this handoff as well. The payload owner and destination
reservation use the same `DatabaseWorkMeter`.
Success, conversion/adaptation failure, admission failure and cancellation
release temporary claims exactly once; no raw payload escapes an unclaimed
interval. Payload-owner collection capacity is admitted through the existing
retained-array builder. No shared mutable state or new isolation boundary is
introduced.

## Failure, Concurrency, and Constraints

Malformed rich fields, unknown retained capacity, cancellation and budget
failure propagate explicitly. S01 adds no mutable shared state or isolation
branch in this component.

## Verification and Change Impact

DatabaseEngineTests mutation/model-projection suites verify identity,
preconditions, failure mapping and request claims. S01 rich-model persistence
and negative adaptation cases exercise the changed caller boundary. Changes
revalidate Kit Model and direct server error mapping; source compilation alone
is not behavioral evidence.

Statement-assignment tests use compiled CHAR/VARCHAR field descriptors and the
real statement executor to check INSERT/UPDATE success, Unicode codepoint
length, padding, trailing-space handling and overlength failure. A multi-row
failure leaves every stored row unchanged; an invalid assignment signature is
rejected before source scanning. Exact/one-past resource admission, failure and
cancel cleanup, and subsequent valid mutation verify the temporary-to-destination
ownership handoff. Existing statement mutation lifecycle/precondition/budget
tests remain regressions for primitive fields and model adaptation.

## S06 Selected Temporal Integration

This section specifies the Mutation caller of the
[selected temporal contract](../QueryExecution/SQLTemporal/DESIGN.md).
It is a design contract; implementation starts only after the parent S06
freeze. QueryExecution owns overloads, casts, temporal identity and algorithms.
Core, Transaction and Read own the admitted `SQLTemporalEnvironment` and its
clock/zone lifetime. Mutation does not sample another clock, resolve a host
timezone or create an execution context to evaluate an assignment.

```text
admitted transaction + one statement temporal environment
  -> bind all destination casts and predicates before reads
    -> evaluateCanonicalScalar(bound, on: row,
         temporalEnvironment: environment, workMeter: meter)
      -> selected temporal result and retained leases
        -> descriptor-selected persistence encoding
          -> existing prepared-change admission and atomic publication
```

The selected assignment gate includes SQL DATE, TIME, TIMETZ, TIMESTAMP,
TIMESTAMPTZ and INTERVAL, including their modifiers and admitted infinity
carriers. Primitive temporal model fields retain their existing intrinsic
meaning; a conversion into a primitive destination succeeds only when the
selected result is representable without losing its SQL value. Declared SQL
TIME24, offset time and infinity are never replaced by an ordinary primitive
value or an inferred object representation.

INSERT supplied cells, UPDATE and conflict-update assignments preserve explicit
inner CAST contexts and use `.assignment` only at the outer compiled destination.
UPDATE, DELETE and conflict predicates use the same selected temporal comparison
as SELECT. Invalid signatures and declared modifiers fail before a source scan
or conflicting-row lookup, including an empty source. Value-dependent range,
zone, precision, input and formatting errors remain decisive typed failures.

One statement environment is reused for every candidate row and assignment,
including conflict processing. The caller passes the admitted transaction's
logical transaction instant and the statement owner's captured statement
instant; retries and internal read sessions do not resample them. Volatile clock
reads occur only when the selected expression occurrence is evaluated. Lazy
CASE/COALESCE and strict NULL keep the shared evaluator's evaluation rules.

Rich temporal results use `QueryFieldStorageAdapter` under the compiled field
descriptor. Mutation retains source-row and returned payload leases through
encoding and destination admission under the same meter. A late conversion,
unavailable-zone, cancellation or budget failure publishes no prepared changes
and releases all temporary claims. The immutable environment grants no storage,
transaction or publication authority.

| Case | Falsifiable caller evidence |
|---|---|
| S06-M01 | Empty UPDATE/INSERT conflict paths reject invalid temporal signatures and modifiers before reads. |
| S06-M02 | DATE/TIME24/TIMETZ/local timestamp/instant/mixed interval/infinity INSERT and UPDATE round trips retain their declared values and precision. |
| S06-M03 | Multiple rows and conflict assignments share transaction/statement snapshots; selected volatile occurrences advance while skipped lazy branches do not read the clock. |
| S06-M04 | Temporal predicates, interval-equivalent values and TIMETZ offset ties select the same rows as public SELECT. |
| S06-M05 | A later invalid zone/range or unrepresentable primitive destination leaves all stored rows unchanged; subsequent valid work succeeds with temporary retained rows/bytes zero. |
| S06-M06 | Exact and one-past payload admission, cancellation and destination handoff preserve meter identity and last-owner release. |

## S13 SQL Constraint Composition

[SQLConstraints](../SQLConstraints/DESIGN.md) owns SQL binding and validation semantics. Transaction owns attempt-local timing modes and pending state, checkpoint restoration, non-snapshot retained scans, referential actions through its existing persistence path and final validation against its retained schema lease. Mutation applies SQL expression defaults and identity before model adaptation and recomputes stored generated columns before prepared changes are applied; raw model writes preserve their canonical adaptation contract and enforce SQL invariants. SQL default expressions remain independent of FieldSchema.defaultValue.

### Referential action authorization

Only Transaction constructs SQLReferentialActionAuthorization after matching an actual referencing row against a declared foreign key's old target key. The token retains the exact attempt owner, operation identifier, identity and schema lease. DatabaseDataStore validates that token before bypassing caller entity/field write policy for this one integrity action. Storage authority, Directory root and partition, preconditions, model canonicalization, index maintenance and transaction final validation remain unchanged. No public mutation API issues or accepts the token; hidden row values never leave the private integrity path. Ordinary saves and deletes pass no token and retain normal policy enforcement.

SQL policy execution binds the immutable admitted session and explicit statement temporal environment around the entire mutation operation. Transaction row CHECK and cascading defaults inherit that same scope, including direct statement-executor callers. The scope confers no additional storage or identity authority.


## S14 SQL Mutation Execution

This section is the contract for S14 implementation, not a claim of behavioral
completion. Its semantic authority is the Kit SQL profile; PostgreSQL 18 edge
contracts and source observations are recorded in the workspace S14 discovery
evidence. The generic SPI `execute` keeps its entity-identity conflict contract.
The package SQL entry consumes logical SQL relation/column/constraint identities.

### Ownership and interfaces

| Owner | Input | Guarantee consumed by Mutation |
|---|---|---|
| Kit Query | INSERT, UPDATE, DELETE and MERGE declarations; ordered tuple targets; conflict target; transition aliases | Syntax retains every semantic child without executing it. |
| QueryExecution | Statement read scope, source/SELECT plans, correlated expression evaluation, projection plans | Canonical typed positional rows and metadata; statement base snapshot and injected writable-CTE relations remain distinct. |
| Transaction | Admitted physical target rows and ordinary requested changes | Existing authorization, preconditions, relocation, index/FK effects, journal/checkpoints and final validation remain authoritative. |
| SQLConstraints | Declared key columns, null policy and canonical key/equality functions | Request-local arbiter lookup and all-constraint enforcement use the same SQL equality. |
| SQLBlock | Immutable session/temporal environment, exact meter, caller-owned transaction | No public output until authoritative block close; main-statement count excludes writable-CTE effects. |

`executeSQL` returns a Mutation-owned `SQLMutationOutcome`: statement effects,
affected-row count and optional collecting canonical RETURNING relation. It does
not return a public `QueryResponse` or mint a Ready admission. QueryExecution
owns the relation's ordered columns, nullability, values and payload claims;
SQLBlock owns promotion after transaction close. Output metadata is bound even
when no target rows match. Existing SPI callers retain the effects-only result.

Mutation requests source and correlated evaluation from the same QueryExecution
statement scope rather than opening independent reads or recreating relational
operators. Physical target identity is carried alongside the canonical target
row, never inferred by comparing visible values or inserting a hidden visible
column. A SQL source read remains subject to normal read authorization.
RETURNING uses the admitted old image and the canonical image actually accepted
by ordinary persistence, without an additional GET operation. This does not grant
integrity-action authorization or bypass field-read authorization.

```text
immutable statement source view + catalog/schema lease + exact request meter
    -> bind targets, predicates, casts, tuple widths and RETURNING metadata
    -> retain target identities + source candidates
    -> choose one logical action per effective target
    -> evaluate old-row RHS / defaults / generated values
    -> ordinary prepared mutation transaction path
        -> data + indexes + FK actions + identity relocation + journal
    -> project actual old/new/source images into collecting RETURNING relation
    -> SQLBlock authoritative close -> publish command count and rows
```

### Input and action contracts

| Form | Binding and candidate behavior | Failure boundary |
|---|---|---|
| INSERT VALUES / DEFAULT VALUES / SELECT | Explicit columns bind left-to-right; omitted list uses the first N declared columns; defaults evaluate per proposed row; empty SELECT inserts zero. Assignment casts and S13 identity/generated policies run before conflict matching. | Duplicate/unknown columns, excess or inconsistent arity, invalid destination signature fail before effects. Row-dependent conversion failure rolls back all data effects. |
| UPDATE FROM | Join/filter against old target and source rows; choose one qualifying candidate per physical target. Every scalar/tuple RHS reads the same old target, independently of assignment order. | Multiple source matches do not produce a MERGE cardinality failure. Tuple subquery zero rows supplies NULLs, one row supplies its columns, more than one is a cardinality error. |
| DELETE USING | Select TRUE join/filter candidates and delete each physical target once. | NULL predicate skips; duplicate join matches neither duplicate count nor output. |
| MERGE | Freeze candidate match class once. Evaluate ordered clauses only in that class; first TRUE clause, including DO NOTHING, wins. BY TARGET supports source-only insertion; BY SOURCE supports target-only actions. | Illegal action/class and unreachable later same-class clauses fail before effects. Repeated effective UPDATE/DELETE of one physical target fails cardinality; harmless DO NOTHING does not mark it modified. Duplicate inserted logical keys retain uniqueness failure. |

Target aliases hide the original target name. Joined source scopes remain
available to action expressions where SQL permits them. Assignment DEFAULT is a
policy request, not SQL NULL. Tuple RHS evaluates once, then each destination
receives its own assignment conversion. UPDATE FROM nondeterminism is limited
to selection of one allowed matching source; no output order is promised.

### Conflict arbitration

SQL INSERT uses actual logical PRIMARY KEY and UNIQUE declarations, independent
of persisted entity identity. A bounded request-local composite-key collection
is sufficient for existing catalog constraints; persistent, expression and
partial index declarations remain S15-owned. The collection retains canonical
key components using `SQLConstraintRows.key/equal`, exact column IDs and the
constraint's NULLS DISTINCT/NOT DISTINCT policy. Keys containing NULL do not
conflict under DISTINCT. No identity-key fallback is permitted.

Column inference matches a complete declared key as an unordered exact-name
set. ON CONSTRAINT resolves a declared constraint name. Only nondeferrable keys
may arbitrate; effective SET CONSTRAINTS timing does not change eligibility.
DO UPDATE requires a target; targetless DO NOTHING considers every eligible key.
A conflict against an unselected constraint still fails ordinary validation.
Inference predicates are bound in the admitted inference scope; unsupported
partial/expression-index arbitration is a typed failure, never an unfiltered
substitute key.

The proposed row obtains defaults and stored generated values before arbitration.
DO UPDATE SET/WHERE alone receive EXCLUDED, whose values are that proposed row.
FALSE/NULL action WHERE performs no update/count/RETURNING. EXCLUDED is absent
from RETURNING and conflict inference. The request-local key state includes
prior proposed inserts and updates, so duplicate input is not missed. Repeated
effective DO UPDATE on one target, including a prior insert, fails cardinality;
already modified targets are rejected before evaluating a later action WHERE.
Repeated candidates whose filters have all been FALSE may still perform one later
update. A prior insert followed by a conflicting candidate fails cardinality even
when that candidate filter would be FALSE. These boundaries match the pinned
PostgreSQL 18.6 oracle; a blanket source-duplicate rejection is invalid. Ordinary constraint validation still owns
unrelated uniqueness, checks and referential invariants.

### RETURNING and affected rows

INSERT and UPDATE use the final canonical new target; DELETE uses its admitted
old target. Explicit OLD/NEW qualifications use transition images, NULL-extended
when absent. Explicit transition aliases hide the corresponding default name.
UPDATE FROM and DELETE USING retain the selected source through projection.
MERGE also exposes its source, action-dependent target and `merge_action()`;
the action function is rejected outside MERGE RETURNING. EXCLUDED does not leak.

Star and expression projections use ordered positional metadata, so duplicate
labels survive. Binding occurs before candidate enumeration, including empty
sources, and all mixed MERGE actions share one nullable output schema. Matched
no-op UPDATE counts one; skipped conflict/MERGE actions count zero. INSERT count
includes conflict updates; cascades never inflate the statement count. Each
effective requested action yields exactly one RETURNING row when requested.
Later statement failure, cancellation or uncertain commit publishes no ready
rows or definite successful count.

### State, limits and verification

All candidate rows, target memberships, key components, changes and RETURNING
payloads are request-local retained owners on the exact meter. Capacity is
admitted before allocation; shared source payloads retain their existing claims
until destination admission. No global mutable registration, new synchronization
primitive or Embedded-specific state branch is introduced. Target membership
uses physical entity/partition/identifier identity. Within a writable WITH,
QueryExecution allocates a distinct owner for each prepared CTE/main mutation.
After statement-local conflict/MERGE cardinality validation, Mutation claims only
effective old/new physical identities for that owner before applying changes.
Disjoint rows in the same relation remain valid; an identity claimed by another
CTE/main owner rejects the whole statement. A relation ID is not a row claim.
Cancellation, budget failure,
late expression failure and checkpoint rollback release temporary owners and
restore ordinary transaction state.

The Mutation behavioral owner is
`Tests/DatabaseEngineTests/S14SQLMutationExecutionTests.swift`. It covers M01
SELECT/default/omission/cast behavior; M02 old-row simultaneous and tuple
cardinality/source multiplicity; M03 USING de-duplication; M04 ordered duplicate
labels, empty metadata, source/transition images and exact count; M05 composite
logical arbiters, null policy, EXCLUDED and repeated effects; M07 clause priority,
all action classes and target cardinality. Existing statement assignment,
mutation lifecycle and S13 constraints remain regression owners. Transaction
owns hidden identity/relocation/FK authorization/checkpoint tests; QueryExecution
owns shared statement source visibility and writable CTE scope; SQLBlock owns
post-close publication, cross-statement counts and aggregate rollback. Native
focused execution, affected-target/backend verification and portable compile/link
are orchestrated once by the root task after the shared snapshot converges.


### Frozen preparation and hidden identity seams

Mutation owns the internal Sendable `SQLMutationPreparation` retained handoff.
`prepareSQLReads(_:scope:transaction:workMeter:)` retains the complete admitted
physical target snapshot through Transaction's
`scanSQLMutationModels(relation:fields:maximumRows:workMeter:)`, constructs the
old-target logical layout and prebinds required plans. It asks the same
`SQLStatementReadScope.capture(_:outerLayout:)` to authorize/capture the original
SELECT source and scalar-subquery roots. SQLBlock prepares every writable CTE
and main statement before sealing the scope and before applying any mutation.
`executeSQL` consumes the same preparation and read scope; it cannot acquire a
fresh target/source snapshot after a sibling writes. Read fields supplied to
Transaction include all predicate, assignment, RETURNING and conflict expression
dependencies; hidden model images remain internal and confer no public read
permission. QueryExecution owns correlated phase/type binding and projection.

Mutation owns hidden physical row identity, independently of SQL sequences.
`SQLMutationRequest` is a package actor initialized with the exact request meter
outside the transaction retry loop. `beginAttempt()` resets its replay cursor.
`allocateHiddenIdentity(relationID:column:statement:)` records one immutable
allocation event and retains its admission claim; later attempts reuse the value
only when the same statement, relation and physical field declaration recur at
the same cursor. Divergent replay fails explicitly. `SQLMutationRuntime.request`
and `statementIndex` are package TaskLocal bindings supplied by SQLBlock across
all attempts; there is no global mutable registration.

Allocation is eligible only for a schema-driven runtime
(`compiledModelContract == nil`) whose physical identifier field is not mapped
to a visible SQL column. The actual physical field declaration chooses the
supported UUID, string or bytes representation. The random value is generated
once per event, not once per retry; unsupported identifier types fail with a
typed error. Mutation never fills an arbitrary missing field, changes a compiled
model's declared identity, treats a visible SQL primary key as a hidden ID, or
makes the hidden identifier visible through star/RETURNING. Its lifetime is one
request and its ownership is one actor on every target. Transaction remains the
owner of identity validation, placement and ordinary persistence.


### S14 target synchronization review

| Logical state | Native / WASM / Embedded storage | Isolation and entry | Release |
|---|---|---|---|
| Hidden identifier timeline/cursor | Same `SQLMutationRequest` event array and cursor | Actor `beginAttempt` / `allocateHiddenIdentity`; TaskLocal carries the actor reference | Request actor lifetime; event reservations retain allocated values |
| Target membership and planned actions | Same invocation-local admitted `SQLMutationArray` | One statement task; no escaping mutable reference or callback | Statement execution returns or throws |
| Prepared target/source/RETURNING frames | Same immutable retained owners | Prepared once before scope seal; reads borrow retained canonical rows | Last preparation/output owner |

The source audit finds no Embedded/Synchronization availability branch,
`nonisolated(unsafe)`, unchecked conformance or target-conditioned Sendable
contract in the new Mutation path. The existing generic SPI remains scalar and
rejects WITH/MERGE/TRUNCATE before effects. Both prepared and scope-owning SQL
entries use the same SQL outcome and logical-arbiter implementation. Runtime
and cross-target evidence remain owned by the root verification lanes.


### Explicit target Partition selection

The resolved target Partition is part of Mutation's input contract.
`prepareSQLReads` passes `entity.partition` to Transaction's admitted target scan,
so UPDATE, DELETE and MERGE select old rows only inside an explicitly named
Partition. An absent explicit Partition retains the dynamic all-Directory SQL
scan contract. SQL target resolution therefore leaves an omitted selector nil;
the generic entity SPI retains its explicit dynamic-Partition requirement.
Candidate placement still comes from the complete runtime identity derived from
the model's declared dynamic fields. Selecting an old Partition does not restrict a subsequent UPDATE
from relocating its new image through the ordinary transaction path.

INSERT and MERGE INSERT apply defaults and model adaptation, derive the complete
candidate identity, then validate an explicitly supplied target Partition against
that candidate through `DatabaseEntityMutationExecutor.resolveReference(_:model:)`.
A mismatch remains the existing typed identity/Partition failure before conflict
selection or physical mutation. Without an explicit target Partition, the
candidate's complete canonical identity determines placement.


### INSERT reads and RETURNING authorization

A plain INSERT without ON CONFLICT has no old target candidates. Preparation
retains an empty target batch without LIST/GET on existing target rows; INSERT
SELECT and scalar-subquery source reads keep their own admitted read contracts.
Logical UNIQUE/FK validation remains inside Transaction's integrity authority.
ON CONFLICT, UPDATE, DELETE and MERGE retain their actual target-read contract.

RETURNING records separate current/OLD/NEW physical field demands. After ordinary
mutation produces canonical transition images, Mutation calls the scope's admitted
`DatabaseReadPolicy.authorizeGet(_:fields:)` for each actual image and its demanded
fields before projection. A missing transition contributes NULL and no row read;
a constant projection has no target-field demand. Read denial aborts the statement
and publishes neither rows nor a successful count. This authorization never adds
LIST on unrelated existing rows or broadens caller authority.


### SQL mutation session ownership

The scope-owning SQL entry joins its supplied, already write-admitted
`DatabaseTransaction` through Read's `withSQLMutationSession` factory. The factory
validates that exact transaction, container, root and generation and retains the
ordinary session cleanup contract. It does not request an independent Base read
grant merely to execute the mutation. The statement's real source LIST/GET and
RETURNING field checks remain governed by the captured read policy; ordinary
query entry points keep their read-admission factory.


### Bounded async row and candidate advances

Mutation applies the existing [SQLRelations owned-advance scheduling
contract](../QueryExecution/SQLRelations/DESIGN.md) at two operation boundaries:
`sqlRow` converts one captured/RETURNING model image, and `sqlMatches` evaluates
one candidate predicate, including the absent predicate's TRUE result. Each
boundary yields before its operation and before returning, checking the same
meter's cancellation/deadline after resumption. Error unwind yields and preserves
the original failure. This bounds synchronous per-row continuation accumulation
on the supported WASM runtime without changing stack capacity or depth limits.

Both boundaries remain in the same task, session and transaction scope, with the
same ordered values, authorization and resource reservations. The async
`Entry.withModel` borrow already retains its payload lease across awaited row
conversion, so cooperative suspension remains within that owner lifetime.
Checkpoint checks charge no work units. The original statement coordinator and
its action/projection bodies remain intact; no duplicate row codec or target
conditional is introduced. The unchanged cumulative ordinary/Embedded runtime
input owns proof of bounded execution under the default stack and resource
limits.

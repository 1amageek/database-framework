# Schema Generations and Publication

## Purpose and Scope

This component owns immutable database execution generations, retained schema
leases, database-wide schema/catalog publication, and atomic restoration. Its
parent is [DatabaseEngine](../DESIGN.md). It has no child components. It
coordinates with [SQLCatalog](../SQLCatalog/DESIGN.md), which owns the
validated SQL catalog snapshot, deterministic persistence, and
SQL-to-physical binding checks.

S11 changes the publication aggregate. S12 adds trusted transaction-owned
candidate staging and checkpoint restoration. SQL text DDL execution belongs
to S15; block sequencing belongs to its execution owner. Constraint evaluation and SQL DML remain outside this component. S13 extends
the catalog aggregate with declarative constraints, defaults, stored/virtual
generation, identity, and namespace-owned sequences. Those values participate
in catalog digest and immutable generation identity; ordinary physical schema
fingerprints and stored row fields retain their separate contracts.
`Schema`, `SchemaManifest`, DatabaseWire, and schema JSON retain their existing
physical-schema contracts.

## Responsibilities and Boundaries

- Schema owns aggregate validation, persistence, candidate identity, and
  authoritative publication of physical schema, SQL catalog, active
  fingerprints, physical-index metadata, and generation together. Standalone
  management calls open their transaction; S12 staging consumes the outer
  transaction's attenuated access and never opens or commits another one.
- SQLCatalog owns declaration validation, canonical bytes and digest, persisted
  static-model protection, and logical-to-physical mappings. It never commits
  or obtains an independent transaction.
- Registry owns model adaptation and the compiled-model witness. Schema
  consumes that witness without inferring compiled status from grouping
  identity fields, runtime names, or registration order.
- Core owns container creation, storage ownership, admission, and shutdown.
  Schema retains those lifetimes through existing leases and publication hooks.
- A local `DBContainer` publication call is a trusted application-management
  operation. Ordinary SQL text, a query context, a catalog declaration, or a
  schema lease does not grant schema-administration authority.
- With `MultiBase`, the database-wide aggregate lives in the control domain;
  data-root migration snapshots stay in their selected data domain. Persisted
  Grant evaluation remains inside the control publication transaction.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Admission and module composition | Composes immutable generations with execution. | Leases do not grant mutation authority. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | coordinates with | Validated snapshot and transaction-scoped persistence | Supplies immutable SQL meaning and protection witnesses. | Never opens or commits a transaction. |
| [Core](../Core/DESIGN.md) | coordinates with | Container and storage lifetime | Opens, restores, and publishes generations. | Global and data-root metadata are different authorities. |
| [Transaction](../Transaction/DESIGN.md) | coordinates with | Attempt-local candidate snapshots, rollback barrier, authoritative commit transfer | Owns candidate selection and checkpoint lifetime inside its actor. | Storage rollback precedes candidate restore; callback return does not permit publication. |
| [Registry](../Registry/DESIGN.md) | depends on | Physical schema persistence and model-runtime witnesses | Supplies physical entities and compiled model contracts. | Schema-driven factories cannot erase persisted protection. |
| [DatabaseKit Schema](../../../../database-kit/Sources/DatabaseKit/Schema/DESIGN.md) | depends on | Physical schema and SQL catalog declarations | Separates persistent layout from logical SQL naming. | Catalog publication does not change the Schema manifest. |
| [StorageKit](../../../../storage-kit/Sources/StorageKit/DESIGN.md) | depends on | Caller-owned transaction and commit outcome | Provides atomic aggregate storage and conflict reads. | A failed or unknown commit is not published in memory. |
| [Base](../Base/DESIGN.md) | coordinates with | Control-domain publication and data-root migration | Preserves target and Grant boundaries under MultiBase. | Base state is absent from the default product graph. |

## Architecture

```text
Trusted local application
  -> DBContainer.publishSchema / publishSQLCatalog
      -> validate candidate physical/runtime/catalog aggregate
      -> caller-owned control/default-root transaction
          -> SchemaRegistry entity entries
          -> SQLCatalogStore snapshot + static protection
          -> active fingerprints + layouts + generation + idempotency result
      -> confirmed commit
      -> DatabaseSchemaGenerationStore.publish
          -> one Mutex<State>
          -> immutable generation
              -> Schema + SQLCatalogSnapshot + runtime + security + layouts
                  -> existing and newly acquired leases

restore one storage snapshot
  -> complete physical schema + catalog + metadata
  -> runtimeFactory(physical Schema)
  -> validate persisted catalog protection against physical schema and runtime
  -> publish the validated immutable aggregate
```

## Contracts and Invariants

### Existing implementation facts

The current production paths are
`DBContainer.open`, `openRestoringSchema`,
`initializeSchemaCatalogIfNeeded`, `publishSchema`, and
`publishSchemaCatalogIfAllActiveBasesMatch`. Their code is in
[Core/DBContainer.swift](../Core/DBContainer.swift) and
[DBContainer+SchemaPublication.swift](DBContainer+SchemaPublication.swift).
The latter restores the schema and generation in one storage transaction.
[DatabaseSchemaGenerationStore](DatabaseSchemaGenerationStore.swift) already
publishes under one mutex and retains old immutable values through lease
tokens. It resumes drain continuations outside the lock.

`setCurrentSchemaSnapshot` is also used by data-root bootstrap, migration, and
`installDataRootSchemaSnapshot`. It therefore remains a physical data-root
snapshot writer; adding global catalog persistence to that function would
cross the control/data-root boundary. The database-wide metadata subspace uses
`systemRoot.subspace("_metadata")`; selected-root migration version metadata
uses `systemRoot.subspace("metadata")`. The catalog store receives the former
as its root and writes only its `schema/sql_catalog` key.

### Publication aggregate

One `DatabaseSchemaGeneration` and its `DatabaseSchemaLease` contain the same
immutable internal `SQLCatalogSnapshot`, exposed inside DatabaseEngine through
the lease's `sqlCatalog` getter. Read-owned resolution passes an opaque
package-level value across the module boundary rather than exposing the
snapshot type. `DatabaseRestoredSchemaState` carries the
persisted catalog snapshot state into runtime construction. A catalog-only
rename advances the monotonically increasing generation and changes the
execution-runtime fingerprint while leaving the physical schema fingerprint,
physical-index fingerprint, index layouts, Directory identity, and row bytes
unchanged.

`DatabasePreparedSchemaGeneration` hashes the catalog digest into the
execution-runtime fingerprint only. Its physical-index fingerprint continues
to depend on the physical schema and index layouts. Generation-store equality
therefore detects different SQL meaning through execution identity even when
the physical schema is identical.

Prepared generations own index/fingerprint preparation only. Their initializer
accepts an exact SHA-256 catalog digest and stores no catalog snapshot. Global
aggregate helpers receive the independently validated snapshot alongside the
prepared fingerprints. Historical data-root migration stages use the current
lease's catalog digest and retain their existing target-runtime preparation
contract; they neither construct a fictitious historical runtime nor write a
database-wide SQL catalog. Global migration publication validates a candidate
snapshot only after the selected roots reach the candidate physical schema.

A migration's durable intermediate boundary is metadata, not an admitted
executable generation. Its schema/catalog pair is validated without constructing
an executable historical runtime. A trusted migration plan may reconcile that
metadata to the next declared stage while retaining persisted compiled-model
source classification and updating its exact physical contract digest. The
existing installed runtime identity, security mode, and index policies combine
with that stage's physical schema, layouts, and catalog digest to prepare the
durable boundary fingerprint. Only the final compiled target receives an
executable catalog snapshot and replaces the in-memory generation. The
migration admission gate continues to block data operations until completion.

When the durable schema fingerprint and layouts already equal a historical
stage, complete metadata validation is sufficient to keep that boundary; the
latest runtime is not checked against the historical schema. Intermediate
boundaries remain restorable after a later stage fails. A restoring caller
must supply an actual runtime matching the restored schema before execution.

Global initialization, schema publication, migration reconciliation, and
restoration share transaction-scoped aggregate load/store helpers. The writer
accepts the physical schema, validated catalog snapshot, registry persistence
mode, complete prepared fingerprints/layouts, and generation; it writes all
parts in the caller's transaction. The reader loads all parts in that same
transaction and distinguishes a wholly absent aggregate from every partial,
malformed, or mismatched aggregate. Only a wholly absent store may synthesize
the initial empty/default catalog. Existing data-root snapshot writes keep
their narrower contract.

### Publication API and conflict checks

The management seam is:

```swift
@_spi(DatabaseExecution)
public func publishSQLCatalog(
    _ catalog: SQLCatalog,
    expectedGeneration: UInt64,
    idempotencyKey: String,
    authorization: AuthorizationContext
) async throws -> DatabaseSchemaPublicationResult
```

An SPI `DBContainer.sqlCatalog: SQLCatalog` getter exposes the current leased
declaration. The validated snapshot remains module-internal.

This consumes the current physical schema and runtime; callers cannot supply
replacement physical contracts through a catalog-only operation. It uses the
existing publication result, idempotency namespace, authorization checks,
transition admission, and confirmed-commit-before-publish path. A fresh request
compares both the caller's expected generation and the leased complete
execution identity with durable active state inside the transaction. A stale
physical fingerprint alone cannot authorize a catalog-only rename.

Catalog-only publication reads the active schema-transition marker in its
publication transaction and rejects an active transition. Existing physical
`publishSchema` retains caller-owned schema-job admission and generation CAS:
the coordinator that owns an active application marker must be able to publish
that application's physical schema. The shared aggregate writer selects this
admission rule from the management entry point, without exposing a caller-set
bypass or changing catalog-only admission.

An idempotent retry succeeds only when its physical, index-physical, and
catalog-bearing execution identities equal the recorded request. It republishes
an in-memory generation only when the recorded generation is still the active
complete aggregate. An older successful request remains an old result and
cannot roll the current aggregate backward. Same-key/different-catalog
requests fail with the existing typed idempotency error.

### Integrity publication across retained roots

An integrity-changing catalog publication validates every active and retired
retained Base in the aggregate's own transaction. A serializable Base catalog
range read conflicts with concurrent provisioning, retirement, movement, and
deletion. Tombstones contain no retained rows; intermediate lifecycle states
reject with `transitionInProgress`. Each scan acquires the existing counted
schema-maintenance lease, requires the caller's persisted exact-Base
`administer` Grant, leases the actual Tenant Partition in that transaction,
and binds both the Base root and the transaction execution context before
reading rows. Integrity publication rejects `StorageError.unsupportedOperation` for the unavailable atomic transaction-domain capability
when a retained root has another transaction domain, before aggregate or writer
floor writes. The default single-root path uses the same validation boundary.

```text
owning transaction -> non-snapshot Base enumeration -> same-domain admission
  -> each retained Base: maintenance lease + Partition authority + bound scan
  -> catalog + minimum writer generation + idempotency -> one commit
```

The durable minimum SQL writer generation advances only for integrity changes
(constraints, primary-key meaning, or generated/identity policies). Renames,
sequence declarations, and input defaults do not advance it. Mutating attempts
read that floor without snapshot semantics in their own transaction and reject
an older admitted schema generation. They never read control keys from a foreign
data domain. Trusted staging checks the attempt's original generation before
constructing a candidate; its own checkpoint-aware candidate permits its own
floor advance at commit. Rollback restores both the physical floor write and
the candidate allowance. A candidate cannot retroactively admit a stale writer.

| Invariant | Behavioral evidence |
|---|---|
| All retained rows share one validation/publication commit | `S13SQLMultiBasePublicationTests` second-root rejection and successful publication |
| Rejected publication changes neither catalog nor writer floor | Failed publication followed by old-generation mutation |
| New integrity rules reject stale writers | Retained original lease mutation after publication |
| Harmless rename retains old meaning | Old-lease mutation after rename |
| Foreign roots are never scanned through control storage | Explicit cross-domain rejection |

### S15 physical candidate preparation and transformation

SQL DDL extends the transaction-owned candidate path with an explicitly prepared
physical schema. Application reconciliation remains a separate authority and is
never used by SQL DDL to replace or remove a persisted compiled-model witness.
The candidate retains the old generation while constructing its new runtime,
Directory layer map, security delegate, physical layouts, catalog and admission
reservation before any storage mutation. Unchanged runtime registrations and
injected capability registries are retained; changed dynamic entities receive
schema-driven registrations from the same explicitly composed providers.

```text
old generation + proposed physical schema/catalog
  -> static protection + complete candidate runtime/layout/security admission
  -> caller-owned transaction and same storage domain
      -> CAS + staged complete target metadata and writer floor
      -> old runtime: read/decode + old index removal
      -> target runtime: transform/validate + item/blob write + new index insertion
      -> exact retired index cleanup (Directory nodes remain)
      -> final SQL integrity validation -> idempotent success record
  -> checkpoint retains exact immutable candidate
  -> authoritative outer commit -> synchronous prepared publication
```

The physical aggregate writer may admit breaking registry changes only for the
exact entity set whose complete source rows were transformed or deleted in the
same attempt. A static Directory is opened once from its declaration; only
entities with dynamic Directory components use partition-catalog enumeration.
The Directory enumerator intentionally returns no partitions for a static
entity, which must never be interpreted as absence of its physical row store. A candidate schema alone does not prove that transformation.
The complete target aggregate is staged after generation CAS and before row
conversion and integrity validation, so target-bound reads validate against the
same transaction's target physical fingerprint. Validation failure poisons the
transaction; only checkpoint rollback or full abort can discard its staged
metadata and row effects. An idempotent success record is written only after
validation succeeds, and publication still waits for confirmed outer commit.
Physical field and index write-meaning changes advance the durable SQL writer
floor; catalog-only renames retain the existing harmless-rename behavior.
The transaction selects mutation maintainers from its current immutable lease;
checkpoint restoration therefore restores runtime and maintainer selection as
one value. Failed conversion, policy, index, constraint or storage operations
abort or roll back through the existing checkpoint owner.

S15 schema tests must exercise successful conversion and failure rollback,
checkpoint restoration, old-lease writer rejection, static protection, item/blob
cleanup, exact index cleanup and retained Directory identity. File-backed reopen
and the whole SQL DDL matrix are owned by the task integration verification.

### S12 transaction-owned candidate contract

The S12 trusted staging seam accepts a catalog-only declaration plus the outer
transaction access, authorization, expected generation, and idempotency key.
Its fixed internal signature is:

```swift
func stageSQLCatalog(
    _ catalog: SQLCatalog,
    expectedGeneration: UInt64,
    idempotencyKey: String,
    authorization: AuthorizationContext,
    sourceLease: DatabaseSchemaLease,
    transaction: any TransactionAccess,
    workMeter: DatabaseWorkMeter
) async throws -> DatabaseSchemaStagingOutcome
```

`DatabaseSchemaStagingOutcome.publication` is the management result;
`state` is an optional replacement `DatabaseSchemaStagingState`. The state's
`lease` supplies execution meaning and its `publication` is an immutable
`DatabasePreparedSchemaPublication`. The actor facade derives authorization,
source lease, storage access and meter from its admitted owner. It consumes the
transaction's current candidate lease when present, otherwise its original
lease. SQL text and ordinary block statements do not acquire management
authority through this seam. It retains the existing physical schema, runtime,
maintainers, and index layouts. Physical candidate staging and the required
dynamic runtime/maintainer replacement belong to S15 alongside transactional
schema/table/index DDL. S12 adds no physical-staging declaration or DDL block
step.

`DatabaseSchemaStagingState` is an immutable candidate owner containing a
fully constructed `DatabaseSchemaGeneration` and its request-only lease. The
generation's catalog, runtime, security delegate, Directory layer map, layouts,
and fingerprints are validated before the state can become current. The
candidate retains its original published lease and its admitted metadata
reservation. Checkpoints share this owner; sharing does not add or release a
reservation. The reservation is released only after the last candidate or
checkpoint reference is discarded. A candidate lease is never registered as a
published generation in the global store.

Admission measures the bounded durable catalog footprint before constructing
the candidate snapshot and reserves that footprint plus the fixed generation
and candidate metadata values on the existing request meter. This is encoded
payload and semantic metadata accounting, not a claim about allocator heap
bytes. The prepared publication and request-only lease retain that same claim
through commit; checkpoint sharing never duplicates it.

The `DatabaseTransaction` actor owns the optional current staging state. Its
checkpoint records retain the optional value. The Schema seam returns the
publication result separately from a replacement candidate: replaying an old
idempotency result cannot erase a newer candidate or roll its meaning backward.
Catalog replacement continues to use `SQLCatalogSnapshot.replacingCatalog`.
It preserves S11 static protection, generation CAS, and the in-transaction
transition guard. Existing standalone application-owned physical publication
retains its separate authority and runtime validation.

```text
trusted stage call
  -> current candidate lease or original attempt lease
  -> validate complete candidate before retaining it
  -> caller transaction: Grant + transition guard + CAS + aggregate/idempotency
  -> immutable candidate becomes current for later statements in this attempt
      -> checkpoint retains candidate owner
      -> ROLLBACK TO: close statement resources -> storage rollback
          -> restore Framework state -> restore retained candidate owner
      -> prepare: transfer latest fully constructed candidate to commit owner
          -> authoritative commit success -> synchronous nonthrow publication
          -> failure / cancellation / unknown -> discard without publication
```

Aggregate and idempotency entries are journaled physical writes. Their rollback
uses the same StorageKit checkpoint as row/index/Directory effects; Schema
does not introduce a second reservation registry or a compensating transaction.
Consumed work and deadline never rewind. Abandoned candidate owners release
their retained claims; candidates retained by surviving checkpoints remain
charged. A restored candidate must again supply the exact lease used for SQL
binding, policy, runtime, and later staging.

The caller's transaction must address the ordinary root, or the control root
under `MultiBase`, that owns this aggregate. An unrelated data-domain
transaction cannot stage global metadata. Candidate staging is serialized by
the transaction actor and participates in its operation-entry gate; a failure
leaves the attempt failed until the documented checkpoint recovery succeeds.

Publication consumes only the candidate transferred out of successful
precommit closure. All fallible generation construction precedes commit;
post-commit publication calls only the existing generation-store mutex update.
`DatabasePreparedSchemaPublication.publish()` calls the synchronous nonthrow
Core hook `publishPreparedSchemaGeneration(_:)`; Core delegates directly to
the existing monotonic generation store. Transaction's existing
`prepareForCommit()` keeps its Void contract and the checkpoint-aware caller
then transfers ownership with `takePreparedSchemaPublication()`.
Neither callback return, cleanup completion, persisted writes seen through the
overlay, nor an unknown commit outcome establishes authoritative success.
Independent leases keep the committed generation until confirmed publication;
reopen reads only the committed complete aggregate. Unknown outcomes retain
the S11 idempotent confirmation rule.

### Compiled model protection and application evolution

`EntityRuntimeDefinition` captures a package-visible
`compiledModelContract: Schema.Entity?` in its generic `Persistable`
initializer and transfers it unchanged to `EntityRuntimeRegistration`.
The captured entity includes explicitly added index descriptors, matching the
registration's executable physical contract. The schema-driven initializer
sets this witness to `nil`. Existing `groupingIdentityFields` continues to own
grouping identity only.

Catalog-only publication validates both the active persisted protection and
the target runtime's compiled witness. It may rename logical namespace,
relation, or column names without changing stable IDs or physical mappings.
It may not remove or retarget a compiled relation, change column IDs/mappings,
or alter field, identity, index, Directory, security, or protected SQL
policy/constraint contracts. It may add SQL-only virtual generated columns when
all existing physical bindings and protected declarations remain unchanged. A
virtual column has no physical field or serialized value. Initial explicit
application SQL metadata is authoritative, including for compiled protection. Restoring
with a schema-driven runtime factory preserves the protection embedded in the
persisted snapshot; absence of a newly compiled witness does not downgrade it.

Existing application-owned `publishSchema` and migration paths retain authority
to evolve the physical schema under their existing compatibility and migration
rules. They reconcile surviving logical IDs/names to the target physical
schema and regenerate protection from the target compiled runtime. They do
not apply the catalog-only frozen physical-contract check to a legitimate
application upgrade. Persistence/restoration validation still requires the
resulting target aggregate to be internally consistent. Reconciliation fails
with a typed catalog-transition error if a physical change removes a declared
primary-key column; it never silently drops the key declaration. An
application upgrade that intentionally changes that declaration supplies an
explicit candidate SQL catalog through the optional `sqlCatalog` argument to
`publishSchema`. Automatic startup/migration reconciliation cannot invent that
decision.

## Runtime Flows

| Entry | Physical schema | Catalog | Publication boundary |
|---|---|---|---|
| New compiled open | Application declaration | Deterministic initial mapping with compiled protection | Initialize aggregate, commit, publish |
| Existing compiled open | Existing compatibility and migration rules | Preserve persisted logical identities; reconcile application-owned physical evolution | Validate/commit aggregate before generation replacement |
| Restoring open | One restored physical snapshot | Required persisted catalog and protection | Runtime factory consumes physical schema; validate complete aggregate before publication |
| `publishSQLCatalog` | Current physical schema only | Caller declaration validated against current protection and runtime | Generation CAS and idempotency in the caller transaction, then publish |
| `publishSchema` | Application-owned target | Reconciled target catalog, preserving surviving logical identity | Existing schema/index transition transaction, then publish |
| Data-root migration snapshot | Stage's physical schema | No database-wide catalog write | Existing selected-root transaction |
| Global migration reconciliation | Candidate physical schema after active roots match | Validated stage metadata; final target also validates an executable snapshot | One control/default-root aggregate transaction; only final compiled target publishes in memory |

## State, Ownership, and Lifecycle

The generation store retains the current aggregate and active lease counts in
its existing `Mutex<State>`. The snapshot is immutable and adds no global
registration, mutable cache, actor, background task, or second lock. Old
leases retain their original SQL names, physical schema, runtime, and security
until their token finishes. Schema-drain cancellation, waiter removal,
exactly-once token completion, and container storage shutdown retain their
existing owners and behavior.

Catalog-only renaming has no physical storage retirement and does not require
old leases to drain before publication. C06 retains an old lease while
publishing a rename, proves that a new lease observes the new names, and then
proves that the held lease still resolves its old names against the same
physical backing.

| Logical state | Native | WASM | Embedded |
|---|---|---|---|
| Published generation and lease counts | Existing `Mutex<State>` | Same storage and read/mutation entry points | Same storage and read/mutation entry points |
| SQL snapshot and protection | Immutable generation-owned value | Same | Same |
| Current candidate / checkpoint snapshots | Transaction actor / immutable candidate owner | Same actor and owners | Same actor and owners |
| Candidate metadata claim | Existing reservation `Mutex<State>`; request-only lease retains the claim and original published lease | Same claim and retention | Same claim and retention |
| Drain continuation completion | Resume outside existing mutex | Same | Same |
| Durable aggregate | Caller-owned storage transaction | Backend transaction contract | Backend transaction contract |

No target branch weakens `Sendable`, ownership, locking, callback, or shutdown
contracts. Target compile/link/runtime evidence must name its actual toolchain,
SDK, and backend; source symmetry alone is not runtime evidence.

## Failure, Concurrency, and Constraints

Partial metadata, missing catalog beside an existing schema, malformed catalog
bytes, stale physical binding, inconsistent digest, and compiled-protection
mismatch are typed failures. There is no schema-derived fallback for an
existing but invalid aggregate. SQLCatalog owns catalog admission
bounds; aggregate persistence preserves those bounds without materializing
additional copies beyond the persistence boundary.

Catalog-only direct publication validates existing rows using a DatabaseTransaction
facade over the exact admitted aggregate storage transaction, the held published
schema lease, and the caller's real authorization. The optional validation hook
runs after idempotency replay and CAS/transition checks and before aggregate
persistence. It forces all candidate constraints, including initially deferred
ones. Failure preserves the previous durable catalog and generation. The facade
owns no independent storage transaction, commit, or cancel operation. One default
ExecutionBudget meter is retained outside the storage retry loop, so retries do
not reset consumed work or the operation deadline. Physical-schema publication
continues to use its separate application/migration validation contract.

```text
same aggregate transaction
  -> Grant / idempotency / transition / CAS checks
  -> candidate SQL constraints against existing rows
  -> persist complete aggregate + idempotency result
  -> authoritative commit -> generation publication
```

CAS, idempotency lookup, Grant checks, transition checks, and aggregate writes
share one transaction. Cancellation or commit failure does not replace the
published generation. Unknown commit outcomes preserve existing transaction
semantics and are resolved by idempotent retry rather than assumed success.

## Verification and Change Impact

| Invariant | Behavioral evidence owner |
|---|---|
| Rename changes only execution meaning and survives restart | Schema lifecycle tests in `Tests/SQLiteTests`, using a real file-backed SQLite restart |
| C06 holds an old lease through rename publication; old/new leases resolve their own names over unchanged backing without an unnecessary drain | `Tests/DatabaseEngineTests/S11SchemaGenerationPublicationTests.swift` |
| Catalog-only stale-generation and reused-key requests fail | Schema publication tests in `Tests/DatabaseEngineTests` |
| Failed/unknown commit and partial state never publish synthetic success | `Tests/DatabaseEngineTests/S11SchemaGenerationPublicationTests.swift` |
| S12-SC01: later statement binding consumes the current candidate while independent leases retain the committed catalog | `Tests/DatabaseEngineTests/S12SchemaStagingTests.swift`, `candidateVisibilityAndCommit`; actual row read in `Tests/SQLiteTests/S12SchemaStagingSQLiteTests.swift` |
| S12-SC02: nested rollback restores candidate identity and idempotency entries, releases only abandoned claims, and preserves surviving checkpoint claims | `S12SchemaStagingTests.nestedRollbackRestoresCandidate` |
| S12-SC03: full rollback, failed commit, cancellation, and commit-unknown never publish; successful outer commit publishes exactly the final candidate | `S12SchemaStagingTests` failure/outcome cases plus both file-backed restart cases in `S12SchemaStagingSQLiteTests` |
| S12-SC04: staging preserves stale CAS, transition admission, compiled protection, and old idempotency replay behavior | `S12SchemaStagingTests` admission/replay cases; budget/domain cases verify rejection before aggregate writes |
| Static compiled contracts survive schema-driven restore and reject catalog retarget/delete | Catalog protection and publication tests in `Tests/DatabaseEngineTests` |
| Application-owned compiled schema evolution and historical stage preparation remain valid | `Tests/DatabaseEngineTests/S11SchemaGenerationPublicationTests.swift` plus existing bootstrap/evolution tests |
| Historical boundary reconciliation does not apply the latest runtime contract to an older stage; committed V2 survives a failing V3 stage and reopens with a matching V2 runtime | `Tests/SQLiteTests/MigrationExecutionSQLiteTests.swift`, `multiStageMigrationExecutesInOrderAndPersistsBetweenStages` and `failedLaterStageKeepsEarlierStageCommitted`; existing SQLite migration suites cover one-stage, concurrent, Directory, and polymorphic transitions |
| Data-root migration does not write global catalog and control publication is atomic | Existing SQLite/MultiBase migration suites with catalog assertions |
| Decoder bounds and exact logical/physical binding | [SQLCatalog verification](../SQLCatalog/DESIGN.md#verification-and-change-impact) |

Changes to snapshot admission or catalog digest require rechecking prepared
execution identity, idempotency, restoration, and old-lease behavior. Changes
to physical schema evolution require rechecking Registry compatibility,
application-owned protection regeneration, and global migration reconciliation.
The package-owned harness runs narrow selectors first, then affected target
and consolidated package/backend lanes once the implementation converges.
No build or behavioral execution is claimed by this design document.

### Normal application composition with durable SQL entities

Normal `DBContainer.open(for:)` composes the current application declaration with
persisted SQL-owned physical entities through SQLCatalog's internal durable
witness. Runtime reconstruction uses the application's explicitly composed entity
and authorization factories. No identifier prefix, missing compiled witness, or
current runtime name establishes SQL ownership. The final composed schema,
runtime, catalog and physical layout are validated together before bootstrap or
publication. This preserves ordinary normal-open behavior after SQL CREATE TABLE
or MATERIALIZED VIEW without switching the host to a different opening API.

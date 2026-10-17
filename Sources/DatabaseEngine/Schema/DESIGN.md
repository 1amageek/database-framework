# Schema Generations and Publication

## Purpose and Scope

This component owns immutable database execution generations, retained schema
leases, database-wide schema/catalog publication, and atomic restoration. Its
parent is [DatabaseEngine](../DESIGN.md). It has no child components. It
coordinates with [SQLCatalog](../SQLCatalog/DESIGN.md), which owns the
validated SQL catalog snapshot, deterministic persistence, and
SQL-to-physical binding checks.

S11 changes the publication aggregate. SQL text DDL execution, block execution,
constraint enforcement, and SQL DML belong to later execution components.
`Schema`, `SchemaManifest`, DatabaseWire, and schema JSON retain their existing
physical-schema contracts.

## Responsibilities and Boundaries

- Schema owns the transaction that publishes the physical schema, SQL catalog,
  active fingerprints, physical-index metadata, and generation together.
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
or alter field, identity, index, Directory, or security contracts. Restoring
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

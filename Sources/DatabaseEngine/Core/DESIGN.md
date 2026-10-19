# Core

## Purpose and Scope

The Core component owns polymorphic runtime orchestration: group-directory
access, type-code resolution, retained identifier construction, runtime-bound
model decoding, per-entity authorization, and ordered assembly of a retained
batch.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none in the DF-06F0 scope.

## Responsibilities and Boundaries

Core consumes a Read-issued operation admission and the session-owned
transaction. It resolves only runtimes belonging to the requested group and
produces a purpose-neutral retained aggregate.

Core does not make query-level authorization decisions, own the aggregate
representation, implement stored-value framing, or materialize specialized
index output. Public output conversion occurs at an explicit `DatabaseContext`
boundary after a retained operation succeeds.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Cross-component flow and failure contract | Defines the composed scan/fetch guarantee. | Core is not a substitute session boundary. |
| [Read](../Read/DESIGN.md) | depends on | Transaction, operation admission, schema/runtime view | Supplies authority and exact request meter. | A bare session plus caller assertion is insufficient admission. |
| [Serialization](../Serialization/DESIGN.md) | depends on | Retained bounded item read | Loads item envelopes and external chunks. | Raw unbounded reads are excluded from the retained path. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Builder and retained aggregate | Commits pre-admitted present/missing slots. | General model or identifier escape is forbidden. |

## Architecture

```text
Read operation admission
       |
       v
polymorphic group directory + session runtime map
       |
       +-> scan cursor -----------+
       |                          |
       +-> ordered retained keys -+-> retained ItemStorage read
                                      -> runtime-bound decode
                                      -> per-entity authorization
                                      -> pre-admitted aggregate slot
```

## Contracts and Invariants

- Core accepts only a Read-issued admission matching the group, session, and
  meter used for the operation.
- Runtime lookup comes from the session's schema generation. An unknown or
  nonmember type code is a typed failure.
- Identifier bytes, decoded tuple, stored bytes, decoded model, and collection
  slot use the same work meter and remain owned until moved into the aggregate
  or destroyed on failure.
- A scan opens its cursor only after aggregate setup and session admission.
- Ordered fetch produces exactly one slot per retained input key. Directory or
  value absence commits `nil`; malformed identifiers and unknown types fail.
- Core calls dynamic per-entity authorization after decode and before commit.
- It never returns a raw array of `PolymorphicEntity` or `PersistedModel` as an
  intermediate contract.

## Runtime Flows

### Scan

```text
admission -> create aggregate builder -> open group directory/cursor
    -> pre-admit slot -> retain identifier -> resolve runtime
    -> retained decode -> authorize entity -> commit present slot
```

### Ordered point fetch

```text
admission -> pre-admit aggregate capacity -> for each retained key
    -> pre-admit slot -> retain identifier and storage key -> resolve runtime
    -> bounded retained read -> commit missing OR decode/authorize/commit model
```

## State, Ownership, and Lifecycle

Core holds no global mutable state. Directory subspaces and runtime maps are
immutable operation values. Each loop-local owner either moves into the
aggregate entry or is destroyed before the next iteration/error escape.

## Failure, Concurrency, and Constraints

The point-fetch loop is serial on the session transaction. Budget failure,
malformed identifiers, unknown type codes, missing chunks, checksum failure,
authorization failure, cancellation, and session revocation propagate without
synthetic success. Scan cleanup is awaited by the Serialization cursor scope;
point cleanup is owned by the Read operation scope and local retained owners.

## Verification and Change Impact

- [PolymorphicRetainedResourceContractTests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
  owns ordering, missing-slot, later-failure, cancellation, meter, and
  admission-before-storage evidence.
- Public full-scan behavior remains covered by existing polymorphic fetch tests
  after consuming output promotion.
- A changed group key, type-code, runtime canonicalization, or transaction
  contract invalidates this component and QueryExecution destination evidence.


### S06 clock and configuration ownership

Core owns DBConfiguration.sqlTemporal (default SQLTemporalConfiguration.utc),
its existing injected WallClock, and the operation bridge to the
[SQLTemporal environment](../QueryExecution/SQLTemporal/DESIGN.md). Both standard
and MultiBase initializers retain the same immutable configuration and
Sendable capability; no platform branch replaces isolation.

DatabaseContext.makeSQLTemporalEnvironment captures statement time once and
uses a matching active transaction's logical start, or that same statement
time for an implicit transaction. A container-bound immutable TaskLocal
statement binding propagates the explicit environment through internal
reentry. Public SQL starts a fresh statement before parsing; typed read and
Mutation entries also capture once. Helpers preserve explicit/inherited
environments instead of reading the clock during row evaluation. This binding
confers no transaction, schema, Base or principal authority.

withTransaction and withStorageAccess capture logical transaction time outside
TransactionRunner retry closures and pass it into each attempt binding/actor.
Nested/attenuated bindings preserve it even if databaseTransaction becomes nil.
Composition's single container captures one operation environment and relays it
to member/decision bindings. A context from another container starts its own
configuration/environment rather than inheriting a foreign statement binding.

Clock output conversion is the SQLTemporal contract; monotonic scheduling and
deadlines stay unchanged. Advancing-clock tests prove fresh public statements,
stable transaction time through retries, live per-occurrence CLOCK_TIMESTAMP,
attenuation, nested reads and Composition inheritance. Stored state remains
immutable or under the existing actor/Mutex owners.

### S12 Storage operation propagation

ContainerTransaction and ContainerTransactionAccess forward the StorageKit
`performOperation` requirement to their retained underlying transaction,
preserving whole-operation checkpoint barriers through container lifecycle
adaptation. Both forward the configured mutable checkpoint capability; only
ContainerTransaction forwards lifecycle-owned checkpoint configuration.
Read attenuation and the no-rollback-authority contract are owned by
[Transaction](../Transaction/DESIGN.md#read-wrapper-propagation).
No new Core mutable state or target-specific synchronization is introduced.

The checkpoint callback in DatabaseContext carries Schema's optional fully
prepared generation publication beside the callback value through the
existing runner. Only authoritative commit success permits the synchronous
nonthrow DBContainer publication hook, after which the applicable cancellation
check precedes result return. Schema owns that small hook in DBContainer and
its prepared-generation type; Core does not rebuild or reinterpret the
candidate after commit. Failure and unknown commit outcomes discard the
candidate without publishing it.

The lifecycle owner passes physical checkpoint control privately to the
transaction actor independently of attenuating data access. A callback admitted with
read access therefore preserves read-only storage and Directory enforcement
while the actor can restore its own logical/physical checkpoint. No raw
checkpoint or transaction lifecycle authority escapes through read wrappers.


### S13 explicit SQL database identity

DBConfiguration.sqlDatabaseName is an optional immutable SQL-facing name supplied
by application composition. It is distinct from the debugging-only configuration
name and the physical databaseRootPath. QueryExecution consumes it for
CURRENT_DATABASE and reports typed absence for nil/empty values. The value neither
selects nor authorizes a storage root. All configuration initializers preserve it
with the same semantics on default and MultiBase compositions. No mutable state,
second lifecycle or backend-specific isolation is added.

### S13 sequence request admission

DBContainer retains one SQLSequenceCoordinator actor for FIFO SQL request
admission through allocator replay and mandatory recovery close. The same actor
and Sendable contract exist on Native, WASM and Embedded. SQLBlock's outer
withDataOperation scope retains the selected schema/Base root across every data
attempt and independent allocator transaction. Its collecting box leaves that
scope before the final Ready transition; no transaction, schema/Base request
lease or sequence recovery operation survives public promotion. SQLSequences
owns queue cancellation/deadline behavior and durable allocator semantics.


### S15 SQL-owned physical index generations

Core bootstrap and schema restoration consume Schema's validated SQL catalog
snapshot and SQLIndexes' storage identities. The SDK runtime validator first
resolves schema-owned layouts; Schema then adds catalog-owned SQL index layouts.
Both index sets enter the retained generation and persisted layout admission.
SQL namespaces are lookup metadata and never select a Directory or authority.

```text
declared application schema + runtime
    -> provider/layout validation before any storage transaction
    -> persisted schema + SQL catalog
    -> ownership merge + application contract reconciliation
    -> merged SDK layout validation + SQL index identities/layouts
    -> physical fingerprint and persisted-layout equality
    -> immutable generation publication
    -> index initialization
```

The physical fingerprint includes SQL index definition and layout identities,
so changing an expression, predicate, binding, uniqueness or NULL semantics
invalidates an old physical generation even when its storage name is unchanged.
Ordinary application open and schema-driven restore retain the same complete
layout map, reject persisted mismatches before index initialization, and preserve
compiled model protection. Normal application open validates the supplied
application runtime and index providers before any storage transaction or
Directory preparation. It then asks SQLCatalog to merge its persisted
physical-ownership witnesses with the current declared application schema. Only entities created by admitted SQL physical candidates survive absent
application declarations; current application and compiled-model contracts remain
authoritative. Core rebuilds the immutable runtime through its existing injected
factories, then validates the merged schema, storage requirements and layouts
before reconciliation and publication. An unchanged schema reuses its admitted
provider layouts; a merged schema validates its effective layouts again. Invalid
application providers therefore fail without storage access, while SQL ownership
restoration still validates the complete generation. Logical catalog edits cannot grant physical
ownership, and SQL namespaces do not choose this merge.
Migration catalog publication derives identities from
the exact reconciled catalog it persists. Schema owns candidate preparation and
transactional publication; Core consumes that aggregate without bypassing
reconciliation or separating row, runtime and catalog visibility.

[S15CoreSQLIndexGenerationTests](../../../Tests/DatabaseEngineTests/S15CoreSQLIndexGenerationTests.swift)
owns definition-sensitive fingerprints and both reopen paths, including rejection
of incomplete persisted SQL layouts. The existing actor/Mutex state owners and
Sendable contracts remain common to Native, WASM and Embedded.

### Finalized SQL catalog policies

Initial and restored opens bind SQL authorization only after catalog validation,
before constructing the observable runtime generation/security delegate, under
[Configuration's logical SQL policy contract](../Configuration/DESIGN.md#logical-sql-authorization).
The same binding precedes a generation replacement; existing leases retain the
old immutable runtime. No container or storage lifecycle changes are introduced.

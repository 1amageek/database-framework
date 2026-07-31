# Architecture and Ownership

database-framework is the execution layer of the database stack. It consumes
portable contracts from database-kit, applies database semantics, and delegates
physical key-value operations to storage-kit.

## Package Boundaries

~~~text
database-types
  FieldValue, FieldObject, ByteString, Timestamp, civil time values,
  ExactDecimal, UUID, vectors, geographic values, RDFTerm
        |
        v
database-kit
  Persistable contracts, schema metadata, EntityReference, QueryIR,
  mutation contracts, DatabaseWire
        |
        v
database-framework
  DBContainer, DatabaseContext, transaction orchestration, persistence,
  query planning and execution, graph semantics, migrations, indexes
        |
        v
storage-kit
  StorageEngine, transaction and range contracts, Tuple, Subspace,
  directory services, concrete storage adapters
~~~

database-types owns only portable primitive database values. It does not own
models, schemas, queries, indexes, persistence, or storage behavior.

database-kit owns the contracts that describe a database program. It may depend
on database-types, but it does not execute a query or open a storage transaction.

database-framework owns database behavior. It resolves schemas, coordinates one
logical transaction, enforces mutation and relationship rules, maintains indexes,
executes QueryIR, and performs migrations. It does not implement a concrete
storage engine or transport.

storage-kit owns the physical storage contract and backend adapters. It does not
interpret models, QueryIR, graph semantics, or application commands.

## Runtime Forms

Native and Embedded WASI builds use the same database-framework sources and the
same synchronization, transaction, and error contracts. A full Cloudflare
database runtime links the application-specific schema and database-framework
into a Swift 6.4 Embedded WASM reactor. `DatabaseTypesFoundation`,
`DatabaseKitFoundation`, and `DatabaseServerFoundation` are adapter products and
do not enter that reactor dependency graph.

An Embedded application, such as Calendar, remains a separate WASM artifact. It
uses database-client and DatabaseWire to call the full Embedded database reactor;
it does not link the database execution engine into the Calendar artifact.

~~~text
Embedded application WASM
        |
        | DatabaseWire
        v
Full database-framework Embedded WASM reactor
        |
        | StorageEngine transaction contract
        v
host storage adapter
~~~

### Runtime Feature Selection

The package exposes database runtime capabilities through SwiftPM traits. A
trait changes the target dependency graph before compilation; it is not a
runtime flag. Consequently, an application-specific Embedded reactor links only
the index implementations and runtime registrations selected by its dependency
traits.

| Trait | Runtime capability |
|---|---|
| `ScalarIndexes` | scalar and composite indexes |
| `VectorIndexes` | vector maintainers and polymorphic readers |
| `FullTextIndexes` | full-text and autocomplete indexes |
| `SpatialIndexes` | spatial indexes |
| `RankIndexes` | rank indexes and readers |
| `BitmapIndexes` | bitmap indexes and readers |
| `VersionIndexes` | version indexes and readers |
| `PermutedIndexes` | permuted indexes and readers |
| `GraphIndexes` | property graph, RDF, and SPARQL; enables `ScalarIndexes` |
| `AggregationIndexes` | count, numeric, distinct, and percentile indexes |
| `LeaderboardIndexes` | time-window leaderboard indexes |
| `Relationships` | relationship mutation maintenance and typed remote error mapping |
| `AllRuntimeFeatures` | every runtime capability above |

`AllRuntimeFeatures` is enabled by default for the standard all-in-one runtime.
Applications that provide an explicit trait set replace that default. For
example, a graph application can select `GraphIndexes` and `Relationships`
without linking vector, full-text, aggregation, or leaderboard implementations.

Feature selection never weakens runtime validation. The selected providers,
readers, logical-source executors, and mutation maintainers are still assembled
into one container-scoped `DatabaseRuntimeConfiguration`. During bootstrap,
schema validation rejects every declared index or relationship whose required
runtime capability is absent. Unsupported declarations therefore fail before
the container serves requests; they do not fall back to scans or no-op
maintenance.

## Value and Ownership Contract

- FieldValue and its primitive members come from database-types. Framework-local
  replacement value types are not permitted.
- FieldObject is the canonical ordered object value. Encoding plans are private
  execution details and must not become competing public value models.
- ByteString is the owned byte primitive. Borrowed storage views are retained only
  for the lifetime guaranteed by their owner.
- Bounded scan pages and typed resumable-operation slices are single-owner
  values. Asynchronous typed boundaries transfer them with `sending`; callers
  consume each page or slice once instead of implicitly copying a potentially
  large work product.
- A typed resumable-operation slice is encoded into its erased persistent form
  immediately. The erased form is a copyable ByteString handle whose backing
  storage remains shared; copying that handle does not copy payload bytes. The
  framework does not cache both representations or materialize an intermediate
  byte array.
- Large binary paths prepare and measure once, write directly into a destination
  buffer, and avoid intermediate Data or Array materialization.
- Copies are allowed at explicit ownership, persistence, or external API
  boundaries where the receiving side must own independent storage.

## Transaction and Index Invariants

- A model mutation, relationship delete rule, derived index update, precondition,
  and idempotency decision belong to one logical transaction.
- Disabled indexes are not maintained or read.
- Write-only indexes are maintained but are not query candidates.
- Readable indexes are maintained and may be selected by the planner.
- A missing lifecycle key represents an index that has not entered its
  lifecycle and is interpreted as disabled where the operation permits it.
  Every present lifecycle value must be exactly one known state byte; malformed
  or unknown values fail without being overwritten.
- Read admission is stricter than lifecycle administration. An unregistered
  dynamic partition is an empty dataset, but an existing namespace with a
  missing lifecycle key is corrupt metadata and fails with
  `IndexStateError.missingPersistedState`. Only `readable` may reach a physical
  index reader; `disabled`, `writeOnly`, malformed, and unknown states fail.
- Namespace lookup, lifecycle admission, and the physical index read use the
  same caller-owned transaction and read version. A read never creates a
  namespace or initializes lifecycle metadata.
- An online build validates its configuration before changing lifecycle state.
- An index becomes readable only after its build, uniqueness checks, and progress
  finalization succeed.
- A provider declares whether its index kind supports uniqueness. Runtime
  configuration rejects a unique index when that capability is absent.
- The concrete uniqueness-capable maintainer owns physical key interpretation and
  conflict discovery. DatabaseEngine owns lifecycle policy, violation persistence,
  and the decision to reject a readable-index conflict.
- Mutation maintenance and online builds invoke the same uniqueness policy inside
  the caller's transaction, so a partially built index cannot observe weaker
  uniqueness semantics than ordinary writes.
- Unsupported backend capabilities fail explicitly; they are not silently
  emulated with weaker semantics.

## Dependency Injection

DBContainer owns one DatabaseRuntimeConfiguration. Registries for persistable
types, readers, mutation maintainers, index maintainers, graph executors, and
other runtime services are resolved from that configuration. Global mutable
registration is not part of the runtime contract.

Each registered entity runtime must contain the complete `Schema.Entity` used
by the container. Bootstrap compares identifier, fields, directories, indexes,
relationships, authorization rules, enum metadata, ontology, and polymorphic
membership before preparing storage. An entity name match alone never
authorizes a typed runtime.

Application-composed indexes that are not declared on the model must be passed
in the same canonical order to both schema and runtime compilation:

```swift
let additionalIndexes: [IndexDescriptor] = [
    applicationIndex
]
let entity = try Schema.Entity(
    from: Event.self,
    including: additionalIndexes
)
let runtimeEntity = try DatabaseFrameworkRuntime.entity(
    Event.self,
    including: additionalIndexes
)
```

The runtime factory accepts the typed model and additional indexes rather than
an arbitrary `Schema.Entity`, so a manually altered field schema cannot be
paired with the model's static decoder.

## Index read ownership

The schema owns index identity. A read resolves the entity, exact index name,
and exact kind from the container schema before selecting an executor. Feature
modules receive the resolved descriptor and an admitted index subspace; they do
not derive an index name from fields or search the schema globally.

Raw key-value iteration uses a caller-owned `KeyValueCursor`. Keys and values
remain in their backend-owned `ByteString` buffers, and readers do not
materialize an intermediate array or `AsyncStream`. Consumers must finish a
cursor when they stop before exhaustion. Tuple range bounds use prefix
successors so inclusive and exclusive bounds include or exclude the complete
set of compound keys sharing the indexed tuple prefix.

## Synchronization

Shared mutable state uses the same actor or Synchronization.Mutex owner on native
and WASI targets. Target conditions may select an ABI or platform facility, but
must not weaken isolation, Sendable, ownership, or shutdown semantics.

DBContainer and DatabaseTransaction retain actor ownership of asynchronous
database behavior. DatabaseTransactionScope protects only short, in-memory
admission state with `Synchronization.Mutex`: no I/O or suspension occurs while
the lock is held, and close continuations resume after the lock is released.
This synchronization and lifecycle contract is identical on native and WASI
targets.

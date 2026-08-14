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
  namespace resolution/catalog capabilities, concrete storage adapters
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

### Framework And Native Server Are Different Layers

The default customization boundary is `database-framework`. A product owns its
schema, runtime feature traits, commands, policy, clocks, and injected
`StorageEngine`, then opens `DBContainer` in the application process. The
framework does not depend on `database-server` and this path does not create a
listener or process boundary.

The independent `database-server` repository has two layers. Its
Foundation-independent `DatabaseServerRuntime` owns DatabaseWire operation
dispatch, remote command composition, durable server jobs, and schema
administration. Its `DatabaseServerHost` owns native transport, TLS,
credential persistence, routing, signals, and process lifecycle. Both invoke
the framework's execution APIs; neither duplicates query or index semantics.

~~~mermaid
flowchart TB
    Definition["Application definition<br/>schema / policy"] --> Framework["database-framework<br/>DBContainer + execution"]
    Framework --> Engine["Injected StorageEngine"]
    NativeHost["DatabaseServerHost<br/>native standalone"] --> ServerRuntime["DatabaseServerRuntime<br/>Wire operations + jobs"]
    Cloudflare["Cloudflare Durable Object host"] --> ServerRuntime
    ServerRuntime --> Framework
    Remote["CLI / DatabaseClient"] --> NativeHost
~~~

| Owner | Must not own |
|---|---|
| Application + `database-framework` | Wire dispatch, remote jobs, HTTP/WS listener, token files, native signals |
| `DatabaseServerRuntime` | storage backend lifecycle, query planning, index semantics |
| `DatabaseServerHost` | operation semantics or framework execution internals |

### Storage Injection And Ownership

`DatabaseEngine` depends on `StorageEngine` only. It does not import, select,
or construct FoundationDB, SQLite, PostgreSQL, or Cloudflare adapters. Concrete
adapter construction belongs to the composition layer; the `Database` umbrella
provides native facade overloads only when their package traits are selected.

~~~text
default composition
  one StorageEngine
    -> host-selected Subspace (engine root or resolved shared-backend root)
      -> DBConfiguration(storageEngine:, databaseRoot:)
        -> DBContainer(retained database root)
       |-- no Base catalog / placement / persisted Grant lookup
       `-- tuple-derived schema / metadata / data / index subspaces

MultipleBases composition
  DatabaseStorageTopology -> DBConfiguration(storageTopology:) -> DBContainer
       |-- control domain + named data placements
       `-- Base / Composition / persisted Grant execution

both ownership paths
  open failure -> authoritative engine shutdown
  shutdown()   -> authoritative engine shutdown
  deinit       -> same exactly-once release path
~~~

The default configuration carries only one engine lifecycle owner. It does not
construct a one-domain topology as a hidden approximation. The topology owner,
catalogs, target leases, and persisted Grant checks are compiled only when the
`MultipleBases` trait is selected. Once injected, an engine must not be reused
or shut down by its former caller. Dedicated backends use their engine root.
FoundationDB and other shared backends require the host to resolve an explicit
application-selected root before injection. Startup installs a format
descriptor only when that selected root is empty. A populated root without the
current descriptor fails explicitly and is not mutated or treated as a new
database.

## Runtime Forms

Native and Embedded WASI builds use the same database-framework sources and the
same synchronization, transaction, and error contracts. A full Cloudflare
database runtime links `DatabaseServerRuntime`, the application-specific
schema, and database-framework into a Swift 6.4 Embedded WASM reactor.
`DatabaseTypesFoundation` and `DatabaseKitFoundation` are adapter products and
do not enter that reactor dependency graph.

An Embedded application, such as Calendar, remains a separate WASM artifact. It
uses database-client and DatabaseWire to call the full Embedded database reactor;
it does not link the database execution engine into the Calendar artifact.

~~~text
Embedded application WASM
        |
        | DatabaseWire
        v
DatabaseServerRuntime + database-framework Embedded WASM reactor
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

`Database` remains one umbrella product in every composition. Traits apply to
the `database-framework` package and determine which optional target
dependencies and re-exports are present inside that umbrella:

~~~text
consumer Package.swift
  database-framework traits: [GraphIndexes]
        |
        v
Database umbrella
  DatabaseEngine + DatabaseRuntime + QueryAST
  ScalarIndex + GraphIndex + OntologyIndex
        |
        x  RelationshipIndex, VectorIndex, FullTextIndex, SpatialIndex, ...
~~~

The same trait conditions control `DatabaseRuntime` provider registration.
Therefore an implementation cannot be re-exported without being registered, or
registered without being part of the selected dependency graph.

`DatabaseServerRuntime` uses the same composition. Its operation registry and
`capabilities.describe` response contain graph, ontology, and SHACL operations
only when `GraphIndexes` is active. A request for an operation outside the
compiled composition fails with the typed `OPERATION_UNAVAILABLE` error; it
never falls back to a partial implementation.

Graph algorithm, ontology, SHACL, RDF document storage, graph query paging, and
SPARQL mutation services are compiled out of `DatabaseServerRuntime` when
`GraphIndexes` is absent. DatabaseWire's closed query and operation algebra
remains available so a smaller runtime can decode a request and reject an
unavailable operation or statement deterministically.

`DatabaseOperationLimits` contains limits shared by every server composition.
`GraphOperationLimits`, including the SPARQL LOAD document byte limit, exists
only in a `GraphIndexes` composition and is passed from runtime configuration
through the service context into the graph-capable statement executor.

The service composition type follows the selected traits. Without
`GraphIndexes`, `DatabaseOperationServices` requires a statement executor. With
`GraphIndexes`, it instead requires one non-optional `GraphOperationServices`
value containing that executor together with the graph algorithm, ontology,
and SHACL services. The graph-enabled build has no initializer that can create
a partial service composition, so missing graph services are rejected by the
compiler rather than by a runtime capability assertion.

When an application adds commands, it uses
`DatabaseOperationServices.replacingCommandRegistries(read:write:)`; this preserves
the trait-specific service composition, maintenance service, and job service
instead of disassembling and reconstructing feature state.

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
| `GraphIndexes` | ScalarIndex, GraphIndex, OntologyIndex, RDF, and SPARQL; enables `ScalarIndexes` |
| `AggregationIndexes` | count, numeric, distinct, and percentile indexes |
| `LeaderboardIndexes` | time-window leaderboard indexes |
| `Relationships` | relationship mutation maintenance and typed remote error mapping |
| `MultipleBases` | Base lifecycle, placement, Base-local Grants, and read-only Composition execution |
| `AllRuntimeFeatures` | every index and relationship capability above; excludes `MultipleBases` |

`MultipleBases` is a storage and authorization model, not a baseline database
feature. Without it, the Base/Composition/topology/Grant implementation is
conditionally compiled out and the hot transaction path uses the one engine
directly. The standard DatabaseWire v2 graph is target-free. Enabling
`MultipleBases` compiles the Base and Grant values together with the
target-bound DatabaseWire v3 graph; the two package graphs do not expose a
half-enabled target model.

The framework package has no default traits. Every backend, runtime feature,
and `MultipleBases` is selected explicitly by the consuming package. The
independent native `database-server` package currently defaults its own
standalone composition to SQLite and all runtime features, but that host
default does not change the framework dependency graph for in-process or
Embedded applications. For example, a graph application can select
`GraphIndexes` without linking FoundationDB, relationship, vector, full-text,
aggregation, or leaderboard implementations.

Backend traits are platform-gated independently from runtime feature traits.
FoundationDB is available only on macOS and Linux. SQLite and PostgreSQL facade
adapters are available on macOS, iOS, and Linux. WASI/Embedded compositions use
an injected host storage engine; Cloudflare Durable Object SQLite is composed
by database-framework-cloudflare rather than selected as a native backend trait.

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

## Graph selection vocabulary

Graph execution names each value for its concrete responsibility. A `Target`
selects the graph or graph set consumed by one operation, an
`SPARQLExecutionDataset` is the normalized logical dataset for a query, and an
`RDFDatasetGraphMapping` maps persisted entity data into RDF graphs. Blank-node
`Resolver` values derive deterministic identities, while
`GraphResultNodeNamespace` provides domain separation for result identities.
`SPARQLVariableScope` retains the standards-defined lexical binding meaning of
scope; these execution values do not represent an authorization or knowledge
boundary. The approved [Base and Composition](base-composition.md) contract
owns that boundary and its cross-Base read composition.

An RDF named-graph union is an RDF merge, not a concatenation of stored quads.
`RDFNamedGraphSet` canonicalizes the selected graph set, and the scanner
standardizes blank nodes apart by source graph before triple deduplication.
Canonical graph bytes stream directly into the identity digest; only the new
semantic blank-node identifier is materialized.

## Synchronization

Shared mutable state uses the same actor or Synchronization.Mutex owner on native
and WASI targets. Target conditions may select an ABI or platform facility, but
must not weaken isolation, Sendable, ownership, or shutdown semantics.

DBContainer and DatabaseTransaction retain actor ownership of asynchronous
database behavior. TransactionOperationGate protects only short, in-memory
admission state with `Synchronization.Mutex`: no I/O or suspension occurs while
the lock is held, and close continuations resume after the lock is released.
This synchronization and lifecycle contract is identical on native and WASI
targets.

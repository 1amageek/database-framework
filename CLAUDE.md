# Repository Guide

This document records the current architecture and implementation rules for
`database-framework`. It describes responsibilities and contracts, not file
counts or historical module layouts.

## Purpose

`database-framework` is the execution runtime for models and queries declared
by `database-kit`. It owns schema registration, persistence, transactions,
query planning, migrations, index maintenance, graph execution, and canonical
DatabaseWire operation execution. Native listener and process lifecycle belong
to the independent `database-server` package. Storage is provided through
`storage-kit`.

```text
database-types
  FieldValue and portable field primitives
        |
        v
database-kit
  model, schema, query, relationship, ontology, and DatabaseWire contracts
        |
        v
database-framework
  execution, persistence, indexes, migrations, and server handlers
        |
        v
storage-kit
  StorageEngine, transaction, tuple, subspace, and backend adapters
```

The dependency direction is one-way. Primitive values do not depend on model,
query, runtime, transport, or storage behavior.

## Package Responsibilities

| Package | Owns | Does not own |
|---|---|---|
| `database-types` | `FieldValue`, `ByteString`, temporal, decimal, UUID, vector, geo, RDF, and object primitives | Models, schemas, queries, transport, persistence |
| `database-kit` | `Persistable`, schema metadata, index declarations, QueryIR, relationships, ontology contracts, DatabaseWire | Database execution and storage engines |
| `database-framework` | Runtime composition, transactions, persistence, planning, index behavior, migrations, server operation handlers | Portable primitives and backend transaction implementations |
| `storage-kit` | Storage contracts and FoundationDB, SQLite, and PostgreSQL adapters | Model, query, index, and graph semantics |

`FieldValue` is the canonical database field value. Do not introduce another
general-purpose database value type in this package.

## Framework Modules

| Module | Responsibility |
|---|---|
| `DatabaseEngine` | `DBContainer`, `DatabaseContext`, persistence, transaction coordination, catalog, planning, migration, and index extension contracts |
| `DatabaseRuntime` | Explicit full-runtime composition through `DatabaseFrameworkRuntime` |
| `ScalarIndex` | Scalar equality, range, uniqueness, and scalar fusion execution |
| `VectorIndex` | Flat, HNSW, IVF, and product-quantized vector execution |
| `FullTextIndex` | Full-text search, facets, and autocomplete |
| `SpatialIndex` | Spatial indexing and spatial queries |
| `RankIndex` | Rank maintenance and rank queries |
| `PermutedIndex` | Permuted compound-key indexes |
| `GraphIndex` | RDF graph storage, SPARQL, SQL/PGQ, SHACL data access, and graph algorithms |
| `OntologyIndex` | Ontology storage and reasoning |
| `RelationshipIndex` | Relationship maintenance, inverse lookup, referential integrity, and delete rules |
| `AggregationIndex` | Incremental aggregation indexes |
| `BitmapIndex` | Bitmap indexing and bitmap fusion |
| `VersionIndex` | Model version history |
| `LeaderboardIndex` | Time-window leaderboard indexes |
| `QueryAST` | SQL and SPARQL parsing and syntax representation; semantic QueryIR remains in `database-kit` |
| `DatabaseWireRuntime` | Bounded DatabaseWire decoding, typed operation dispatch, limits, jobs, idempotency, and typed response encoding; it is not a network listener |
| `Database` | Convenience facade that re-exports the selected runtime and storage adapter |

Index modules depend on `DatabaseEngine`, `database-kit`, `database-types`, and
the backend-neutral `StorageKit` contract. They must not import a concrete
storage adapter.

## Runtime Composition

Runtime behavior is container-scoped and explicit.

```text
Schema + compiled Persistable types
            |
            v
DatabaseFrameworkRuntime.configuration(...)
            |
            v
DatabaseRuntimeConfiguration
  - maintainer providers
  - read executors
  - logical source executors
  - relationship mutation maintainers
  - authorization policies
            |
            v
DBContainer.open(...)
```

`DatabaseRuntimeConfiguration.validate(schema:)` must fail before opening a
container when a compiled model, maintainer, read executor, uniqueness
capability, relationship maintainer, or required logical source executor is
missing. Do not add global registration or silent fallback.

`DatabaseFrameworkRuntime.configuration` is the full feature composition. A
smaller application-specific runtime may construct
`DatabaseRuntimeConfiguration` directly, but it must meet the same validation
contract.

## Container and Transaction Ownership

| Type | Owns | Must not do |
|---|---|---|
| `DBContainer` | Schema, selected `StorageEngine`, runtime configuration, physical format catalog, partitions, directory resolution, store cache, index lifecycle | Stage application model changes |
| `DatabaseContext` | Change tracking, context-scoped read-version cache, application transaction entry points, model query API | Implement backend transaction semantics |
| `DatabaseTransaction` | One logical transaction's reads, writes, preconditions, mutation effects, and lifecycle | Open an unrelated transaction |
| `DatabaseDataStore` / `ItemStorage` | Schema-aware model storage inside a supplied transaction | Select a backend or create an application transaction |

```text
context.insert / update / delete
            |
            v
DatabaseContext pending mutations
            |
            v
context.save()
            |
            v
one StorageEngine transaction
  - precondition checks
  - canonical item write/delete
  - relationship rules
  - index maintenance
  - mutation metadata
            |
            v
commit or typed failure
```

Every mutation and its relationship/index side effects must share the same
transaction. A failure must remain a failure; do not translate it to an empty
result or default value.

## Persistence Format

`PersistableStorageCodec`, `PersistableFieldFrameCodec`, `ItemEnvelope`, and
`ItemStorage` implement the canonical model persistence path. Field values are
encoded from `database-types`; schema and compiled model metadata come from
`database-kit`.

The storage format and DatabaseWire are separate protocols:

```text
DatabaseWire request
    -> bounded server decode
    -> typed operation handler
    -> database runtime
    -> canonical storage frames
    -> StorageEngine transaction
```

Do not use a transport envelope as a storage record, or expose storage keys as
the database protocol.

For performance-sensitive byte paths, preserve an owning buffer and pass
borrowed ranges/views through decoding and execution. Materialize `Array`,
`Data`, or `String` only at an output, persistence, or external API boundary.
Any required copy on a hot path must be documented and measured.

## Backend Selection

SwiftPM traits select the adapter included in the `Database` facade.

| Trait | Default | Adapter | Intended deployment |
|---|---:|---|---|
| `FoundationDB` | Yes | `FDBStorageEngine` | Distributed server database |
| `SQLite` | No | `SQLiteStorageEngine` | Local, embedded, tests, single-instance services |
| `PostgreSQL` | No | `PostgreSQLStorageEngine` | PostgreSQL and Cloud SQL services |

The execution and index layers remain backend-neutral. Backend-specific
behavior belongs in `storage-kit`; unsupported semantics must be represented by
a typed error or an explicit capability contract.

Compile-time flags used by the facade are `FOUNDATION_DB`, `SQLITE`, and
`POSTGRESQL`. Do not use them to weaken synchronization, ownership, `Sendable`,
or error contracts.

## Server Contract

`DatabaseWireRuntime` consumes the canonical `DatabaseWire` protocol declared by
`database-kit`. It owns operation routing and execution, not transport framing
outside DatabaseWire and not storage implementation.

The 14 registered operations are grouped into these families:

1. capabilities and schema description;
2. schema plan and apply;
3. query and mutation execution;
4. graph algorithms;
5. ontology and SHACL execution;
6. application commands;
7. maintenance operations;
8. job start, status, result, and cancellation.

Requests must pass bounded decoding, admission, authorization, deadline, and
resource-limit checks before execution. Mutations use typed idempotency and
precondition state. Responses and failures are encoded as their declared
DatabaseWire types.

TypeScript or another host adapter may transport bytes and implement a storage
host ABI. It must not own query, schema, index, graph, or transaction semantics.

## Index Extension Contract

New index behavior is registered through explicit protocols:

- `IndexMaintainerProvider` creates typed maintainers for declared index kinds.
- `IndexMaintainer` updates physical entries inside the caller's transaction.
- `IndexReadExecutor` and `PolymorphicIndexReadExecutor` implement reads.
- `FusionReadExecutor` participates in fusion queries.
- `IndexRuntimeRequirements` declares required readers and uniqueness support.

The schema declares what an index means. The framework implementation owns how
it is maintained and read. Registration is supplied to the container through
`DatabaseRuntimeConfiguration`.

## Concurrency and Ownership

Shared mutable state uses the same `Synchronization.Mutex<State>` or actor
contract on Native, WASM, and Embedded builds. `hasFeature(Embedded)` must not
replace synchronized state with raw mutable state or weaken ownership.

- Use an actor when an operation suspends or ordered state transitions matter.
- Use `Mutex` for short, non-suspending memory access.
- Do not call external callbacks, perform I/O, or `await` while holding a mutex.
- Do not use `@unchecked Sendable`, `DispatchQueue`, or `EventLoopFuture`.
- Unsafe pointers stay inside a minimal borrowing closure and never escape
  without an explicit owner/lease contract.

Before completing a shared-state change, inspect every occurrence of
`hasFeature(Embedded)`, `canImport(Synchronization)`, `Mutex`,
`nonisolated(unsafe)`, `@unchecked Sendable`, and mutable stored state in the
affected modules.

## Error and Completeness Contract

- Public failures use typed errors where the API permits.
- Do not use `try?` or silent fallback.
- Unsupported behavior returns a typed failure, never placeholder success.
- A callable partial implementation carries a
  `FIXME(INCOMPLETE_IMPLEMENTATION)` comment immediately before it, naming the
  production path and the behavior required before success is valid.
- Removing that marker requires success and failure behavior tests in the same
  change.

## Naming

Names describe domain responsibility, observable behavior, state transition,
ownership, or lifecycle. Do not encode implementation language, ABI, calling
convention, module identity, binary format, toolchain, build mode, or memory
layout into Swift names unless the term is the user-selected semantic adapter.

`Database` is appropriate only when the declaration represents the database
domain itself. It is not a generic prefix for values, models, bytes, codecs, or
callbacks.

## Build and Validation

Use `swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a` and the matching normal or
Embedded WASM SDK fixed by the workspace instructions. Do not mix snapshot
dates or silently fall back to Swift 6.3.

Build products with the selected trait:

```bash
swift build --product Database
swift build --disable-default-traits --traits SQLite --product Database
swift build --disable-default-traits --traits PostgreSQL --product Database
swift build --swift-sdk <matching-wasi-sdk> --disable-default-traits -c release
```

Run tests through `xcodebuild test`, with an external timeout and the narrowest
focused scheme or test target that covers the change:

```bash
perl -e 'alarm shift; exec @ARGV' 120 \
  xcodebuild test \
  -scheme database-framework-Package \
  -destination 'platform=macOS,arch=arm64'
```

Available focused schemes are maintained for the engine, runtime composition,
server, graph, QueryAST, aggregation, benchmarks, and digest tests. FoundationDB
tests run through the isolated `scripts/fdb-test-env` wrapper.

Validation must cover actual behavior:

1. the public API reaches a real implementation rather than a stub;
2. failure remains observable by the caller;
3. tests exercise success and failure behavior;
4. the selected backend product builds;
5. Native, standard WASM, and Embedded WASM compile/link where the affected
   module supports them;
6. zero-copy or unsafe performance claims include allocation/copy evidence or a
   benchmark.

Before a release, confirm that no `Package.swift` contains `.package(path:)`,
that dependency releases are committed, and that the release tag resolves to
the same commit as `origin/main`.

## Released Dependency Baseline

| Package | Minimum release |
|---|---:|
| `database-types` | `26.0730.0` |
| `database-kit` | `26.0809.4` |
| `storage-kit` | `26.0807.0` |
| `swift-hnsw` | `1.1.4` |

Repository branches and local paths are development conveniences, not part of
the package or release contract.

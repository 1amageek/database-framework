# database-framework

A backend-neutral Swift execution layer for models defined with
[database-kit](https://github.com/1amageek/database-kit).

database-framework owns schema registration, transactions, query planning,
persistence, migrations, and index maintenance. Storage is supplied through
the backend-neutral protocols in
[storage-kit](https://github.com/1amageek/storage-kit).

FoundationDB is the default SwiftPM trait for compatibility with the original
deployment path. It is one backend choice, not a requirement of the framework.
The same execution layer can run with FoundationDB, SQLite, PostgreSQL, an
in-memory engine, or another StorageEngine implementation.

## Architecture

The package separates application behavior from storage deployment:

    database-kit
      Models, schema metadata, IndexKind, QueryIR, DatabaseWire
            |
            v
    database-framework
      DBContainer, Context, query planner, migrations, index maintainers
            |
            v
    storage-kit
      StorageEngine, Transaction, Tuple, Subspace, DirectoryService
            |
            +----------------+------------------+------------------+
            v                v                  v                  v
      FoundationDB       SQLite             PostgreSQL          Custom
      distributed       local/embedded     server/Cloud SQL    InMemory/remote

Application model, query, and index declarations stay the same when the
backend changes. Backend selection happens through a SwiftPM trait and a
storage configuration at initialization.

### Responsibilities

- DBContainer owns the schema, storage engine, directory resolution, and
  index lifecycle.
- DatabaseContext is the user-facing change-tracking context and transaction entry
  point. The name is historical: it is used with every StorageEngine, not only
  FoundationDB.
- DatabaseEngine provides backend-neutral persistence, transaction
  coordination, query planning, migrations, security, and schema catalog logic.
- Index modules maintain and query Scalar, Vector, FullText, Spatial, Graph,
  Aggregation, Rank, Bitmap, Version, Leaderboard, Permuted, Relationship,
  and Ontology indexes.
- StorageKit defines the storage contract and supplies concrete backend
  engines. Only this layer knows backend-specific transaction APIs.

## Backend Selection

| Backend | Trait | Typical deployment | Storage engine | FoundationDB required |
|---|---|---|---|---:|
| FoundationDB | default / FoundationDB | distributed server database | FDBStorageEngine | Yes |
| SQLite | SQLite | local, embedded, tests, single-instance services | SQLiteStorageEngine | No |
| PostgreSQL | PostgreSQL | server, Cloud SQL, Vapor on Cloud Run | PostgreSQLStorageEngine | No |
| Custom | application-defined | in-memory, proxy, or another storage system | any StorageEngine | No |

The common execution path is:

    Schema + DBConfiguration
                |
                v
           DBContainer
                |
                v
            DatabaseContext
                |
                v
      StorageEngine.withTransaction

The framework does not silently assume FoundationDB when a custom engine is
provided. Backend-specific operations are implemented by StorageKit. When a
backend cannot provide an operation, it must expose an explicit error or a
documented semantic mapping.

## Installation

### Requirements

- Swift 6.2 or later
- macOS 26 or later, iOS 26 or later, or a supported Linux Swift toolchain
- One StorageKit backend selected for the target

    dependencies: [
        .package(
            url: "https://github.com/1amageek/database-framework.git",
            from: "26.0629.0"
        )
    ]

The all-in-one Database product re-exports the model, storage, execution, and
index modules selected by the active traits. Individual products such as
DatabaseEngine and VectorIndex are available when smaller dependency graphs
are preferred.

## Quick Start

The model and application code are backend-neutral:

    import Database

    @Persistable
    struct User {
        #Directory<User>("app", "users")
        #Index(ScalarIndexKind<User>(fields: [\.email]), unique: true)

        var email: String
        var name: String
    }

    let schema = Schema([User.self])

    // Supply a backend-specific configuration here.
    let container = try await DBContainer(
        for: schema,
        configuration: DBConfiguration(backend: .custom(engine))
    )

    let context = container.newContext()
    context.insert(User(email: "alice@example.com", name: "Alice"))
    try await context.save()

    let users = try await context.fetch(User.self)
        .where(\.name == "Alice")
        .execute()

The engine in the example is intentionally abstract. Choose one of the
backend initializers below for the target you are building.

## Backend Examples

### FoundationDB

FoundationDB is the default trait. It provides distributed transactions,
native versionstamps, and the dynamic FoundationDB DirectoryLayer.

    swift build
    swift test

    import Database

    let container = try await DBContainer(for: schema)

The explicit form is useful when supplying FoundationDB configuration:

    let configuration = DBConfiguration(backend: .fdb())
    let container = try await DBContainer(
        for: schema,
        configuration: configuration
    )

For local testing, the repository includes an isolated cluster wrapper:

    scripts/fdb-test-env run --clean -- \
      perl -e 'alarm shift; exec @ARGV' 240 \
      swift test --filter FDBContextTests

### SQLite

SQLite is the local and embedded backend. It does not load libfdb_c and does
not require a FoundationDB process.

    swift build --traits SQLite
    swift test --traits SQLite

    import Database
    import SQLiteStorage

    let container = try await DBContainer(
        for: schema,
        configuration: SQLiteStorageEngine.Configuration.file(
            "/path/to/application.sqlite"
        )
    )

For tests and disposable processes:

    let container = try await DBContainer(
        for: schema,
        configuration: SQLiteStorageEngine.Configuration.inMemory
    )

### PostgreSQL and Cloud SQL

PostgreSQL is the server-side backend. The default isolation level is
SERIALIZABLE so transaction behavior is aligned with the framework's strong
consistency model. It can connect over TCP, a Unix domain socket, or the
Cloud SQL socket mounted into Cloud Run.

    swift build --traits PostgreSQL
    swift test --traits PostgreSQL

    import Database
    import PostgreSQLStorage

    let postgres = PostgreSQLConfiguration(
        host: "127.0.0.1",
        port: 5432,
        username: "app",
        password: password,
        database: "app",
        schemaManagement: .createIfNeeded
    )

    let container = try await DBContainer(
        for: schema,
        configuration: postgres
    )

Cloud Run with Cloud SQL uses the same DBContainer API; only the storage
configuration changes:

    let postgres = PostgreSQLConfiguration(
        cloudSQLInstanceConnectionName: "PROJECT:REGION:INSTANCE",
        username: username,
        password: password,
        database: databaseName,
        schemaManagement: .assumeExists
    )

    let container = try await DBContainer(
        for: schema,
        configuration: postgres
    )

Use assumeExists when the Cloud SQL role is restricted to DML and the KV table
is provisioned separately. See
[docs/deployment/cloud-run-vapor-postgresql.md](docs/deployment/cloud-run-vapor-postgresql.md) for
the Cloud Run and Vapor deployment shape.

### Custom and In-Memory Engines

DatabaseEngine accepts any StorageEngine. This is the extension point for
tests, local tools, storage proxies, and future backends.

    import Database
    import StorageKit

    let engine = InMemoryEngine()
    let container = try await DBContainer(
        for: schema,
        configuration: DBConfiguration(backend: .custom(engine))
    )

The same injection path is used for a custom remote or host-provided engine:

    let configuration = DBConfiguration(
        name: "application-storage",
        backend: .custom(customEngine),
        indexConfigurations: [vectorConfiguration]
    )
    let container = try await DBContainer(
        for: schema,
        configuration: configuration
    )

## Transaction and Context Model

DBContainer is a resource manager. It does not create application
transactions. A context owns unit-of-work state and uses the selected engine
for transactions.

    DBContainer
      owns: Schema, StorageEngine, directory and index state
           |
           +--> newContext()
                    |
                    v
                DatabaseContext
                  owns: pending changes, read-version/cache state,
                        transaction orchestration

    let context = container.newContext()

    context.insert(user)
    context.insert(order)
    context.delete(previousUser)

    // All staged mutations are committed as one transaction.
    try await context.save()

For direct transactional work:

    try await context.withTransaction { transaction in
        let value = try await transaction.getValue(for: key)
        try transaction.setValue(updatedValue, for: key)
        _ = value
    }

Transactions, range scans, key selectors, tuple encoding, and directory
resolution are expressed through StorageKit. The selected backend controls
connection management and physical implementation.

## Indexes

Index declarations are part of the model schema. Index maintainers use the
same StorageKit contracts regardless of the selected backend.

| Module | Index | Typical capability |
|---|---|---|
| ScalarIndex | scalar / composite | equality, ranges, sorting, uniqueness |
| VectorIndex | HNSW / flat | similarity search and binary vector payloads |
| FullTextIndex | inverted text | token search and ranking |
| SpatialIndex | S2 / Geohash / Morton | geospatial queries |
| RankIndex | skip list | ordered rankings and top-K |
| GraphIndex | graph / SPARQL | traversal, graph patterns, OWL reasoning |
| AggregationIndex | count / sum / min / max / average | incremental aggregation |
| VersionIndex | temporal versions | history and version-aware reads |
| BitmapIndex | compressed bitmaps | categorical membership |
| LeaderboardIndex | time-windowed ranking | rolling leaderboards |
| PermutedIndex | alternate field order | query-specific key layouts |
| RelationshipIndex | cross-type references | relationship queries |

Index declarations remain independent of backend choice:

    @Persistable
    struct Document {
        #Directory<Document>("app", "documents")
        #Index(VectorIndexKind<Document>(embedding: \.embedding, dimensions: 1536))

        var title: String
        var embedding: [Float]
    }

Backend capability differences are handled at the storage boundary. FoundationDB
provides a native distributed DirectoryLayer and versionstamp operations, while
SQLite and PostgreSQL use their own directory and transaction implementations.
An operation that cannot be represented by a backend must fail explicitly; the
framework does not silently downgrade transactional behavior.

## Migrations and Schema

DBContainer registers the schema catalog and initializes declared indexes.
Versioned schemas can provide a migration plan:

    let migration = Migration(
        fromVersion: Schema.Version(1, 0, 0),
        toVersion: Schema.Version(2, 0, 0),
        description: "Add email index"
    ) { context in
        try await context.addIndex(emailIndexDescriptor)
    }

Migration execution uses the same StorageEngine selected for the container.
Backend-specific provisioning remains outside application migration code when
the backend requires administrative setup, such as a DML-only Cloud SQL role.

## Optional Features

### Graph and Ontology

GraphIndex supports graph traversal, SPARQL-oriented queries, and OWL
integration. Persistable handles storage; OWLClass, OWLDataProperty, and
OWLObjectProperty add ontology metadata.

See [Sources/GraphIndex/README.md](Sources/GraphIndex/README.md) for graph
query and reasoning APIs.

### Database Server

DatabaseServer exposes a DBContainer to
[database-client](https://github.com/1amageek/database-client) through the
DatabaseWire protocol. The server layer is separate from the storage engine;
it can host a container backed by any engine supported by the target.

### Cloudflare Durable Objects

Cloudflare Durable Object SQLite is a deployment adapter, not a FoundationDB
mode hidden inside this repository. It is maintained in the separate
[database-framework-cloudflare](https://github.com/1amageek/database-framework-cloudflare)
package. That package connects the DatabaseWire boundary to Durable Object
SQLite and provides the Worker/WASM host integration.

    Swift application
          |
          v
    database-framework APIs
          |
          v
    database-framework-cloudflare adapter
          |
          v
    Durable Object SQLite

swift-web itself does not need to depend on the adapter. An application built
with swift-web adds the Cloudflare package only when it selects that storage
deployment.

The web host and the database adapter are separate composition points:

    swift-web application
          |
          +--> swift-web-vapor       -> Vapor 5 / Cloud Run host
          |
          +--> swift-web-cloudflare  -> Workers / Durable Object actor host
          |
          +--> database-framework-cloudflare
                    -> DatabaseWire / Durable Object SQLite

`swift-web-cloudflare` hosts SwiftWeb actors and is not a replacement for
`database-framework-cloudflare`. The former owns web actor execution; the
latter owns database access through Durable Object SQLite. An application may
use either one independently or compose both.

## Data Layout

The logical layout is expressed through Subspace and DirectoryService, then
mapped by the backend:

    [directory]/R/[type]/[id]               -> encoded item envelope
    [directory]/I/[indexName]/[values]/[id] -> index entry
    [directory]/state/[indexName]           -> index state
    [_catalog]/[typeName]                   -> schema catalog

The key-value contract is shared. Physical storage differs by backend: FDB uses
keyspace prefixes and its DirectoryLayer, while SQL backends store the same
logical key/value model in their own tables and indexes.

## Modules

| Product | Role |
|---|---|
| Database | all-in-one facade and selected backend re-exports |
| DatabaseEngine | container, context, persistence, planning, migrations |
| DatabaseRuntime | runtime assembly for index maintainers |
| ScalarIndex, VectorIndex, FullTextIndex, ... | individual index modules |
| QueryAST | SQL/SPARQL parsing and serialization |
| DatabaseServer | DatabaseWire/WebSocket server endpoint |
| DatabaseCLICore | embeddable inspection and administration library |

Import Database for the standard application path, or import individual
products when compile time and dependency size matter.

## Build and Test

    # FoundationDB trait (default)
    swift build
    swift test

    # SQLite: no FoundationDB process required
    swift build --traits SQLite
    swift test --traits SQLite

    # PostgreSQL: requires a reachable PostgreSQL instance
    swift build --traits PostgreSQL
    swift test --traits PostgreSQL

    # Release build for the selected traits
    swift build -c release

Test targets are split by backend. FoundationDB tests require a running
cluster, SQLite tests use isolated in-memory or file databases, and PostgreSQL
tests use the configured PostgreSQL test environment. This allows the
backend-neutral engine and index behavior to be validated without installing
FoundationDB.

## Performance

Performance benchmarks are in the PerformanceBenchmarks test target. Results
depend on the selected backend and must not be compared across backends as if
they were the same deployment.

    swift test --filter 'PerformanceBenchmarks.CoveringIndexBenchmark'
    swift test --filter 'PerformanceBenchmarks.IndexedQueryAndWriteBenchmarkTests'
    swift test --filter 'PerformanceBenchmarks.SerializationBenchmark'

The latest checked-in snapshot is documented in the individual index READMEs.
FoundationDB benchmark numbers describe a local Docker cluster and are not a
claim about SQLite, PostgreSQL, Cloud SQL, or Durable Object latency.

## Platform and Runtime Notes

| Runtime | Status |
|---|---|
| macOS | supported by the package manifest |
| iOS | supported by the package manifest |
| Linux | supported where selected dependencies are available |
| Cloudflare Workers / WASM | use database-framework-cloudflare adapter |

FoundationDB-specific modules and imports are conditionally compiled only for
the FoundationDB trait. SQLite and PostgreSQL builds do not link libfdb_c. The
core engine depends on StorageKit protocols and does not embed a FoundationDB
client into every backend build.

## Ecosystem Repositories

The repositories below are related but do not all have the same dependency
direction. The database core stays independent from web hosts and UI tools.

### Core Database Packages

| Repository | Role | Relationship |
|---|---|---|
| [database-kit](https://github.com/1amageek/database-kit) | Models, schema metadata, IndexKind, QueryIR, and DatabaseWire | Direct dependency |
| [storage-kit](https://github.com/1amageek/storage-kit) | StorageEngine, Transaction, Tuple, directory abstraction, and backend engines | Direct dependency |
| [swift-hnsw](https://github.com/1amageek/swift-hnsw) | Swift HNSW graph index used by VectorIndex | Direct dependency |
| [database-client](https://github.com/1amageek/database-client) | Native client SDK, typed queries, and transport layer | Client of the server layer |

### Deployment And Web Integration

| Repository | Role | Relationship |
|---|---|---|
| [database-framework-cloudflare](https://github.com/1amageek/database-framework-cloudflare) | DatabaseWire, Swift WASM runtime, Worker host, routing, and Durable Object SQLite adapter | Database deployment adapter |
| [swift-web](https://github.com/1amageek/swift-web) | Host-neutral Swift server/browser runtime with HTML rendering and WASM client islands | Independent application framework |
| [swift-web-vapor](https://github.com/1amageek/swift-web-vapor) | Optional Vapor 5 host adapter for swift-web | Web host adapter |
| [swift-web-cloudflare](https://github.com/1amageek/swift-web-cloudflare) | SwiftWeb actor hosting on Workers and Durable Objects | Web host adapter |
| [swift-web-cloud-run](https://github.com/1amageek/swift-web-cloud-run) | Cloud Run project templates for swift-web | Deployment templates |

`swift-web` does not depend on `database-framework`. A service built with
swift-web can add `database-framework`, `database-framework-cloudflare`, or
another backend integration according to its deployment. This preserves the
boundary between the web framework and database implementation.

### Tools And Validation

| Repository | Role |
|---|---|
| [database-studio](https://github.com/1amageek/database-studio) | Native macOS data browser and graph visualizer for database-framework |
| [database-framework-benchmark](https://github.com/1amageek/database-framework-benchmark) | PostgreSQL benchmark and framework-overhead comparison |

## License

MIT License

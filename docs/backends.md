# Backend Guide

database-framework executes against StorageKit protocols. `DatabaseEngine`
never imports or selects a concrete backend. Its only storage construction
contract is an initialized `StorageEngine` passed to
`DBConfiguration(storageEngine:)`.

## Backend Matrix

| Backend | SwiftPM trait | Package platforms | Engine | External service |
|---|---|---|---|---|
| FoundationDB | `FoundationDB` | macOS, Linux | `FDBStorageEngine` | FoundationDB cluster |
| SQLite | `SQLite` | macOS, iOS, Linux | `SQLiteStorageEngine` | none |
| PostgreSQL | `PostgreSQL` | macOS, iOS, Linux | `PostgreSQLStorageEngine` | PostgreSQL |
| Custom/host | none in database-framework | implementation-defined | any `StorageEngine` | implementation-defined |

All application data access passes through the same conceptual path:

~~~text
DBContainer -> DatabaseContext -> StorageEngine -> Transaction
~~~

DatabaseContext is backend-neutral. Backend traits only decide which facade
adapters are available to the consuming package. The injected engine decides
which backend one container uses.

## SwiftPM Traits

~~~bash
scripts/fdb-test-env run --clean -- \
  scripts/xcode-test-harness \
    --traits FoundationDB,AllRuntimeFeatures \
    --skip-testing BenchmarkFrameworkTests \
    --skip-testing PerformanceBenchmarks \
    --expected-count 3918 \
    --require-zero-skips \
    --require-zero-expected-failures \
    --require-zero-runtime-warnings
~~~

The default full-host profile enables both `FoundationDB` and
`AllRuntimeFeatures`. A consuming package that specifies an explicit trait set
without `.defaults` replaces that profile. For example, a graph runtime can
select `GraphIndexes`; that trait includes `ScalarIndexes` and makes
GraphIndex/OntologyIndex available without enabling FoundationDB or unrelated
index implementations. `Relationships` remains independent.

~~~swift
.package(
    url: "https://github.com/1amageek/database-framework.git",
    from: "26.0731.3",
    traits: ["SQLite", "GraphIndexes"]
)
~~~

SwiftPM unifies traits from every dependency path, so the effective package
composition is the union requested by the complete consuming graph.

~~~bash
scripts/xcode-test-harness \
  --traits SQLite,AllRuntimeFeatures \
  --only-testing SQLiteTests \
  --expected-count 101 \
  --require-zero-skips \
  --require-zero-expected-failures \
  --require-zero-runtime-warnings
~~~

SQLite builds do not link libfdb_c.

~~~bash
POSTGRES_TEST_HOST=database.test \
POSTGRES_TEST_PORT=5432 \
POSTGRES_TEST_USER=postgres \
POSTGRES_TEST_PASSWORD=test \
POSTGRES_TEST_DB=database_framework_test \
scripts/xcode-test-harness \
  --traits PostgreSQL,AllRuntimeFeatures \
  --only-testing PostgreSQLTests \
  --expected-count 71 \
  --require-zero-skips \
  --require-zero-expected-failures \
  --require-zero-runtime-warnings
~~~

PostgreSQL builds require the PostgreSQL dependency but not a running server
for compilation. Integration tests require a reachable test database.

Backend traits are also platform-gated. FoundationDB has no iOS or WASI
adapter in this package. SQLite and PostgreSQL are native-platform adapters and
are not the Durable Object storage boundary. A WASI/Embedded runtime injects a
host-provided `StorageEngine`, such as the adapter composed by
database-framework-cloudflare.

## FoundationDB

~~~swift
import Database

let container = try await DBContainer.open(
    for: schema,
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

Use the facade overload when an explicit FoundationDB configuration is
required:

~~~swift
let container = try await DBContainer.open(
    for: schema,
    configuration: FDBStorageEngine.Configuration(),
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

The facade creates `FDBStorageEngine` and transfers it into the same
backend-neutral `DBConfiguration` contract. FoundationDB provides distributed
transactions, native versionstamps, and the dynamic DirectoryLayer.

## SQLite

~~~swift
import Database
import SQLiteStorage

let container = try await DBContainer.open(
    for: schema,
    configuration: SQLiteStorageEngine.Configuration.file(
        "/path/to/application.sqlite"
    ),
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

Use SQLiteStorageEngine.Configuration.inMemory for isolated tests. SQLite uses
the StorageKit key/value contract and its own transaction and directory
implementation.

## PostgreSQL And Cloud SQL

~~~swift
import Database
import PostgreSQLStorage

let configuration = PostgreSQLConfiguration(
    cloudSQLInstanceConnectionName: "PROJECT:REGION:INSTANCE",
    username: username,
    password: password,
    database: databaseName,
    schemaManagement: .assumeExists
)

let container = try await DBContainer.open(
    for: schema,
    configuration: configuration,
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

The default isolation level is SERIALIZABLE. Use assumeExists when the
database role cannot execute DDL and the StorageKit table is provisioned by a
separate deployment step.

For Cloud Run, see
[Cloud Run and Cloud SQL PostgreSQL](deployment/cloud-run-vapor-postgresql.md).

## Custom Engines

~~~swift
import Database
import StorageKit

let engine = InMemoryEngine()
let container = try await DBContainer.open(
    for: schema,
    configuration: DBConfiguration(
        storageEngine: engine,
        monotonicClock: applicationMonotonicClock,
        wallClock: applicationWallClock
    ),
    runtimeConfiguration: runtime
)
~~~

Custom engines are the extension point for test doubles, storage proxies,
remote hosts, and future backend implementations. Unsupported capabilities
must fail explicitly; they must not silently fall back to another backend.

## Storage Engine Lifecycle

Passing an engine to `DBConfiguration(storageEngine:)` transfers its lifecycle
to a shared configuration/container owner. Do not use or shut down the engine
through another owner afterward.

~~~text
StorageEngine
    |
    | transfer ownership
    v
DBConfiguration -> DBContainer.open
                      |-- open failure -> shutdown exactly once
                      |-- shutdown() --> shutdown exactly once
                      `-- deinit ------> shutdown exactly once
~~~

`DBContainer.shutdown()` is synchronous, thread-safe, and idempotent. It is the
explicit service shutdown hook. Deinitialization is a safety net, not the
preferred operational shutdown signal. No operation may start after this
terminal transition.

## Cloudflare

Durable Object SQLite is not a SwiftPM trait in this repository. The
database-framework-cloudflare repository provides the Worker/WASM and
DatabaseWire bridge.

The web host and database adapter remain independent:

~~~text
swift-web
  +--> swift-web-cloudflare       web actor host
  +--> database-framework-cloudflare  database adapter
                                      |
                                      v
                              Durable Object SQLite
~~~

swift-web itself does not depend on database-framework. An application
composes the packages it needs.

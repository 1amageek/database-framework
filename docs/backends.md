# Backend Guide

database-framework is built on StorageKit protocols. FoundationDB is enabled by
default for compatibility, but the execution layer is not FoundationDB-only.

## Backend Matrix

| Backend | SwiftPM trait | Engine | Use case | External service |
|---|---|---|---|---|
| FoundationDB | FoundationDB | FDBStorageEngine | distributed server database | FoundationDB cluster |
| SQLite | SQLite | SQLiteStorageEngine | local and embedded persistence | none |
| PostgreSQL | PostgreSQL | PostgreSQLStorageEngine | server and Cloud SQL | PostgreSQL |
| Custom | application-defined | any StorageEngine | tests, proxies, future backends | implementation-defined |

All application data access passes through the same conceptual path:

~~~text
DBContainer -> DatabaseContext -> StorageEngine -> Transaction
~~~

DatabaseContext is backend-neutral. The DBConfiguration supplied to
DBContainer selects the storage engine.

## SwiftPM Traits

~~~bash
swift build
xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS,arch=arm64'
~~~

The default build enables FoundationDB.

~~~bash
swift build --disable-default-traits --traits SQLite
xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS,arch=arm64'
~~~

SQLite builds do not link libfdb_c.

~~~bash
swift build --disable-default-traits --traits PostgreSQL
xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS,arch=arm64'
~~~

PostgreSQL builds require the PostgreSQL dependency but not a running server
for compilation. Integration tests require a reachable test database.

## FoundationDB

~~~swift
import Database

let container = try await DBContainer.open(
    for: schema,
    runtimeConfiguration: runtime
)
~~~

Use DBConfiguration(backend: .fdb(...)) when a custom FoundationDB
configuration is required. FoundationDB provides distributed transactions,
native versionstamps, and the dynamic DirectoryLayer.

## SQLite

~~~swift
import Database
import SQLiteStorage

let container = try await DBContainer.open(
    for: schema,
    configuration: SQLiteStorageEngine.Configuration.file(
        "/path/to/application.sqlite"
    ),
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
    configuration: DBConfiguration(backend: .custom(engine)),
    runtimeConfiguration: runtime
)
~~~

Custom engines are the extension point for test doubles, storage proxies,
remote hosts, and future backend implementations. Unsupported capabilities
must fail explicitly; they must not silently fall back to another backend.

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

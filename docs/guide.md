# User Guide

This guide describes the stable application-facing API. The same model and
query code can run against different StorageKit backends; only container
configuration changes.

## 1. Define A Model

~~~swift
import Database

@Persistable
struct User {
    #Directory<User>("app", "users")
    #Index(.ordered(
        name: "User_email",
        keys: [.ascending(\User.email)],
        unique: true
    ))

    var id: String = ""
    var email: String
    var name: String
}

let schema = try Schema(
    entities: [try User.schemaEntity],
    version: .init(1, 0, 0)
)
let runtime = try DatabaseFrameworkRuntime.configuration(
    executionIdentity: DatabaseExecutionRuntimeIdentity(
        identifier: "application",
        revision: 1
    ),
    entityRuntimes: [try DatabaseFrameworkRuntime.entity(User.self)]
)
~~~

Keep the execution identity identifier stable for the application and increase
its revision whenever executable runtime behavior changes without a schema
change. This prevents continuations from being reused across incompatible
authorization or execution behavior.

database-kit owns model metadata and index declarations. The framework
registers that metadata and provides the execution path.

## 2. Create A Container

Use a backend-specific configuration:

~~~swift
import Database
import SQLiteStorage

let container = try await DBContainer.open(
    for: schema,
    configuration: SQLiteStorageEngine.Configuration.file(
        "/var/lib/app/application.sqlite"
    ),
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

The same schema can use PostgreSQL or FoundationDB:

~~~swift
import PostgreSQLStorage

let postgres = PostgreSQLConfiguration(
    host: "127.0.0.1",
    username: "app",
    password: password,
    database: "app"
)

let container = try await DBContainer.open(
    for: schema,
    configuration: postgres,
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

When an application explicitly selects the `FoundationDB` trait on macOS or
Linux, it can use the FoundationDB convenience composition:

~~~swift
let container = try await DBContainer.open(
    for: schema,
    monotonicClock: applicationMonotonicClock,
    wallClock: applicationWallClock,
    runtimeConfiguration: runtime
)
~~~

See [Backend Guide](backends.md) for trait selection and deployment
requirements. The clocks are explicit runtime dependencies: native
applications may use Foundation-backed adapters, while Embedded targets inject
their platform implementations without linking Foundation.

The consuming package selects optional capabilities. `DatabaseRuntime` is the
lightweight runtime-composition import, while `Database` is the broader
umbrella import. `GraphIndexes` makes ScalarIndex, GraphIndex, and OntologyIndex
available through both. `Relationships` is selected separately when the schema
declares relationship maintenance.

~~~swift
.package(
    url: "https://github.com/1amageek/database-framework.git",
    from: "26.0818.0",
    traits: ["GraphIndexes"]
)
~~~

## 3. Change Tracking

A context is a unit of work. Stage mutations first, then save them in one
transaction:

~~~swift
let context = container.newContext(authorization: authorization)

try context.insert(User(id: "alice", email: "alice@example.com", name: "Alice"))
try context.insert(User(id: "bob", email: "bob@example.com", name: "Bob"))

try await context.save()
~~~

In the default composition, DBContainer owns one StorageEngine, Schema, and
runtime configuration. DatabaseContext is a backend-neutral unit of work for
that database. Engine ownership transfers through
`DBConfiguration(storageEngine:)`; opening failure, explicit shutdown, and
deinitialization converge on the same exactly-once release path.

When the consuming package explicitly enables `MultiBase`, use a session
to select the Base. That trait adds storage topology, Base and Composition
catalogs, target leases, and persisted Grants:

~~~swift
let session = container.session(authorization: authorization)
let context = session.base(baseID).newContext()
~~~

`MultiBase` is not implied by `AllRuntimeFeatures` and does not affect the
default transaction path.

## 4. Queries

~~~swift
let users = try await context.fetch(User.self)
    .where(User.fields.name == "Alice")
    .execute()
~~~

Use partition binding for a dynamic directory:

~~~swift
@Persistable
struct TenantOrder {
    #Directory<TenantOrder>(
        "tenants",
        \TenantOrder.tenantID,
        "orders",
        layer: .partition
    )

    var id: String = ""
    var tenantID: String
    var status: String
}

let orders = try await context.fetch(TenantOrder.self)
    .partition(TenantOrder.fields.tenantID, equals: "tenant-1")
    .where(TenantOrder.fields.status == "open")
    .execute()
~~~

A dynamic-directory query without all required partition values fails with a
typed directory error. This prevents an accidental cross-tenant scan.

## 5. Transactions

For operations that do not use change tracking:

~~~swift
try await context.withTransaction { transaction in
    let value = try await transaction.getValue(for: key)
    try transaction.setValue(updatedValue, for: key)
    _ = value
}
~~~

The selected StorageEngine controls transaction creation, retry behavior,
isolation, and physical I/O. Application code uses the same transaction
protocol across supported backends.

## 6. Indexes

Declare indexes with the model:

~~~swift
@Persistable
struct Document {
    #Directory<Document>("app", "documents")
    #Index(.vector(
        name: "Document_embedding",
        embedding: \Document.embedding,
        dimensions: 1536,
        metric: .cosine
    ))

    var id: String = ""
    var title: String
    var embedding: Vector
}
~~~

Available index modules include Scalar, Vector, FullText, Spatial, Rank,
Graph, Aggregation, Version, Bitmap, Leaderboard, Relationship, and Ontology.

Module-specific query APIs are documented in the corresponding
Sources/*Index/README.md files.

## 7. Migrations

Versioned schemas can provide a migration plan:

~~~swift
let migration = Migration(
    fromVersion: Schema.Version(1, 0, 0),
    toVersion: Schema.Version(2, 0, 0),
    description: "Add email index"
) { context in
    try await context.addIndex(emailIndexDescriptor)
}
~~~

Migrations run through the configured StorageEngine. Administrative
provisioning, such as creating a PostgreSQL table for a DML-only role, remains
a deployment concern.

Opening a container with a migration plan activates a container-scoped
admission boundary. Run `migrateIfNeeded()` before serving application traffic;
ordinary data operations remain unavailable after a bounded partial run or a
failed stage and are admitted only after the complete plan succeeds. Containers
opened without a migration plan do not allocate or lock this migration gate.
With `MultiBase`, each Base migrates through its own `AdminContext`, and
container-wide data admission reopens only after every active Base matches the
compiled schema and physical index generation. The database-wide execution
runtime is published only after that condition holds.

## 8. Client And Server

Remote invocation is owned by the independent `database-server` package.
Its internal operation runtime maps canonical DatabaseWire requests to this
framework's execution APIs and owns durable server jobs and remote schema
administration. Its native host adds HTTP, WebSocket, stdio, TLS, credentials,
signals, and process shutdown. The supported artifact is the standalone
`database-server` executable.

Cloudflare Workers use the separate
database-framework-cloudflare repository. An application combines that adapter
with database-framework, its own schema, and its own request codec. The adapter
uses an application session and Durable Object SQLite; it does not depend on
database-server or require DatabaseWire.

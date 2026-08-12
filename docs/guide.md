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
    #Index(
        .scalar,
        fields: [\User.email],
        unique: true,
        name: "User_email"
    )

    var id: String = ""
    var email: String
    var name: String
}

let schema = try Schema(
    entities: [try User.schemaEntity],
    version: .init(1, 0, 0)
)
let runtime = try DatabaseFrameworkRuntime.configuration(
    entityRuntimes: [try DatabaseFrameworkRuntime.entity(User.self)]
)
~~~

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

The consuming package selects optional capabilities. `Database` remains the
umbrella import; traits change what enters that umbrella's dependency graph.
`GraphIndexes` includes ScalarIndex, GraphIndex, and OntologyIndex.
`Relationships` is selected separately when the schema declares relationship
maintenance.

~~~swift
.package(
    url: "https://github.com/1amageek/database-framework.git",
    from: "26.0812.1",
    traits: ["GraphIndexes"]
)
~~~

## 3. Change Tracking

A context is a unit of work. Stage mutations first, then save them in one
transaction:

~~~swift
let session = container.session(authorization: authorization)
let context = session.base(baseID).newContext()

try context.insert(User(id: "alice", email: "alice@example.com", name: "Alice"))
try context.insert(User(id: "bob", email: "bob@example.com", name: "Bob"))

try await context.save()
~~~

DBContainer owns the storage topology, Schema and target catalogs, and runtime
configuration. `DatabaseSession` binds authorization, and DatabaseContext is a
backend-neutral unit of work fixed to one Base.

Engine ownership transfers when a `DatabaseStorageTopology` is passed to
`DBConfiguration(storageTopology:)`. Backend facades create a one-domain
topology with the same contract. Opening failure shuts every transferred
engine down. An opened container exposes idempotent `shutdown()`, and
deinitialization uses the same exactly-once path. Do not retain a second
operational owner for an injected engine.

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
    #Index(
        .vector(dimensions: 1536),
        embedding: \Document.embedding,
        name: "Document_embedding"
    )

    var id: String = ""
    var title: String
    var embedding: Vector
}
~~~

Available index modules include Scalar, Vector, FullText, Spatial, Rank,
Graph, Aggregation, Version, Bitmap, Leaderboard, Permuted, Relationship, and
Ontology.

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

## 8. Client And Server

DatabaseOperations executes canonical operations against a container, while
DatabaseWireAdapter provides the bounded frame boundary used by hosts. The
client protocol is defined in database-kit; execution and storage policy remain
in this package.

Cloudflare Workers use the separate
database-framework-cloudflare repository, which bridges DatabaseWire to
Durable Object SQLite.

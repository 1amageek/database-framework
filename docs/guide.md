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
    #Index(ScalarIndexKind<User>(fields: [\.email]), unique: true)

    var email: String
    var name: String
}

let schema = Schema([User.self])
~~~

database-kit owns model metadata and index declarations. The framework
registers that metadata and provides the execution path.

## 2. Create A Container

Use a backend-specific configuration:

~~~swift
import Database
import SQLiteStorage

let container = try await DBContainer(
    for: schema,
    configuration: SQLiteStorageEngine.Configuration.file(
        "/var/lib/app/application.sqlite"
    )
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

let container = try await DBContainer(
    for: schema,
    configuration: postgres
)
~~~

FoundationDB uses the default trait:

~~~swift
let container = try await DBContainer(for: schema)
~~~

See [Backend Guide](backends.md) for trait selection and deployment
requirements.

## 3. Change Tracking

A context is a unit of work. Stage mutations first, then save them in one
transaction:

~~~swift
let context = container.newContext()

context.insert(User(email: "alice@example.com", name: "Alice"))
context.insert(User(email: "bob@example.com", name: "Bob"))

try await context.save()
~~~

DBContainer owns the storage engine and schema. FDBContext is the historical
public type name for the backend-neutral user context.

## 4. Queries

~~~swift
let users = try await context.fetch(User.self)
    .where(\.name == "Alice")
    .execute()
~~~

Use partition binding for a dynamic directory:

~~~swift
@Persistable
struct TenantOrder {
    #Directory<TenantOrder>(
        "tenants",
        Field(\.tenantID),
        "orders",
        layer: .partition
    )

    var tenantID: String
    var status: String
}

let orders = try await context.fetch(TenantOrder.self)
    .partition(\.tenantID, equals: "tenant-1")
    .where(\.status == "open")
    .execute()
~~~

A dynamic-directory query without all required partition values fails with a
typed directory error. This prevents an accidental cross-tenant scan.

## 5. Transactions

For operations that do not use change tracking:

~~~swift
try await context.withTransaction { transaction in
    let value = try await transaction.getValue(for: key)
    transaction.setValue(updatedValue, for: key)
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
    #Index(VectorIndexKind<Document>(
        embedding: \.embedding,
        dimensions: 1536
    ))

    var title: String
    var embedding: [Float]
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

DatabaseServer exposes a container through DatabaseWire for database-client.
The client protocol is defined in database-kit; server execution and storage
policy remain in this package.

Cloudflare Workers use the separate
database-framework-cloudflare repository, which bridges DatabaseWire to
Durable Object SQLite.

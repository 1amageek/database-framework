# Database

[![Swift](https://img.shields.io/badge/Swift-6.2-orange.svg)](https://swift.org)
[![Platform](https://img.shields.io/badge/Platform-macOS%2015%2B-blue.svg)](https://www.apple.com/macos)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/1amageek/database-framework)](https://github.com/1amageek/database-framework/releases)

A Swift package providing server-side database operations powered by FoundationDB. Database implements the index maintenance logic for index types defined in [DatabaseKit](https://github.com/1amageek/database-kit).

## Overview

Database provides:

- **FDBContainer**: Application resource manager for database operations
- **FDBContext**: Change tracking and batch operations
- **Index Maintainers**: Concrete implementations for all index types
- **Migration System**: Schema versioning and online index building
- **Polymorphable**: Union type support with shared directories and polymorphic queries
- **Infrastructure**: Query optimization, transaction management, serialization, batch operations

## Two-Package Architecture

Database is designed as a two-package system to enable **client-server model sharing**:

```
database-kit (client-safe)          database-framework (server-only)
├── Core/                           ├── DatabaseEngine/
│   ├── Persistable (protocol)      │   ├── FDBContainer
│   ├── IndexKind (protocol)        │   ├── FDBContext
│   └── IndexDescriptor             │   ├── IndexMaintainer (protocol)
│                                   │   └── IndexKindMaintainable (protocol)
├── Vector/, FullText/, etc.        ├── VectorIndex/, FullTextIndex/, etc.
│   └── VectorIndexKind             │   └── VectorIndexKind+Maintainable
```

### Why Two Packages?

| Package | Purpose | Platform |
|---------|---------|----------|
| **database-kit** | Model definitions, index type metadata | iOS, macOS, Linux, Windows |
| **database-framework** | Index maintenance, FoundationDB operations | macOS, Linux, Windows (server) |

This separation allows:
- **Shared models**: Same `@Persistable` structs on iOS client and server
- **Type-safe indexes**: Define indexes in database-kit, execute in database-framework
- **No client dependencies**: iOS apps don't need FoundationDB

### How They Connect

```
┌─────────────────────────────────────────────────────────────────┐
│                      database-kit                               │
│  ┌─────────────────┐    ┌──────────────────────────────────┐   │
│  │   IndexKind     │    │     @Persistable Model           │   │
│  │  (protocol)     │    │  #Index(ScalarIndexKind<User>...)│   │
│  └────────┬────────┘    └──────────────────────────────────┘   │
└───────────┼─────────────────────────────────────────────────────┘
            │ defines WHAT to index
            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    database-framework                           │
│  ┌─────────────────────────┐    ┌────────────────────────────┐ │
│  │ IndexKindMaintainable   │───▶│    IndexMaintainer         │ │
│  │ (protocol extension)    │    │  (concrete implementation) │ │
│  └─────────────────────────┘    └────────────────────────────┘ │
│            │                               │                    │
│            │ creates                       │ executes           │
│            ▼                               ▼                    │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    FoundationDB                             ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Protocol Bridge

The `IndexKindMaintainable` protocol bridges metadata and runtime:

```swift
// In database-kit: Defines WHAT to index
public struct ScalarIndexKind: IndexKind {
    public static let identifier = "scalar"
    // ... metadata only, no FDB dependency
}

// In database-framework: Defines HOW to maintain index
extension ScalarIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(...) -> any IndexMaintainer<Item> {
        return ScalarIndexMaintainer(...)  // FDB-dependent implementation
    }
}
```

### Dependency Flow

```swift
// Server-side Package.swift
dependencies: [
    .package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),
    .package(url: "https://github.com/1amageek/database-framework.git", from: "1.0.0"),
]

// Import both packages - framework extends kit's types
import Core           // from database-kit
import Database       // from database-framework (includes IndexKindMaintainable extensions)
```

When you import `Database`, all `IndexKindMaintainable` extensions are automatically registered, enabling the framework to create the appropriate `IndexMaintainer` for each `IndexKind`.

## Supported Platforms

| Platform | Support |
|----------|---------|
| macOS | 15.0+ |
| Linux | Swift 6.2+ |
| Windows | Swift 6.2+ |
| iOS/watchOS/tvOS/visionOS | Not supported (requires FoundationDB) |

## Prerequisites

FoundationDB must be installed and running locally:

```bash
# macOS (Homebrew)
brew install foundationdb

# Start the service
brew services start foundationdb
```

The linker expects `libfdb_c` at `/usr/local/lib`.

## Installation

### Swift Package Manager

Add database-framework to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/1amageek/database-framework.git", from: "0.1.0")
]
```

### Prerequisites

- **FoundationDB 7.3+** must be installed and running
  - macOS: `brew install foundationdb`
  - Linux: See [FoundationDB Downloads](https://github.com/apple/foundationdb/releases)
- **Swift 6.2+**
- **macOS 15+** or Linux

## Modules

| Module | Description |
|--------|-------------|
| `DatabaseEngine` | FDBContainer, FDBContext, IndexMaintainer protocol, Relationship support |
| `ScalarIndex` | VALUE index implementation |
| `VectorIndex` | HNSW and flat vector search |
| `FullTextIndex` | Inverted index for text search |
| `SpatialIndex` | S2/Geohash spatial indexing |
| `RankIndex` | Skip-list based rankings |
| `PermutedIndex` | Alternative field orderings |
| `GraphIndex` | Adjacency list traversal |
| `AggregationIndex` | COUNT, SUM, MIN, MAX, AVG |
| `VersionIndex` | Version history with versionstamps |
| `CrossTypeIndex` | Cross-type indexes spanning relationships |
| `QueryAST` | SQL/SPARQL query AST parser, builder, and serializer |
| `Database` | All-in-one re-export |

## Infrastructure Components

Cross-cutting functionality used by all index types:

| Category | Components | Purpose |
|----------|------------|---------|
| **Query Planner** | QueryPlannerConfiguration, InPredicateOptimizer | Plan complexity limits, IN predicate optimization |
| **Transaction** | TransactionPriority, ReadVersionCache, AsyncCommitHook | Priority levels, weak reads, commit hooks |
| **Serialization** | LargeValueSplitter, TransformingSerializer | Large value chunking, compression |
| **Batch Operations** | AdaptiveThrottler, BatchFetcher, IndexFromIndexBuilder | Adaptive throttling, prefetching |
| **Storage** | FormatVersion, FormatVersionManager | Format versioning and migration |

## Build and Test

```bash
# Build the project
swift build

# Run all tests (requires local FoundationDB running)
swift test

# Run a specific test file
swift test --filter DatabaseEngineTests.ScalarIndexKindTests

# Run a specific test function
swift test --filter "DatabaseEngineTests.ScalarIndexKindTests/testScalarIndexKindIdentifier"

# Build with release optimization
swift build -c release
```

## Quick Start

```swift
import Database  // All-in-one import
import Core

// 1. Define your model in database-kit
@Persistable
struct User {
    #Directory<User>("app", "users")
    #Index(ScalarIndexKind<User>(fields: [\.email]), unique: true)
    #Index(ScalarIndexKind<User>(fields: [\.createdAt]))

    var email: String
    var name: String
    var createdAt: Date = Date()
}

// 2. Create container with schema
let schema = Schema([User.self])
let container = try await FDBContainer(for: schema)

// 3. Use context for operations
let context = await container.mainContext

let user = User(email: "alice@example.com", name: "Alice")
context.insert(user)
try await context.save()

// 4. Query data
let users = try await context.fetch(FDBFetchDescriptor<User>())
```

## SPARQL() SQL Function

Database provides a powerful `SPARQL()` SQL function that enables **hybrid SQL/SPARQL queries**, allowing you to combine relational filtering with graph pattern matching in a single query.

### Overview

The `SPARQL()` function executes a SPARQL subquery against a graph index and returns matching IDs, which can be used in SQL `IN` predicates. This enables seamless integration between SQL and SPARQL query paradigms.

### Basic Usage

```swift
import Database
import Core
import Graph

// 1. Define models with graph index
@Persistable
struct User {
    #Directory<User>("app", "users")
    var id: String = UUID().uuidString
    var name: String = ""
    var email: String = ""
}

@Persistable
struct Connection {
    #Directory<Connection>("app", "connections")
    var id: String = UUID().uuidString
    var from: String = ""
    var to: String = ""
    var relationship: String = ""

    #Index(GraphIndexKind<Connection>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// 2. Insert data
let context = container.newContext()
context.insert(User(id: "alice", name: "Alice", email: "alice@example.com"))
context.insert(User(id: "bob", name: "Bob", email: "bob@example.com"))
context.insert(Connection(from: "alice", to: "bob", relationship: "follows"))
try await context.save()

// 3. Execute hybrid SQL/SPARQL query
let sql = """
SELECT * FROM User
WHERE id IN (SPARQL(Connection, 'SELECT ?from WHERE { ?from "follows" "bob" }'))
"""
let users = try await context.executeSQL(sql, as: User.self)
// Returns: [User(id: "alice", name: "Alice", ...)]
```

### Advanced Features

#### Multiple SPARQL() Calls

Combine multiple SPARQL subqueries with SQL logic:

```swift
let sql = """
SELECT * FROM User
WHERE id IN (SPARQL(Connection, 'SELECT ?from WHERE { ?from "follows" ?to }'))
  AND id IN (SPARQL(Connection, 'SELECT ?from WHERE { ?from "likes" "tech" }'))
"""
// Returns users who follow someone AND like tech
```

#### Explicit Variable Selection

When SPARQL returns multiple variables, explicitly select which one to use:

```swift
let sql = """
SELECT * FROM User
WHERE id IN (
    SPARQL(Connection, 'SELECT ?from ?to WHERE { ?from "follows" ?to }', '?from')
)
"""
// Explicitly select ?from variable from multi-variable result
```

#### Combining with SQL Filters

Mix SPARQL pattern matching with SQL predicates:

```swift
let sql = """
SELECT * FROM User
WHERE age > 18
  AND email LIKE '%@example.com'
  AND id IN (SPARQL(Connection, 'SELECT ?from WHERE { ?from "verified" "true" }'))
ORDER BY name
"""
```

### Error Handling

The `SPARQL()` function provides clear error messages:

```swift
// Error: Type not found
WHERE id IN (SPARQL(NonExistentType, '...'))
// Throws: SPARQLFunctionError.typeNotFound("NonExistentType")

// Error: No graph index
WHERE id IN (SPARQL(User, '...'))
// Throws: SPARQLFunctionError.graphIndexNotFound("User")

// Error: Multiple variables without explicit selection
WHERE id IN (SPARQL(Connection, 'SELECT ?from ?to WHERE { ... }'))
// Throws: SPARQLFunctionError.multipleVariablesNotSupported
```

### Implementation Details

**Execution Flow**:
1. Parse SQL string → `SelectQuery` (QueryIR)
2. Detect `SPARQL()` functions in expression tree
3. Execute SPARQL subqueries within parent transaction
4. Inline results as literal arrays in SQL query
5. Execute rewritten query via standard fetch path

**Transaction Isolation**:
- SPARQL subqueries share the same transaction snapshot as the parent SQL query
- Ensures consistent reads across SQL and SPARQL operations
- All operations participate in the same ACID transaction

**Performance**:
- SPARQL execution is optimized for index-backed graph traversal
- Results are cached within the transaction scope
- Suitable for large result sets (tested with 100+ items)

### Limitations

- **Single-variable projection**: `IN` predicate semantics require scalar values
  - Use explicit variable selection for multi-variable SPARQL results
- **Dynamic directories**: Types with dynamic directory partitions are not supported
- **Query complexity**: Complex SPARQL patterns may impact performance

### Use Cases

**Social Network**:
```swift
// Find users who are followed by influencers
SELECT * FROM User
WHERE id IN (
    SPARQL(Follow, 'SELECT ?follower WHERE {
        ?follower "follows" ?influencer .
        ?influencer "type" "influencer"
    }')
)
```

**Knowledge Graph**:
```swift
// Find products related to user interests
SELECT * FROM Product
WHERE id IN (
    SPARQL(Interest, 'SELECT ?product WHERE {
        ?user "interested_in" ?category .
        ?product "category" ?category
    }')
)
AND price < 100
```

**Access Control**:
```swift
// Find resources accessible to user
SELECT * FROM Resource
WHERE id IN (
    SPARQL(Permission, 'SELECT ?resource WHERE {
        "user:123" "can_access" ?resource
    }')
)
```

## Extensible Architecture

### Design Philosophy

Database follows a **protocol-based extensible architecture** that cleanly separates:

1. **Index Type Definition** (`IndexKind` in DatabaseKit): Platform-independent metadata
2. **Index Maintenance** (`IndexKindMaintainable` in Database): Server-side implementation

This separation allows third parties to:
- Define custom index types in DatabaseKit (works on all platforms)
- Implement index maintenance in Database (server-only)

### IndexKindMaintainable Protocol

The `IndexKindMaintainable` protocol bridges `IndexKind` definitions to actual index maintenance:

```swift
public protocol IndexKindMaintainable: IndexKind {
    /// Create an IndexMaintainer for this index kind
    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item>
}
```

### IndexMaintainer Protocol

The `IndexMaintainer` protocol defines the interface for index operations:

```swift
public protocol IndexMaintainer<Item>: Sendable {
    associatedtype Item: Persistable

    /// Update index when item changes
    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws

    /// Scan item during batch indexing
    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws

    /// Optional: Custom bulk build strategy (e.g., for HNSW)
    var customBuildStrategy: (any IndexBuildStrategy<Item>)? { get }

    /// Compute expected index keys for an item (used by OnlineIndexScrubber)
    func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes]
}
```

## Creating Custom Index Implementations

### Step 1: Define IndexKind (in your DatabaseKit extension)

```swift
// MyIndexKit/TimeSeriesIndexKind.swift
import Core

public struct TimeSeriesIndexKind: IndexKind {
    public static let identifier = "com.mycompany.timeseries"
    public static let subspaceStructure = SubspaceStructure.hierarchical

    public let resolution: TimeResolution
    public let bucketSize: Int

    public enum TimeResolution: String, Codable, Sendable {
        case second, minute, hour, day
    }

    public init(resolution: TimeResolution = .minute, bucketSize: Int = 1000) {
        self.resolution = resolution
        self.bucketSize = bucketSize
    }

    public static func validateTypes(_ types: [Any.Type]) throws {
        guard types.count >= 1, types[0] == Date.self else {
            throw IndexTypeValidationError.typeMismatch(
                field: "timestamp", expected: "Date", actual: String(describing: types[0])
            )
        }
    }
}
```

### Step 2: Implement IndexMaintainer (in your Database extension)

```swift
// MyIndexFramework/TimeSeriesIndexMaintainer.swift
import DatabaseEngine
import Core
import FoundationDB

public struct TimeSeriesIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let kind: TimeSeriesIndexKind
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var customBuildStrategy: (any IndexBuildStrategy<Item>)? { nil }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old entry
        if let old = oldItem {
            let bucket = computeBucket(for: old)
            let key = makeKey(bucket: bucket, item: old)
            transaction.clear(key: key)
        }

        // Add new entry
        if let new = newItem {
            let bucket = computeBucket(for: new)
            let key = makeKey(bucket: bucket, item: new)
            transaction.setValue([], for: key)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let bucket = computeBucket(for: item)
        let key = makeKey(bucket: bucket, item: item)
        transaction.setValue([], for: key)
    }

    private func computeBucket(for item: Item) -> Int64 {
        // Compute time bucket based on resolution
        let timestamp = try! DataAccess.extractField(from: item, keyPath: "timestamp")
        // ... bucket computation logic
        return 0
    }

    private func makeKey(bucket: Int64, item: Item) -> [UInt8] {
        // ... key construction
        return subspace.pack(Tuple(bucket))
    }
}
```

### Step 3: Conform to IndexKindMaintainable

```swift
// MyIndexFramework/TimeSeriesIndexKind+Maintainable.swift
import DatabaseEngine
import Core
import MyIndexKit

extension TimeSeriesIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return TimeSeriesIndexMaintainer<Item>(
            index: index,
            kind: self,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
```

### Step 4: Use in Your Application

```swift
import Core
import DatabaseEngine
import MyIndexKit
import MyIndexFramework  // Registers IndexKindMaintainable conformance

@Persistable
struct SensorReading {
    #Index(TimeSeriesIndexKind<SensorReading>(
        fields: [\.timestamp, \.sensorId],
        resolution: .second
    ))

    var timestamp: Date
    var sensorId: String
    var value: Double
}

// The framework automatically uses your custom maintainer
let container = try FDBContainer(for: schema)
```

## IndexConfiguration

Custom index types can accept runtime configuration:

```swift
public protocol IndexConfiguration: Sendable {
    static var kindIdentifier: String { get }
    var keyPath: AnyKeyPath { get }
    var modelTypeName: String { get }
    var indexName: String { get }  // Default: "{modelTypeName}_{fieldName}"
}

// Example: Vector index configuration (generic per model type)
public struct VectorIndexConfiguration<Model: Persistable>: IndexConfiguration {
    public static var kindIdentifier: String { "vector" }
    public var keyPath: AnyKeyPath { _keyPath }
    public var modelTypeName: String { String(describing: Model.self) }

    private let _keyPath: KeyPath<Model, [Float]>
    public let algorithm: VectorAlgorithm  // .flat or .hnsw(VectorHNSWParameters)
}

// Apply configuration when creating container
// Each model type requires its own VectorIndexConfiguration instance
let container = try FDBContainer(
    for: schema,
    indexConfigurations: [
        // Configure Product.embedding with HNSW
        VectorIndexConfiguration<Product>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        ),
        // Configure Article.contentVector with high recall HNSW
        VectorIndexConfiguration<Article>(
            keyPath: \.contentVector,
            algorithm: .hnsw(.highRecall)
        ),
        // Configure User.faceEmbedding with flat search (exact)
        VectorIndexConfiguration<User>(
            keyPath: \.faceEmbedding,
            algorithm: .flat
        )
    ]
)
```

## Polymorphable (Union Types)

Polymorphable enables multiple `Persistable` types to share a directory and indexes, allowing polymorphic queries across different concrete types.

### Defining Polymorphic Types

```swift
// In database-kit: Define a polymorphic protocol
@Polymorphable
protocol Document: Polymorphable {
    var id: String { get }
    var title: String { get }

    #Directory<Document>("app", "documents")
}

// Conforming types
@Persistable
struct Article: Document {
    var id: String = ULID().ulidString
    var title: String
    var content: String

    #Directory<Article>("app", "articles")  // Enables dual-write
}

@Persistable
struct Report: Document {
    var id: String = ULID().ulidString
    var title: String
    var data: Data
    // Uses default directory (persistableType)
}
```

### Dual-Write Behavior

When a type conforms to `Polymorphable` AND has its own `#Directory`:
- **Save**: Data written to both type-specific AND polymorphic directories
- **Delete**: Data removed from both directories

### Polymorphic Queries

```swift
let schema = Schema([Article.self, Report.self])
let container = try FDBContainer(for: schema)
let context = container.newContext()

// Save items (automatic dual-write for Article)
let article = Article(title: "Hello", content: "World")
let report = Report(title: "Q4 Report", data: Data())
context.insert(article)
context.insert(report)
try await context.save()

// Fetch all documents (returns [any Persistable])
let allDocuments = try await context.fetchPolymorphic(Article.self)
// Returns both Article and Report instances

// Fetch by ID from polymorphic directory
let doc = try await context.fetchPolymorphic(Article.self, id: someId)
```

### Swift Type System Note

Due to Swift's type system limitations, protocol types cannot be passed to generic functions:

```swift
// ❌ Compile error
try await context.fetchPolymorphic(Document.self)

// ✅ Use any conforming concrete type
try await context.fetchPolymorphic(Article.self)
// All conforming types share the same polymorphic directory
```

## Relationship Support

Database provides relationship support via the `@Relationship` macro (defined in database-kit) and extension methods on `FDBContext`.

### Defining Relationships

```swift
// In database-kit
@Persistable
struct Order {
    var id: String = ULID().ulidString
    var total: Double

    // To-one relationship: generates hidden `customerId` field
    @Relationship(inverse: \Customer.orders)
    var customer: Customer?
}

@Persistable
struct Customer {
    var id: String = ULID().ulidString
    var name: String

    // To-many relationship: derived from inverse
    @Relationship(deleteRule: .cascade, inverse: \Order.customer)
    var orders: [Order]
}
```

### How It Works

The `@Relationship` macro automatically:
1. Generates a foreign key field (`customerId: String?` for `customer: Customer?`)
2. Creates a relationship index (`Order_customer`) via `ScalarIndexKind`
3. Marks the relationship property as transient (not stored directly)

**Storage Layout**:
```
[fdb]/R/Order/["O001"] → { id: "O001", total: 99.99, customerId: "C001" }
[fdb]/I/Order_customer/["C001"]/["O001"] → ''  // Index for relationship queries
```

### Querying Relationships

Use extension methods on `FDBContext`:

```swift
import DatabaseEngine

let context = container.newContext()

// To-one: O(1) direct ID lookup
let order = try await context.model(for: "O001", as: Order.self)!
let customer = try await context.related(order, \.customer)

// To-many: O(k) index scan where k = number of related items
let customer = try await context.model(for: "C001", as: Customer.self)!
let orders = try await context.related(customer, \.orders)
```

### Delete Rules

Delete rules are enforced via explicit extension methods, not automatically:

```swift
// ⚠️ Basic delete (NO relationship rules - may leave orphan references)
context.delete(customer)
try await context.save()

// ✅ Delete with relationship rule enforcement
// - .cascade: Delete all related orders
// - .deny: Throw error if orders exist
// - .nullify: Set FK fields to nil on related items
// - .noAction: Do nothing
try await context.deleteEnforcingRelationshipRules(customer)
```

> **⚠️ Important**: The basic `delete()` + `save()` does NOT enforce any delete rules.
> If you have `@Relationship` declarations with delete rules, you MUST use
> `deleteEnforcingRelationshipRules()` to enforce them. Using basic `delete()`
> will ignore all relationship rules and may leave orphan references in your database.

**Design Philosophy**: Relationship features are optional. Users who don't use `@Relationship` have zero overhead. Delete rule enforcement requires explicit method calls rather than being embedded in the core `delete()` path.

### Extension Pattern

This follows the framework's extension pattern for optional features:

```swift
// DatabaseEngine/Relationship/FDBContext+Relationship.swift
extension FDBContext {
    // Query methods
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, R?>) async throws -> R?
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, [R]>) async throws -> [R]

    // Delete rule enforcement (explicit call required)
    public func deleteEnforcingRelationshipRules<T>(_ model: T) async throws
}
```

## Security

Database provides comprehensive security features including declarative access control and data encryption.

### Access Control (SecurityPolicy)

Define per-type access control rules using the `SecurityPolicy` protocol:

```swift
import Core

@Persistable
struct Post {
    var id: String = UUID().uuidString
    var title: String
    var content: String
    var authorID: String
    var isPublic: Bool = false
}

extension Post: SecurityPolicy {
    static func allowGet(resource: Post, auth: (any AuthContext)?) -> Bool {
        // Public posts or author can read
        resource.isPublic || resource.authorID == auth?.userID
    }

    static func allowList(query: SecurityQuery<Post>, auth: (any AuthContext)?) -> Bool {
        // Authenticated users with reasonable limit
        auth != nil && (query.limit ?? 0) <= 100
    }

    static func allowCreate(newResource: Post, auth: (any AuthContext)?) -> Bool {
        // Authors can create their own posts
        auth != nil && newResource.authorID == auth?.userID
    }

    static func allowUpdate(resource: Post, newResource: Post, auth: (any AuthContext)?) -> Bool {
        // Authors can update, can't change authorID
        resource.authorID == auth?.userID
            && newResource.authorID == resource.authorID
    }

    static func allowDelete(resource: Post, auth: (any AuthContext)?) -> Bool {
        resource.authorID == auth?.userID
    }
}
```

**Default behavior**: All operations are denied by default (secure by default).

### Security Configuration

```swift
// Security enabled with strict mode (default - recommended)
let container = try FDBContainer(for: schema)

// Security enabled with custom admin roles
let container = try FDBContainer(
    for: schema,
    security: .enabled(adminRoles: ["admin", "superuser"])
)

// Non-strict mode (allows models without SecurityPolicy)
let container = try FDBContainer(
    for: schema,
    security: .enabled(strict: false)
)

// Security disabled (ONLY for testing)
let testContainer = try FDBContainer(
    for: schema,
    security: .disabled
)
```

| Mode | Behavior |
|------|----------|
| `strict: true` (default) | Types without `SecurityPolicy` are denied |
| `strict: false` | Types without `SecurityPolicy` are allowed |
| `adminRoles` | Roles that bypass security evaluation |

### Authentication Context

Set authentication context per-request using TaskLocal:

```swift
struct MyAuthContext: AuthContext {
    var userID: String?
    var roles: Set<String>
}

// In request handler
try await AuthContextKey.$current.withValue(MyAuthContext(userID: "user-123", roles: ["user"])) {
    let context = container.newContext()
    // All operations in this scope use the auth context
    let posts = try await context.fetch(FDBFetchDescriptor<Post>())
    try await context.save()
}
```

### Data Encryption

Encrypt data at rest using AES-256-GCM authenticated encryption:

```swift
import Crypto

// Create a key provider
let key = SymmetricKey(size: .bits256)
let keyProvider = StaticKeyProvider(key: key, keyId: "key-v1")

// Configure encryption
let config = TransformConfiguration(
    compressionEnabled: true,
    compressionAlgorithm: .zlib,
    encryptionEnabled: true,
    keyProvider: keyProvider
)

let container = try FDBContainer(
    for: schema,
    transformConfiguration: config
)
```

### Key Providers

| Provider | Use Case |
|----------|----------|
| `StaticKeyProvider` | Development/testing with fixed key |
| `RotatingKeyProvider` | Production with key rotation support |
| `DerivedKeyProvider` | Derive keys from master key using HKDF |
| `EnvironmentKeyProvider` | Read keys from environment variables |

**Key Rotation Example**:

```swift
let provider = RotatingKeyProvider()
provider.addKey(key: oldKey, keyId: "key-v1")
provider.addKey(key: newKey, keyId: "key-v2")
try provider.setCurrentKeyId("key-v2")  // New writes use v2

// Old data encrypted with v1 is still readable
// Gradually re-encrypt data, then remove old key
provider.removeKey(keyId: "key-v1")
```

### Compression

Reduce storage size with configurable compression:

```swift
let config = TransformConfiguration(
    compressionEnabled: true,
    compressionAlgorithm: .lz4,      // or .zlib, .lzma, .lzfse
    compressionMinSize: 100,         // Skip compression for small data
    skipIneffectiveCompression: true // Skip if no size reduction
)
```

| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| LZ4 | Fastest | Lower | Real-time, high throughput |
| zlib | Balanced | Medium | General purpose (default) |
| LZFSE | Fast | Good | Apple platforms |
| LZMA | Slowest | Best | Archival, cold storage |

### Security Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Request Flow                              │
│                                                                  │
│  1. AuthContextKey.$current.withValue(auth) { ... }             │
│                          │                                       │
│                          ▼                                       │
│  2. FDBContext operations (fetch, save, delete)                 │
│                          │                                       │
│                          ▼                                       │
│  3. DataStoreSecurityDelegate.evaluate*(...)                    │
│     ├── Check admin roles → bypass if admin                     │
│     ├── Check SecurityPolicy conformance                        │
│     │   ├── strict=true: deny if not conforming                 │
│     │   └── strict=false: allow if not conforming               │
│     └── Call SecurityPolicy.allow*() methods                    │
│                          │                                       │
│                          ▼                                       │
│  4. Data serialization with TransformingSerializer              │
│     ├── Compression (if enabled)                                │
│     └── Encryption (AES-256-GCM, if enabled)                    │
│                          │                                       │
│                          ▼                                       │
│  5. FoundationDB transaction                                    │
└─────────────────────────────────────────────────────────────────┘
```

## Migration System

```swift
enum AppSchemaV1: VersionedSchema {
    static let version = Schema.Version(1, 0, 0)
    static let entities: [any Persistable.Type] = [User.self]
}

enum AppSchemaV2: VersionedSchema {
    static let version = Schema.Version(2, 0, 0)
    static let entities: [any Persistable.Type] = [User.self, Product.self]

    static let migration = Migration(from: AppSchemaV1.self) { context in
        // Add new index
        try await context.addIndex(
            "User_createdAt",
            to: "User",
            expression: .field("createdAt")
        )
    }
}

// Run migrations
try await container.migrateIfNeeded(to: AppSchemaV2.self)
```

## Built-in Index Maintainers

| IndexKind | Maintainer | Features |
|-----------|------------|----------|
| `ScalarIndexKind` | `ScalarIndexMaintainer` | Range queries, sorting |
| `CountIndexKind` | `CountIndexMaintainer` | Atomic increment/decrement |
| `SumIndexKind` | `SumIndexMaintainer` | Atomic sum aggregation |
| `MinIndexKind` | `MinIndexMaintainer` | Min value tracking |
| `MaxIndexKind` | `MaxIndexMaintainer` | Max value tracking |
| `AverageIndexKind` | `AverageIndexMaintainer` | Atomic average aggregation |
| `VectorIndexKind` | `FlatVectorIndexMaintainer`, `HNSWIndexMaintainer` | Similarity search |
| `FullTextIndexKind` | `FullTextIndexMaintainer` | Tokenization, inverted index |
| `SpatialIndexKind` | `SpatialIndexMaintainer` | S2, Geohash, Morton encoding |
| `RankIndexKind` | `RankIndexMaintainer` | Skip-list rankings |
| `PermutedIndexKind` | `PermutedIndexMaintainer` | Alternative field orderings |
| `AdjacencyIndexKind` | `AdjacencyIndexMaintainer` | Graph adjacency list traversal |
| `VersionIndexKind` | `VersionIndexMaintainer` | FDB versionstamp history |

## Adding a New Index Type

To add a new index type to database-framework:

### Step 1: Define IndexKind in database-kit

Create the index type definition in the `database-kit` package:

```swift
// database-kit/Sources/TimeSeries/TimeSeriesIndexKind.swift
import Core

public struct TimeSeriesIndexKind: IndexKind {
    public static let identifier = "timeseries"
    public static let subspaceStructure = SubspaceStructure.hierarchical

    public let resolution: TimeResolution

    public enum TimeResolution: String, Codable, Sendable {
        case second, minute, hour, day
    }

    public init(resolution: TimeResolution = .minute) {
        self.resolution = resolution
    }

    public static func validateTypes(_ types: [Any.Type]) throws {
        guard types.count >= 1, types[0] == Date.self else {
            throw IndexTypeValidationError.typeMismatch(
                field: "timestamp", expected: "Date", actual: String(describing: types.first)
            )
        }
    }
}
```

### Step 2: Create Index Module in database-framework

Create a new directory `Sources/TimeSeriesIndex/` with two files:

**File 1: IndexKindMaintainable extension**

```swift
// Sources/TimeSeriesIndex/TimeSeriesIndexKind.swift
import Core
import DatabaseEngine
import FoundationDB
import TimeSeries  // from database-kit

extension TimeSeriesIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return TimeSeriesIndexMaintainer<Item>(
            index: index,
            kind: self,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
```

**File 2: IndexMaintainer implementation**

```swift
// Sources/TimeSeriesIndex/TimeSeriesIndexMaintainer.swift
import Core
import DatabaseEngine
import FoundationDB
import TimeSeries

public struct TimeSeriesIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let kind: TimeSeriesIndexKind
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var customBuildStrategy: (any IndexBuildStrategy<Item>)? { nil }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        if let old = oldItem {
            let key = try buildKey(for: old)
            transaction.clear(key: key)
        }
        if let new = newItem {
            let key = try buildKey(for: new)
            transaction.setValue([], for: key)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let key = try buildKey(for: item, id: id)
        transaction.setValue([], for: key)
    }

    public func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes] {
        return [try buildKey(for: item, id: id)]
    }

    private func buildKey(for item: Item, id: Tuple? = nil) throws -> [UInt8] {
        // Implementation details...
        return subspace.pack(Tuple())
    }
}
```

### Step 3: Update Package.swift

Add the new module to `Package.swift`:

```swift
// Add product
.library(name: "TimeSeriesIndex", targets: ["TimeSeriesIndex"]),

// Add target
.target(
    name: "TimeSeriesIndex",
    dependencies: [
        "DatabaseEngine",
        .product(name: "Core", package: "database-kit"),
        .product(name: "TimeSeries", package: "database-kit"),
        .product(name: "FoundationDB", package: "fdb-swift-bindings"),
    ]
),

// Add to Database target dependencies
.target(
    name: "Database",
    dependencies: [
        // ... existing dependencies
        "TimeSeriesIndex",
    ]
),
```

### Step 4: Add Tests

Create `Tests/TimeSeriesIndexTests/TimeSeriesIndexBehaviorTests.swift`:

```swift
import Testing
import FoundationDB
@testable import TimeSeriesIndex
@testable import DatabaseEngine
@testable import Core

@Suite("TimeSeriesIndex Behavior Tests")
struct TimeSeriesIndexBehaviorTests {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test("Index entries are created correctly")
    func indexEntriesCreated() async throws {
        // Test implementation...
    }
}
```

### Index Maintainer Pattern

Each index module follows this structure:

```
Sources/
└── TimeSeriesIndex/
    ├── TimeSeriesIndexKind.swift      # extension TimeSeriesIndexKind: IndexKindMaintainable
    └── TimeSeriesIndexMaintainer.swift # struct TimeSeriesIndexMaintainer: IndexMaintainer
```

The `IndexMaintainer` protocol requires:

| Method | Purpose |
|--------|---------|
| `updateIndex(oldItem:newItem:transaction:)` | Update index on insert/update/delete |
| `scanItem(_:id:transaction:)` | Build index entry during batch indexing |
| `computeIndexKeys(for:id:)` | Return expected keys for scrubber verification |
| `customBuildStrategy` | Optional custom build strategy (e.g., HNSW) |

## Testing

Tests use a shared singleton for FDB initialization to avoid "API version may be set only once" errors:

```swift
import Testing
@testable import DatabaseEngine

@Suite struct MyTests {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func testCustomIndex() async throws {
        let container = try await FDBContainer(for: testSchema)
        let context = await container.mainContext
        // Test your index operations...
    }
}
```

## Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → Protobuf-encoded item
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
```

Subspace keys are single characters for storage efficiency. Use `SubspaceKey.items`, `SubspaceKey.indexes`, etc. for semantic clarity in code.

## Directory Layer

FDBContainer uses FoundationDB's DirectoryLayer for hierarchical namespace management. Directories provide efficient key prefix allocation and logical organization of data.

### Directory Macro

Define storage paths declaratively using the `#Directory` macro in database-kit:

```swift
@Persistable
struct User {
    #Directory<User>("app", "users")
    #Index(ScalarIndexKind<User>(fields: [\.email]), unique: true)

    var email: String
    var name: String
}
```

### Multi-Tenant Partitioning

Use `Field(\.property)` for dynamic path segments based on record fields:

```swift
@Persistable
struct Order {
    #Directory<Order>("tenants", Field(\.accountID), "orders", layer: .partition)

    var orderID: Int64
    var accountID: String  // Partition key
}
```

### Multi-level Partitioning

```swift
@Persistable
struct Message {
    #Directory<Message>(
        "tenants", Field(\.accountID),
        "channels", Field(\.channelID),
        "messages",
        layer: .partition
    )

    var messageID: Int64
    var accountID: String  // First partition key
    var channelID: String  // Second partition key
}
```

### Directory Layer Types

| Layer | Description |
|-------|-------------|
| `.default` | Default directory |
| `.partition` | Multi-tenant partition (requires at least one Field in path) |

### Directory Operations (Runtime)

```swift
// Get or create a directory
let userSubspace = try await container.getOrOpenDirectory(path: ["app", "users"])

// Create a new directory (fails if exists)
let logsSubspace = try await container.createDirectory(path: ["app", "logs"])

// Open an existing directory
let existingSubspace = try await container.openDirectory(path: ["app", "users"])

// Check if directory exists
let exists = try await container.directoryExists(path: ["app", "users"])

// Move a directory
let movedSubspace = try await container.moveDirectory(
    oldPath: ["app", "users"],
    newPath: ["archive", "users"]
)

// Remove a directory
try await container.removeDirectory(path: ["archive", "users"])
```

### Low-level Multi-Tenant Isolation

For manual control, use custom subspaces:

```swift
let tenantSubspace = Subspace(prefix: Tuple("tenant", tenantID).pack())
let container = FDBContainer(
    database: database,
    schema: schema,
    subspace: tenantSubspace
)
// Data stored at: tenant/[tenantID]/R/..., tenant/[tenantID]/I/...
```

## Distributed Systems Architecture

This framework is designed to work correctly with **Swift Distributed Actors** and other distributed computing patterns. Understanding FoundationDB's architecture is essential for building scalable distributed applications.

### FoundationDB's Client-Direct Model

FoundationDB uses a **client-direct connection model** where clients connect directly to StorageServers:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Your Application Cluster                      │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │   Node A    │   │   Node B    │   │   Node C    │            │
│  │ FDBContainer│   │ FDBContainer│   │ FDBContainer│            │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘            │
│         │                 │                 │                    │
│         └────────┬────────┴────────┬────────┘                    │
│                  │  Direct Client Connection                     │
└──────────────────┼───────────────────────────────────────────────┘
                   ▼
      ┌────────────────────────────────────────┐
      │         FoundationDB Cluster           │
      │  ┌────────┐ ┌────────┐ ┌────────┐      │
      │  │   SS   │ │   SS   │ │   SS   │      │
      │  │(shard1)│ │(shard2)│ │(shard3)│      │
      │  └────────┘ └────────┘ └────────┘      │
      │       ▲          ▲          ▲          │
      │       └──────────┴──────────┘          │
      │         Auto load balancing            │
      └────────────────────────────────────────┘
```

**Key insight**: FDB clients automatically discover and connect to the optimal StorageServer for each key range. Adding middleware layers between your application and FDB reduces throughput and defeats FDB's built-in load balancing.

### What FoundationDB Already Provides

| Feature | FDB Implementation | Do NOT re-implement |
|---------|-------------------|---------------------|
| **Distributed Transactions** | Commit Proxy + Resolver | ✗ |
| **Data Sharding** | DataDistributor (automatic) | ✗ |
| **Replication** | Team-based redundancy | ✗ |
| **Failure Detection** | ClusterController + SWIM protocol | ✗ |
| **Read Consistency** | MVCC + Read Version | ✗ |
| **Load Balancing** | Client locationCache | ✗ |

### ✅ Correct Architecture with Distributed Actors

Use distributed actors for **business logic distribution**, not data access:

```swift
import DistributedCluster
import Database
import Core

// ✅ CORRECT: Each actor has its own FDBContainer
distributed actor OrderService {
    typealias ActorSystem = ClusterSystem

    // FDBContainer is lightweight and stateless
    private let container: FDBContainer

    init(actorSystem: ActorSystem, schema: Schema) throws {
        self.actorSystem = actorSystem
        self.container = try FDBContainer(for: schema)
    }

    // Business logic is distributed across actors
    distributed func placeOrder(
        userId: String,
        items: [OrderItem]
    ) async throws -> Order {
        let context = container.newContext()

        // FDB handles distributed transactions automatically
        let inventory = try await context.fetch(Inventory.self)
            .where(\.productId, in: items.map(\.productId))
            .execute()

        guard inventory.allSatisfy({ $0.available }) else {
            throw OrderError.insufficientInventory
        }

        let order = Order(userId: userId, items: items)
        context.insert(order)

        // Inventory updates in same transaction
        for item in items {
            if var inv = inventory.first(where: { $0.productId == item.productId }) {
                inv.quantity -= item.quantity
                context.insert(inv)
            }
        }

        try await context.save()  // FDB ensures ACID across all shards
        return order
    }
}

// ✅ CORRECT: Multiple services, each with own container
distributed actor InventoryService {
    typealias ActorSystem = ClusterSystem

    private let container: FDBContainer

    init(actorSystem: ActorSystem, schema: Schema) throws {
        self.actorSystem = actorSystem
        self.container = try FDBContainer(for: schema)
    }

    distributed func checkAvailability(productIds: [String]) async throws -> [String: Int] {
        let context = container.newContext()
        let items = try await context.fetch(Inventory.self)
            .where(\.productId, in: productIds)
            .execute()
        return Dictionary(uniqueKeysWithValues: items.map { ($0.productId, $0.quantity) })
    }
}
```

### ❌ Anti-Pattern: Wrapping FDB Access in Distributed Actors

```swift
// ❌ WRONG: Don't create a "database service" actor
distributed actor DatabaseService {
    private let container: FDBContainer

    // This defeats FDB's direct-connection model!
    distributed func fetch<T: Persistable>(_ type: T.Type) async throws -> [T] {
        // All requests now go through this single actor = bottleneck
        let context = container.newContext()
        return try await context.fetch(Query<T>())
    }
}

// ❌ WRONG: Routing all DB access through one point
let dbService = try await system.singleton.host(name: "database") { actorSystem in
    DatabaseService(actorSystem: actorSystem)
}
// This creates a single point of failure and bottleneck
```

### Why FDBContainer is Safe for Distributed Use

`FDBContainer` is designed to be instantiated per-actor:

```swift
public final class FDBContainer: Sendable {
    // Thread-safe database connection (managed by FDB client library)
    nonisolated(unsafe) public let database: any DatabaseProtocol

    // Immutable configuration
    public let schema: Schema
    public let subspace: Subspace
    // ...
}
```

- **Stateless**: No mutable state that needs synchronization
- **Connection Pooling**: FDB client library manages connections internally
- **Location Cache**: Each client maintains its own cache, auto-updated by FDB
- **Lightweight**: Creating multiple containers has minimal overhead

### Scaling Patterns

#### Pattern 1: Service-per-Domain

```
┌─────────────────────────────────────────────────────────────────┐
│                    Distributed Actor Cluster                     │
│                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  OrderService   │  │  UserService    │  │ InventoryService│  │
│  │  (distributed)  │  │  (distributed)  │  │  (distributed)  │  │
│  │                 │  │                 │  │                 │  │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │
│  │ │FDBContainer │ │  │ │FDBContainer │ │  │ │FDBContainer │ │  │
│  │ └──────┬──────┘ │  │ └──────┬──────┘ │  │ └──────┬──────┘ │  │
│  └────────┼────────┘  └────────┼────────┘  └────────┼────────┘  │
│           │                    │                    │            │
│           └────────────────────┼────────────────────┘            │
│                                │                                 │
└────────────────────────────────┼─────────────────────────────────┘
                                 ▼
                    ┌────────────────────────┐
                    │   FoundationDB Cluster │
                    │   (handles all the     │
                    │   distributed magic)   │
                    └────────────────────────┘
```

#### Pattern 2: Sharded Actors with Shared Schema

```swift
// Shard actors by entity ID for locality
distributed actor UserActor {
    typealias ActorSystem = ClusterSystem

    private let userId: String
    private let container: FDBContainer

    init(actorSystem: ActorSystem, userId: String, schema: Schema) throws {
        self.actorSystem = actorSystem
        self.userId = userId
        self.container = try FDBContainer(for: schema)
    }

    distributed func updateProfile(name: String, email: String) async throws {
        let context = container.newContext()

        guard var user = try await context.model(for: userId, as: User.self) else {
            throw UserError.notFound
        }

        user.name = name
        user.email = email
        context.insert(user)
        try await context.save()
    }
}

// Use consistent hashing to route requests to the correct actor
extension ClusterSystem {
    func userActor(for userId: String) async throws -> UserActor {
        // ClusterSingleton or virtual actor pattern
        // Routes to actor responsible for this user's shard
    }
}
```

#### Pattern 3: Read Replicas with Snapshot Reads

```swift
distributed actor ReadReplicaService {
    typealias ActorSystem = ClusterSystem

    private let container: FDBContainer

    distributed func searchProducts(query: String) async throws -> [Product] {
        let context = container.newContext()

        // Snapshot reads don't conflict with writes
        // FDB automatically routes to nearest replica
        return try await context.fetch(Product.self)
            .where(\.name, contains: query)
            .execute()
    }
}
```

### Cross-Transaction Considerations

FDB transactions have a **5-second limit**. For long-running operations:

```swift
distributed actor BatchProcessor {
    private let container: FDBContainer

    distributed func processLargeDataset(ids: [String]) async throws {
        // Split into chunks for separate transactions
        for chunk in ids.chunked(into: 100) {
            try await processChunk(chunk)
        }
    }

    private func processChunk(_ ids: [String]) async throws {
        let context = container.newContext()

        for id in ids {
            // Process each item
        }

        try await context.save()  // Commits within 5s limit
    }
}
```

### Summary: Distribution Responsibilities

| Layer | Responsibility | Technology |
|-------|---------------|------------|
| **Business Logic** | Domain operations, orchestration | Swift Distributed Actors |
| **Data Access** | CRUD, queries, transactions | FDBContainer (this framework) |
| **Data Distribution** | Sharding, replication, consistency | FoundationDB (automatic) |

**The key principle**: Let FoundationDB handle data distribution. Use distributed actors for scaling business logic, not for wrapping database access.

## Related Packages

- **[DatabaseKit](https://github.com/1amageek/database-kit)**: Platform-independent model and index type definitions
- **[swift-distributed-actors](https://github.com/apple/swift-distributed-actors)**: Peer-to-peer cluster implementation for Swift's distributed actor feature

## License

MIT License

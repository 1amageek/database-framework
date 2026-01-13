# database-framework Guide

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Architecture](#2-architecture)
3. [Data Modeling](#3-data-modeling)
4. [CRUD Operations](#4-crud-operations)
5. [Indexes](#5-indexes)
6. [Fusion Query](#6-fusion-query)
7. [Query Optimization](#7-query-optimization)
8. [Transactions](#8-transactions)
9. [Schema Evolution](#9-schema-evolution)
10. [Online Index Building](#10-online-index-building)
11. [Serialization](#11-serialization)
12. [Security](#12-security)
13. [Instrumentation and Monitoring](#13-instrumentation-and-monitoring)

---

## 1. Getting Started

### 1.1 What is database-framework?

database-framework is a database framework for Swift built on top of FoundationDB. It provides type-safe data modeling, 13 index types, transaction management, and query optimization.

### 1.2 Requirements

- Swift 5.9+
- macOS 14+ / Linux
- FoundationDB 7.1+

### 1.3 Installation

**Package.swift**:

```swift
dependencies: [
    .package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),
    .package(url: "https://github.com/1amageek/database-framework.git", branch: "main"),
]

targets: [
    .target(
        name: "YourApp",
        dependencies: [
            .product(name: "Core", package: "database-kit"),
            .product(name: "Database", package: "database-framework"),
        ]
    ),
]
```

### 1.4 Quick Start

```swift
import Core
import Database

// 1. Define Model
@Persistable
struct User {
    #Directory<User>("app", "users")

    var id: String = ULID().ulidString
    var name: String = ""
    var email: String = ""

    #Index<User>(ScalarIndexKind<User>(fields: [\.email]), unique: true)
}

// 2. Create Container
let schema = Schema([User.self])
let container = try FDBContainer(database: database, schema: schema)

// 3. Data Operations
let context = container.newContext()

var user = User()
user.name = "Alice"
user.email = "alice@example.com"
context.insert(user)

try await context.save()

// 4. Fetch
let users = try await context.fetch(User.self)
    .where(\.email == "alice@example.com")
    .execute()
```

---

## 2. Architecture

### 2.1 Overall Structure

```
┌─────────────────────────────────────────────────────────────────┐
│ FDBContainer (Resource Management)                              │
│   - database: DatabaseProtocol                                  │
│   - schema: Schema                                              │
│   - securityDelegate: DataStoreSecurityDelegate?                │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ FDBDataStore (Data Operations + Transaction Provider)           │
│   - executeBatchInTransaction()                                 │
│   - withRawTransaction()                                        │
│   - Index maintenance                                           │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ FDBContext (User-facing API)                                    │
│   - insert(), delete(), save()                                  │
│   - fetch()                                                     │
│   - Change tracking                                             │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 FDBContainer

The entry point for your application that manages resources.

```swift
// Basic initialization
let schema = Schema([User.self, Order.self])
let container = try FDBContainer(
    database: database,
    schema: schema
)

// Initialization with configuration
let config = FDBConfiguration(
    url: URL(fileURLWithPath: "/etc/foundationdb/fdb.cluster"),
    indexConfigurations: [
        VectorIndexConfiguration(
            indexName: "User_embedding",
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )
    ]
)
let container = try FDBContainer(
    database: database,
    schema: schema,
    configuration: config
)
```

### 2.3 FDBContext

Provides change tracking and CRUD operations.

```swift
let context = container.newContext()

// Change tracking
context.insert(user)
context.delete(order)

// Check change state
print(context.insertedModels)  // Models pending insertion
print(context.deletedModels)   // Models pending deletion

// Save (commit all changes at once)
try await context.save()
```

### 2.4 Schema / Entity

Schema manages metadata for model definitions.

```swift
// Create from type array
let schema = Schema([User.self, Order.self, Product.self])

// With version
let schema = Schema(
    [User.self, Order.self],
    version: Schema.Version(1, 0, 0)
)

// Access entity information
for entity in schema.entities {
    print("Entity: \(entity.name)")
    print("Fields: \(entity.allFields)")
    print("Indexes: \(entity.indexDescriptors.map(\.name))")
}
```

### 2.5 Directory Structure

Uses FoundationDB's directory layer to organize data.

```
[fdb]/
├── app/
│   ├── users/
│   │   ├── R/User/[id]                → Item data
│   │   ├── I/User_email/[email]/[id]  → Index
│   │   └── B/[blob-key]               → Large data
│   └── orders/
│       ├── R/Order/[id]
│       └── I/Order_status/[status]/[id]
└── _metadata/
    ├── schema/version
    └── index/[indexName]/state
```

---

## 3. Data Modeling

### 3.1 @Persistable Macro

Define models using the `@Persistable` macro.

```swift
@Persistable
struct User {
    var id: String = ULID().ulidString
    var name: String = ""
    var email: String = ""
    var age: Int = 0
    var createdAt: Date = Date()
}
```

**Generated components**:
- `Persistable` protocol conformance
- `persistableType` static property
- `allFields` static property
- `fieldName(for:)` method
- `dynamicMember` subscript

### 3.2 Field Definitions and Types

Supported types:

```swift
@Persistable
struct AllTypes {
    var id: String = ULID().ulidString

    // Basic types
    var stringField: String = ""
    var intField: Int = 0
    var int64Field: Int64 = 0
    var doubleField: Double = 0.0
    var boolField: Bool = false

    // Date
    var dateField: Date = Date()

    // Binary
    var dataField: Data = Data()

    // Optional
    var optionalString: String? = nil

    // Array
    var tags: [String] = []

    // Nested types (Codable)
    var address: Address = Address()
}

struct Address: Codable, Sendable {
    var city: String = ""
    var country: String = ""
}
```

### 3.3 ID Design

```swift
// ULID (Recommended) - time-sortable
@Persistable
struct User {
    var id: String = ULID().ulidString
}

// UUID
@Persistable
struct Session {
    var id: String = UUID().uuidString
}

// Custom ID
@Persistable
struct Product {
    var id: String = ""  // Set externally
}
```

### 3.4 #Directory (Static Directory)

Specifies the storage directory for data.

```swift
@Persistable
struct User {
    #Directory<User>("app", "users")

    var id: String = ULID().ulidString
    var name: String = ""
}

// Storage location: [fdb]/app/users/R/User/[id]
```

### 3.5 Dynamic Directories (Partitioning)

Include field values in the directory path for partitioning.

```swift
@Persistable
struct TenantOrder {
    #Directory<TenantOrder>("app", Field(\.tenantID), "orders")

    var id: String = ULID().ulidString
    var tenantID: String = ""
    var total: Double = 0
}

// Storage location: [fdb]/app/[tenantID]/orders/R/TenantOrder/[id]
```

**Specifying partition when querying**:

```swift
// Query with partition
let orders = try await context.fetch(TenantOrder.self)
    .partition(\.tenantID, equals: "tenant_123")
    .where(\.total > 100)
    .execute()

// Using DirectoryPath
var path = DirectoryPath<TenantOrder>()
path.set(\.tenantID, to: "tenant_123")
let order = try await context.model(for: orderID, as: TenantOrder.self, partition: path)
```

### 3.6 Polymorphable (Union Types)

Allows multiple types to share a directory and enables polymorphic queries.

```swift
// Protocol definition
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
    var title: String = ""
    var content: String = ""

    #Directory<Article>("app", "articles")  // For dual-write
}

@Persistable
struct Report: Document {
    var id: String = ULID().ulidString
    var title: String = ""
    var data: Data = Data()
}
```

**Dual-write behavior**:
- `Article` is saved to both `app/articles/` and `app/documents/`
- `Report` is saved only to `app/documents/`

**Polymorphic queries**:

```swift
// Get all Documents
let allDocuments = try await context.fetchPolymorphic(Article.self)
// Returns [Article, Report, ...]

// Get by ID
let doc = try await context.fetchPolymorphic(Article.self, id: someId)
```

---

## 4. CRUD Operations

### 4.1 insert / update / delete / save

```swift
let context = container.newContext()

// Insert
var user = User()
user.name = "Alice"
user.email = "alice@example.com"
context.insert(user)

// Update (insert with same ID updates)
user.name = "Alice Smith"
context.insert(user)

// Delete
context.delete(user)

// Save (atomically commit all changes)
try await context.save()
```

### 4.2 fetch

```swift
// Get all
let allUsers = try await context.fetch(User.self).execute()

// Get single item (by ID)
let user = try await context.model(for: userID, as: User.self)

// Conditional fetch
let activeUsers = try await context.fetch(User.self)
    .where(\.isActive == true)
    .execute()
```

### 4.3 Filtering / Sorting / Limiting

```swift
let results = try await context.fetch(User.self)
    // Filtering
    .where(\.age >= 18)
    .where(\.email != nil)

    // Sorting
    .sort(\.createdAt, order: .descending)

    // Limiting
    .limit(20)
    .offset(0)

    .execute()
```

**Supported operators**:

```swift
// Equality
.where(\.status == "active")
.where(\.status != "deleted")

// Comparison
.where(\.age > 18)
.where(\.age >= 18)
.where(\.age < 65)
.where(\.age <= 65)

// Range
.where(\.age, in: 18...65)

// IN
.where(\.status, in: ["active", "pending"])

// NULL check
.where(\.deletedAt == nil)
.where(\.email != nil)

// String
.where(\.name, hasPrefix: "A")
.where(\.email, hasSuffix: "@example.com")
.where(\.bio, contains: "Swift")
```

### 4.4 Pagination (Cursor)

```swift
// First request
let firstPage = try await context.fetch(User.self)
    .sort(\.createdAt, order: .descending)
    .limit(20)
    .executeCursor()

print("Items: \(firstPage.items)")
print("Has more: \(firstPage.hasMore)")

// Next page
if let continuation = firstPage.continuation {
    let nextPage = try await context.fetch(User.self)
        .sort(\.createdAt, order: .descending)
        .limit(20)
        .continuation(continuation)
        .executeCursor()
}
```

**CursorResult structure**:

```swift
struct CursorResult<T> {
    let items: [T]           // Retrieved items
    let hasMore: Bool        // Whether next page exists
    let continuation: ContinuationToken?  // Token for next page
}
```

---

## 5. Indexes

### 5.1 Basics

**#Index macro**:

```swift
@Persistable
struct User {
    var id: String = ULID().ulidString
    var email: String = ""
    var name: String = ""
    var age: Int = 0

    // Basic index
    #Index<User>(ScalarIndexKind<User>(fields: [\.email]))

    // Uniqueness constraint
    #Index<User>(ScalarIndexKind<User>(fields: [\.email]), unique: true)

    // Compound index
    #Index<User>(ScalarIndexKind<User>(fields: [\.name, \.age]))

    // Named index
    #Index<User>(ScalarIndexKind<User>(fields: [\.age]), name: "User_age_idx")
}
```

**Index states**:

| State | Read | Write | Use Case |
|-------|------|-------|----------|
| `readable` | ✅ | ✅ | Normal operation |
| `writeOnly` | ❌ | ✅ | During build |
| `disabled` | ❌ | ❌ | Disabled |

### 5.2 Scalar Index (Equality/Range Queries)

The most basic index supporting equality and range searches.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var category: String = ""
    var price: Double = 0
    var inStock: Bool = true

    // Single field
    #Index<Product>(ScalarIndexKind<Product>(fields: [\.category]))

    // Compound index (field order matters)
    #Index<Product>(ScalarIndexKind<Product>(fields: [\.category, \.price]))
}
```

**Query examples**:

```swift
// Equality search
let electronics = try await context.fetch(Product.self)
    .where(\.category == "electronics")
    .execute()

// Range search
let affordable = try await context.fetch(Product.self)
    .where(\.price >= 10)
    .where(\.price <= 100)
    .execute()

// Using compound index
let cheapElectronics = try await context.fetch(Product.self)
    .where(\.category == "electronics")
    .where(\.price < 50)
    .execute()
```

### 5.3 Vector Index (Semantic Search)

Provides similarity search for high-dimensional vectors.

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var title: String = ""
    var embedding: [Float] = []

    // Flat (Brute-force) - for small datasets
    #Index<Article>(VectorIndexKind<Article>.flat(
        field: \.embedding,
        dimensions: 384,
        metric: .cosine
    ))

    // HNSW (Approximate nearest neighbor) - for large datasets
    #Index<Article>(VectorIndexKind<Article>.hnsw(
        field: \.embedding,
        dimensions: 384,
        metric: .cosine,
        m: 16,
        efConstruction: 200
    ))
}
```

**Runtime configuration** (HNSW parameter tuning):

```swift
let config = FDBConfiguration(
    indexConfigurations: [
        VectorIndexConfiguration(
            indexName: "Article_embedding",
            parameters: HNSWParameters(
                m: 16,              // Number of connections
                efConstruction: 200, // Search width during construction
                efSearch: 100       // Search width during search
            )
        )
    ]
)
```

**Query examples**:

```swift
let queryVector: [Float] = generateEmbedding("Swift programming")

let similar = try await context.findSimilar(Article.self)
    .vector(\.embedding)
    .query(queryVector)
    .topK(10)
    .execute()

for result in similar {
    print("\(result.item.title): \(result.score)")
}
```

### 5.4 FullText Index (Full-Text Search)

Provides tokenization, stemming, and scoring for text.

```swift
@Persistable
struct BlogPost {
    var id: String = ULID().ulidString
    var title: String = ""
    var body: String = ""

    #Index<BlogPost>(FullTextIndexKind<BlogPost>(
        field: \.body,
        language: .english,
        enableStemming: true,
        enableFuzzy: true
    ))
}
```

**Query examples**:

```swift
// Basic search
let results = try await context.search(BlogPost.self)
    .field(\.body)
    .query("swift programming")
    .execute()

// Fuzzy search (tolerates typos)
let fuzzyResults = try await context.search(BlogPost.self)
    .field(\.body)
    .query("progrmaing")  // typo
    .fuzzy(maxDistance: 2)
    .execute()

// With highlighting
let highlighted = try await context.search(BlogPost.self)
    .field(\.body)
    .query("swift")
    .highlight(prefix: "<mark>", suffix: "</mark>")
    .execute()

for result in highlighted {
    print(result.highlights)  // ["<mark>Swift</mark> is a powerful..."]
}
```

### 5.5 Spatial Index (Geospatial)

Provides geospatial queries based on latitude and longitude.

```swift
@Persistable
struct Restaurant {
    var id: String = ULID().ulidString
    var name: String = ""
    var latitude: Double = 0
    var longitude: Double = 0

    // Geohash encoding
    #Index<Restaurant>(SpatialIndexKind<Restaurant>.geohash(
        latitude: \.latitude,
        longitude: \.longitude,
        precision: 8
    ))

    // S2 Geometry (higher precision)
    #Index<Restaurant>(SpatialIndexKind<Restaurant>.s2(
        latitude: \.latitude,
        longitude: \.longitude,
        level: 16
    ))
}
```

**Query examples**:

```swift
// Nearby search
let nearbyRestaurants = try await context.nearby(Restaurant.self)
    .location(latitude: 35.6812, longitude: 139.7671)  // Tokyo Station
    .radius(kilometers: 1.0)
    .execute()

// With distance
let withDistance = try await context.nearby(Restaurant.self)
    .location(latitude: 35.6812, longitude: 139.7671)
    .radius(kilometers: 5.0)
    .includeDistance()
    .execute()

for result in withDistance {
    print("\(result.item.name): \(result.distance)km")
}
```

### 5.6 Graph Index (Graph/RDF/Reasoning)

Manages graph structures and RDF triples. Supports three storage strategies.

```swift
// Social graph (adjacency strategy)
@Persistable
struct Follow {
    var id: String = ULID().ulidString
    var followerID: String = ""
    var followeeID: String = ""

    #Index<Follow>(GraphIndexKind<Follow>.adjacency(
        source: \.followerID,
        target: \.followeeID
    ))
}

// RDF triple store (tripleStore strategy)
@Persistable
struct Statement {
    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index<Statement>(GraphIndexKind<Statement>.rdf(
        subject: \.subject,
        predicate: \.predicate,
        object: \.object,
        strategy: .tripleStore
    ))
}

// High-performance knowledge graph (hexastore strategy)
@Persistable
struct KnowledgeTriple {
    var id: String = ULID().ulidString
    var entity: String = ""
    var relation: String = ""
    var value: String = ""

    #Index<KnowledgeTriple>(GraphIndexKind<KnowledgeTriple>.knowledgeGraph(
        entity: \.entity,
        relation: \.relation,
        value: \.value
    ))
}
```

**Storage strategy comparison**:

| Strategy | Index Count | Write Cost | Use Case |
|----------|-------------|------------|----------|
| `adjacency` | 2 | Low | Social graphs |
| `tripleStore` | 3 | Medium | RDF/SPARQL-like queries |
| `hexastore` | 6 | High | Read-optimized |

**Query examples**:

```swift
// Get followers
let followers = try await context.graph(Follow.self)
    .outgoing(from: userID)
    .execute()

// RDF query
let facts = try await context.graph(Statement.self)
    .subject("Tokyo")
    .predicate("locatedIn")
    .execute()

// Graph traversal
let traverser = GraphTraverser(context: context)
let path = try await traverser.shortestPath(
    from: "Alice",
    to: "Bob",
    edgeType: Follow.self
)
```

**OWL DL Reasoning**:

```swift
let reasoner = OWLReasoner(context: context)

// Class hierarchy inference
let subclasses = try await reasoner.subClassesOf("Animal")

// Instance inference
let instances = try await reasoner.instancesOf("Mammal")

// Consistency check
let isConsistent = try await reasoner.isConsistent()
```

### 5.7 Rank Index (Ranking)

Efficiently processes rank-based queries.

```swift
@Persistable
struct Player {
    var id: String = ULID().ulidString
    var name: String = ""
    var score: Int64 = 0

    #Index<Player>(RankIndexKind<Player, Int64>(
        scoreField: \.score,
        order: .descending
    ))
}
```

**Query examples**:

```swift
// Top N
let topPlayers = try await context.rank(Player.self)
    .score(\.score)
    .top(10)
    .execute()

// Rank of specific player
let rank = try await context.rank(Player.self)
    .score(\.score)
    .rankOf(playerID)

print("Rank: \(rank)")  // e.g., 42

// Rank range
let range = try await context.rank(Player.self)
    .score(\.score)
    .range(from: 10, to: 20)
    .execute()
```

### 5.8 Aggregation Index (Aggregations)

Provides materialized aggregations.

```swift
@Persistable
struct Order {
    var id: String = ULID().ulidString
    var customerID: String = ""
    var amount: Double = 0
    var status: String = ""

    // Count
    #Index<Order>(CountIndexKind<Order>(
        groupBy: [\.customerID]
    ))

    // Sum
    #Index<Order>(SumIndexKind<Order, Double>(
        field: \.amount,
        groupBy: [\.customerID]
    ))

    // Average
    #Index<Order>(AverageIndexKind<Order, Double>(
        field: \.amount,
        groupBy: [\.status]
    ))

    // Min/Max
    #Index<Order>(MinMaxIndexKind<Order, Double>(
        field: \.amount,
        groupBy: [\.customerID]
    ))
}
```

**Query examples**:

```swift
// Order count per customer
let orderCounts = try await context.aggregate(Order.self)
    .count()
    .groupBy(\.customerID)
    .execute()

// Total amount per customer
let totalAmounts = try await context.aggregate(Order.self)
    .sum(\.amount)
    .groupBy(\.customerID)
    .execute()

// Average amount by status
let avgByStatus = try await context.aggregate(Order.self)
    .average(\.amount)
    .groupBy(\.status)
    .execute()
```

### 5.9 Version Index (History)

Manages version history using Versionstamps.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: String = ""
    var version: Versionstamp?

    #Index<Document>(VersionIndexKind<Document>(
        versionField: \.version
    ))
}
```

**Query examples**:

```swift
// Get state at specific point in time
let snapshot = try await context.version(Document.self)
    .at(versionstamp)
    .execute()

// History list
let history = try await context.version(Document.self)
    .id(documentID)
    .history(limit: 10)
    .execute()

// Diff between two versions
let changes = try await context.version(Document.self)
    .id(documentID)
    .between(fromVersion, toVersion)
    .execute()
```

### 5.10 Bitmap Index (Set Operations)

Provides efficient set operations (Roaring Bitmap implementation).

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var tags: [String] = []
    var categories: [String] = []

    #Index<Article>(BitmapIndexKind<Article>(
        field: \.tags
    ))
}
```

**Query examples**:

```swift
// Single tag
let swiftArticles = try await context.bitmap(Article.self)
    .field(\.tags)
    .contains("swift")
    .execute()

// AND (has both tags)
let iosSwift = try await context.bitmap(Article.self)
    .field(\.tags)
    .containsAll(["swift", "ios"])
    .execute()

// OR (has any of the tags)
let mobileArticles = try await context.bitmap(Article.self)
    .field(\.tags)
    .containsAny(["ios", "android"])
    .execute()

// NOT (does not have tag)
let notDeprecated = try await context.bitmap(Article.self)
    .field(\.tags)
    .excludes("deprecated")
    .execute()
```

### 5.11 Leaderboard Index (Time-Windowed Ranking)

Manages time-windowed leaderboards.

```swift
@Persistable
struct GameScore {
    var id: String = ULID().ulidString
    var playerID: String = ""
    var score: Int64 = 0
    var gameMode: String = ""

    // Daily leaderboard (keep 7 days)
    #Index<GameScore>(TimeWindowLeaderboardIndexKind<GameScore, Int64>(
        scoreField: \.score,
        window: .daily,
        windowCount: 7
    ))

    // Weekly leaderboard by game mode
    #Index<GameScore>(TimeWindowLeaderboardIndexKind<GameScore, Int64>(
        scoreField: \.score,
        groupBy: [\.gameMode],
        window: .weekly,
        windowCount: 4
    ))
}
```

**Time window options**:

| Window | Description |
|--------|-------------|
| `.hourly` | 1 hour |
| `.daily` | 1 day |
| `.weekly` | 1 week |
| `.monthly` | 1 month |

**Query examples**:

```swift
// Today's top 10
let todayTop = try await context.leaderboard(GameScore.self)
    .score(\.score)
    .window(.daily)
    .top(10)
    .execute()

// Leaderboard for specific day
let specificDay = try await context.leaderboard(GameScore.self)
    .score(\.score)
    .window(.daily)
    .date(someDate)
    .top(100)
    .execute()

// By game mode
let rankedTop = try await context.leaderboard(GameScore.self)
    .score(\.score)
    .window(.weekly)
    .group(\.gameMode, equals: "ranked")
    .top(10)
    .execute()
```

### 5.12 Permuted Index (Permutations)

Creates indexes with all permutations of fields, optimizing queries for any combination.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var category: String = ""
    var brand: String = ""
    var color: String = ""

    #Index<Product>(PermutedIndexKind<Product>(
        fields: [\.category, \.brand, \.color]
    ))
}
```

**Generated indexes**:
- `[category, brand, color]`
- `[category, color, brand]`
- `[brand, category, color]`
- `[brand, color, category]`
- `[color, category, brand]`
- `[color, brand, category]`

**Query examples**:

```swift
// Efficient for any combination
let results1 = try await context.fetch(Product.self)
    .where(\.category == "electronics")
    .execute()

let results2 = try await context.fetch(Product.self)
    .where(\.brand == "Apple")
    .where(\.color == "silver")
    .execute()

let results3 = try await context.fetch(Product.self)
    .where(\.color == "black")
    .execute()
```

### 5.13 Relationship Index (Relations/JOIN)

Manages relationships between models.

```swift
@Persistable
struct Customer {
    var id: String = ULID().ulidString
    var name: String = ""

    @Relationship(Order.self)
    var orderIDs: [String] = []
}

@Persistable
struct Order {
    var id: String = ULID().ulidString
    var total: Double = 0

    @Relationship(Customer.self)
    var customerID: String? = nil
}
```

**Relationship types**:

| Type | Declaration |
|------|-------------|
| to-one | `var customerID: String? = nil` |
| to-many | `var orderIDs: [String] = []` |

**Delete Rules**:

```swift
@Relationship(Order.self, deleteRule: .cascade)
var orderIDs: [String] = []

@Relationship(Customer.self, deleteRule: .deny)
var customerID: String? = nil

@Relationship(Product.self, deleteRule: .nullify)
var productID: String? = nil

@Relationship(Log.self, deleteRule: .noAction)
var logIDs: [String] = []
```

| Rule | Behavior |
|------|----------|
| `.cascade` | Delete related items |
| `.deny` | Refuse deletion if relation exists |
| `.nullify` | Set relation to null |
| `.noAction` | Do nothing |

**Query examples**:

```swift
// Get related items (lazy loading)
let customer = try await context.model(for: customerID, as: Customer.self)
let orders = try await context.related(customer, \.orderIDs)

// JOIN query
let customersWithOrders = try await context.fetch(Customer.self)
    .joining(\.orderIDs)
    .where(Order.self, \.total > 100)
    .execute()

// Delete with relationship rules applied
try await context.deleteEnforcingRelationshipRules(customer)
```

---

## 6. Fusion Query

Provides multi-modal search combining multiple indexes.

### 6.1 Multi-Index Search

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var name: String = ""
    var description: String = ""
    var embedding: [Float] = []
    var category: String = ""
    var price: Double = 0

    #Index<Product>(FullTextIndexKind<Product>(field: \.description))
    #Index<Product>(VectorIndexKind<Product>.hnsw(field: \.embedding, dimensions: 384))
    #Index<Product>(ScalarIndexKind<Product>(fields: [\.category]))
}
```

### 6.2 Pipeline Construction

```swift
let results = try await context.fusion(Product.self)
    // Vector similarity search
    .similar(\.embedding, to: queryVector, weight: 0.6)

    // Full-text search
    .search(\.description, query: "wireless bluetooth", weight: 0.3)

    // Filtering
    .filter(\.category == "electronics")
    .filter(\.price < 200)

    // Score aggregation
    .scoreStrategy(.weightedSum)

    // Result limit
    .topK(20)

    .execute()
```

### 6.3 Score Aggregation Strategies

```swift
// Weighted sum (default)
.scoreStrategy(.weightedSum)

// Maximum
.scoreStrategy(.max)

// Geometric mean
.scoreStrategy(.geometricMean)

// Reciprocal Rank Fusion
.scoreStrategy(.rrf(k: 60))
```

### 6.4 Results

```swift
for result in results {
    print("ID: \(result.item.id)")
    print("Score: \(result.score)")
    print("Scores by source: \(result.scoreBreakdown)")
}
```

---

## 7. Query Optimization

### 7.1 Cascades Optimizer

Built-in cost-based optimizer based on the Cascades framework.

```swift
// Get query plan
let plan = try await context.fetch(User.self)
    .where(\.age > 18)
    .where(\.status == "active")
    .sort(\.createdAt)
    .explain()

print(plan)
// Output:
// IndexScan(User_status, key: "active")
//   -> Filter(age > 18)
//   -> Sort(createdAt DESC)
```

### 7.2 Cost Estimation and Statistics

```swift
// Collect statistics
let stats = try await context.statistics(for: User.self)
print("Row count: \(stats.rowCount)")
print("Cardinality of 'status': \(stats.cardinality(\.status))")

// Histogram
let histogram = stats.histogram(\.age)
print("Distribution: \(histogram.buckets)")
```

### 7.3 Index Selection

The optimizer considers the following for index selection:

- Selectivity
- Cardinality
- Index coverage
- Sort requirements

```swift
// Index-Only Scan (covering index)
let emailsOnly = try await context.fetch(User.self)
    .select(\.email)  // Only fields included in index
    .where(\.status == "active")
    .execute()

// Skip Scan (skip leading field)
// With compound index [category, price]
let results = try await context.fetch(Product.self)
    .where(\.price < 100)  // Query on price without specifying category
    .execute()
```

### 7.4 EXPLAIN

```swift
let explanation = try await context.fetch(User.self)
    .where(\.email == "test@example.com")
    .explain()

print(explanation.planTree)
print(explanation.estimatedCost)
print(explanation.selectedIndexes)
```

---

## 8. Transactions

### 8.1 Basics

```swift
// Implicit transaction (context.save())
let context = container.newContext()
context.insert(user)
try await context.save()  // Commits in single transaction

// Explicit transaction
try await container.database.withTransaction { transaction in
    let txContext = TransactionContext(transaction: transaction, container: container)
    try await txContext.set(user)
    try await txContext.set(order)
    // Auto-commit
}
```

### 8.2 TransactionConfiguration

```swift
// Custom configuration
let config = TransactionConfiguration(
    timeout: 5000,           // 5 seconds
    retryLimit: 10,          // Max 10 retries
    maxRetryDelay: 2000,     // Max 2 second backoff
    priority: .default,      // Priority
    readPriority: .normal    // Read priority
)

try await container.database.withTransaction(configuration: config) { tx in
    // ...
}
```

### 8.3 Presets

```swift
// For batch processing
TransactionConfiguration.batch
// - timeout: 30 seconds
// - retryLimit: 20
// - priority: .batch
// - cachePolicy: .server (strict consistency for data integrity)

// For system administration
TransactionConfiguration.system
// - timeout: 2 seconds
// - priority: .system
// - readPriority: .high

// For interactive use
TransactionConfiguration.interactive
// - timeout: 1 second
// - retryLimit: 3

// For long-running operations
TransactionConfiguration.longRunning
// - timeout: 60 seconds
// - retryLimit: 50
// - cachePolicy: .stale(60)

// For read-only dashboard queries
TransactionConfiguration.readOnly
// - timeout: 2 seconds
// - retryLimit: 3
// - cachePolicy: .cached
```

### 8.4 Cache Policy

```swift
// Strict consistency (default)
let users = try await context.fetch(User.self)
    .cachePolicy(.server)
    .execute()

// Use cache if available (no time limit)
let users = try await context.fetch(User.self)
    .cachePolicy(.cached)
    .execute()

// Use cache only if younger than 30 seconds
let users = try await context.fetch(User.self)
    .cachePolicy(.stale(30))
    .execute()

// With TransactionConfiguration
let config = TransactionConfiguration(
    cachePolicy: .stale(60)  // Allow up to 60 seconds staleness
)

// ReadVersionCache behavior
// - Reuses cached read version based on CachePolicy
// - Reduces getReadVersion() network round-trips
```

### 8.5 CommitCheck (Pre-commit Validation)

```swift
// Uniqueness validation
let registry = CommitCheckRegistry()
registry.add(UniquenessCommitCheck(
    indexSubspace: emailIndexSubspace,
    value: user.email,
    fieldName: "email"
))

try await registry.executeAll(transaction: transaction)

// Custom validation
let customCheck = ClosureCommitCheck(name: "balance-check") { tx in
    let balance = try await fetchBalance(tx)
    if balance < 0 {
        throw ValidationError.insufficientBalance
    }
}
registry.add(customCheck)
```

### 8.6 PostCommit (Post-commit Hooks)

```swift
let registry = PostCommitRegistry()

// Cache invalidation
registry.add(name: "cache-invalidation", priority: 10) {
    await cache.invalidate("user:\(userID)")
}

// Send notification (with retry)
registry.add(RetryingPostCommit(
    name: "send-notification",
    maxRetries: 3
) {
    try await notificationService.send(notification)
})

// Execute after commit
await registry.executeAll()
```

### 8.7 TransactionListener (Lifecycle Events)

```swift
let listenerRegistry = TransactionListenerRegistry()

// Metrics collection
listenerRegistry.add(MetricsTransactionListener())

// Custom listener
listenerRegistry.add { event in
    switch event {
    case .created(let id):
        print("Transaction \(id) created")
    case .committed(let id, let duration):
        print("Transaction \(id) committed in \(duration)ms")
    case .failed(let id, let error):
        print("Transaction \(id) failed: \(error)")
    default:
        break
    }
}
```

---

## 9. Schema Evolution

### 9.1 VersionedSchema

```swift
// Version 1
enum AppSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [UserV1.self]
}

@Persistable(type: "User")
struct UserV1 {
    var id: String = ULID().ulidString
    var name: String = ""
    var email: String = ""

    #Index<UserV1>(ScalarIndexKind<UserV1>(fields: [\.email]), unique: true)
}

// Version 2 (add age field and index)
enum AppSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [UserV2.self]
}

@Persistable(type: "User")
struct UserV2 {
    var id: String = ULID().ulidString
    var name: String = ""
    var email: String = ""
    var age: Int = 0  // New field

    #Index<UserV2>(ScalarIndexKind<UserV2>(fields: [\.email]), unique: true)
    #Index<UserV2>(ScalarIndexKind<UserV2>(fields: [\.age]))  // New index
}
```

### 9.2 SchemaMigrationPlan

```swift
enum AppMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [AppSchemaV1.self, AppSchemaV2.self, AppSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [migrateV1toV2, migrateV2toV3]
    }

    // Lightweight migration (index addition only)
    static let migrateV1toV2 = MigrationStage.lightweight(
        fromVersion: AppSchemaV1.self,
        toVersion: AppSchemaV2.self
    )

    // Custom migration (with data transformation)
    static let migrateV2toV3 = MigrationStage.custom(
        fromVersion: AppSchemaV2.self,
        toVersion: AppSchemaV3.self,
        willMigrate: { context in
            // Pre-migration processing
            print("Starting migration to V3")
        },
        didMigrate: { context in
            // Post-migration data transformation
            for try await user in context.enumerate(UserV3.self) {
                if user.displayName.isEmpty {
                    var updated = user
                    updated.displayName = user.name
                    try await context.update(updated)
                }
            }
        }
    )
}
```

### 9.3 MigrationStage

**lightweight**:
- Index addition/removal
- Field addition (using default values)
- Automatically processed

**custom**:
- `willMigrate`: Pre-migration hook
- `didMigrate`: Post-migration hook
- Use when data transformation is needed

### 9.4 migrateIfNeeded()

```swift
// Create container with migration plan
let container = try FDBContainer(
    for: AppSchemaV3.self,
    migrationPlan: AppMigrationPlan.self,
    configuration: config
)

// Execute migration
try await container.migrateIfNeeded()

// Internal behavior:
// 1. Get current storage version
// 2. Compare with target version
// 3. Calculate migration path
// 4. Execute each stage sequentially
// 5. Update version
```

### 9.5 MigrationContext

```swift
MigrationStage.custom(
    fromVersion: V1.self,
    toVersion: V2.self,
    willMigrate: nil,
    didMigrate: { context in
        // Index operations
        try await context.addIndex(newIndexDescriptor)
        try await context.removeIndex(indexName: "old_index", addedVersion: v1)
        try await context.rebuildIndex(indexName: "existing_index")

        // Data enumeration
        for try await item in context.enumerate(User.self, batchSize: 1000) {
            // Process
        }

        // Batch update
        try await context.batchUpdate(updatedItems, batchSize: 100)

        // Batch delete
        try await context.batchDelete(itemsToDelete, batchSize: 100)

        // Count
        let count = try await context.count(User.self)
        let approxCount = try await context.count(User.self, approximate: true)
    }
)
```

---

## 10. Online Index Building

### 10.1 OnlineIndexer

Builds indexes on a running system.

```swift
let indexer = OnlineIndexer(
    database: database,
    storeSubspace: storeSubspace,
    index: index,
    indexStateManager: stateManager,
    configuration: .default
)

// Execute build
try await indexer.buildIndex()

// Monitor progress
let progress = indexer.getProgress()
print("Processed: \(progress.processedCount)")
print("Remaining: \(progress.remainingRanges)")
```

**Configuration options**:

```swift
let config = OnlineIndexerConfiguration(
    batchSize: 100,           // Batch size
    throttleConfig: .default,  // Throttling configuration
    enableProgressPersistence: true  // Persist progress
)
```

### 10.2 MultiTargetOnlineIndexer

Builds multiple indexes in a single scan.

```swift
let indexer = MultiTargetOnlineIndexer(
    database: database,
    storeSubspace: storeSubspace,
    indexes: [index1, index2, index3],
    indexStateManager: stateManager
)

try await indexer.buildAllIndexes()
```

### 10.3 MutualOnlineIndexer

Builds bidirectional indexes (e.g., followers/following) simultaneously.

```swift
let indexer = MutualOnlineIndexer(
    database: database,
    storeSubspace: storeSubspace,
    forwardIndex: followingIndex,
    reverseIndex: followersIndex,
    indexStateManager: stateManager
)

try await indexer.buildMutualIndexes()
```

### 10.4 IndexFromIndexBuilder

Builds new index from existing index (reduces I/O).

```swift
let builder = IndexFromIndexBuilder(
    database: database,
    sourceIndex: existingIndex,
    targetIndex: newIndex,
    indexStateManager: stateManager
)

try await builder.build()
```

### 10.5 OnlineIndexScrubber

Verifies and repairs index consistency.

```swift
let scrubber = OnlineIndexScrubber(
    database: database,
    storeSubspace: storeSubspace,
    index: index,
    indexStateManager: stateManager
)

// Verify only
let issues = try await scrubber.scrub(mode: .reportOnly)
for issue in issues {
    print("Issue: \(issue)")
}

// Repair
let repaired = try await scrubber.scrub(mode: .repair)
print("Repaired \(repaired.count) issues")
```

### 10.6 AdaptiveThrottler

Adjusts throttling based on backpressure.

```swift
let throttler = AdaptiveThrottler(configuration: .default)

while !isComplete {
    let batchSize = throttler.currentBatchSize

    do {
        let items = try await processBatch(size: batchSize)
        throttler.recordSuccess(itemCount: items.count, durationNs: elapsed)
    } catch {
        throttler.recordFailure(error: error)
        if !throttler.isRetryable(error) { throw error }
    }

    try await throttler.waitBeforeNextBatch()
}
```

**Configuration presets**:

```swift
ThrottleConfiguration.default      // Balanced
ThrottleConfiguration.conservative // Cautious (small batches, long waits)
ThrottleConfiguration.aggressive   // Aggressive (large batches, short waits)
```

---

## 11. Serialization

### 11.1 ItemStorage / ItemEnvelope

All items are stored in `ItemEnvelope` format.

```
[ITEM magic (4 bytes)] [payload...]
```

**Direct access is prohibited**:

```swift
// ❌ Wrong: Direct read
let data = try await transaction.getValue(for: key)
let item = try decoder.decode(User.self, from: Data(data))
// Error: "Data is not in ItemEnvelope format"

// ✅ Correct: Via ItemStorage
let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
if let data = try await storage.read(for: key) {
    let item = try decoder.decode(User.self, from: Data(data))
}
```

### 11.2 Automatic Large Value Splitting

Values exceeding 100KB are automatically split.

```swift
// Handled automatically
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: Data = Data()  // 100KB+ is OK
}

// Internally:
// [fdb]/app/documents/R/Document/[id] → ItemEnvelope(metadata)
// [fdb]/app/documents/B/[blob-key]/0  → Chunk 0
// [fdb]/app/documents/B/[blob-key]/1  → Chunk 1
// ...
```

### 11.3 Compression

```swift
let serializer = TransformingSerializer(
    compression: .lz4  // Fast
    // compression: .zlib   // Balanced
    // compression: .lzma   // High compression
    // compression: .lzfse  // Apple optimized
)

let compressed = try serializer.serialize(data)
let decompressed = try serializer.deserialize(compressed)
```

### 11.4 Encryption

```swift
// Static key
let keyProvider = StaticKeyProvider(key: symmetricKey)

// From environment variable
let keyProvider = EnvironmentKeyProvider(variableName: "ENCRYPTION_KEY")

// Key rotation
let keyProvider = RotatingKeyProvider(
    current: currentKey,
    previous: [oldKey1, oldKey2]
)

// Derived key (different key per record)
let keyProvider = DerivedKeyProvider(
    masterKey: masterKey,
    info: "record-encryption"
)

// Apply to serializer
let serializer = TransformingSerializer(
    compression: .lz4,
    encryption: .aes256gcm(keyProvider: keyProvider)
)
```

---

## 12. Security

### 12.1 SecurityConfiguration

```swift
let securityConfig = SecurityConfiguration(
    enableAccessControl: true,
    defaultPolicy: .deny,  // Deny by default
    auditLog: true
)

let container = try FDBContainer(
    database: database,
    schema: schema,
    securityConfiguration: securityConfig
)
```

### 12.2 DataStoreSecurityDelegate

```swift
class MySecurityDelegate: DataStoreSecurityDelegate {
    func canRead<T: Persistable>(
        _ type: T.Type,
        id: String,
        context: SecurityContext
    ) async throws -> Bool {
        // Check user permissions
        guard let userID = context.userID else { return false }
        return try await checkReadPermission(userID: userID, resourceType: T.persistableType)
    }

    func canWrite<T: Persistable>(
        _ item: T,
        operation: WriteOperation,
        context: SecurityContext
    ) async throws -> Bool {
        guard let userID = context.userID else { return false }

        switch operation {
        case .insert:
            return try await checkCreatePermission(userID: userID)
        case .update:
            return try await checkUpdatePermission(userID: userID, itemID: item.id)
        case .delete:
            return try await checkDeletePermission(userID: userID, itemID: item.id)
        }
    }

    func filterResults<T: Persistable>(
        _ items: [T],
        context: SecurityContext
    ) async throws -> [T] {
        // Filter results
        guard let userID = context.userID else { return [] }
        return items.filter { canAccess(userID: userID, item: $0) }
    }
}

// Apply
let container = try FDBContainer(
    database: database,
    schema: schema,
    securityDelegate: MySecurityDelegate()
)
```

### 12.3 Authentication Context

```swift
// Propagate authentication info via TaskLocal
extension SecurityContext {
    @TaskLocal static var current: SecurityContext?
}

// Set in request handler
func handleRequest(userID: String) async throws {
    let context = SecurityContext(userID: userID, roles: ["user"])

    try await SecurityContext.$current.withValue(context) {
        // Context applies to all operations inside
        let items = try await dbContext.fetch(User.self).execute()
    }
}
```

---

## 13. Instrumentation and Monitoring

### 13.1 StoreTimer

```swift
let timer = StoreTimer()

// Record events
timer.record(.fetchStart)
// ... processing ...
timer.record(.fetchEnd)

// Get statistics
let stats = timer.getStatistics()
print("Fetch count: \(stats.fetchCount)")
print("Avg fetch time: \(stats.avgFetchDuration)ms")
print("Total read bytes: \(stats.totalReadBytes)")
```

### 13.2 InstrumentedTransaction

```swift
let metrics = TransactionMetrics()

try await database.withTransaction { transaction in
    let instrumented = InstrumentedTransaction(
        underlying: transaction,
        metrics: metrics
    )

    // Use normally
    let value = try await instrumented.getValue(for: key)
    instrumented.setValue(newValue, for: key)
}

// Check metrics
print("Read operations: \(metrics.readCount)")
print("Write operations: \(metrics.writeCount)")
print("Bytes read: \(metrics.bytesRead)")
print("Bytes written: \(metrics.bytesWritten)")
print("Duration: \(metrics.duration)ms")
```

### 13.3 Metrics Collection

```swift
// Auto-collect with MetricsTransactionListener
let listenerRegistry = TransactionListenerRegistry()
let metricsListener = MetricsTransactionListener()
listenerRegistry.add(metricsListener)

// Periodically get metrics
let aggregated = metricsListener.getAggregatedMetrics()
print("Total transactions: \(aggregated.totalCount)")
print("Success rate: \(aggregated.successRate)")
print("Avg duration: \(aggregated.avgDuration)ms")
print("P99 duration: \(aggregated.p99Duration)ms")
```

### 13.4 Log Integration

```swift
// Enable tracing with TransactionConfiguration
let config = TransactionConfiguration(
    tracing: .init(
        transactionID: "request-\(requestID)",
        logTransaction: true,  // Detailed logging
        serverRequestTracing: true,  // Server-side tracing
        tags: ["api-v2", "user-service"]
    )
)

try await database.withTransaction(configuration: config) { tx in
    // Logs include transactionID
}
```

---

## Index

### Type List

| Type | Category | Description |
|------|----------|-------------|
| `FDBContainer` | Core | Resource management |
| `FDBContext` | Core | CRUD operations |
| `Schema` | Core | Schema definition |
| `TransactionConfiguration` | Transaction | Configuration |
| `CommitCheck` | Transaction | Pre-commit validation |
| `PostCommit` | Transaction | Post-commit hooks |
| `OnlineIndexer` | Index | Online building |
| `AdaptiveThrottler` | Index | Throttling |
| `ItemStorage` | Serialization | Storage |
| `TransformingSerializer` | Serialization | Compression/Encryption |

### Index Types

| Index | Use Case |
|-------|----------|
| Scalar | Equality/Range queries |
| Vector | Semantic search |
| FullText | Full-text search |
| Spatial | Geospatial search |
| Graph | Graph/RDF |
| Rank | Ranking |
| Aggregation | Aggregations |
| Version | History |
| Bitmap | Set operations |
| Leaderboard | Time-windowed ranking |
| Permuted | Permutations |
| Relationship | Relations |

---

*This document is a comprehensive guide for database-framework. For detailed API reference, please refer to the source code of each module.*

# Database

A Swift package providing server-side database operations powered by FoundationDB. Database implements the index maintenance logic for index types defined in DatabaseKit.

## Overview

Database provides:

- **FDBContainer**: SwiftData-like container for database management
- **FDBContext**: Change tracking and batch operations
- **Index Maintainers**: Concrete implementations for all index types
- **Migration System**: Schema versioning and online index building

## Supported Platforms

| Platform | Support |
|----------|---------|
| macOS | 15.0+ |
| Linux | Swift 6.2+ |
| Windows | Swift 6.2+ |
| iOS/watchOS/tvOS/visionOS | Not supported (requires FoundationDB) |

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/anthropics/database.git", from: "1.0.0")
]
```

## Modules

| Module | Description |
|--------|-------------|
| `DatabaseEngine` | FDBContainer, FDBContext, IndexMaintainer protocol |
| `ScalarIndex` | VALUE index implementation |
| `VectorIndex` | HNSW and flat vector search |
| `FullTextIndex` | Inverted index for text search |
| `SpatialIndex` | S2/Geohash spatial indexing |
| `RankIndex` | Skip-list based rankings |
| `PermutedIndex` | Alternative field orderings |
| `GraphIndex` | Adjacency list traversal |
| `AggregationIndex` | COUNT, SUM, MIN, MAX, AVG |
| `VersionIndex` | Version history with versionstamps |
| `Database` | All-in-one re-export |

## Quick Start

```swift
import DatabaseEngine
import Core

// 1. Initialize FoundationDB (once at startup)
try await FDBClient.initialize()

// 2. Create container with schema
let schema = Schema([User.self])
let container = try FDBContainer(for: schema)

// 3. Use context for operations
let context = await container.mainContext

let user = User(email: "alice@example.com", name: "Alice")
context.insert(user)
try await context.save()
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
    #Index<SensorReading>([\.timestamp, \.sensorId],
                          type: TimeSeriesIndexKind(resolution: .second))

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

## Testing

```swift
import Testing
import DatabaseEngine

@Test func testCustomIndex() async throws {
    // Tests run against local FoundationDB
    try await FDBClient.initialize()

    let container = try FDBContainer(for: testSchema)
    let context = await container.mainContext

    // Test your index operations...
}
```

## Related Packages

- **[DatabaseKit](../database-kit)**: Platform-independent model and index type definitions

## License

MIT License

# Missing Features Design Document

## Overview

This document describes the design for three features missing from database-framework compared to FDB Record Layer:

1. **Union Record Type** - Store multiple types in single logical store
2. **Uniqueness Enforcement** - Detect and prevent duplicate values in unique indexes
3. **Weak Read Semantics** - Reuse cached read versions for improved throughput

---

## 1. Union Record Type

### Record Layer Reference

Record Layer uses Protobuf's `oneof` to store multiple record types in a single `RecordTypeUnion`:

```protobuf
message RecordTypeUnion {
  oneof record {
    User user = 1;
    Order order = 2;
    Product product = 3;
  }
}
```

### Design Goals

1. Store multiple `Persistable` types in a shared subspace
2. Query across types with type discrimination
3. Unified indexing across types
4. Preserve type safety at compile time

### Proposed Design

#### 1.1 UnionStore Protocol (database-kit)

```swift
/// Defines a union of Persistable types that share storage
public protocol UnionStore: Sendable {
    /// All types in the union
    static var memberTypes: [any Persistable.Type] { get }

    /// Shared directory path for all types
    static var directoryPathComponents: [any DirectoryPathElement] { get }

    /// Union name (used as subspace prefix)
    static var unionName: String { get }
}

/// Example usage:
struct AppUnion: UnionStore {
    static let memberTypes: [any Persistable.Type] = [User.self, Order.self, Product.self]
    static let directoryPathComponents = [Path("app"), Path("main")]
    static let unionName = "AppUnion"
}
```

#### 1.2 Storage Format

```
Key Structure:
[union_subspace]/R/[typeId:UInt16]/[primaryKey] → Protobuf data

Where typeId is a stable numeric identifier for each member type.
```

**Type Registry**:
```swift
/// Maps type names to stable numeric IDs
/// Stored in metadata subspace for evolution
[union_subspace]/_meta/types/[typeName] → UInt16 (typeId)
```

#### 1.3 FDBUnionContext (database-framework)

```swift
public final class FDBUnionContext<U: UnionStore>: Sendable {
    private let container: FDBContainer
    private let typeRegistry: TypeRegistry

    /// Insert a model of any union member type
    public func insert<T: Persistable>(_ model: T) where T: UnionMember<U>

    /// Fetch all items of a specific type
    public func fetch<T: Persistable>(_ type: T.Type) -> QueryExecutor<T>

    /// Fetch all items across all types (type-erased)
    public func fetchAll() async throws -> [any Persistable]

    /// Save all pending changes atomically
    public func save() async throws
}
```

#### 1.4 Cross-Type Indexing

```swift
/// Index that spans multiple types in a union
@UnionIndex<AppUnion>([\.createdAt], name: "created_at_idx")

// Key structure:
[union_subspace]/I/[indexName]/[values...]/[typeId:UInt16]/[primaryKey]
```

### Implementation Phases

1. **Phase 1**: Basic UnionStore with type registry and storage
2. **Phase 2**: Cross-type querying with type discrimination
3. **Phase 3**: Cross-type indexing
4. **Phase 4**: Schema evolution (add/remove types)

---

## 2. Uniqueness Enforcement

### Record Layer Reference

```java
// StandardIndexMaintainer.java
void checkUniqueness(Tuple valueKey, Tuple primaryKey) {
    // Scan for existing entries with same value
    Range range = TupleRange.allOf(valueKey).toRange();
    AsyncIterable<KeyValue> scan = transaction.getRange(range);

    for (KeyValue kv : scan) {
        Tuple existingPK = extractPrimaryKey(kv);
        if (!existingPK.equals(primaryKey)) {
            throw new RecordIndexUniquenessViolation(index, valueKey, existingPK, primaryKey);
        }
    }
}
```

### Design Goals

1. Detect duplicate values BEFORE transaction commit
2. Support both immediate rejection and violation tracking
3. Work with online indexing (write-only mode)
4. Clear error messages with conflicting record info

### Proposed Design

#### 2.1 UniquenessViolation Error (database-kit)

```swift
/// Error thrown when uniqueness constraint is violated
public struct UniquenessViolationError: Error, Sendable {
    /// Name of the violated index
    public let indexName: String

    /// Type of the conflicting records
    public let persistableType: String

    /// Value that caused the conflict
    public let conflictingValue: [any TupleElement]

    /// Primary key of existing record
    public let existingPrimaryKey: Tuple

    /// Primary key of new record attempting to insert
    public let newPrimaryKey: Tuple
}
```

#### 2.2 UniquenessViolationTracker (database-framework)

For write-only indexes during online indexing:

```swift
/// Tracks uniqueness violations for later resolution
public final class UniquenessViolationTracker: Sendable {
    /// Storage subspace for violations
    private let subspace: Subspace  // [index_subspace]/_violations/

    /// Record a violation (during write-only mode)
    public func recordViolation(
        _ violation: UniquenessViolation,
        transaction: any TransactionProtocol
    )

    /// Scan all violations for an index
    public func scanViolations(
        indexName: String,
        limit: Int? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [UniquenessViolation]

    /// Resolve a violation (after user fixes data)
    public func resolveViolation(
        indexName: String,
        valueKey: Tuple,
        transaction: any TransactionProtocol
    )
}

/// Violation record
public struct UniquenessViolation: Sendable, Codable {
    public let indexName: String
    public let valueKey: [UInt8]  // Packed tuple
    public let primaryKeys: [[UInt8]]  // All conflicting PKs
    public let detectedAt: Date
}

/// Storage format:
[index_subspace]/_violations/[valueKey] → UniquenessViolation (Protobuf)
```

#### 2.3 Integration with ScalarIndexMaintainer

```swift
extension ScalarIndexMaintainer {
    /// Check uniqueness before adding index entry
    public func checkUniqueness(
        valueKey: Tuple,
        excludingPrimaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Only check if index is unique
        guard index.isUnique else { return }

        // Build range for value prefix
        let valueSubspace = subspace.subspace(valueKey)
        let (begin, end) = valueSubspace.range()

        // Scan for existing entries
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: false,  // Need conflict detection
            streamingMode: .small  // Expect 0-1 results
        )

        for try await (key, _) in sequence {
            let existingPK = try extractPrimaryKey(from: key, subspace: valueSubspace)

            // Different PK = conflict
            if existingPK != excludingPrimaryKey {
                throw UniquenessViolationError(
                    indexName: index.name,
                    persistableType: Item.persistableType,
                    conflictingValue: valueKey.elements,
                    existingPrimaryKey: existingPK,
                    newPrimaryKey: excludingPrimaryKey
                )
            }
        }
    }
}
```

#### 2.4 Integration with FDBContext.updateIndexes()

```swift
private func updateIndexes(
    oldData: [UInt8]?,
    newModel: (any Persistable)?,
    id: Tuple,
    store: FDBDataStore,
    transaction: any TransactionProtocol
) async throws {
    // ... existing code ...

    for descriptor in indexDescriptors {
        let indexSubspace = store.indexSubspace.subspace(descriptor.name)

        // NEW: Check uniqueness BEFORE writing
        if descriptor.isUnique, let newModel = newModel {
            let newValues = extractIndexValues(from: newModel, keyPaths: descriptor.keyPaths)
            let valueTuple = Tuple(newValues)

            // Check index state
            let indexState = try await store.indexStateManager.state(of: descriptor.name)

            if indexState.isReadable {
                // Immediate check - throw on violation
                try await checkUniqueness(
                    indexSubspace: indexSubspace,
                    valueTuple: valueTuple,
                    excludingPrimaryKey: id,
                    transaction: transaction
                )
            } else if indexState == .writeOnly {
                // Track violation for later resolution
                try await trackPotentialViolation(
                    indexName: descriptor.name,
                    valueTuple: valueTuple,
                    primaryKey: id,
                    transaction: transaction
                )
            }
        }

        // ... existing index update code ...
    }
}
```

### Implementation Phases

1. **Phase 1**: Basic `checkUniqueness()` with immediate error
2. **Phase 2**: `UniquenessViolationTracker` for write-only indexes
3. **Phase 3**: `scanUniquenessViolations()` API
4. **Phase 4**: Automatic violation check after online indexing

---

## 3. Weak Read Semantics

### Record Layer Reference

```java
// FDBRecordContextConfig.java
WeakReadSemantics weakReadSemantics = new WeakReadSemantics(
    minVersion,           // Minimum acceptable version
    maxStalenessMillis,   // Maximum age of cached version
    useCache              // Whether to use cached version
);

// FDBDatabase.java
Long cachedReadVersion = lastSeenVersion.get();
if (weakReadSemantics != null && cachedReadVersion != null) {
    if (cachedReadVersion >= weakReadSemantics.getMinVersion()) {
        long staleness = System.currentTimeMillis() - lastSeenVersionTime;
        if (staleness <= weakReadSemantics.getMaxStalenessMillis()) {
            transaction.setReadVersion(cachedReadVersion);
        }
    }
}
```

### Design Goals

1. Cache read version from successful commits
2. Allow transactions to reuse cached version within bounds
3. Reduce `getReadVersion()` calls (network round-trip)
4. Configurable staleness tolerance

### Proposed Design

#### 3.1 WeakReadSemantics Configuration (database-kit)

```swift
/// Configuration for weak read semantics
public struct WeakReadSemantics: Sendable, Hashable {
    /// Minimum acceptable read version (0 = any)
    public let minVersion: Int64

    /// Maximum staleness in milliseconds (default: 5000 = 5 seconds)
    public let maxStalenessMillis: Int64

    /// Whether to use cached read version
    public let useCachedVersion: Bool

    /// Default: 5 second staleness, use cache
    public static let `default` = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 5000,
        useCachedVersion: true
    )

    /// Strict consistency: never use cache
    public static let strict = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 0,
        useCachedVersion: false
    )

    /// Relaxed: up to 30 second staleness
    public static let relaxed = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 30_000,
        useCachedVersion: true
    )

    public init(
        minVersion: Int64 = 0,
        maxStalenessMillis: Int64 = 5000,
        useCachedVersion: Bool = true
    ) {
        self.minVersion = minVersion
        self.maxStalenessMillis = maxStalenessMillis
        self.useCachedVersion = useCachedVersion
    }
}
```

#### 3.2 ReadVersionCache (database-framework)

```swift
/// Caches read versions from successful transactions
public final class ReadVersionCache: Sendable {
    private struct CachedVersion: Sendable {
        let version: Int64
        let timestamp: UInt64  // nanoseconds since boot (monotonic)
    }

    private let cache: Mutex<CachedVersion?>

    public init() {
        self.cache = Mutex(nil)
    }

    /// Update cache after successful commit
    public func updateFromCommit(version: Int64) {
        let now = DispatchTime.now().uptimeNanoseconds
        cache.withLock { $0 = CachedVersion(version: version, timestamp: now) }
    }

    /// Update cache after reading version
    public func updateFromRead(version: Int64) {
        let now = DispatchTime.now().uptimeNanoseconds
        cache.withLock { cached in
            // Only update if newer
            if cached == nil || version > cached!.version {
                cached = CachedVersion(version: version, timestamp: now)
            }
        }
    }

    /// Get cached version if valid according to semantics
    public func getCachedVersion(semantics: WeakReadSemantics) -> Int64? {
        guard semantics.useCachedVersion else { return nil }

        return cache.withLock { cached in
            guard let cached = cached else { return nil }

            // Check minimum version
            guard cached.version >= semantics.minVersion else { return nil }

            // Check staleness
            let now = DispatchTime.now().uptimeNanoseconds
            let ageNanos = now - cached.timestamp
            let ageMillis = Int64(ageNanos / 1_000_000)

            guard ageMillis <= semantics.maxStalenessMillis else { return nil }

            return cached.version
        }
    }

    /// Clear cache (useful for testing or after schema changes)
    public func clear() {
        cache.withLock { $0 = nil }
    }
}
```

#### 3.3 Integration with TransactionConfiguration

```swift
public struct TransactionConfiguration: Sendable, Hashable {
    // ... existing properties ...

    /// Weak read semantics (nil = strict consistency)
    public let weakReadSemantics: WeakReadSemantics?

    // Updated presets:

    /// Batch: Allow stale reads for background processing
    public static let batch = TransactionConfiguration(
        timeout: 30_000,
        retryLimit: 20,
        priority: .batch,
        readPriority: .low,
        weakReadSemantics: .relaxed  // NEW
    )

    /// Interactive: Strict consistency for user-facing operations
    public static let interactive = TransactionConfiguration(
        timeout: 1_000,
        retryLimit: 3,
        weakReadSemantics: nil  // Strict
    )
}
```

#### 3.4 Integration with DatabaseProtocol

```swift
extension DatabaseProtocol {
    /// Execute transaction with weak read semantics
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        readVersionCache: ReadVersionCache?,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await self.withTransaction { transaction in
            // Apply configuration
            try configuration.apply(to: transaction)

            // Apply cached read version if available
            if let cache = readVersionCache,
               let semantics = configuration.weakReadSemantics,
               let cachedVersion = cache.getCachedVersion(semantics: semantics) {
                try transaction.setReadVersion(cachedVersion)
            }

            // Execute operation
            let result = try await operation(transaction)

            // Update cache after success
            if let cache = readVersionCache {
                if let committedVersion = try? await transaction.getCommittedVersion() {
                    cache.updateFromCommit(version: committedVersion)
                } else if let readVersion = try? await transaction.getReadVersion() {
                    cache.updateFromRead(version: readVersion)
                }
            }

            return result
        }
    }
}
```

#### 3.5 Integration with FDBContainer

```swift
public final class FDBContainer: Sendable {
    // ... existing properties ...

    /// Shared read version cache for weak read semantics
    public let readVersionCache: ReadVersionCache

    public init(...) {
        // ...
        self.readVersionCache = ReadVersionCache()
    }
}
```

#### 3.6 Usage Example

```swift
let container = try await FDBContainer(schema: schema)
let context = container.newContext()

// Strict read (default) - always gets fresh data
let user = try await context.fetch(User.self)
    .where(\.id == userId)
    .first()

// Weak read - may return slightly stale data
let stats = try await context.fetch(Stats.self,
    weakReadSemantics: .relaxed)
    .where(\.category == category)
    .execute()
```

### Implementation Phases

1. **Phase 1**: `ReadVersionCache` implementation with tests
2. **Phase 2**: Integration with `TransactionConfiguration`
3. **Phase 3**: Integration with `FDBContainer` and `FDBContext`
4. **Phase 4**: Metrics for cache hit rate

---

## Summary: Priority and Dependencies

| Feature | Priority | Dependencies | Complexity |
|---------|----------|--------------|------------|
| Uniqueness Enforcement | High | None | Medium |
| Weak Read Semantics | Medium | None | Low |
| Union Record Type | Medium | Schema evolution | High |

### Recommended Implementation Order

1. **Uniqueness Enforcement** - Most critical for data integrity
2. **Weak Read Semantics** - Quick win for read performance
3. **Union Record Type** - Complex, needs schema evolution design first

---

## References

- FDB Record Layer: [OnlineIndexer.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java)
- FDB Record Layer: [StandardIndexMaintainer.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java)
- FDB Record Layer: [FDBRecordContextConfig.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordContextConfig.java)
- Graefe, G. "The Cascades Framework for Query Optimization", 1995

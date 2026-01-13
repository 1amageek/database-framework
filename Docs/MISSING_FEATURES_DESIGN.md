# Missing Features Design Document

## Overview

This document describes the design for three features missing from database-framework compared to FDB Record Layer:

1. **Union Record Type** - Store multiple types in single logical store
2. **Uniqueness Enforcement** - Detect and prevent duplicate values in unique indexes
3. **Cache Policy** - ✅ Implemented - Control cache behavior for read operations

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

## 3. Cache Policy (✅ Implemented)

### Status: Complete

This feature has been implemented as `CachePolicy` with a simpler, more intuitive API than FDB Record Layer's `WeakReadSemantics`.

### Record Layer Reference

```java
// FDBRecordContextConfig.java
WeakReadSemantics weakReadSemantics = new WeakReadSemantics(
    minVersion,           // Minimum acceptable version
    maxStalenessMillis,   // Maximum age of cached version
    useCache              // Whether to use cached version
);
```

### Implemented Design

#### 3.1 CachePolicy Enum

**File**: `Sources/DatabaseEngine/Transaction/CachePolicy.swift`

```swift
/// Cache policy for read operations
public enum CachePolicy: Sendable, Hashable {
    /// Fetch latest data from server (no cache)
    case server

    /// Use cached read version if available (no time limit)
    case cached

    /// Use cache only if younger than specified seconds
    case stale(TimeInterval)
}
```

**Design Rationale**:
- Simpler than Record Layer's 3-property `WeakReadSemantics`
- Clear, intuitive semantics for each case
- `.cached` uses cache without arbitrary time limits (user controls behavior explicitly)

#### 3.2 ReadVersionCache Integration

**File**: `Sources/DatabaseEngine/Transaction/ReadVersionCache.swift`

```swift
public final class ReadVersionCache: Sendable {
    /// Get cached version based on policy
    public func getCachedVersion(policy: CachePolicy) -> Int64? {
        switch policy {
        case .server:
            return nil  // Never use cache
        case .cached:
            return cache.withLock { $0?.version }  // Use if available
        case .stale(let seconds):
            return cache.withLock { cached in
                guard let cached = cached else { return nil }
                let ageSeconds = Double(now - cached.timestamp) / 1_000_000_000
                return ageSeconds <= seconds ? cached.version : nil
            }
        }
    }
}
```

#### 3.3 TransactionConfiguration Integration

**File**: `Sources/DatabaseEngine/Transaction/TransactionConfiguration.swift`

```swift
public struct TransactionConfiguration: Sendable, Hashable {
    /// Cache policy (default: .server for strict consistency)
    public let cachePolicy: CachePolicy

    // Presets:
    public static let `default` = TransactionConfiguration(cachePolicy: .server)
    public static let readOnly = TransactionConfiguration(cachePolicy: .cached)
    public static let longRunning = TransactionConfiguration(cachePolicy: .stale(60))
}
```

#### 3.4 Query API Integration

**File**: `Sources/DatabaseEngine/Fetch/FDBFetchDescriptor.swift`

```swift
// Query fluent API
let results = try await context.fetch(User.self)
    .cachePolicy(.cached)
    .where(\.isActive == true)
    .execute()

// QueryExecutor fluent API
let products = try await context.fetch(Product.self)
    .cachePolicy(.stale(30))
    .limit(100)
    .execute()
```

#### 3.5 Usage Examples

```swift
let context = container.newContext()

// Default: strict consistency (.server)
let user = try await context.fetch(User.self)
    .where(\.id == userId)
    .first()

// Dashboard query: use cached version
let stats = try await context.fetch(Stats.self)
    .cachePolicy(.cached)
    .execute()

// Analytics: allow up to 30-second stale data
let metrics = try await context.fetch(Metrics.self)
    .cachePolicy(.stale(30))
    .execute()
```

### Comparison with Record Layer

| Aspect | Record Layer | database-framework |
|--------|--------------|-------------------|
| Configuration | `WeakReadSemantics(minVersion, maxStaleness, useCache)` | `CachePolicy` enum |
| Complexity | 3 properties | 3 simple cases |
| Default behavior | Configurable | `.server` (strict) |
| Cache scope | Database-level | Context-level |
| API | Constructor parameters | Fluent `.cachePolicy()` |

### Implementation Complete

- ✅ `CachePolicy` enum
- ✅ `ReadVersionCache.getCachedVersion(policy:)`
- ✅ `TransactionConfiguration.cachePolicy`
- ✅ `Query.cachePolicy()` fluent method
- ✅ `QueryExecutor.cachePolicy()` fluent method
- ✅ `FDBContext.fetch()` integration
- ✅ Comprehensive tests in `CachePolicyTests.swift`

---

## Summary: Priority and Dependencies

| Feature | Priority | Dependencies | Status |
|---------|----------|--------------|--------|
| Uniqueness Enforcement | High | None | ⚠️ Partial |
| Cache Policy | Medium | None | ✅ Complete |
| Union Record Type | Medium | Schema evolution | ❌ Not Started |

### Recommended Implementation Order

1. **Uniqueness Enforcement** - Most critical for data integrity (partial implementation exists)
2. ~~Weak Read Semantics~~ → ✅ Implemented as **Cache Policy**
3. **Union Record Type** - Complex, needs schema evolution design first

---

## References

- FDB Record Layer: [OnlineIndexer.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/OnlineIndexer.java)
- FDB Record Layer: [StandardIndexMaintainer.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java)
- FDB Record Layer: [FDBRecordContextConfig.java](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordContextConfig.java)
- Graefe, G. "The Cascades Framework for Query Optimization", 1995

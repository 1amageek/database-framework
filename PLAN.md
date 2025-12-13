# Dynamic Directory (Partitioned Directory) Support Design

## Overview

Enable `#Directory` with `Field(\.tenantID)` to work correctly throughout the framework.

**Goal API**:
```swift
@Persistable
struct Order {
    #Directory<Order>("tenants", Field(\.tenantID), "orders", layer: .partition)

    var id: String = ULID().ulidString
    var tenantID: String = ""
    var status: String = ""
}

// Save: tenantID extracted from model automatically
context.insert(order)
try await context.save()

// Fetch: partition specified via .partition() - REQUIRED for dynamic directories
let orders = try await context.fetch(Order.self)
    .partition(\.tenantID, equals: "tenant_123")
    .where(\.status == "open")
    .execute()
```

## Current State

### What Works
- `FDBContainer.resolveDirectory(for:with:)` - resolves dynamic paths from instance
- `Field<Root>` struct in database-kit - holds `PartialKeyPath<Root>`
- Directory caching based on resolved path

### What Doesn't Work
1. **Save path**: `FDBContext.save()` → `container.store(for: type)` → `resolveDirectory(for: type)` → **ERROR** on Field
2. **Fetch path**: No `.partition()` API exists, no way to specify Field values for read
3. **Index operations**: Index subspaces not partition-aware

## Design

### 1. Type Analysis: Static vs Dynamic Directory

Add utility to detect if a type has dynamic directory:

```swift
// In FDBContainer or utility
extension Persistable {
    /// Returns true if directoryPathComponents contains any Field<Self>
    static var hasDynamicDirectory: Bool {
        directoryPathComponents.contains { $0 is Field<Self> }
    }

    /// Extract Field keyPaths from directoryPathComponents
    static var directoryFieldKeyPaths: [PartialKeyPath<Self>] {
        directoryPathComponents.compactMap { ($0 as? Field<Self>)?.value }
    }
}
```

### 2. PartitionBinding: Capture Field Values for Query

New type to hold partition field values:

```swift
// Sources/DatabaseEngine/Fetch/PartitionBinding.swift

/// Holds field values needed to resolve a partitioned directory
public struct PartitionBinding<T: Persistable>: Sendable {
    /// Field-value pairs for directory resolution
    internal var bindings: [(keyPath: PartialKeyPath<T>, value: any Sendable & TupleElement)] = []

    public init() {}

    /// Add a partition field binding
    public mutating func bind<V: Sendable & TupleElement>(_ keyPath: KeyPath<T, V>, to value: V) {
        bindings.append((keyPath, value))
    }

    /// Validate all required directory fields are bound
    public func validate() throws {
        let requiredKeyPaths = Set(T.directoryFieldKeyPaths.map { $0 as AnyKeyPath })
        let boundKeyPaths = Set(bindings.map { $0.keyPath as AnyKeyPath })

        let missing = requiredKeyPaths.subtracting(boundKeyPaths)
        guard missing.isEmpty else {
            let fieldNames = missing.map { T.fieldName(for: $0) }
            throw PartitionError.missingPartitionFields(fieldNames)
        }
    }

    /// Build path components by replacing Field placeholders with bound values
    internal func resolvePathComponents() -> [String] {
        var path: [String] = []
        for component in T.directoryPathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            } else if let fieldElement = component as? Field<T> {
                // Find binding for this field
                if let binding = bindings.first(where: { $0.keyPath == fieldElement.value }) {
                    path.append(String(describing: binding.value))
                }
            }
        }
        return path
    }
}

public enum PartitionError: Error, CustomStringConvertible {
    case missingPartitionFields([String])
    case partitionRequiredForDynamicDirectory(typeName: String, fields: [String])

    public var description: String {
        switch self {
        case .missingPartitionFields(let fields):
            return "Missing partition field bindings: \(fields.joined(separator: ", "))"
        case .partitionRequiredForDynamicDirectory(let typeName, let fields):
            return "Type '\(typeName)' has dynamic directory requiring partition fields: \(fields.joined(separator: ", ")). Use .partition() to specify values."
        }
    }
}
```

### 3. Query<T> Extension for Partition

Extend Query to hold partition binding:

```swift
// In FDBFetchDescriptor.swift

public struct Query<T: Persistable>: Sendable {
    // ... existing fields ...

    /// Partition binding for dynamic directories
    public var partitionBinding: PartitionBinding<T>?

    // ... existing init ...

    /// Bind a partition field value (for dynamic directories)
    public func partition<V: Sendable & TupleElement & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> Query<T> {
        var copy = self
        var binding = copy.partitionBinding ?? PartitionBinding<T>()
        binding.bind(keyPath, to: value)
        copy.partitionBinding = binding

        // Also add as a where clause for filtering (defense in depth)
        copy.predicates.append(.comparison(FieldComparison(keyPath: keyPath, op: .equal, value: value)))
        return copy
    }
}
```

### 4. QueryExecutor Extension

```swift
// In FDBFetchDescriptor.swift

extension QueryExecutor {
    /// Bind a partition field value (required for types with dynamic directories)
    ///
    /// **Usage**:
    /// ```swift
    /// let orders = try await context.fetch(Order.self)
    ///     .partition(\.tenantID, equals: "tenant_123")
    ///     .where(\.status == "open")
    ///     .execute()
    /// ```
    public func partition<V: Sendable & TupleElement & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.partition(keyPath, equals: value)
        return copy
    }
}
```

### 5. FDBContainer: Partition-Aware Directory Resolution

Add new method for query-based resolution:

```swift
// In FDBContainer.swift

/// Resolve directory using partition binding (for queries)
public func resolveDirectory<T: Persistable>(
    for type: T.Type,
    with binding: PartitionBinding<T>
) async throws -> Subspace {
    // Validate binding has all required fields
    try binding.validate()

    let path = binding.resolvePathComponents()
    let cacheKey = path.joined(separator: "/")

    if let cached = directoryCache.withLock({ $0[cacheKey] }) {
        return cached
    }

    let layer = convertDirectoryLayer(type.directoryLayer)
    let directoryLayer = DirectoryLayer(database: database)
    let dirSubspace = try await directoryLayer.createOrOpen(path: path, type: layer)
    let subspace = dirSubspace.subspace

    directoryCache.withLock { $0[cacheKey] = subspace }
    return subspace
}

/// Get DataStore for a type with partition binding
public func store<T: Persistable>(
    for type: T.Type,
    partition binding: PartitionBinding<T>
) async throws -> any DataStore {
    let subspace = try await resolveDirectory(for: type, with: binding)
    return FDBDataStore(
        database: database,
        subspace: subspace,
        schema: schema,
        securityDelegate: securityDelegate
    )
}
```

### 6. FDBContext.save() Modification

Modify save to handle dynamic directories:

```swift
// In FDBContext.swift - save() method

// Replace the current type-based grouping with partition-aware grouping
do {
    // Group models by (type, resolvedPath) for partition-aware batching
    struct StoreKey: Hashable {
        let typeName: String
        let resolvedPath: String  // Empty for static directories
    }

    var insertsByStore: [StoreKey: [any Persistable]] = [:]
    var deletesByStore: [StoreKey: [any Persistable]] = [:]

    for model in insertsSnapshot {
        let modelType = type(of: model)
        let typeName = modelType.persistableType

        // Check if type has dynamic directory
        let resolvedPath: String
        if hasDynamicDirectory(modelType) {
            // Build binding from model instance
            let binding = buildPartitionBinding(from: model)
            resolvedPath = binding.resolvePathComponents().joined(separator: "/")
        } else {
            resolvedPath = ""
        }

        let key = StoreKey(typeName: typeName, resolvedPath: resolvedPath)
        insertsByStore[key, default: []].append(model)
    }

    // Similar for deletes...

    // Pre-resolve stores for each (type, partition) combination
    var resolvedStores: [StoreKey: any DataStore] = [:]
    for key in Set(insertsByStore.keys).union(deletesByStore.keys) {
        let sampleModel = insertsByStore[key]?.first ?? deletesByStore[key]?.first
        guard let model = sampleModel else { continue }

        let modelType = type(of: model)
        let store: any DataStore

        if hasDynamicDirectory(modelType) {
            let binding = buildPartitionBinding(from: model)
            store = try await container.store(for: modelType, partition: binding)
        } else {
            store = try await container.store(for: modelType)
        }
        resolvedStores[key] = store
    }

    // ... rest of save logic using resolvedStores ...
}

// Helper: Check if type has dynamic directory
private func hasDynamicDirectory(_ type: any Persistable.Type) -> Bool {
    type.directoryPathComponents.contains { component in
        // Field is generic, check by excluding known static types
        !(component is Path) && !(component is String)
    }
}

// Helper: Build partition binding from model instance
private func buildPartitionBinding<T: Persistable>(from model: T) -> PartitionBinding<T> {
    var binding = PartitionBinding<T>()
    for component in T.directoryPathComponents {
        if let fieldElement = component as? Field<T> {
            let keyPath = fieldElement.value
            if let value = model[keyPath: keyPath] as? (any Sendable & TupleElement) {
                binding.bindings.append((keyPath, value))
            }
        }
    }
    return binding
}
```

### 7. FDBContext.fetch() Modification

Modify internal fetch to use partition binding:

```swift
// In FDBContext.swift

internal func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
    // Check if type requires partition
    if hasDynamicDirectory(T.self) {
        guard let binding = query.partitionBinding else {
            let fieldNames = T.directoryFieldKeyPaths.map { T.fieldName(for: $0) }
            throw PartitionError.partitionRequiredForDynamicDirectory(
                typeName: T.persistableType,
                fields: fieldNames
            )
        }
        try binding.validate()
    }

    let (pendingInserts, pendingDeleteKeys) = stateLock.withLock { ... }

    // Get store with partition if needed
    let store: any DataStore
    if let binding = query.partitionBinding {
        store = try await container.store(for: T.self, partition: binding)
    } else {
        store = try await container.store(for: T.self)
    }

    var results = try await store.fetch(query)

    // ... rest of pending insert/delete handling ...
}
```

### 8. CursorQueryBuilder Extension

```swift
// In QueryCursor.swift

extension CursorQueryBuilder {
    /// Bind a partition field value (required for types with dynamic directories)
    public func partition<V: Sendable & TupleElement & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.partition(keyPath, equals: value)
        return copy
    }
}
```

### 9. IndexQueryContext Update

Update IndexQueryContext to support partitioned subspaces:

```swift
// In IndexQueryContext.swift

public struct IndexQueryContext: Sendable {
    public let context: FDBContext

    /// Partition binding for dynamic directories (optional)
    public var partitionBinding: (any Sendable)?  // Type-erased PartitionBinding

    /// Get index subspace for a type
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let baseSubspace: Subspace

        if let binding = partitionBinding as? PartitionBinding<T> {
            baseSubspace = try await context.container.resolveDirectory(for: type, with: binding)
        } else {
            baseSubspace = try await context.container.resolveDirectory(for: type)
        }

        return baseSubspace.subspace(SubspaceKey.indexes)
    }

    /// Get item subspace for a type
    public func itemSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let baseSubspace: Subspace

        if let binding = partitionBinding as? PartitionBinding<T> {
            baseSubspace = try await context.container.resolveDirectory(for: type, with: binding)
        } else {
            baseSubspace = try await context.container.resolveDirectory(for: type)
        }

        return baseSubspace.subspace(SubspaceKey.items)
    }
}
```

## Data Flow Summary

### Save Flow (with Dynamic Directory)

```
context.insert(order)  // order.tenantID = "tenant_123"
    ↓
context.save()
    ↓
Group by (type, resolvedPath)
    ↓
buildPartitionBinding(from: order)
    → PartitionBinding { \.tenantID: "tenant_123" }
    ↓
container.store(for: Order.self, partition: binding)
    ↓
resolveDirectory(for: Order.self, with: binding)
    → Path: ["tenants", "tenant_123", "orders"]
    → Subspace: /tenants/tenant_123/orders
    ↓
FDBDataStore executes in resolved subspace
```

### Fetch Flow (with Dynamic Directory)

```
context.fetch(Order.self)
    .partition(\.tenantID, equals: "tenant_123")
    .where(\.status == "open")
    .execute()
    ↓
QueryExecutor.execute()
    ↓
context.fetch(query)
    ↓
Validate: hasDynamicDirectory → partitionBinding required ✓
    ↓
container.store(for: Order.self, partition: query.partitionBinding)
    ↓
resolveDirectory(for: Order.self, with: binding)
    → Path: ["tenants", "tenant_123", "orders"]
    ↓
store.fetch(query) within partition subspace
```

## Edge Cases & Validation

### 1. Missing Partition on Fetch
```swift
// ERROR: Type has dynamic directory but no partition specified
try await context.fetch(Order.self).execute()
// Throws: PartitionError.partitionRequiredForDynamicDirectory
```

### 2. Incomplete Partition Binding
```swift
#Directory<Message>("tenants", Field(\.tenantID), "channels", Field(\.channelID), "messages")

// ERROR: Missing channelID
try await context.fetch(Message.self)
    .partition(\.tenantID, equals: "t1")
    .execute()
// Throws: PartitionError.missingPartitionFields(["channelID"])
```

### 3. Static Directory (No Change)
```swift
#Directory<User>("app", "users")

// Works as before - no partition needed
try await context.fetch(User.self).execute()
```

### 4. deleteAll / enumerate on Dynamic Directory
```swift
// Should require partition or be explicitly disallowed
try await context.deleteAll(Order.self)
// Throws: PartitionError.partitionRequiredForDynamicDirectory

// With partition
try await context.deleteAll(Order.self, partition: \.tenantID, equals: "tenant_123")
```

## Files to Modify

1. **New Files**:
   - `Sources/DatabaseEngine/Fetch/PartitionBinding.swift`

2. **Modified Files**:
   - `Sources/DatabaseEngine/Fetch/FDBFetchDescriptor.swift` - Add partition to Query, QueryExecutor
   - `Sources/DatabaseEngine/Core/FDBContainer.swift` - Add partition-aware resolution
   - `Sources/DatabaseEngine/Core/FDBContext.swift` - Modify save() and fetch()
   - `Sources/DatabaseEngine/Cursor/QueryCursor.swift` - Add partition to CursorQueryBuilder
   - `Sources/DatabaseEngine/QueryPlanner/IndexQueryContext.swift` - Partition support

3. **Tests**:
   - `Tests/DatabaseEngineTests/PartitionedDirectoryTests.swift` - New test file

## Implementation Order

1. Create `PartitionBinding` and `PartitionError`
2. Extend `Query<T>` with partition support
3. Extend `QueryExecutor` and `CursorQueryBuilder`
4. Add `FDBContainer.resolveDirectory(for:with:)` for binding
5. Modify `FDBContext.save()` for partition-aware grouping
6. Modify `FDBContext.fetch()` to require partition for dynamic types
7. Update `IndexQueryContext` for partition support
8. Add validation for operations that scan all data (deleteAll, enumerate)
9. Write comprehensive tests

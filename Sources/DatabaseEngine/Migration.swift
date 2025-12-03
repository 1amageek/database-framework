import Foundation
import FoundationDB
import Core

/// Migration Definition
///
/// Defines a schema migration from one version to another.
/// Migrations are applied automatically to evolve the schema and data over time.
///
/// **Migration Types**:
/// 1. **Index Migration**: Add/remove/rebuild indexes
/// 2. **Data Migration**: Transform item data
/// 3. **Schema Migration**: Change field types or constraints
///
/// **Example**:
/// ```swift
/// let migration = Migration(
///     fromVersion: Schema.Version(1, 0, 0),
///     toVersion: Schema.Version(2, 0, 0),
///     description: "Add email index"
/// ) { context in
///     // Add new index using KeyPath
///     let emailIndex = IndexDescriptor(
///         name: "email_index",
///         keyPaths: [\User.email],
///         kind: ScalarIndexKind(),
///         commonOptions: .init()
///     )
///     try await context.addIndex(emailIndex)
/// }
/// ```
public struct Migration: Sendable {
    // MARK: - Properties

    /// Source schema version
    public let fromVersion: Schema.Version

    /// Target schema version
    public let toVersion: Schema.Version

    /// Human-readable description of this migration
    public let description: String

    /// Migration execution function
    public let execute: @Sendable (MigrationContext) async throws -> Void

    // MARK: - Initialization

    /// Initialize a migration
    ///
    /// - Parameters:
    ///   - fromVersion: Source schema version
    ///   - toVersion: Target schema version
    ///   - description: Description of the migration
    ///   - execute: Migration execution closure
    public init(
        fromVersion: Schema.Version,
        toVersion: Schema.Version,
        description: String,
        execute: @escaping @Sendable (MigrationContext) async throws -> Void
    ) {
        self.fromVersion = fromVersion
        self.toVersion = toVersion
        self.description = description
        self.execute = execute
    }
}

// MARK: - Migration Store Info

/// Subspace information for a store during migrations
///
/// This is a lightweight struct that holds only the subspace information
/// needed during migrations, without requiring the full typed FDBStore.
public struct MigrationStoreInfo: Sendable {
    /// Root subspace for the store
    public let subspace: Subspace

    /// Index subspace for the store
    public let indexSubspace: Subspace

    public init(subspace: Subspace, indexSubspace: Subspace) {
        self.subspace = subspace
        self.indexSubspace = indexSubspace
    }
}

// MARK: - Migration Context

/// Context provided to migrations during execution
///
/// Provides access to database operations and migration utilities.
public struct MigrationContext: Sendable {
    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) public let database: any DatabaseProtocol

    /// Schema being migrated to
    public let schema: Schema

    /// Metadata subspace for storing migration progress
    public let metadataSubspace: Subspace

    /// Store info registry
    ///
    /// Maps item type names to their store information.
    private let storeRegistry: [String: MigrationStoreInfo]

    /// Index configurations from FDBContainer
    ///
    /// Maps index names to their runtime configurations (HNSW params, full-text settings, etc.)
    /// Used when building indexes via EntityIndexBuilder.
    internal let indexConfigurations: [String: [any IndexConfiguration]]

    // MARK: - Initialization

    internal init(
        database: any DatabaseProtocol,
        schema: Schema,
        metadataSubspace: Subspace,
        storeRegistry: [String: MigrationStoreInfo],
        indexConfigurations: [String: [any IndexConfiguration]] = [:]
    ) {
        self.database = database
        self.schema = schema
        self.metadataSubspace = metadataSubspace
        self.storeRegistry = storeRegistry
        self.indexConfigurations = indexConfigurations
    }

    // MARK: - Store Access

    /// Get store info for an item type
    ///
    /// - Parameter itemType: Item type name
    /// - Returns: MigrationStoreInfo
    /// - Throws: Error if store not found
    public func storeInfo(for itemType: String) throws -> MigrationStoreInfo {
        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry. " +
                "Available stores: \(storeRegistry.keys.sorted().joined(separator: ", "))"
            )
        }
        return info
    }

    // MARK: - Index Operations

    /// Add a new index and build it online
    ///
    /// **Important Constraint**:
    /// - Index names **must be unique across all entities** in the schema
    /// - This allows `identifyTargetEntity()` to unambiguously match an index to its owner
    /// - If multiple entities have indexes with the same name, an error is thrown
    ///
    /// **Implementation**:
    /// 1. Identify target entity from index name or keyPaths
    /// 2. Convert IndexDescriptor to Index with proper itemTypes
    /// 3. Register index with IndexManager for target entity store only
    /// 4. Enable index (sets to writeOnly via IndexStateManager)
    /// 5. Build index (via OnlineIndexer using EntityIndexBuilder)
    /// 6. Mark as readable (automatically done by OnlineIndexer after build completes)
    ///
    /// - Parameter indexDescriptor: The index descriptor to add
    /// - Parameter batchSize: Number of items to process per batch (default: 100)
    /// - Throws: Error if index addition fails or target entity cannot be determined
    public func addIndex(_ indexDescriptor: IndexDescriptor, batchSize: Int = 100) async throws {
        // 1. Identify target entity from Schema
        let targetEntity = try identifyTargetEntity(for: indexDescriptor)

        // 2. Get store info for target entity
        guard let info = storeRegistry[targetEntity.name] else {
            throw FDBRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in registry. " +
                "Available stores: \(storeRegistry.keys.sorted().joined(separator: ", "))"
            )
        }

        let indexManager = IndexManager(
            database: database,
            subspace: info.indexSubspace
        )

        // 3. Convert IndexDescriptor to Index with itemTypes
        let index = try convertDescriptorToIndex(
            indexDescriptor,
            entity: targetEntity,
            itemTypes: Set([targetEntity.name])
        )

        // 4. Register index (in-memory, fails if already registered)
        do {
            try indexManager.register(index: index)
        } catch IndexManagerError.duplicateIndex {
            // Index already registered in this IndexManager instance - OK
            // This can happen if migration is run multiple times
        }

        // 5. Enable index (disabled → writeOnly)
        // Check current state first to ensure idempotency
        let currentState = try await indexManager.state(of: index.name)

        switch currentState {
        case .disabled:
            // Normal case: enable the index
            try await indexManager.enable(index.name)
        case .readable:
            // Index already built - nothing to do
            return
        case .writeOnly:
            // Index enabled but not built - continue to build
            break
        }

        // 6. Build index via OnlineIndexer using EntityIndexBuilder
        //
        // Uses the persistableType stored in Schema.Entity directly,
        // avoiding the need for a separate registration step.
        // The EntityIndexBuilder.buildIndex(forPersistableType:) method
        // uses the _EntityIndexBuildable protocol to dispatch to the
        // concrete type's buildEntityIndex implementation.
        let itemSubspace = info.subspace.subspace(SubspaceKey.items)

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = indexConfigurations[index.name] ?? []

        do {
            // Use the persistableType directly from Entity
            try await EntityIndexBuilder.buildIndex(
                forPersistableType: targetEntity.persistableType,
                database: database,
                itemSubspace: itemSubspace,
                indexSubspace: info.indexSubspace,
                index: index,
                indexStateManager: indexManager.stateManager,
                batchSize: batchSize,
                configurations: configs
            )
        } catch let error as EntityIndexBuilderError {
            // Re-throw with more context
            switch error {
            case .typeNotBuildable(let typeName, let reason):
                throw FDBRuntimeError.internalError(
                    "Cannot build index for entity '\(targetEntity.name)' (type: \(typeName)): \(reason). " +
                    "Ensure the type was created from a Persistable & Codable type, not manually."
                )
            default:
                throw error
            }
        }
    }

    /// Remove an index and add FormerIndex entry
    ///
    /// **Implementation** (all in single atomic transaction):
    /// 1. Identify target entity from Schema
    /// 2. Create FormerIndex metadata entry
    /// 3. Disable index (via IndexStateManager)
    /// 4. Clear all index data (range clear)
    ///
    /// - Parameters:
    ///   - indexName: Name of the index to remove
    ///   - addedVersion: Version when index was originally added
    /// - Throws: Error if index removal fails or index not found in schema
    public func removeIndex(
        indexName: String,
        addedVersion: Schema.Version
    ) async throws {
        // 1. Find index descriptor in schema to identify target entity
        guard let indexDescriptor = schema.indexDescriptor(named: indexName) else {
            throw FDBRuntimeError.indexNotFound(
                "Index '\(indexName)' not found in schema. Cannot determine target entity."
            )
        }

        // 2. Identify target entity
        let targetEntity = try identifyTargetEntity(for: indexDescriptor)

        // 3. Get store info for target entity
        guard let info = storeRegistry[targetEntity.name] else {
            throw FDBRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in registry"
            )
        }

        let indexManager = IndexManager(
            database: database,
            subspace: info.indexSubspace
        )

        // 4. Atomic transaction: FormerIndex entry + disable + clear data
        let formerIndexKey = info.subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple(indexName))

        let indexRange = info.indexSubspace.subspace(indexName).range()

        try await database.withTransaction(configuration: .batch) { transaction in
            // Write FormerIndex entry
            let timestamp = Date().timeIntervalSince1970
            transaction.setValue(
                Tuple(
                    Int64(addedVersion.major),
                    Int64(addedVersion.minor),
                    Int64(addedVersion.patch),
                    timestamp
                ).pack(),
                for: formerIndexKey
            )

            // Disable index state
            try await indexManager.stateManager.disable(indexName, transaction: transaction)

            // Clear index data
            transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
        }
    }

    /// Rebuild an existing index
    ///
    /// **Implementation**:
    /// 1. Identify target entity from Schema
    /// 2. Convert and register index
    /// 3. Atomic transaction: disable + clear + enable (→ writeOnly state)
    /// 4. Build index (via OnlineIndexer using EntityIndexBuilder)
    /// 5. Mark as readable (automatically done by OnlineIndexer after build completes)
    ///
    /// - Parameter indexName: Name of the index to rebuild
    /// - Parameter batchSize: Number of items to process per batch (default: 100)
    /// - Throws: Error if rebuild fails or index not found in schema
    public func rebuildIndex(indexName: String, batchSize: Int = 100) async throws {
        // 1. Find index descriptor in schema
        guard let indexDescriptor = schema.indexDescriptor(named: indexName) else {
            throw FDBRuntimeError.indexNotFound(
                "Index '\(indexName)' not found in schema"
            )
        }

        // 2. Identify target entity
        let targetEntity = try identifyTargetEntity(for: indexDescriptor)

        // 3. Get store info for target entity
        guard let info = storeRegistry[targetEntity.name] else {
            throw FDBRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in registry"
            )
        }

        let indexManager = IndexManager(
            database: database,
            subspace: info.indexSubspace
        )

        // 4. Convert and register index first (needed for IndexManager operations)
        let index = try convertDescriptorToIndex(
            indexDescriptor,
            entity: targetEntity,
            itemTypes: Set([targetEntity.name])
        )
        do {
            try indexManager.register(index: index)
        } catch IndexManagerError.duplicateIndex {
            // Index already registered - OK
        }

        // 5. Atomic transaction: disable + clear + enable
        // This ensures the index is in a consistent state before building
        let indexRange = info.indexSubspace.subspace(indexName).range()

        try await database.withTransaction(configuration: .batch) { transaction in
            // Disable index (from any state)
            try await indexManager.stateManager.disable(indexName, transaction: transaction)

            // Clear existing data
            transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )

            // Enable index (disabled → writeOnly)
            // Note: We just disabled it above, so this will succeed
            try await indexManager.stateManager.enable(indexName, transaction: transaction)
        }

        // 6. Build index via OnlineIndexer using EntityIndexBuilder
        let itemSubspace = info.subspace.subspace(SubspaceKey.items)

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = indexConfigurations[indexName] ?? []

        do {
            try await EntityIndexBuilder.buildIndex(
                entityName: targetEntity.name,
                database: database,
                itemSubspace: itemSubspace,
                indexSubspace: info.indexSubspace,
                index: index,
                indexStateManager: indexManager.stateManager,
                batchSize: batchSize,
                configurations: configs
            )
        } catch EntityIndexBuilderError.entityNotRegistered {
            throw FDBRuntimeError.internalError(
                "Cannot rebuild index for entity '\(targetEntity.name)': " +
                "Entity not registered in IndexBuilderRegistry. " +
                "Ensure FDBContainer is created with Schema([YourType.self, ...]) " +
                "or manually register using IndexBuilderRegistry.shared.register(YourType.self)"
            )
        }
    }

    // MARK: - Utility

    /// Execute arbitrary database operation
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (default: `.default`)
    ///   - operation: Operation to execute
    /// - Returns: Operation result
    /// - Throws: Any error from the operation
    public func executeOperation<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @escaping @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        return try await database.withTransaction(configuration: configuration) { transaction in
            try await operation(transaction)
        }
    }

    // MARK: - Batch Data Operations (FDB Extensions)

    /// Enumerate all items of a Persistable type with batch processing
    ///
    /// This method iterates through all items in batches, with each batch
    /// processed in a separate transaction to respect FDB's 5-second limit.
    ///
    /// **Usage**:
    /// ```swift
    /// let migration = Migration(...) { context in
    ///     for try await user in context.enumerate(User.self) {
    ///         // Process each user
    ///         if user.needsUpdate {
    ///             var updated = user
    ///             updated.status = .migrated
    ///             try await context.update(updated)
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to enumerate
    ///   - batchSize: Number of items to fetch per batch (default: 1000)
    /// - Returns: AsyncThrowingStream of items
    public func enumerate<T: Persistable>(
        _ type: T.Type,
        batchSize: Int = 1000
    ) -> AsyncThrowingStream<T, Error> {
        // Capture self properties for async enumeration
        let enumerator = ItemEnumerator<T>(
            itemType: T.persistableType,
            storeRegistry: self.storeRegistry,
            database: self.database,
            batchSize: batchSize
        )
        return enumerator.makeStream()
    }

    /// Update a single item during migration
    ///
    /// Updates the item in a single transaction. For bulk updates,
    /// consider using `batchUpdate()` instead.
    ///
    /// - Parameter item: The item to update
    /// - Throws: Error if update fails
    public func update<T: Persistable>(_ item: T) async throws {
        let itemType = T.persistableType

        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry"
            )
        }

        let encoder = ProtobufEncoder()
        let data = try encoder.encode(item)
        let validatedID = try item.validateIDForStorage()
        let itemKey = info.subspace.subspace(SubspaceKey.items).subspace(itemType).pack(Tuple(validatedID))

        try await database.withTransaction(configuration: .system) { transaction in
            transaction.setValue(Array(data), for: itemKey)
        }
    }

    /// Delete a single item during migration
    ///
    /// Deletes the item in a single transaction. For bulk deletes,
    /// consider using `batchDelete()` instead.
    ///
    /// - Parameter item: The item to delete
    /// - Throws: Error if delete fails
    public func delete<T: Persistable>(_ item: T) async throws {
        let itemType = T.persistableType

        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry"
            )
        }

        let validatedID = try item.validateIDForStorage()
        let itemKey = info.subspace.subspace(SubspaceKey.items).subspace(itemType).pack(Tuple(validatedID))

        try await database.withTransaction(configuration: .system) { transaction in
            transaction.clear(key: itemKey)
        }
    }

    /// Batch update multiple items
    ///
    /// Updates items in batches, with each batch processed in a separate
    /// transaction to respect FDB's transaction limits.
    ///
    /// - Parameters:
    ///   - items: Items to update
    ///   - batchSize: Number of items per transaction (default: 100)
    /// - Throws: Error if any batch fails
    public func batchUpdate<T: Persistable>(_ items: [T], batchSize: Int = 100) async throws {
        let itemType = T.persistableType

        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry"
            )
        }

        let itemSubspace = info.subspace.subspace(SubspaceKey.items).subspace(itemType)

        // Process in batches
        for batchStart in stride(from: 0, to: items.count, by: batchSize) {
            let batchEnd = min(batchStart + batchSize, items.count)
            let batch = Array(items[batchStart..<batchEnd])

            try await database.withTransaction(configuration: .batch) { transaction in
                let encoder = ProtobufEncoder()
                for item in batch {
                    let data = try encoder.encode(item)
                    let validatedID = try item.validateIDForStorage()
                    let itemKey = itemSubspace.pack(Tuple(validatedID))
                    transaction.setValue(Array(data), for: itemKey)
                }
            }
        }
    }

    /// Batch delete multiple items
    ///
    /// Deletes items in batches, with each batch processed in a separate
    /// transaction to respect FDB's transaction limits.
    ///
    /// - Parameters:
    ///   - items: Items to delete
    ///   - batchSize: Number of items per transaction (default: 100)
    /// - Throws: Error if any batch fails
    public func batchDelete<T: Persistable>(_ items: [T], batchSize: Int = 100) async throws {
        let itemType = T.persistableType

        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry"
            )
        }

        let itemSubspace = info.subspace.subspace(SubspaceKey.items).subspace(itemType)

        // Process in batches
        for batchStart in stride(from: 0, to: items.count, by: batchSize) {
            let batchEnd = min(batchStart + batchSize, items.count)
            let batch = items[batchStart..<batchEnd]

            try await database.withTransaction(configuration: .batch) { transaction in
                for item in batch {
                    let validatedID = try item.validateIDForStorage()
                    let itemKey = itemSubspace.pack(Tuple(validatedID))
                    transaction.clear(key: itemKey)
                }
            }
        }
    }

    /// Count items of a Persistable type
    ///
    /// - Parameters:
    ///   - type: The Persistable type to count
    ///   - approximate: If true, uses FDB's `getEstimatedRangeSizeBytes` for O(1) estimation.
    ///                  Faster but less accurate for large datasets. Default: false (exact count)
    ///   - avgRowSizeBytes: Estimated average row size for approximate counting (default: 500 bytes)
    /// - Returns: Number of items (exact or estimated)
    /// - Throws: Error if count fails
    public func count<T: Persistable>(
        _ type: T.Type,
        approximate: Bool = false,
        avgRowSizeBytes: Int = 500
    ) async throws -> Int {
        let itemType = T.persistableType

        guard let info = storeRegistry[itemType] else {
            throw FDBRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in registry"
            )
        }

        let itemPrefix = info.subspace.subspace(SubspaceKey.items).subspace(itemType)
        let (beginKey, endKey) = itemPrefix.range()

        // Use approximate count for large datasets
        if approximate {
            let sizeBytes = try await database.withTransaction(configuration: .readOnly) { transaction in
                try await transaction.getEstimatedRangeSizeBytes(
                    beginKey: beginKey,
                    endKey: endKey
                )
            }
            let rowSize = max(1, avgRowSizeBytes)
            return max(0, sizeBytes / rowSize)
        }

        // Exact count via full scan
        var totalCount = 0
        var lastKey: FDB.Bytes? = nil
        let batchSize = 10000  // Use large batches for counting

        while true {
            let currentLastKey = lastKey
            let (batchCount, newLastKey): (Int, FDB.Bytes?) = try await database.withTransaction(configuration: .readOnly) { transaction in
                let rangeBegin = currentLastKey.map { FDB.Bytes($0.dropFirst(0)) + [0x00] } ?? beginKey

                var count = 0
                var lastKeyInBatch: FDB.Bytes? = nil

                let sequence = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(rangeBegin),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in sequence {
                    count += 1
                    lastKeyInBatch = key
                    if count >= batchSize {
                        break
                    }
                }

                return (count, lastKeyInBatch)
            }

            // Update lastKey outside transaction
            if let key = newLastKey {
                lastKey = key
            }

            totalCount += batchCount

            if batchCount < batchSize {
                break
            }
        }

        return totalCount
    }

    // MARK: - Private Helpers

    /// Identify target entity for an index descriptor
    ///
    /// This method matches an IndexDescriptor to its corresponding entity in the Schema
    /// by checking which entity contains this index in its indexDescriptors.
    ///
    /// - Parameter descriptor: IndexDescriptor to match
    /// - Returns: Target entity
    /// - Throws: Error if no entity owns this index or multiple entities claim it
    private func identifyTargetEntity(for descriptor: IndexDescriptor) throws -> Schema.Entity {
        var matchingEntities: [Schema.Entity] = []

        for entity in schema.entities {
            // Check if this entity contains the descriptor
            if entity.indexDescriptors.contains(where: { $0.name == descriptor.name }) {
                matchingEntities.append(entity)
            }
        }

        guard !matchingEntities.isEmpty else {
            throw FDBRuntimeError.indexNotFound(
                "Index '\(descriptor.name)' is not associated with any entity in schema. " +
                "Available entities: \(schema.entities.map { $0.name }.joined(separator: ", "))"
            )
        }

        guard matchingEntities.count == 1 else {
            throw FDBRuntimeError.internalError(
                "Index '\(descriptor.name)' is associated with multiple entities: " +
                "\(matchingEntities.map { $0.name }.joined(separator: ", ")). " +
                "Index names must be unique across all entities."
            )
        }

        return matchingEntities[0]
    }

    /// Convert IndexDescriptor to Index with itemTypes
    ///
    /// This converts metadata-only IndexDescriptor to runtime Index objects.
    ///
    /// **Nested Field Support**:
    /// Nested keyPaths (e.g., "address.city") are converted to `NestExpression`.
    /// Uses `KeyExpressionFactory.from(keyPaths:)` to properly handle both
    /// simple fields and nested paths.
    ///
    /// **KeyPath Optimization**:
    /// Preserves original KeyPaths in Index for direct KeyPath-based field extraction.
    /// IndexMaintainer can use `index.keyPaths` for efficient direct subscript access
    /// instead of string-based `@dynamicMemberLookup` lookup.
    ///
    /// - Parameters:
    ///   - descriptor: IndexDescriptor from schema
    ///   - entity: The entity containing the Persistable type for KeyPath → String conversion
    ///   - itemTypes: Set of item type names that this index applies to
    /// - Returns: Index object
    /// - Throws: Error if conversion fails
    private func convertDescriptorToIndex(
        _ descriptor: IndexDescriptor,
        entity: Schema.Entity,
        itemTypes: Set<String>
    ) throws -> Index {
        // Convert AnyKeyPaths to field name strings using the entity's Persistable type
        // (for backward compatibility with KeyExpression-based code)
        let fieldNames = descriptor.keyPaths.map { keyPath in
            entity.persistableType.fieldName(for: keyPath)
        }

        // Build KeyExpression from field names using factory
        // This properly handles nested paths (e.g., "address.city" → NestExpression)
        let keyExpression = KeyExpressionFactory.from(keyPaths: fieldNames)

        // Create Index with both KeyExpression and original KeyPaths
        // KeyPaths enable direct subscript access optimization in IndexMaintainer
        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: keyExpression,
            keyPaths: descriptor.keyPaths,  // Preserve for direct KeyPath extraction
            subspaceKey: descriptor.name,
            itemTypes: itemTypes  // Scoped to specific entity
        )
    }
}

// MARK: - Migration Extensions

extension Migration: Identifiable {
    public var id: String {
        return "\(fromVersion)-\(toVersion)"
    }
}

// MARK: - FDBRuntimeError

/// FDBRuntime error types
public enum FDBRuntimeError: Error, CustomStringConvertible {
    /// Invalid argument
    case invalidArgument(String)

    /// Index not found
    case indexNotFound(String)

    /// Internal error
    case internalError(String)

    public var description: String {
        switch self {
        case .invalidArgument(let message):
            return "Invalid argument: \(message)"
        case .indexNotFound(let message):
            return "Index not found: \(message)"
        case .internalError(let message):
            return "Internal error: \(message)"
        }
    }
}

// MARK: - Sendable Database Wrapper

/// Wrapper to allow DatabaseProtocol to be captured in @Sendable closures
///
/// This is safe because FDB's database connection is thread-safe internally.
@usableFromInline
struct SendableDatabase: @unchecked Sendable {
    @usableFromInline
    let underlying: any DatabaseProtocol

    @usableFromInline
    init(_ database: any DatabaseProtocol) {
        self.underlying = database
    }
}

// MARK: - ItemEnumerator

/// Internal helper for enumerating items with batch processing
///
/// This struct encapsulates all the state needed for async enumeration
/// in a Sendable-safe way.
private struct ItemEnumerator<T: Persistable>: Sendable {
    let itemType: String
    let storeRegistry: [String: MigrationStoreInfo]
    let database: SendableDatabase
    let batchSize: Int

    init(itemType: String, storeRegistry: [String: MigrationStoreInfo], database: any DatabaseProtocol, batchSize: Int) {
        self.itemType = itemType
        self.storeRegistry = storeRegistry
        self.database = SendableDatabase(database)
        self.batchSize = batchSize
    }

    func makeStream() -> AsyncThrowingStream<T, Error> {
        // Capture properties in local variables for sendability
        let itemType = self.itemType
        let storeRegistry = self.storeRegistry
        let database = self.database
        let batchSize = self.batchSize

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    // Get store info for this type
                    guard let info = storeRegistry[itemType] else {
                        continuation.finish(throwing: FDBRuntimeError.invalidArgument(
                            "Store info for '\(itemType)' not found in registry"
                        ))
                        return
                    }

                    // Build prefix for item scanning
                    let itemPrefix = info.subspace.subspace(SubspaceKey.items).subspace(itemType)
                    let (beginKey, endKey) = itemPrefix.range()

                    var lastKey: FDB.Bytes? = nil
                    let decoder = ProtobufDecoder()

                    while !Task.isCancelled {
                        // Capture lastKey for Sendable closure
                        let currentLastKey = lastKey

                        // Each batch is a separate transaction
                        let batch: [(key: FDB.Bytes, value: FDB.Bytes)] = try await database.underlying.withTransaction(configuration: .batch) { transaction in
                            let rangeBegin = currentLastKey.map { FDB.Bytes($0.dropFirst(0)) + [0x00] } ?? beginKey

                            var results: [(key: FDB.Bytes, value: FDB.Bytes)] = []
                            let sequence = transaction.getRange(
                                beginSelector: .firstGreaterOrEqual(rangeBegin),
                                endSelector: .firstGreaterOrEqual(endKey),
                                snapshot: true  // Use snapshot reads for enumeration
                            )

                            var count = 0
                            for try await (key, value) in sequence {
                                results.append((key: key, value: value))
                                count += 1
                                if count >= batchSize {
                                    break
                                }
                            }
                            return results
                        }

                        // Process batch and yield items
                        for (key, value) in batch {
                            do {
                                let item = try decoder.decode(T.self, from: Data(value))
                                continuation.yield(item)
                            } catch {
                                // Log decode error but continue processing
                                continuation.finish(throwing: FDBRuntimeError.internalError(
                                    "Failed to decode \(itemType) item: \(error)"
                                ))
                                return
                            }
                            lastKey = key
                        }

                        // Check if we've processed all items
                        if batch.count < batchSize {
                            break
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }
}

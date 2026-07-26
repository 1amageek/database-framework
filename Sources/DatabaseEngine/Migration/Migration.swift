#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import DatabaseKit

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
/// needed during migrations, without requiring a model persistence service.
public struct MigrationStoreInfo: Sendable {
    /// Root subspace for the store
    public let subspace: Subspace

    /// Index subspace for the store
    public let indexSubspace: Subspace

    /// Blobs subspace for the store (large value chunks)
    public let blobsSubspace: Subspace

    public init(subspace: Subspace, indexSubspace: Subspace, blobsSubspace: Subspace) {
        self.subspace = subspace
        self.indexSubspace = indexSubspace
        self.blobsSubspace = blobsSubspace
    }
}

// MARK: - Migration Context

/// Context provided to migrations during execution
///
/// Provides access to database operations and migration utilities.
public struct MigrationContext: Sendable {
    // MARK: - Properties

    /// FDB Container for transaction execution
    public let container: DBContainer

    /// Schema being migrated to
    public let schema: Schema

    /// Schema being migrated from
    public let sourceSchema: Schema

    /// Metadata subspace for storing migration progress
    public let metadataSubspace: Subspace

    /// Source-schema store info registry (read from this side).
    ///
    /// Maps entity names → their subspaces resolved for the source schema's
    /// `#Directory` definitions. Used by `removeIndex` and as the lookup for
    /// `enumerate` when a type's `#Directory` only exists in the source schema.
    private let sourceStoreRegistry: [String: MigrationStoreInfo]

    /// Target-schema store info registry (write to this side).
    ///
    /// Maps entity names → subspaces resolved for the target schema's
    /// `#Directory` definitions. Used by `addIndex`, `rebuildIndex`, and as
    /// the fallback lookup for data operations.
    private let targetStoreRegistry: [String: MigrationStoreInfo]

    /// Index configurations from DBContainer
    ///
    /// Maps index names to their runtime configurations (HNSW params, full-text settings, etc.)
    /// Used when building indexes via EntityIndexBuilder.
    internal let indexConfigurations: [String: [any IndexRuntimeConfiguration]]

    // MARK: - Initialization

    internal init(
        container: DBContainer,
        schema: Schema,
        sourceSchema: Schema? = nil,
        metadataSubspace: Subspace,
        sourceStoreRegistry: [String: MigrationStoreInfo],
        targetStoreRegistry: [String: MigrationStoreInfo],
        indexConfigurations: [String: [any IndexRuntimeConfiguration]] = [:]
    ) {
        self.container = container
        self.schema = schema
        self.sourceSchema = sourceSchema ?? schema
        self.metadataSubspace = metadataSubspace
        self.sourceStoreRegistry = sourceStoreRegistry
        self.targetStoreRegistry = targetStoreRegistry
        self.indexConfigurations = indexConfigurations
    }

    /// Convenience initializer for callers that use a single registry for both sides.
    internal init(
        container: DBContainer,
        schema: Schema,
        sourceSchema: Schema? = nil,
        metadataSubspace: Subspace,
        storeRegistry: [String: MigrationStoreInfo],
        indexConfigurations: [String: [any IndexRuntimeConfiguration]] = [:]
    ) {
        self.init(
            container: container,
            schema: schema,
            sourceSchema: sourceSchema,
            metadataSubspace: metadataSubspace,
            sourceStoreRegistry: storeRegistry,
            targetStoreRegistry: storeRegistry,
            indexConfigurations: indexConfigurations
        )
    }

    // MARK: - Store Access

    /// Get store info for an item type.
    ///
    /// - Parameters:
    ///   - itemType: Item type name (`entity.name` / `T.persistableType`)
    ///   - source: When `true`, looks up the source-schema registry. Defaults to target.
    /// - Returns: MigrationStoreInfo
    /// - Throws: Error if store not found
    public func storeInfo(for itemType: String, source: Bool = false) throws -> MigrationStoreInfo {
        let registry = source ? sourceStoreRegistry : targetStoreRegistry
        guard let info = registry[itemType] else {
            throw DatabaseRuntimeError.invalidArgument(
                "Store info for '\(itemType)' not found in \(source ? "source" : "target") registry. " +
                "Available stores: \(registry.keys.sorted().joined(separator: ", "))"
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
    /// 3. Register index with DatabaseIndexRegistry for target entity store only
    /// 4. Enable index (sets to writeOnly via IndexLifecycleStore)
    /// 5. Build index (via OnlineIndexer using EntityIndexBuilder)
    /// 6. Mark as readable (automatically done by OnlineIndexer after build completes)
    ///
    /// - Parameter indexDescriptor: The index descriptor to add
    /// - Parameter batchSize: Number of items to process per batch (default: 100)
    /// - Throws: Error if index addition fails or target entity cannot be determined
    public func addIndex(_ indexDescriptor: IndexDescriptor, batchSize: Int = 100) async throws {
        if let group = try identifyPolymorphicTargetGroup(for: indexDescriptor, in: schema) {
            try await addPolymorphicIndex(indexName: indexDescriptor.name, group: group, batchSize: batchSize)
            return
        }

        // 1. Identify target entity from Schema
        let targetEntity = try identifyTargetEntity(for: indexDescriptor, in: schema)

        // 2. Get store info for target entity (written to target-schema directory)
        guard let info = targetStoreRegistry[targetEntity.name] else {
            throw DatabaseRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in target registry. " +
                "Available stores: \(targetStoreRegistry.keys.sorted().joined(separator: ", "))"
            )
        }

        let indexRegistry = DatabaseIndexRegistry(
            container: container,
            subspace: info.subspace
        )

        // 3. Convert IndexDescriptor to Index with itemTypes
        let index = try convertDescriptorToIndex(
            indexDescriptor,
            itemTypes: Set([targetEntity.name])
        )

        // 4. Register index (in-memory, fails if already registered)
        do {
            try indexRegistry.register(index: index)
        } catch DatabaseIndexRegistryError.duplicateIndex {
            // Index already registered in this DatabaseIndexRegistry instance - OK
            // This can happen if migration is run multiple times
        }

        // 5. Enable index (disabled → writeOnly)
        // Check current state first to ensure idempotency
        let currentState = try await indexRegistry.state(of: index.name)

        switch currentState {
        case .disabled:
            // Normal case: enable the index
            try await indexRegistry.enable(index.name)
        case .readable:
            // Index already built - nothing to do
            return
        case .writeOnly:
            // Index enabled but not built - continue to build
            break
        }

        // 6. Build the index from the container-scoped runtime type registry.

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = indexConfigurations[index.name] ?? []

        guard let persistableType = container.runtimeConfiguration
            .persistableTypes.type(named: targetEntity.name) else {
            throw DatabaseRuntimeError.internalError(
                "Entity '\(targetEntity.name)' has no registered runtime type"
            )
        }
        try await EntityIndexBuilder.buildIndex(
            for: persistableType,
            container: container,
            storeSubspace: info.subspace,
            index: index,
            indexLifecycleStore: indexRegistry.lifecycleStore,
            batchSize: batchSize,
            configurations: configs
        )
    }

    /// Add a new logical polymorphic index and build it online.
    public func addPolymorphicIndex(indexName: String, batchSize: Int = 100) async throws {
        guard let group = schema.polymorphicGroup(containingIndexNamed: indexName) else {
            throw DatabaseRuntimeError.indexNotFound(
                "Polymorphic index '\(indexName)' not found in target schema"
            )
        }
        try await addPolymorphicIndex(indexName: indexName, group: group, batchSize: batchSize)
    }

    /// Remove an index and add FormerIndex entry
    ///
    /// **Implementation** (all in single atomic transaction):
    /// 1. Identify target entity from Schema
    /// 2. Create FormerIndex metadata entry
    /// 3. Disable index (via IndexLifecycleStore)
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
        // 1. Find index descriptor in source schema to identify target entity
        guard let indexDescriptor = sourceSchema.indexDescriptor(named: indexName) else {
            if let group = sourceSchema.polymorphicGroup(containingIndexNamed: indexName) {
                try await removePolymorphicIndex(indexName: indexName, group: group, addedVersion: addedVersion)
                return
            }
            throw DatabaseRuntimeError.indexNotFound(
                "Index '\(indexName)' not found in source schema. Cannot determine target entity."
            )
        }

        if let group = try identifyPolymorphicTargetGroup(for: indexDescriptor, in: sourceSchema) {
            try await removePolymorphicIndex(indexName: indexName, group: group, addedVersion: addedVersion)
            return
        }

        // 2. Identify target entity
        let targetEntity = try identifyTargetEntity(for: indexDescriptor, in: sourceSchema)

        // 3. Get store info from the source registry (the index lives under the
        //    source schema's directory — that's where it needs to be cleared).
        guard let info = sourceStoreRegistry[targetEntity.name] else {
            throw DatabaseRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in source registry"
            )
        }

        let indexRegistry = DatabaseIndexRegistry(
            container: container,
            subspace: info.subspace
        )

        // 4. Atomic transaction: FormerIndex entry + disable + clear data
        let formerIndexKey = info.subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple(indexName))

        let indexRange = info.indexSubspace.subspace(indexName).range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            // Write FormerIndex entry
            let timestamp = Date().timeIntervalSince1970
            try transaction.setValue(
                Tuple(
                    Int64(addedVersion.major),
                    Int64(addedVersion.minor),
                    Int64(addedVersion.patch),
                    timestamp
                ).pack(),
                for: formerIndexKey
            )

            // Disable index state
            try await indexRegistry.lifecycleStore.disable(indexName, transaction: transaction)

            // Clear index data
            try transaction.clearRange(
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
            if let group = schema.polymorphicGroup(containingIndexNamed: indexName) {
                try await rebuildPolymorphicIndex(indexName: indexName, group: group, batchSize: batchSize)
                return
            }
            throw DatabaseRuntimeError.indexNotFound(
                "Index '\(indexName)' not found in schema"
            )
        }

        if let group = try identifyPolymorphicTargetGroup(for: indexDescriptor, in: schema) {
            try await rebuildPolymorphicIndex(indexName: indexName, group: group, batchSize: batchSize)
            return
        }

        // 2. Identify target entity
        let targetEntity = try identifyTargetEntity(for: indexDescriptor, in: schema)

        // 3. Get store info for target entity (rebuild writes to target directory)
        guard let info = targetStoreRegistry[targetEntity.name] else {
            throw DatabaseRuntimeError.internalError(
                "Store info for entity '\(targetEntity.name)' not found in target registry"
            )
        }

        let indexRegistry = DatabaseIndexRegistry(
            container: container,
            subspace: info.subspace
        )

        // 4. Convert and register index first (needed for DatabaseIndexRegistry operations)
        let index = try convertDescriptorToIndex(
            indexDescriptor,
            itemTypes: Set([targetEntity.name])
        )
        do {
            try indexRegistry.register(index: index)
        } catch DatabaseIndexRegistryError.duplicateIndex {
            // Index already registered - OK
        }

        // 5. Atomic transaction: disable + clear + enable
        // This ensures the index is in a consistent state before building
        let indexRange = info.indexSubspace.subspace(indexName).range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            // Disable index (from any state)
            try await indexRegistry.lifecycleStore.disable(indexName, transaction: transaction)

            // Clear existing data
            try transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )

            // Enable index (disabled → writeOnly)
            // Note: We just disabled it above, so this will succeed
            try await indexRegistry.lifecycleStore.enable(indexName, transaction: transaction)
        }

        // 6. Build index via OnlineIndexer using EntityIndexBuilder

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = indexConfigurations[indexName] ?? []

        guard let persistableType = container.runtimeConfiguration
            .persistableTypes.type(named: targetEntity.name) else {
            throw DatabaseRuntimeError.internalError(
                "Entity '\(targetEntity.name)' has no registered runtime type"
            )
        }
        try await EntityIndexBuilder.buildIndex(
            for: persistableType,
            container: container,
            storeSubspace: info.subspace,
            index: index,
            indexLifecycleStore: indexRegistry.lifecycleStore,
            batchSize: batchSize,
            configurations: configs
        )
    }

    private func addPolymorphicIndex(
        indexName: String,
        group: PolymorphicGroup,
        batchSize: Int
    ) async throws {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)
        let currentState = try await lifecycleStore.state(of: indexName)

        switch currentState {
        case .disabled:
            try await lifecycleStore.enable(indexName)
        case .readable:
            return
        case .writeOnly:
            break
        }

        try await buildPolymorphicIndexEntries(
            indexName: indexName,
            group: group,
            subspace: subspace,
            lifecycleStore: lifecycleStore,
            batchSize: batchSize
        )
        try await lifecycleStore.makeReadable(indexName)
    }

    private func removePolymorphicIndex(
        indexName: String,
        group: PolymorphicGroup,
        addedVersion: Schema.Version
    ) async throws {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)
        let formerIndexKey = subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple(indexName))
        let indexRange = subspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
            .range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            let timestamp = Date().timeIntervalSince1970
            try transaction.setValue(
                Tuple(
                    Int64(addedVersion.major),
                    Int64(addedVersion.minor),
                    Int64(addedVersion.patch),
                    timestamp
                ).pack(),
                for: formerIndexKey
            )

            try await lifecycleStore.disable(indexName, transaction: transaction)
            try transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
        }
    }

    private func rebuildPolymorphicIndex(
        indexName: String,
        group: PolymorphicGroup,
        batchSize: Int
    ) async throws {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)
        let indexRange = subspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
            .range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            try await lifecycleStore.disable(indexName, transaction: transaction)
            try transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
            try await lifecycleStore.enable(indexName, transaction: transaction)
        }

        try await buildPolymorphicIndexEntries(
            indexName: indexName,
            group: group,
            subspace: subspace,
            lifecycleStore: lifecycleStore,
            batchSize: batchSize
        )
        try await lifecycleStore.makeReadable(indexName)
    }

    private func buildPolymorphicIndexEntries(
        indexName: String,
        group: PolymorphicGroup,
        subspace: Subspace,
        lifecycleStore: IndexLifecycleStore,
        batchSize: Int
    ) async throws {
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let configurations = indexConfigurations[indexName] ?? []
        let maintenanceService = IndexMaintenanceService(
            indexLifecycleStore: lifecycleStore,
            violationTracker: UniquenessViolationTracker(
                container: container,
                metadataSubspace: subspace.subspace(SubspaceKey.metadata)
            ),
            indexSubspace: subspace.subspace(SubspaceKey.indexes),
            maintainerProviders: container.runtimeConfiguration.indexMaintainerProviders,
            configurations: configurations
        )
        let memberTypeNames = Set(group.memberTypeNames)

        for entity in schema.entities where memberTypeNames.contains(entity.name) {
            guard let persistableType = container.runtimeConfiguration
                .persistableTypes.type(named: entity.name) else {
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic member '\(entity.name)' has no registered runtime type"
                )
            }
            guard let polymorphicType = persistableType as? any Polymorphable.Type else {
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic member '\(entity.name)' does not conform to Polymorphable"
                )
            }

            func descriptors<Member: Persistable>(
                for memberType: Member.Type
            ) -> [IndexDescriptor] {
                schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberType: memberType
                )
            }
            let memberDescriptors = _openExistential(
                persistableType,
                do: descriptors
            )
            let descriptors = memberDescriptors.filter { $0.name == indexName }
            guard !descriptors.isEmpty else { continue }

            let typeCode = polymorphicType.typeCode(for: entity.name)
            let typeRange = itemSubspace.subspace(typeCode).range()
            var begin = typeRange.begin

            while true {
                let batchBegin = begin
                let (itemsInBatch, lastProcessedKey) = try await container.engine.withTransaction(
                    configuration: .batch
                ) { transaction -> (Int, Bytes?) in
                    let storage = self.container.itemStorageFactory.make(
                        transaction: transaction,
                        blobsSubspace: blobsSubspace
                    )
                    let scanSequence = storage.scan(
                        begin: batchBegin,
                        end: typeRange.end,
                        snapshot: false,
                        limit: batchSize
                    )

                    var itemsInBatch = 0
                    var lastProcessedKey: Bytes?
                    for try await (key, data) in scanSequence {
                        let item = try DataAccess.deserializeAny(data, as: persistableType)
                        let compositeID = try itemSubspace.unpack(key)
                        try await maintenanceService.updateIndexesUntyped(
                            oldModel: nil as (any Persistable)?,
                            newModel: item,
                            id: compositeID,
                            descriptors: descriptors,
                            logicalTypeName: group.identifier,
                            transaction: transaction
                        )
                        lastProcessedKey = key
                        itemsInBatch += 1
                    }

                    return (itemsInBatch, lastProcessedKey)
                }

                guard itemsInBatch == batchSize, let lastProcessedKey else {
                    break
                }
                begin = lastProcessedKey + [0]
            }
        }
    }

    // MARK: - Utility

    /// Execute arbitrary database operation
    ///
    /// Use `transaction.setOption(forOption:)` within the operation to configure
    /// transaction behavior (e.g., priority, timeout).
    ///
    /// - Parameter operation: Operation to execute
    /// - Returns: Operation result
    /// - Throws: Any error from the operation
    public func executeOperation<T: Sendable>(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> T
    ) async throws -> T {
        return try await container.engine.withTransaction(configuration: .default) { transaction in
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
        // Resolve the storage directory from `T` itself so V1 reads from V1's
        // `#Directory` even when V2 is also registered for the same entity name.
        let itemType = T.persistableType
        let container = self.container
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let subspace = try await container.resolveDirectory(for: type)
                    let info = MigrationStoreInfo(
                        subspace: subspace,
                        indexSubspace: subspace.subspace(SubspaceKey.indexes),
                        blobsSubspace: subspace.subspace(SubspaceKey.blobs)
                    )
                    let enumerator = ItemEnumerator<T>(
                        itemType: itemType,
                        storeInfo: info,
                        container: container,
                        batchSize: batchSize
                    )
                    for try await item in enumerator.makeStream() {
                        continuation.yield(item)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
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
        let subspace = try await container.resolveDirectory(for: T.self)

        let data = try DataAccess.serialize(item)
        let identifier = try item.persistableIdentifierTuple()
        let itemKey = subspace.subspace(SubspaceKey.items).subspace(itemType).pack(identifier)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        try await container.engine.withTransaction(configuration: .default) { transaction in
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            try await storage.write(data, for: itemKey)
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
        let subspace = try await container.resolveDirectory(for: T.self)

        let identifier = try item.persistableIdentifierTuple()
        let itemKey = subspace.subspace(SubspaceKey.items).subspace(itemType).pack(identifier)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        try await container.engine.withTransaction(configuration: .default) { transaction in
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            try await storage.delete(for: itemKey)
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
        let subspace = try await container.resolveDirectory(for: T.self)

        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(itemType)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        // Process in batches
        for batchStart in stride(from: 0, to: items.count, by: batchSize) {
            let batchEnd = min(batchStart + batchSize, items.count)
            let batch = Array(items[batchStart..<batchEnd])

            try await container.engine.withTransaction(configuration: .batch) { transaction in
                let storage = self.container.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )
                for item in batch {
                    let data = try DataAccess.serialize(item)
                    let identifier = try item.persistableIdentifierTuple()
                    let itemKey = itemSubspace.pack(identifier)
                    try await storage.write(data, for: itemKey)
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
        let subspace = try await container.resolveDirectory(for: T.self)

        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(itemType)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        // Process in batches
        for batchStart in stride(from: 0, to: items.count, by: batchSize) {
            let batchEnd = min(batchStart + batchSize, items.count)
            let batch = items[batchStart..<batchEnd]

            try await container.engine.withTransaction(configuration: .batch) { transaction in
                let storage = self.container.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )
                for item in batch {
                    let identifier = try item.persistableIdentifierTuple()
                    let itemKey = itemSubspace.pack(identifier)
                    try await storage.delete(for: itemKey)
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
        let subspace = try await container.resolveDirectory(for: T.self)

        let itemPrefix = subspace.subspace(SubspaceKey.items).subspace(itemType)
        let (beginKey, endKey) = itemPrefix.range()

        // Use approximate count for large datasets
        if approximate {
            let sizeBytes = try await container.engine.withTransaction(configuration: .batch) { transaction in
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
        var lastKey: Bytes? = nil
        let batchSize = 10000  // Use large batches for counting

        while true {
            let currentLastKey = lastKey
            let (batchCount, newLastKey): (Int, Bytes?) = try await container.engine.withTransaction(configuration: .batch) { transaction in
                let rangeBegin = currentLastKey.map { Bytes($0.dropFirst(0)) + [0x00] } ?? beginKey

                var count = 0
                var lastKeyInBatch: Bytes? = nil

                // Use limit and wantAll mode to reduce round-trips for counting
                let sequence = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(rangeBegin),
                    to: .firstGreaterOrEqual(endKey),
                    limit: batchSize,
                    snapshot: true,
                    streamingMode: .wantAll
                )

                for (key, _) in sequence {
                    count += 1
                    lastKeyInBatch = key
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

    // MARK: - Source-Schema Storage Cleanup

    /// Range-clear a source-schema type's storage directory.
    ///
    /// When a schema migration moves a type from one `#Directory` to another
    /// (e.g., V1 `#Directory<V1>("old", "users")` → V2 `#Directory<V2>("new", "users")`),
    /// custom migration code is responsible for copying the data. Afterwards
    /// the V1 location still holds the original rows and their indexes.
    /// Call this with the source-schema type (`V1.self`) to reclaim that space.
    ///
    /// Uses the type's own `#Directory` definition via
    /// `container.resolveDirectory(for: T.self)`, so V1 and V2 — even with the
    /// same `persistableType` — clear independent subspaces.
    ///
    /// - Parameter type: The source-schema Persistable type whose storage should be removed.
    /// - Throws: Any error from directory resolution or the clearRange transaction.
    public func purgeSourceSchemaStorage<T: Persistable>(_ type: T.Type) async throws {
        let subspace = try await container.resolveDirectory(for: T.self)
        let (beginKey, endKey) = subspace.range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            try transaction.clearRange(beginKey: beginKey, endKey: endKey)
        }
    }

    // MARK: - Private Helpers

    private func identifyPolymorphicTargetGroup(
        for descriptor: IndexDescriptor,
        in schema: Schema
    ) throws -> PolymorphicGroup? {
        let matchingGroups = schema.polymorphicGroups.filter { group in
            group.indexes.contains { $0.name == descriptor.name }
        }

        guard matchingGroups.count <= 1 else {
            throw DatabaseRuntimeError.internalError(
                "Index '\(descriptor.name)' is associated with multiple polymorphic groups: " +
                "\(matchingGroups.map(\.identifier).joined(separator: ", ")). " +
                "Index names must be unique across all polymorphic groups."
            )
        }

        return matchingGroups.first
    }

    /// Identify target entity for an index descriptor
    ///
    /// This method matches an IndexDescriptor to its corresponding entity in the Schema
    /// by checking which entity contains this index in its indexDescriptors.
    ///
    /// - Parameter descriptor: IndexDescriptor to match
    /// - Returns: Target entity
    /// - Throws: Error if no entity owns this index or multiple entities claim it
    private func identifyTargetEntity(
        for descriptor: IndexDescriptor,
        in schema: Schema
    ) throws -> Schema.Entity {
        var matchingEntities: [Schema.Entity] = []

        for entity in schema.entities {
            // Check if this entity contains the descriptor
            if entity.indexDescriptors.contains(where: { $0.name == descriptor.name }) {
                matchingEntities.append(entity)
            }
        }

        guard !matchingEntities.isEmpty else {
            throw DatabaseRuntimeError.indexNotFound(
                "Index '\(descriptor.name)' is not associated with any entity in schema. " +
                "Available entities: \(schema.entities.map { $0.name }.joined(separator: ", "))"
            )
        }

        guard matchingEntities.count == 1 else {
            throw DatabaseRuntimeError.internalError(
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
    ///   - itemTypes: Set of item type names that this index applies to
    /// - Returns: Index object
    /// - Throws: Error if conversion fails
    private func convertDescriptorToIndex(
        _ descriptor: IndexDescriptor,
        itemTypes: Set<String>
    ) throws -> Index {
        // Build KeyExpression from field names using factory
        // This properly handles nested paths (e.g., "address.city" → NestExpression)
        let keyExpression = KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        )

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: keyExpression,
            subspaceKey: descriptor.name,
            itemTypes: itemTypes,  // Scoped to specific entity
            storedFieldNames: descriptor.storedFieldNames
        )
    }
}

// MARK: - Migration Extensions

extension Migration: Identifiable {
    public var id: String {
        return "\(fromVersion)-\(toVersion)"
    }
}

// MARK: - DatabaseRuntimeError

/// Database runtime errors.
public enum DatabaseRuntimeError: Error, CustomStringConvertible {
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

// MARK: - ItemEnumerator

/// Internal helper for enumerating items with batch processing
///
/// This struct encapsulates all the state needed for async enumeration
/// in a Sendable-safe way.
	private struct ItemEnumerator<T: Persistable>: Sendable {
    let itemType: String
    let storeInfo: MigrationStoreInfo
    let container: DBContainer
    let batchSize: Int

    init(itemType: String, storeInfo: MigrationStoreInfo, container: DBContainer, batchSize: Int) {
        self.itemType = itemType
        self.storeInfo = storeInfo
        self.container = container
        self.batchSize = batchSize
    }

	    func makeStream() -> AsyncThrowingStream<T, Error> {
        // Capture properties in local variables for sendability
        let itemType = self.itemType
        let info = self.storeInfo
        let container = self.container
        let batchSize = self.batchSize

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
	                    // Build prefix for item scanning
	                    let itemPrefix = info.subspace.subspace(SubspaceKey.items).subspace(itemType)
	                    let (beginKey, endKey) = itemPrefix.range()
	                    let blobsSubspace = info.subspace.subspace(SubspaceKey.blobs)

	                    var lastKey: Bytes? = nil
                    while !Task.isCancelled {
                        // Capture lastKey for Sendable closure
                        let currentLastKey = lastKey

	                        // Each batch is a separate transaction
	                        let batch: [(key: Bytes, value: Bytes)] = try await container.engine.withTransaction(configuration: .batch) { transaction in
	                            let rangeBegin = currentLastKey.map { $0 + [0x00] } ?? beginKey

	                            let storage = self.container.itemStorageFactory.make(
	                                transaction: transaction,
	                                blobsSubspace: blobsSubspace
	                            )

	                            var results: [(key: Bytes, value: Bytes)] = []
	                            for try await (key, value) in storage.scan(
	                                begin: rangeBegin,
	                                end: endKey,
	                                snapshot: true,
	                                limit: batchSize
	                            ) {
	                                results.append((key: key, value: value))
	                            }
	                            return results
	                        }

	                        // Process batch and yield items
	                        for (key, value) in batch {
	                            do {
	                                let item: T = try DataAccess.deserialize(value)
	                                continuation.yield(item)
                            } catch {
                                // Log decode error but continue processing
                                continuation.finish(throwing: DatabaseRuntimeError.internalError(
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

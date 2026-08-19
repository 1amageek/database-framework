import DatabaseKit
import DatabaseTypes
import StorageKit

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
package struct MigrationStoreInfo: Sendable {
    /// Root subspace for the store
    package let subspace: Subspace

    /// Blobs subspace for the store (large value chunks)
    package let blobsSubspace: Subspace

    package init(subspace: Subspace, blobsSubspace: Subspace) {
        self.subspace = subspace
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
    package let container: DBContainer

    /// Schema being migrated to
    public let schema: Schema

    /// Schema being migrated from
    public let sourceSchema: Schema

    /// Metadata subspace for storing migration progress
    package let metadataSubspace: Subspace

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

    /// Runtime registrations and index policy for the target generation.
    internal let runtimeConfiguration: DatabaseRuntimeConfiguration

    /// Physical layouts paired with `schema` for this migration stage.
    private let targetIndexPhysicalLayouts: [String: IndexPhysicalLayout]

    // MARK: - Initialization

    internal init(
        container: DBContainer,
        schema: Schema,
        sourceSchema: Schema? = nil,
        metadataSubspace: Subspace,
        sourceStoreRegistry: [String: MigrationStoreInfo],
        targetStoreRegistry: [String: MigrationStoreInfo],
        runtimeConfiguration: DatabaseRuntimeConfiguration? = nil,
        targetIndexPhysicalLayouts: [String: IndexPhysicalLayout]? = nil
    ) {
        self.container = container
        self.schema = schema
        self.sourceSchema = sourceSchema ?? schema
        self.metadataSubspace = metadataSubspace
        self.sourceStoreRegistry = sourceStoreRegistry
        self.targetStoreRegistry = targetStoreRegistry
        self.runtimeConfiguration =
            runtimeConfiguration
            ?? container.runtimeConfiguration
        self.targetIndexPhysicalLayouts =
            targetIndexPhysicalLayouts
            ?? container.indexPhysicalLayouts
    }

    /// Convenience initializer for callers that use a single registry for both sides.
    internal init(
        container: DBContainer,
        schema: Schema,
        sourceSchema: Schema? = nil,
        metadataSubspace: Subspace,
        storeRegistry: [String: MigrationStoreInfo],
        runtimeConfiguration: DatabaseRuntimeConfiguration? = nil,
        targetIndexPhysicalLayouts: [String: IndexPhysicalLayout]? = nil
    ) {
        self.init(
            container: container,
            schema: schema,
            sourceSchema: sourceSchema,
            metadataSubspace: metadataSubspace,
            sourceStoreRegistry: storeRegistry,
            targetStoreRegistry: storeRegistry,
            runtimeConfiguration: runtimeConfiguration,
            targetIndexPhysicalLayouts: targetIndexPhysicalLayouts
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
    package func storeInfo(for itemType: String, source: Bool = false) throws -> MigrationStoreInfo {
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
    /// 1. Identify the target entity from descriptor ownership
    /// 2. Resolve the descriptor with its key expression and item types
    /// 3. Bind lifecycle state to the target schema and physical layout
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

        // 3. Convert IndexDescriptor to Index with itemTypes
        let index = try resolveIndex(
            indexDescriptor,
            itemTypes: Set([targetEntity.name])
        )

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: info.subspace,
            schema: schema,
            indexPhysicalLayouts: targetIndexPhysicalLayouts
        )

        // 5. Enable index (disabled → writeOnly)
        // Check current state first to ensure idempotency
        let currentState = try await lifecycleStore.state(of: index.name)

        switch currentState {
        case .disabled:
            // Normal case: enable the index
            try await lifecycleStore.enable(index.name)
        case .readable:
            // Index already built - nothing to do
            return
        case .writeOnly:
            // Index enabled but not built - continue to build
            break
        }

        // 6. Build the index from the target generation's runtime registry.

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = runtimeConfiguration.indexConfigurations(
            named: index.name
        )

        guard
            let entityRuntime = runtimeConfiguration
                .entityRuntimes.registration(named: targetEntity.name) else {
            throw DatabaseRuntimeError.internalError(
                "Entity '\(targetEntity.name)' has no registered runtime type"
            )
        }
        try await EntityIndexBuilder.buildIndex(
            for: entityRuntime,
            container: container,
            storeSubspace: info.subspace,
            index: index,
            indexLifecycleStore: lifecycleStore,
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
    /// 3. Clear every physical data and lifecycle-state generation
    /// 4. Clear pending build metadata
    ///
    /// - Parameters:
    ///   - indexName: Name of the index to remove
    ///   - addedVersion: Version when index was originally added
    /// - Throws: Error if index removal fails or index not found in schema
    public func removeIndex(
        indexName: String,
        addedVersion: Schema.Version
    ) async throws {
        try await withAuthorizedTransaction(configuration: .batch) {
            transaction in
            try removeIndex(
                indexName: indexName,
                addedVersion: addedVersion,
                transaction: transaction
            )
        }
    }

    package func removeIndex(
        indexName: String,
        addedVersion: Schema.Version,
        transaction: any TransactionAccess
    ) throws {
        // 1. Find index descriptor in source schema to identify target entity
        guard let indexDescriptor = sourceSchema.indexDescriptor(named: indexName) else {
            if let group = sourceSchema.polymorphicGroup(containingIndexNamed: indexName) {
                try removePolymorphicIndex(indexName: indexName, group: group, addedVersion: addedVersion,
                    transaction: transaction
                )
                return
            }
            throw DatabaseRuntimeError.indexNotFound(
                "Index '\(indexName)' not found in source schema. Cannot determine target entity."
            )
        }

        if let group = try identifyPolymorphicTargetGroup(for: indexDescriptor, in: sourceSchema) {
            try removePolymorphicIndex(indexName: indexName, group: group, addedVersion: addedVersion,
                transaction: transaction
            )
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

        // 4. Atomic transaction: FormerIndex entry + clear every retained
        // generation owned by the removed logical declaration.
        let formerIndexKey = info.subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple(indexName))

        let timestamp = container.wallClock.now
        try transaction.setValue(
            Tuple(
                Int64(addedVersion.major),
                Int64(addedVersion.minor),
                Int64(addedVersion.patch),
                timestamp.secondsSinceUnixEpoch,
                UInt64(timestamp.nanoseconds)
            ).pack(),
            for: formerIndexKey
        )
        try IndexStorageRetirer.retire(
            indexName: indexName,
            selection: .allGenerations,
            storeSubspace: info.subspace,
            transaction: transaction
        )
        try container.clearSchemaIndexBuildPending(
            scope: .entity(
                name: targetEntity.name,
                directoryComponents: targetEntity.directoryComponents
            ),
            index: indexName,
            selection: .allGenerations,
            transaction: transaction
        )
    }

    /// Retires exactly the source physical generation selected by a framework
    /// transition plan. The caller commits this with the schema snapshot so an
    /// old generation cannot disappear while the old schema remains active.
    package func retireIndexStorage(
        _ target: DatabaseIndexTransitionPlan.Target,
        transaction: any TransactionAccess
    ) throws {
        let selection = DatabaseIndexStorageRetirement.physicalGeneration(
            definitionFingerprint: target.identity.definitionFingerprint,
            layoutFingerprint: target.identity.layoutFingerprint
        )
        switch target.scope {
        case .entity(let name, _):
            guard let info = sourceStoreRegistry[name] else {
                throw DatabaseRuntimeError.internalError(
                    "Store info for entity '\(name)' not found in source registry"
                )
            }
            try IndexStorageRetirer.retire(
                indexName: target.identity.name,
                selection: selection,
                storeSubspace: info.subspace,
                transaction: transaction
            )
        case .polymorphicGroup(_, let directoryPath):
            let subspace = try container.operationDataSubspace(
                relativePath: directoryPath
            )
            try IndexStorageRetirer.retire(
                indexName: target.identity.name,
                selection: selection,
                storeSubspace: subspace,
                transaction: transaction
            )
        }
        try container.clearSchemaIndexBuildPending(
            scope: target.scope,
            index: target.identity.name,
            selection: selection,
            transaction: transaction
        )
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

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: info.subspace,
            schema: schema,
            indexPhysicalLayouts: targetIndexPhysicalLayouts
        )

        // 4. Resolve the target definition for this migration generation.
        let index = try resolveIndex(
            indexDescriptor,
            itemTypes: Set([targetEntity.name])
        )

        // 5. Atomic transaction: disable + clear + enable
        // This ensures the index is in a consistent state before building
        let indexRange = try lifecycleStore.indexSubspace(
            for: indexName
        ).range()

        try await withAuthorizedTransaction(configuration: .batch) { transaction in
            // Disable index (from any state)
            try await lifecycleStore.disable(indexName, transaction: transaction)

            // Clear existing data
            try transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )

            // Enable index (disabled → writeOnly)
            // Note: We just disabled it above, so this will succeed
            try await lifecycleStore.enable(indexName, transaction: transaction)
        }

        // 6. Build index via OnlineIndexer using EntityIndexBuilder

        // Get configurations for this index (HNSW params, full-text settings, etc.)
        let configs = runtimeConfiguration.indexConfigurations(
            named: indexName
        )

        guard
            let entityRuntime = runtimeConfiguration
                .entityRuntimes.registration(named: targetEntity.name) else {
            throw DatabaseRuntimeError.internalError(
                "Entity '\(targetEntity.name)' has no registered runtime type"
            )
        }
        try await EntityIndexBuilder.buildIndex(
            for: entityRuntime,
            container: container,
            storeSubspace: info.subspace,
            index: index,
            indexLifecycleStore: lifecycleStore,
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
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: subspace,
            schema: schema,
            indexPhysicalLayouts: targetIndexPhysicalLayouts
        )
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
        try await withAuthorizedTransaction(configuration: .batch) {
            transaction in
            try removePolymorphicIndex(
                indexName: indexName,
                group: group,
                addedVersion: addedVersion,
                transaction: transaction
            )
        }
    }

    private func removePolymorphicIndex(
        indexName: String,
        group: PolymorphicGroup,
        addedVersion: Schema.Version,
        transaction: any TransactionAccess
    ) throws {
        let subspace = try container.operationDataSubspace(
            relativePath: group.resolvedDirectoryPath()
        )
        let formerIndexKey =
            subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple(indexName))
        let timestamp = container.wallClock.now
        try transaction.setValue(
            Tuple(
                Int64(addedVersion.major),
                Int64(addedVersion.minor),
                Int64(addedVersion.patch),
                timestamp.secondsSinceUnixEpoch,
                UInt64(timestamp.nanoseconds)
            ).pack(),
            for: formerIndexKey
        )
        try IndexStorageRetirer.retire(
            indexName: indexName,
            selection: .allGenerations,
            storeSubspace: subspace,
            transaction: transaction
        )
        try container.clearSchemaIndexBuildPending(
            scope: .polymorphicGroup(
                identifier: group.identifier,
                directoryPath: try group.resolvedDirectoryPath()
            ),
            index: indexName,
            selection: .allGenerations,
            transaction: transaction
        )
    }

    private func rebuildPolymorphicIndex(
        indexName: String,
        group: PolymorphicGroup,
        batchSize: Int
    ) async throws {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let lifecycleStore = IndexLifecycleStore(container: container, subspace: subspace,
            schema: schema,
            indexPhysicalLayouts: targetIndexPhysicalLayouts
        )
        let indexRange = try lifecycleStore.indexSubspace(
            for: indexName
        )
            .range()

        try await withAuthorizedTransaction(configuration: .batch) { transaction in
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
        let configurations = runtimeConfiguration.indexConfigurations(
            named: indexName
        )
        let maintenanceService = IndexMaintenanceService(
            indexLifecycleStore: lifecycleStore,
            violationTracker: UniquenessViolationTracker(
                container: container,
                metadataSubspace: subspace.subspace(SubspaceKey.metadata)
            ),
            configurations: configurations
        )
        let memberTypeNames = Set(group.memberTypeNames)
        var finalization: (runtime: EntityRuntimeRegistration, index: ResolvedIndex)?

        for entity in schema.entities where memberTypeNames.contains(entity.name) {
            guard let entityRuntime = runtimeConfiguration
                    .entityRuntimes.registration(named: entity.name) else {
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic member '\(entity.name)' has no registered runtime type"
                )
            }
            guard entity.polymorphicMembership?.identifier
                    == group.identifier else {
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic member '\(entity.name)' does not conform to Polymorphable"
                )
            }

            let memberDescriptors = schema.polymorphicIndexDescriptors(
                identifier: group.identifier,
                memberTypeName: entity.name
            )
            let descriptors = memberDescriptors.filter { $0.name == indexName }
            guard !descriptors.isEmpty else { continue }
            if finalization == nil, let descriptor = descriptors.first {
                finalization = (
                    runtime: entityRuntime,
                    index: ResolvedIndex(
                        descriptor: descriptor,
                        rootExpression: KeyExpressionFactory.from(
                            keyPaths: descriptor.fieldNames
                        ),
                        itemTypes: Set([group.identifier]),
                    )
                )
            }

            let typeCode = PolymorphicTypeCode.value(for: entity.name)
            let typeRange = itemSubspace.subspace(typeCode).range()
            var begin = typeRange.begin

            while true {
                let batchBegin = begin
                let (itemsInBatch, lastProcessedKey) = try await withAuthorizedTransaction(
                    configuration: .batch
                ) { transaction -> (Int, ByteString?) in
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

                    // SQLite cannot mutate a transaction while its range cursor
                    // is open. The batch is bounded by `batchSize`; `ByteString`
                    // retains the backend-owned storage without materializing
                    // `[UInt8]` or `Data`. Reading the complete bounded page
                    // first also preserves the transaction's range conflict
                    // before index writes begin.
                    var batch: [(key: ByteString, data: ByteString)] = []
                    batch.reserveCapacity(batchSize)
                    var iterator = scanSequence.makeAsyncIterator()
                    while let element = try await iterator.next() {
                        batch.append(element)
                    }

                    for (key, data) in batch {
                        let item = try DataAccess.deserializePersistedModel(
                            data,
                            expectedEntity: entity.name
                        )
                        let compositeID = try itemSubspace.unpack(key)
                        try await maintenanceService.updateIndexesUntyped(
                            runtime: entityRuntime,
                            oldModel: nil,
                            newModel: item,
                            id: compositeID,
                            descriptors: descriptors,
                            logicalTypeName: group.identifier,
                            transaction: transaction
                        )
                    }

                    return (batch.count, batch.last?.key)
                }

                guard itemsInBatch == batchSize, let lastProcessedKey else {
                    break
                }
                begin = lastProcessedKey.appending(0)
            }
        }

        guard let finalization else {
            throw DatabaseRuntimeError.internalError(
                "Polymorphic index '\(indexName)' has no registered member runtime"
            )
        }
        try await withAuthorizedTransaction(
            configuration: .batch
        ) { transaction in
            try await finalization.runtime.finalizeIndex(
                container: self.container,
                storeSubspace: subspace,
                index: finalization.index,
                configurations: configurations,
                transaction: transaction
            )
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
        try await withAuthorizedTransaction(
            configuration: .default,
            operation
        )
    }

    /// Runs one migration transaction only after re-evaluating the Base Grant
    /// in that same transaction. Migration stages intentionally span multiple
    /// transactions, so authorization cannot be inherited from stage start.
    private func withAuthorizedTransaction<Result: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @escaping @Sendable (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await container.withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: configuration
        ) { transaction in
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
        let targetRuntime = runtimeConfiguration.entityRuntimes
            .registration(named: itemType)
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let subspace = try await container.resolveDirectory(for: type)
                    let info = MigrationStoreInfo(
                        subspace: subspace,
                        blobsSubspace: subspace.subspace(SubspaceKey.blobs)
                    )
                    let enumerator = ItemEnumerator<T>(
                        itemType: itemType,
                        storeInfo: info,
                        container: container,
                        batchSize: batchSize,
                        targetRuntime: targetRuntime
                    )
                    var iterator = enumerator.makeStream().makeAsyncIterator()
                    while let item = try await iterator.next() {
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

        try await withAuthorizedTransaction(configuration: .default) { transaction in
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

        try await withAuthorizedTransaction(configuration: .default) { transaction in
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

            try await withAuthorizedTransaction(configuration: .batch) { transaction in
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

            try await withAuthorizedTransaction(configuration: .batch) { transaction in
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
            let sizeBytes = try await withAuthorizedTransaction(configuration: .batch) { transaction in
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
        var lastKey: ByteString? = nil
        let batchSize = 10000  // Use large batches for counting

        while true {
            let currentLastKey = lastKey
            let (batchCount, newLastKey): (Int, ByteString?) = try await withAuthorizedTransaction(configuration: .batch) { transaction in
                let rangeBegin = currentLastKey.map { $0.appending(0x00) } ?? beginKey

                var count = 0
                var lastKeyInBatch: ByteString? = nil

                // Use limit and wantAll mode to reduce round-trips for counting
                let sequence = try await TransactionRangeCollection.collect(using: transaction,
                    from: .firstGreaterOrEqual(rangeBegin),
                    to: .firstGreaterOrEqual(endKey),
                    limit: batchSize,
                    reverse: false,
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

        try await withAuthorizedTransaction(configuration: .batch) { transaction in
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
                "\(matchingGroups.map { $0.identifier }.joined(separator: ", ")). " +
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

    /// Binds a validated schema descriptor to runtime key extraction.
    ///
    /// - Parameters:
    ///   - descriptor: IndexDescriptor from schema
    ///   - itemTypes: Set of item type names that this index applies to
    /// - Returns: A runtime index scoped to the supplied item types.
    private func resolveIndex(
        _ descriptor: IndexDescriptor,
        itemTypes: Set<String>
    ) throws -> ResolvedIndex {
        // Build KeyExpression from field names using factory
        // This properly handles nested paths (e.g., "address.city" → NestExpression)
        let keyExpression = KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        )

        return ResolvedIndex(
            descriptor: descriptor,
            rootExpression: keyExpression,
            itemTypes: itemTypes,  // Scoped to specific entity
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
    let targetRuntime: EntityRuntimeRegistration?

    init(
        itemType: String,
        storeInfo: MigrationStoreInfo,
        container: DBContainer,
        batchSize: Int,
        targetRuntime: EntityRuntimeRegistration?
    ) {
        self.itemType = itemType
        self.storeInfo = storeInfo
        self.container = container
        self.batchSize = batchSize
        self.targetRuntime = targetRuntime
    }

	    func makeStream() -> AsyncThrowingStream<T, Error> {
        // Capture properties in local variables for sendability
        let itemType = self.itemType
        let info = self.storeInfo
        let container = self.container
        let batchSize = self.batchSize
        let targetRuntime = self.targetRuntime

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
	                    // Build prefix for item scanning
	                    let itemPrefix = info.subspace.subspace(SubspaceKey.items).subspace(itemType)
	                    let (beginKey, endKey) = itemPrefix.range()
	                    let blobsSubspace = info.subspace.subspace(SubspaceKey.blobs)

	                    var lastKey: ByteString? = nil
                    while !Task.isCancelled {
                        // Capture lastKey for Sendable closure
                        let currentLastKey = lastKey

	                        // Each batch is a separate transaction
	                        let batch: [(key: ByteString, value: ByteString)] = try await container.withDatabaseTransaction(
	                            requiredAccess: .administer,
	                            configuration: .batch
	                        ) { transaction in
	                            let rangeBegin = currentLastKey.map { $0.appending(0x00) } ?? beginKey

	                            let storage = self.container.itemStorageFactory.make(
	                                transaction: transaction,
	                                blobsSubspace: blobsSubspace
	                            )

	                            var results: [(key: ByteString, value: ByteString)] = []
	                            var iterator = storage.scan(
	                                begin: rangeBegin,
	                                end: endKey,
	                                snapshot: true,
	                                limit: batchSize
	                            ).makeAsyncIterator()
	                            while let (key, value) = try await iterator.next() {
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
                                // An interrupted or concurrent idempotent stage
                                // can already have rewritten this key in the
                                // target representation. Accept only bytes that
                                // the registered target runtime fully decodes
                                // and canonicalizes; malformed source data must
                                // remain an explicit failure.
                                if let targetRuntime {
                                    do {
                                        let model = try DataAccess
                                            .deserializePersistedModel(
                                                value,
                                                expectedEntity: itemType
                                            )
                                        _ = try targetRuntime.canonicalized(model)
                                        lastKey = key
                                        continue
                                    } catch {
                                        // Preserve the source decode failure
                                        // contract below.
                                    }
                                }
                                continuation.finish(throwing: DatabaseRuntimeError.internalError(
                                    "Failed to decode a persisted \(itemType) item"
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

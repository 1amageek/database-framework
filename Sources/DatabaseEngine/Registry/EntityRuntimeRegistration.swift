import DatabaseKit
import DatabaseTypes
import StorageKit

private struct EntityIndexProvider<Model: Persistable>: Sendable {
    let kindIdentifier: String
    let runtimeRequirements: IndexRuntimeRequirements
    let physicalEntryCapabilities: IndexPhysicalEntryCapabilities?
    let supportsUniquenessConstraints: Bool
    let makeMaintainer: @Sendable (
        Index,
        Subspace,
        KeyExpression,
        [any IndexRuntimeConfiguration],
        any WallClock
    ) throws -> any IndexMaintainer<Model>
    let makeUniquenessMaintainer: @Sendable (
        Index,
        Subspace,
        KeyExpression,
        [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model>

    init<Provider: IndexMaintainerProvider>(_ provider: Provider) {
        self.kindIdentifier = provider.kindIdentifier
        self.runtimeRequirements = provider.runtimeRequirements
        self.physicalEntryCapabilities = provider.physicalEntryCapabilities
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
        self.makeMaintainer = {
            index,
            subspace,
            idExpression,
            configurations,
            wallClock in
            try provider.makeIndexMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: wallClock
            )
        }
        self.makeUniquenessMaintainer = {
            index,
            subspace,
            idExpression,
            configurations in
            try provider.makeIndexUniquenessMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations
            )
        }
    }

    init<Provider: EntityIndexMaintainerProvider>(_ provider: Provider)
    where Provider.Model == Model {
        self.kindIdentifier = provider.kindIdentifier
        self.runtimeRequirements = provider.runtimeRequirements
        self.physicalEntryCapabilities = provider.physicalEntryCapabilities
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
        self.makeMaintainer = {
            index,
            subspace,
            idExpression,
            configurations,
            wallClock in
            try provider.makeIndexMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: wallClock
            )
        }
        self.makeUniquenessMaintainer = {
            index,
            subspace,
            idExpression,
            configurations in
            try provider.makeIndexUniquenessMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations
            )
        }
    }
}

private struct EntityIndexProviderDescriptor: Sendable {
    let runtimeRequirements: IndexRuntimeRequirements
    let supportsUniquenessConstraints: Bool

    init<Model: Persistable>(_ provider: EntityIndexProvider<Model>) {
        self.runtimeRequirements = provider.runtimeRequirements
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
    }
}

struct EntityIndexSliceResult: Sendable {
    let processed: UInt64
    let lastProcessedKey: ByteString?
    let hasMore: Bool
}

struct EntityTableRows: Sendable {
    let rows: [CanonicalSourceRow]
    let residualFilter: DatabaseKit.Expression?
    let residualOrderBy: [SortKey]?
    let limitPushed: Bool
    let offsetPushed: Bool
}

public struct EntityRuntimeDefinition<Model: Persistable>: Sendable {
    public let entity: Schema.Entity

    private var indexReaders: [String: IndexReader]
    private var indexProviders: [String: EntityIndexProvider<Model>]

    fileprivate typealias IndexReader = @Sendable (
        _ context: DatabaseContext,
        _ selectQuery: SelectQuery,
        _ indexScan: IndexScanSource,
        _ options: ReadExecutionContext,
        _ partitions: FieldObject
    ) async throws -> IndexReadResult

    public init(_ model: Model.Type) throws(SchemaEntityError) {
        self.entity = try Schema.Entity(from: model)
        self.indexReaders = [:]
        self.indexProviders = [:]
    }

    public mutating func register<Executor: IndexReadExecutor>(
        _ executor: Executor
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexReaders[executor.kindIdentifier] == nil else {
            throw .duplicateIndexReadExecutor(executor.kindIdentifier)
        }
        indexReaders[executor.kindIdentifier] = {
            context,
            selectQuery,
            indexScan,
            options,
            partitions in
            try await executor.executeRows(
                context: context,
                selectQuery: selectQuery,
                indexScan: indexScan,
                as: Model.self,
                options: options,
                partitions: partitions
            )
        }
    }

    public mutating func register<Provider: IndexMaintainerProvider>(
        _ provider: Provider
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexProviders[provider.kindIdentifier] == nil else {
            throw .duplicateIndexMaintainerProvider(provider.kindIdentifier)
        }
        indexProviders[provider.kindIdentifier] = EntityIndexProvider(provider)
    }

    public mutating func register<Provider: EntityIndexMaintainerProvider>(
        _ provider: Provider
    ) throws(DatabaseRuntimeConfigurationError)
    where Provider.Model == Model {
        guard indexProviders[provider.kindIdentifier] == nil else {
            throw .duplicateIndexMaintainerProvider(provider.kindIdentifier)
        }
        indexProviders[provider.kindIdentifier] = EntityIndexProvider(provider)
    }

    public consuming func registration() -> EntityRuntimeRegistration {
        EntityRuntimeRegistration(
            entity: entity,
            modelType: Model.self,
            indexReaders: indexReaders,
            indexProviders: indexProviders,
            decodeModel: { try $0.decode(as: Model.self) },
            fetchTableRows: {
                context,
                sourceName,
                selectQuery,
                options in
                let plan = try SelectQueryPlanner.plan(
                    selectQuery,
                    as: Model.self,
                    options: options
                )
                let items = try await context.fetch(plan.typedQuery)
                let rows = try items.map { item -> CanonicalSourceRow in
                    try options.workMeter.consume(at: .resultMaterialization)
                    let row = try QueryRowCodec.encode(item)
                    return CanonicalSourceRow.fromBaseFields(
                        row.fields,
                        sourceName: sourceName,
                        annotations: row.annotations,
                        version: row.version
                    )
                }
                return EntityTableRows(
                    rows: rows,
                    residualFilter: plan.residualFilter,
                    residualOrderBy: plan.residualOrderBy,
                    limitPushed: plan.limitPushed,
                    offsetPushed: plan.offsetPushed
                )
            },
            updateIndexes: Self.makeUpdateIndexesOperation(
                entity: entity,
                providers: indexProviders
            ),
            buildIndex: Self.makeBuildIndexOperation(
                providers: indexProviders
            ),
            runIndexSlice: Self.makeIndexSliceOperation(
                providers: indexProviders
            )
        )
    }

    private static func makeUpdateIndexesOperation(
        entity: Schema.Entity,
        providers: [String: EntityIndexProvider<Model>]
    ) -> EntityRuntimeRegistration.UpdateIndexes {
        {
            lifecycleStore,
            violationTracker,
            indexSubspace,
            configurations,
            wallClock,
            oldModel,
            newModel,
            id,
            descriptors,
            logicalTypeName,
            transaction in
            guard oldModel != nil || newModel != nil else { return }
            let typedOld = try oldModel?.decode(as: Model.self)
            let typedNew = try newModel?.decode(as: Model.self)
            let indexDescriptors = descriptors ?? entity.indexDescriptors
            guard !indexDescriptors.isEmpty else { return }
            let states = try await lifecycleStore.states(
                of: indexDescriptors.map { $0.name },
                transaction: transaction
            )
            for descriptor in indexDescriptors {
                let state = states[descriptor.name] ?? .disabled
                guard state.shouldMaintain else { continue }
                guard let provider = providers[descriptor.kindIdentifier] else {
                    throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                        kindIdentifier: descriptor.kindIdentifier,
                        indexName: descriptor.name
                    )
                }
                let index = makeIndex(
                    descriptor,
                    entity: logicalTypeName ?? entity.name
                )
                let subspace = indexSubspace.subspace(descriptor.name)
                let idExpression = TupleKeyExpression(value: id)
                let maintainer = try provider.makeMaintainer(
                    index,
                    subspace,
                    idExpression,
                    configurations,
                    wallClock
                )
                if descriptor.isUnique, let typedNew {
                    let uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                        index,
                        subspace,
                        idExpression,
                        configurations
                    )
                    try await IndexUniquenessConstraint.enforce(
                        index: index,
                        item: typedNew,
                        id: id,
                        state: state,
                        maintainer: uniquenessMaintainer,
                        violationTracker: violationTracker,
                        transaction: transaction
                    )
                }
                try await maintainer.updateIndex(
                    oldItem: typedOld,
                    newItem: typedNew,
                    transaction: transaction
                )
            }
        }
    }

    private static func makeBuildIndexOperation(
        providers: [String: EntityIndexProvider<Model>]
    ) -> EntityRuntimeRegistration.BuildIndex {
        {
            container,
            storeSubspace,
            index,
            lifecycleStore,
            batchSize,
            configurations in
            guard let provider = providers[index.kind.identifier] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    kindIdentifier: index.kind.identifier,
                    indexName: index.name
                )
            }
            let subspace = storeSubspace
                .subspace(SubspaceKey.indexes)
                .subspace(index.subspaceKey)
            let idExpression = FieldKeyExpression(fieldName: "id")
            let maintainer = try provider.makeMaintainer(
                index,
                subspace,
                idExpression,
                configurations,
                container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<Model>)?
            if index.isUnique {
                uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                    index,
                    subspace,
                    idExpression,
                    configurations
                )
            } else {
                uniquenessMaintainer = nil
            }
            let indexer = try OnlineIndexer<Model>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Model.persistableType,
                index: index,
                indexMaintainer: maintainer,
                uniquenessMaintainer: uniquenessMaintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: batchSize
            )
            try await indexer.buildIndex(clearFirst: false)
        }
    }

    private static func makeIndexSliceOperation(
        providers: [String: EntityIndexProvider<Model>]
    ) -> EntityRuntimeRegistration.RunIndexSlice {
        {
            container,
            storeSubspace,
            index,
            lastProcessedKey,
            maximumWorkUnits,
            transaction in
            guard let provider = providers[index.kind.identifier] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    kindIdentifier: index.kind.identifier,
                    indexName: index.name
                )
            }
            let subspace = storeSubspace
                .subspace(SubspaceKey.indexes)
                .subspace(index.subspaceKey)
            let idExpression = FieldKeyExpression(fieldName: "id")
            let configurations = container.indexConfigurations[index.name] ?? []
            let maintainer = try provider.makeMaintainer(
                index,
                subspace,
                idExpression,
                configurations,
                container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<Model>)?
            if index.isUnique {
                uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                    index,
                    subspace,
                    idExpression,
                    configurations
                )
            } else {
                uniquenessMaintainer = nil
            }
            let itemTypeSubspace = storeSubspace
                .subspace(SubspaceKey.items)
                .subspace(Model.persistableType)
            let range = itemTypeSubspace.range()
            let begin = lastProcessedKey.map { $0.appending(0) } ?? range.begin
            let storage = container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: storeSubspace.subspace(SubspaceKey.blobs)
            )
            let sequence = storage.scan(
                begin: begin,
                end: range.end,
                snapshot: false,
                limit: maximumWorkUnits + 1
            )
            var batch: [(item: Model, id: Tuple)] = []
            batch.reserveCapacity(maximumWorkUnits)
            var lastKey: ByteString?
            var hasMore = false
            var iterator = sequence.makeAsyncIterator()
            while let (key, data) = try await iterator.next() {
                if batch.count == maximumWorkUnits {
                    hasMore = true
                    continue
                }
                let item: Model = try DataAccess.deserialize(data)
                batch.append((item: item, id: try itemTypeSubspace.unpack(key)))
                lastKey = key
            }
            try await OnlineIndexBatchWriter.write(
                batch,
                index: index,
                maintainer: maintainer,
                uniquenessMaintainer: uniquenessMaintainer,
                violationTracker: UniquenessViolationTracker(
                    container: container,
                    metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
                ),
                transaction: transaction
            )
            return EntityIndexSliceResult(
                processed: UInt64(batch.count),
                lastProcessedKey: lastKey,
                hasMore: hasMore
            )
        }
    }

    private static func makeIndex(
        _ descriptor: IndexDescriptor,
        entity: String
    ) -> Index {
        Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: KeyExpressionFactory.from(
                keyPaths: descriptor.fieldNames
            ),
            subspaceKey: descriptor.name,
            itemTypes: Set([entity]),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )
    }
}

public struct EntityRuntimeRegistration: Sendable {
    public let entity: Schema.Entity
    let modelType: any Persistable.Type
    private let indexProviders: [String: EntityIndexProviderDescriptor]

    private let indexReaders: [String: IndexReader]
    private let fetchTableRowsOperation: FetchTableRows
    private let decodeModelOperation: DecodeModel
    private let updateIndexesOperation: UpdateIndexes
    private let buildIndexOperation: BuildIndex
    private let runIndexSliceOperation: RunIndexSlice

    fileprivate typealias IndexReader = @Sendable (
        _ context: DatabaseContext,
        _ selectQuery: SelectQuery,
        _ indexScan: IndexScanSource,
        _ options: ReadExecutionContext,
        _ partitions: FieldObject
    ) async throws -> IndexReadResult

    fileprivate typealias FetchTableRows = @Sendable (
        _ context: DatabaseContext,
        _ sourceName: String,
        _ selectQuery: SelectQuery,
        _ options: ReadExecutionContext
    ) async throws -> EntityTableRows

    fileprivate typealias DecodeModel = @Sendable (
        PersistedModel
    ) throws -> any Persistable

    fileprivate typealias UpdateIndexes = @Sendable (
        IndexLifecycleStore,
        UniquenessViolationTracker,
        Subspace,
        [any IndexRuntimeConfiguration],
        any WallClock,
        PersistedModel?,
        PersistedModel?,
        Tuple,
        [IndexDescriptor]?,
        String?,
        any TransactionAccess
    ) async throws -> Void

    fileprivate typealias BuildIndex = @Sendable (
        DBContainer,
        Subspace,
        Index,
        IndexLifecycleStore,
        Int,
        [any IndexRuntimeConfiguration]
    ) async throws -> Void

    fileprivate typealias RunIndexSlice = @Sendable (
        DBContainer,
        Subspace,
        Index,
        ByteString?,
        Int,
        any TransactionAccess
    ) async throws -> EntityIndexSliceResult

    fileprivate init<Model: Persistable>(
        entity: Schema.Entity,
        modelType: any Persistable.Type,
        indexReaders: [String: IndexReader],
        indexProviders: [String: EntityIndexProvider<Model>],
        decodeModel: @escaping DecodeModel,
        fetchTableRows: @escaping FetchTableRows,
        updateIndexes: @escaping UpdateIndexes,
        buildIndex: @escaping BuildIndex,
        runIndexSlice: @escaping RunIndexSlice
    ) {
        self.entity = entity
        self.modelType = modelType
        self.indexReaders = indexReaders
        self.indexProviders = indexProviders.mapValues {
            EntityIndexProviderDescriptor($0)
        }
        self.decodeModelOperation = decodeModel
        self.fetchTableRowsOperation = fetchTableRows
        self.updateIndexesOperation = updateIndexes
        self.buildIndexOperation = buildIndex
        self.runIndexSliceOperation = runIndexSlice
    }

    func executeIndexRows(
        kindIdentifier: String,
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult? {
        guard let read = indexReaders[kindIdentifier] else {
            return nil
        }
        return try await read(
            context,
            selectQuery,
            indexScan,
            options,
            partitions
        )
    }

    func hasIndexReader(for kindIdentifier: String) -> Bool {
        indexReaders[kindIdentifier] != nil
    }

    func hasIndexProvider(for kindIdentifier: String) -> Bool {
        indexProviders[kindIdentifier] != nil
    }

    func runtimeRequirements(
        for kindIdentifier: String
    ) -> IndexRuntimeRequirements? {
        indexProviders[kindIdentifier]?.runtimeRequirements
    }

    func supportsUniquenessConstraints(
        for kindIdentifier: String
    ) -> Bool? {
        indexProviders[kindIdentifier]?.supportsUniquenessConstraints
    }

    package func decode(
        _ model: PersistedModel
    ) throws -> any Persistable {
        try decodeModelOperation(model)
    }

    func fetchTableRows(
        context: DatabaseContext,
        sourceName: String,
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) async throws -> EntityTableRows {
        try await fetchTableRowsOperation(
            context,
            sourceName,
            selectQuery,
            options
        )
    }

    func updateIndexes(
        lifecycleStore: IndexLifecycleStore,
        violationTracker: UniquenessViolationTracker,
        indexSubspace: Subspace,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        id: Tuple,
        descriptors: [IndexDescriptor]? = nil,
        logicalTypeName: String? = nil,
        transaction: any TransactionAccess
    ) async throws {
        try await updateIndexesOperation(
            lifecycleStore,
            violationTracker,
            indexSubspace,
            configurations,
            wallClock,
            oldModel,
            newModel,
            id,
            descriptors,
            logicalTypeName,
            transaction
        )
    }

    func buildIndex(
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        lifecycleStore: IndexLifecycleStore,
        batchSize: Int,
        configurations: [any IndexRuntimeConfiguration]
    ) async throws {
        try await buildIndexOperation(
            container,
            storeSubspace,
            index,
            lifecycleStore,
            batchSize,
            configurations
        )
    }

    func runIndexSlice(
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        lastProcessedKey: ByteString?,
        maximumWorkUnits: Int,
        transaction: any TransactionAccess
    ) async throws -> EntityIndexSliceResult {
        try await runIndexSliceOperation(
            container,
            storeSubspace,
            index,
            lastProcessedKey,
            maximumWorkUnits,
            transaction
        )
    }
}

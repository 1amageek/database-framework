import DatabaseKit
import DatabaseTypes
import StorageKit

private protocol EntityIndexProvider<Model>: Sendable {
    associatedtype Model: Persistable

    var kindIdentifier: String { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var supportsUniquenessConstraints: Bool { get }

    func makeMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Model>

    func makeUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model>
}

private struct ModelIndependentEntityIndexProvider<
    Model: Persistable,
    Provider: IndexMaintainerProvider
>: EntityIndexProvider {
    let provider: Provider

    var kindIdentifier: String { provider.kindIdentifier }
    var runtimeRequirements: IndexRuntimeRequirements {
        provider.runtimeRequirements
    }
    var supportsUniquenessConstraints: Bool {
        provider.supportsUniquenessConstraints
    }

    func makeMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Model> {
        try provider.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            wallClock: wallClock
        )
    }

    func makeUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model> {
        try provider.makeIndexUniquenessMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations
        )
    }
}

private struct ModelSpecificEntityIndexProvider<
    Provider: EntityIndexMaintainerProvider
>: EntityIndexProvider {
    typealias Model = Provider.Model

    let provider: Provider

    var kindIdentifier: String { provider.kindIdentifier }
    var runtimeRequirements: IndexRuntimeRequirements {
        provider.runtimeRequirements
    }
    var supportsUniquenessConstraints: Bool {
        provider.supportsUniquenessConstraints
    }

    func makeMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Model> {
        try provider.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            wallClock: wallClock
        )
    }

    func makeUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model> {
        try provider.makeIndexUniquenessMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations
        )
    }
}

private struct EntityIndexProviderDescriptor: Sendable {
    let runtimeRequirements: IndexRuntimeRequirements
    let supportsUniquenessConstraints: Bool

    init<Model: Persistable>(_ provider: any EntityIndexProvider<Model>) {
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
    private var indexProviders: [String: any EntityIndexProvider<Model>]

    fileprivate typealias IndexReader = @Sendable (
        _ context: DatabaseContext,
        _ selectQuery: SelectQuery,
        _ index: IndexDescriptor,
        _ indexScan: IndexScanSource,
        _ options: ReadExecutionContext,
        _ partitions: FieldObject
    ) async throws -> IndexReadResult

    public init(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor] = []
    ) throws(SchemaEntityError) {
        self.entity = try Schema.Entity(
            from: model,
            including: additionalIndexes
        )
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
            index,
            indexScan,
            options,
            partitions in
            try await executor.executeRows(
                context: context,
                selectQuery: selectQuery,
                index: index,
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
        indexProviders[provider.kindIdentifier] =
            ModelIndependentEntityIndexProvider<Model, Provider>(
                provider: provider
            )
    }

    public mutating func register<Provider: EntityIndexMaintainerProvider>(
        _ provider: Provider
    ) throws(DatabaseRuntimeConfigurationError)
    where Provider.Model == Model {
        guard indexProviders[provider.kindIdentifier] == nil else {
            throw .duplicateIndexMaintainerProvider(provider.kindIdentifier)
        }
        indexProviders[provider.kindIdentifier] =
            ModelSpecificEntityIndexProvider(provider: provider)
    }

    public consuming func registration() -> EntityRuntimeRegistration {
        let compiledIndexDescriptors = entity.indexDescriptors
        return EntityRuntimeRegistration(
            entity: entity,
            indexReaders: indexReaders,
            indexProviders: indexProviders,
            canonicalizeModel: {
                try PersistedModel($0.decode(as: Model.self))
            },
            makePersistedModel: {
                try PersistedModel(Model.decodePersistedObject($0))
            },
            resolveIdentity: {
                try EntityReferenceEncoder.encode($0.decode(as: Model.self))
            },
            fetchTableRows: {
                context,
                sourceName,
                selectQuery,
                options in
                let plan = try SelectQueryPlanner.plan(
                    selectQuery,
                    as: Model.self,
                    indexDescriptors: compiledIndexDescriptors,
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
                indexDescriptors: compiledIndexDescriptors,
                providers: indexProviders
            ),
            buildIndex: Self.makeBuildIndexOperation(
                providers: indexProviders
            ),
            runIndexSlice: Self.makeIndexSliceOperation(
                providers: indexProviders
            ),
            finalizeIndex: Self.makeIndexFinalizationOperation(
                providers: indexProviders
            )
        )
    }

    private static func makeUpdateIndexesOperation(
        entity: Schema.Entity,
        indexDescriptors: [IndexDescriptor],
        providers: [String: any EntityIndexProvider<Model>]
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
            overrideDescriptors,
            logicalTypeName,
            transaction in
            guard oldModel != nil || newModel != nil else { return }
            let typedOld = try oldModel?.decode(as: Model.self)
            let typedNew = try newModel?.decode(as: Model.self)
            let maintainedIndexes = overrideDescriptors ?? indexDescriptors
            guard !maintainedIndexes.isEmpty else { return }
            let states = try await lifecycleStore.states(
                of: maintainedIndexes.map { $0.name },
                transaction: transaction
            )
            for descriptor in maintainedIndexes {
                guard let state = states[descriptor.name] else {
                    throw IndexStateError.missingRequestedState(
                        index: descriptor.name
                    )
                }
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
                    index: index,
                    subspace: subspace,
                    idExpression: idExpression,
                    configurations: configurations,
                    wallClock: wallClock
                )
                if descriptor.isUnique, let typedNew {
                    let uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                        index: index,
                        subspace: subspace,
                        idExpression: idExpression,
                        configurations: configurations
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
        providers: [String: any EntityIndexProvider<Model>]
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
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<Model>)?
            if index.isUnique {
                uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                    index: index,
                    subspace: subspace,
                    idExpression: idExpression,
                    configurations: configurations
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
        providers: [String: any EntityIndexProvider<Model>]
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
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<Model>)?
            if index.isUnique {
                uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                    index: index,
                    subspace: subspace,
                    idExpression: idExpression,
                    configurations: configurations
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

    private static func makeIndexFinalizationOperation(
        providers: [String: any EntityIndexProvider<Model>]
    ) -> EntityRuntimeRegistration.FinalizeIndex {
        { container, storeSubspace, index, configurations, transaction in
            guard let provider = providers[index.kind.identifier] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    kindIdentifier: index.kind.identifier,
                    indexName: index.name
                )
            }
            let maintainer = try provider.makeMaintainer(
                index: index,
                subspace: storeSubspace
                    .subspace(SubspaceKey.indexes)
                    .subspace(index.subspaceKey),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: configurations,
                wallClock: container.wallClock
            )
            try await maintainer.finalizeBuild(transaction: transaction)
        }
    }
}

public struct EntityRuntimeRegistration: Sendable {
    public let entity: Schema.Entity
    private let indexProviders: [String: EntityIndexProviderDescriptor]

    private let indexReaders: [String: IndexReader]
    private let fetchTableRowsOperation: FetchTableRows
    private let canonicalizeModelOperation: CanonicalizeModel
    private let makePersistedModelOperation: MakePersistedModel
    private let resolveIdentityOperation: ResolveIdentity
    private let updateIndexesOperation: UpdateIndexes
    private let buildIndexOperation: BuildIndex
    private let runIndexSliceOperation: RunIndexSlice
    private let finalizeIndexOperation: FinalizeIndex

    fileprivate typealias IndexReader = @Sendable (
        _ context: DatabaseContext,
        _ selectQuery: SelectQuery,
        _ index: IndexDescriptor,
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

    fileprivate typealias CanonicalizeModel = @Sendable (
        PersistedModel
    ) throws -> PersistedModel

    fileprivate typealias MakePersistedModel = @Sendable (
        FieldObject
    ) throws -> PersistedModel

    fileprivate typealias ResolveIdentity = @Sendable (
        PersistedModel
    ) throws -> EntityReference

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

    fileprivate typealias FinalizeIndex = @Sendable (
        DBContainer,
        Subspace,
        Index,
        [any IndexRuntimeConfiguration],
        any TransactionAccess
    ) async throws -> Void

    fileprivate init<Model: Persistable>(
        entity: Schema.Entity,
        indexReaders: [String: IndexReader],
        indexProviders: [String: any EntityIndexProvider<Model>],
        canonicalizeModel: @escaping CanonicalizeModel,
        makePersistedModel: @escaping MakePersistedModel,
        resolveIdentity: @escaping ResolveIdentity,
        fetchTableRows: @escaping FetchTableRows,
        updateIndexes: @escaping UpdateIndexes,
        buildIndex: @escaping BuildIndex,
        runIndexSlice: @escaping RunIndexSlice,
        finalizeIndex: @escaping FinalizeIndex
    ) {
        self.entity = entity
        self.indexReaders = indexReaders
        self.indexProviders = indexProviders.mapValues {
            EntityIndexProviderDescriptor($0)
        }
        self.canonicalizeModelOperation = canonicalizeModel
        self.makePersistedModelOperation = makePersistedModel
        self.resolveIdentityOperation = resolveIdentity
        self.fetchTableRowsOperation = fetchTableRows
        self.updateIndexesOperation = updateIndexes
        self.buildIndexOperation = buildIndex
        self.runIndexSliceOperation = runIndexSlice
        self.finalizeIndexOperation = finalizeIndex
    }

    func executeIndexRows(
        index: IndexDescriptor,
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult? {
        guard entity.indexDescriptors.contains(index) else {
            throw CanonicalReadError.indexHintNotFound(
                "Index '\(index.name)' is not declared by entity '\(entity.name)'"
            )
        }
        guard index.name == indexScan.indexName,
              index.kindIdentifier == indexScan.kindIdentifier else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Index access path does not match the validated schema descriptor"
            )
        }
        guard let read = indexReaders[index.kindIdentifier] else {
            return nil
        }
        return try await read(
            context,
            selectQuery,
            index,
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

    package func canonicalized(
        _ model: PersistedModel
    ) throws -> PersistedModel {
        try canonicalizeModelOperation(model)
    }

    /// Constructs the registered persisted model through its compiled field
    /// adaptation without retaining a concrete model instance.
    public func persistedModel(
        from object: FieldObject
    ) throws -> PersistedModel {
        try makePersistedModelOperation(object)
    }

    package func identity(
        for model: PersistedModel
    ) throws -> EntityReference {
        try resolveIdentityOperation(model)
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

    func finalizeIndex(
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        configurations: [any IndexRuntimeConfiguration],
        transaction: any TransactionAccess
    ) async throws {
        try await finalizeIndexOperation(
            container,
            storeSubspace,
            index,
            configurations,
            transaction
        )
    }
}

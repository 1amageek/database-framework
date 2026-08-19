import DatabaseKit
import DatabaseTypes
import StorageKit

private protocol EntityIndexProvider: Sendable {
    var indexType: IndexType { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var supportsUniquenessConstraints: Bool { get }

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout

    func makeMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel>

    func makeUniquenessMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel>
}

private struct ModelIndependentEntityIndexProvider<
    Provider: IndexMaintainerProvider
>: EntityIndexProvider {
    let provider: Provider

    var indexType: IndexType { provider.indexType }
    var runtimeRequirements: IndexRuntimeRequirements {
        provider.runtimeRequirements
    }
    var supportsUniquenessConstraints: Bool {
        provider.supportsUniquenessConstraints
    }

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        try provider.physicalLayout(
            for: index,
            configurations: configurations
        )
    }

    func makeMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel> {
        try provider.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            wallClock: wallClock
        )
    }

    func makeUniquenessMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel> {
        try provider.makeIndexUniquenessMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations
        )
    }
}

private struct CanonicalEntityIndexProvider<
    Provider: CanonicalEntityIndexMaintainerProvider
>: EntityIndexProvider {
    let provider: Provider

    var indexType: IndexType { provider.indexType }
    var runtimeRequirements: IndexRuntimeRequirements {
        provider.runtimeRequirements
    }
    var supportsUniquenessConstraints: Bool {
        provider.supportsUniquenessConstraints
    }

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        try provider.physicalLayout(
            for: index,
            configurations: configurations
        )
    }

    func makeMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel> {
        try provider.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            wallClock: wallClock
        )
    }

    func makeUniquenessMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: any KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel> {
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
    let resolvePhysicalLayout:
        @Sendable (
            ResolvedIndex,
            [any IndexRuntimeConfiguration]
        ) throws -> IndexPhysicalLayout

    init(_ provider: any EntityIndexProvider) {
        self.runtimeRequirements = provider.runtimeRequirements
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
        self.resolvePhysicalLayout = { index, configurations in
            try provider.physicalLayout(
                for: index,
                configurations: configurations
            )
        }
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
    let pageWindowPushed: Bool
    let continuationPosition: ByteString?
    let stableSnapshotQueryFingerprint: ByteString?
}

public struct EntityRuntimeDefinition: Sendable {
    public let entity: Schema.Entity

    private var indexReaders: [IndexType: IndexReader]
    private var indexProviders: [IndexType: any EntityIndexProvider]
    private let canonicalizeModel: EntityRuntimeRegistration.CanonicalizeModel
    private let makePersistedModel: EntityRuntimeRegistration.MakePersistedModel
    private let resolveIdentity: EntityRuntimeRegistration.ResolveIdentity
    private let fetchTableRows: EntityRuntimeRegistration.FetchTableRows

    fileprivate typealias IndexReader = @Sendable (
        _ context: DatabaseContext,
        _ selectQuery: SelectQuery,
        _ index: IndexDescriptor,
        _ indexScan: IndexScanSource,
        _ options: ReadExecutionContext,
        _ partitions: FieldObject
    ) async throws -> IndexReadResult

    public init<Model: Persistable>(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor] = []
    ) throws(SchemaEntityError) {
        let entity = try Schema.Entity(
            from: model,
            including: additionalIndexes
        )
        let compiledIndexDescriptors = entity.indexDescriptors
        self.entity = entity
        self.indexReaders = [:]
        self.indexProviders = [:]
        self.canonicalizeModel = {
            try PersistedModel($0.decode(as: Model.self))
        }
        self.makePersistedModel = {
            try PersistedModel(Model.decodePersistedObject($0))
        }
        self.resolveIdentity = {
            try EntityReferenceEncoder.encode($0.decode(as: Model.self))
        }
        self.fetchTableRows = {
            context,
            sourceName,
            selectQuery,
            options,
            transaction in
            let plan = try SelectQueryPlanner.plan(
                selectQuery,
                as: Model.self,
                indexDescriptors: compiledIndexDescriptors,
                options: options
            )
            let items: [Model]
            if let transaction {
                items = try await context.fetch(
                    plan.typedQuery,
                    transaction: transaction
                )
            } else {
                items = try await context.fetch(plan.typedQuery)
            }
            var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .resultMaterialization,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self),
                expectedCount: items.count
            )
            for item in items {
                try options.workMeter.consume(at: .resultMaterialization)
                let row = try QueryRowCodec.encode(item)
                let sourceRow = CanonicalSourceRow.fromBaseFields(
                    row.fields,
                    sourceName: sourceName,
                    annotations: row.annotations,
                    version: row.version
                )
                try retainedRows.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: sourceRow,
                        workMeter: options.workMeter
                    ),
                    make: { sourceRow }
                )
            }
            let rows = retainedRows.finish().promoteToOutput()
            let continuationPosition: ByteString?
            if let visiblePageSize = plan.visiblePageSize,
               items.count > visiblePageSize,
               visiblePageSize > 0 {
                continuationPosition = try PersistableIdentifierKeyCodec
                    .tuple(for: items[visiblePageSize - 1])
                    .pack()
            } else {
                continuationPosition = nil
            }
            return EntityTableRows(
                rows: rows,
                residualFilter: plan.residualFilter,
                residualOrderBy: plan.residualOrderBy,
                limitPushed: plan.limitPushed,
                offsetPushed: plan.offsetPushed,
                pageWindowPushed: plan.pageWindowPushed,
                continuationPosition: continuationPosition,
                stableSnapshotQueryFingerprint:
                    plan.stableSnapshotQueryFingerprint
            )
        }
    }

    /// Builds the canonical runtime for an entity loaded from a schema
    /// manifest. No synthetic `Persistable` type is introduced.
    public init(schemaDriven entity: Schema.Entity) {
        self.entity = entity
        self.indexReaders = [:]
        self.indexProviders = [:]
        self.canonicalizeModel = { model in
            try Self.canonicalModel(model, entity: entity)
        }
        self.makePersistedModel = { object in
            try Self.canonicalModel(
                try Self.persistedModel(from: object, entity: entity),
                entity: entity
            )
        }
        self.resolveIdentity = { model in
            try Self.identity(
                for: try Self.canonicalModel(model, entity: entity),
                entity: entity
            )
        }
        self.fetchTableRows = {
            context,
            sourceName,
            selectQuery,
            options,
            transaction in
            try await Self.fetchSchemaDrivenRows(
                entity: entity,
                context: context,
                sourceName: sourceName,
                selectQuery: selectQuery,
                options: options,
                transaction: transaction
            )
        }
    }

    public mutating func register<Executor: IndexReadExecutor>(
        _ executor: Executor
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexReaders[executor.indexType] == nil else {
            throw .duplicateIndexReadExecutor(executor.indexType)
        }
        let registeredEntity = entity
        indexReaders[executor.indexType] = {
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
                entity: registeredEntity,
                options: options,
                partitions: partitions
            )
        }
    }

    public mutating func register<Provider: IndexMaintainerProvider>(
        _ provider: Provider
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexProviders[provider.indexType] == nil else {
            throw .duplicateIndexMaintainerProvider(provider.indexType)
        }
        indexProviders[provider.indexType] =
            ModelIndependentEntityIndexProvider<Provider>(
                provider: provider
            )
    }

    public mutating func register<Provider: CanonicalEntityIndexMaintainerProvider>(
        _ provider: Provider
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexProviders[provider.indexType] == nil else {
            throw .duplicateIndexMaintainerProvider(provider.indexType)
        }
        indexProviders[provider.indexType] =
            CanonicalEntityIndexProvider(provider: provider)
    }

    public consuming func registration() -> EntityRuntimeRegistration {
        let compiledIndexDescriptors = entity.indexDescriptors
        return EntityRuntimeRegistration(
            entity: entity,
            indexReaders: indexReaders,
            indexProviders: indexProviders,
            canonicalizeModel: canonicalizeModel,
            makePersistedModel: makePersistedModel,
            resolveIdentity: resolveIdentity,
            fetchTableRows: fetchTableRows,
            updateIndexes: Self.makeUpdateIndexesOperation(
                entity: entity,
                indexDescriptors: compiledIndexDescriptors,
                providers: indexProviders
            ),
            buildIndex: Self.makeBuildIndexOperation(
                entity: entity,
                providers: indexProviders,
                canonicalizeModel: canonicalizeModel
            ),
            runIndexSlice: Self.makeIndexSliceOperation(
                entity: entity,
                providers: indexProviders,
                canonicalizeModel: canonicalizeModel
            ),
            finalizeIndex: Self.makeIndexFinalizationOperation(
                providers: indexProviders
            )
        )
    }

    private static func canonicalModel(
        _ model: PersistedModel,
        entity: Schema.Entity
    ) throws -> PersistedModel {
        guard model.entity == entity.name else {
            throw SchemaDrivenEntityRuntimeError.entityMismatch(
                expected: entity.name,
                actual: model.entity
            )
        }
        for field in model.fields where entity.fieldMapByName[field.name] == nil {
            throw SchemaDrivenEntityRuntimeError.unknownField(
                entity: entity.name,
                field: field.name
            )
        }

        var canonicalFields: [PersistableField] = []
        canonicalFields.reserveCapacity(entity.fields.count)
        for schema in entity.fields.sorted(by: {
            ($0.fieldNumber, $0.name) < ($1.fieldNumber, $1.name)
        }) {
            guard schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw SchemaDrivenEntityRuntimeError.invalidFieldNumber(
                    entity: entity.name,
                    field: schema.name,
                    number: schema.fieldNumber
                )
            }
            let value: FieldValue
            if let persisted = model.value(forFieldNamed: schema.name) {
                value = persisted
            } else if schema.isOptional {
                value = .null
            } else {
                throw SchemaDrivenEntityRuntimeError.missingRequiredField(
                    entity: entity.name,
                    field: schema.name
                )
            }
            try validate(value, schema: schema, entity: entity.name)
            canonicalFields.append(
                try PersistableField(
                    number: number,
                    name: schema.name,
                    value: value
                )
            )
        }
        return try PersistedModel(
            entity: entity.name,
            fields: canonicalFields
        )
    }

    private static func persistedModel(
        from object: FieldObject,
        entity: Schema.Entity
    ) throws -> PersistedModel {
        var fields: [PersistableField] = []
        fields.reserveCapacity(object.count)
        for field in object.fields {
            guard let schema = entity.fieldMapByName[field.key] else {
                throw SchemaDrivenEntityRuntimeError.unknownField(
                    entity: entity.name,
                    field: field.key
                )
            }
            guard schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw SchemaDrivenEntityRuntimeError.invalidFieldNumber(
                    entity: entity.name,
                    field: schema.name,
                    number: schema.fieldNumber
                )
            }
            fields.append(
                try PersistableField(
                    number: number,
                    name: schema.name,
                    value: field.value
                )
            )
        }
        return try PersistedModel(entity: entity.name, fields: fields)
    }

    private static func validate(
        _ value: FieldValue,
        schema: FieldSchema,
        entity: String
    ) throws {
        if value == .null {
            guard schema.isOptional else {
                throw SchemaDrivenEntityRuntimeError.nullRequiredField(
                    entity: entity,
                    field: schema.name
                )
            }
            return
        }
        if schema.isArray {
            guard case .array(let values) = value,
                  values.allSatisfy({
                      $0 != .null
                          && FieldSchemaValueValidator.accepts(
                              $0,
                              as: schema.type
                          )
                  }) else {
                throw SchemaDrivenEntityRuntimeError.invalidFieldValue(
                    entity: entity,
                    field: schema.name,
                    expected: schema.type
                )
            }
            return
        }
        guard FieldSchemaValueValidator.accepts(value, as: schema.type) else {
            throw SchemaDrivenEntityRuntimeError.invalidFieldValue(
                entity: entity,
                field: schema.name,
                expected: schema.type
            )
        }
    }

    private static func identity(
        for model: PersistedModel,
        entity: Schema.Entity
    ) throws -> EntityReference {
        let identifier: ReferenceIdentifier
        do {
            guard let value = model.value(forFieldNamed: "id") else {
                throw SchemaDrivenEntityRuntimeError.invalidIdentifier(
                    entity: entity.name
                )
            }
            identifier = try referenceIdentifier(
                from: value,
                expectedType: entity.identifierType,
                entity: entity.name
            )
            try PersistableIdentifierValidator.validate(
                identifier,
                as: entity.identifierType
            )
        } catch {
            throw SchemaDrivenEntityRuntimeError.invalidIdentifier(
                entity: entity.name
            )
        }
        var partitions: [(key: String, value: FieldValue)] = []
        partitions.reserveCapacity(entity.dynamicFieldNames.count)
        for fieldName in entity.dynamicFieldNames {
            guard let schema = entity.fieldMapByName[fieldName],
                  let value = model.value(forFieldNamed: fieldName),
                  value != .null,
                  !schema.isOptional,
                  !schema.isArray,
                  FieldSchemaValueValidator.accepts(
                      value,
                      as: schema.type
                  ) else {
                throw SchemaDrivenEntityRuntimeError.invalidPartition(
                    entity: entity.name,
                    field: fieldName
                )
            }
            partitions.append((key: fieldName, value: value))
        }
        return try EntityReference(
            entity: entity.name,
            id: identifier,
            partitions: FieldObject(partitions)
        )
    }

    private static func referenceIdentifier(
        from value: FieldValue,
        expectedType: PersistableIdentifierType,
        entity: String
    ) throws -> ReferenceIdentifier {
        switch (value, expectedType) {
        case (.bool(let value), .bool):
            return .bool(value)
        case (.int8(let value), .int8):
            return .int8(value)
        case (.int16(let value), .int16):
            return .int16(value)
        case (.int32(let value), .int32):
            return .int32(value)
        case (.int64(let value), .int64):
            return .int64(value)
        case (.uint8(let value), .uint8):
            return .uint8(value)
        case (.uint16(let value), .uint16):
            return .uint16(value)
        case (.uint32(let value), .uint32):
            return .uint32(value)
        case (.uint64(let value), .uint64):
            return .uint64(value)
        case (.string(let value), .string):
            return .string(value)
        case (.bytes(let value), .bytes):
            return .bytes(value)
        case (.uuid(let value), .uuid):
            return .uuid(value)
        case (
            .array(let values),
            .composite(let expectedComponents)
        ) where values.count == expectedComponents.count:
            var components: [ReferenceIdentifier] = []
            components.reserveCapacity(values.count)
            for index in values.indices {
                components.append(
                    try referenceIdentifier(
                        from: values[index],
                        expectedType: expectedComponents[index],
                        entity: entity
                    )
                )
            }
            return .composite(components)
        default:
            throw SchemaDrivenEntityRuntimeError.invalidIdentifier(
                entity: entity
            )
        }
    }

    private static func fetchSchemaDrivenRows(
        entity: Schema.Entity,
        context: DatabaseContext,
        sourceName: String,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)?
    ) async throws -> EntityTableRows {
        guard case .table(let table) = selectQuery.source,
              table.table == entity.name else {
            throw CanonicalReadError.unsupportedSource(
                "Schema-driven runtime expected table '\(entity.name)'"
            )
        }
        let stablePageWindow = try schemaDrivenStablePageWindow(
            selectQuery: selectQuery,
            options: options
        )
        let budgetReadLimit = try options.workMeter.storageReadLimitWithSentinel(
            at: .storageRow
        )
        let readLimit = min(
            stablePageWindow?.fetchLimit ?? budgetReadLimit,
            budgetReadLimit
        )
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        let models = try await context.scanPersistedModels(
            entity: entity,
            partitions: table.partitions,
            limit: readLimit,
            offset: stablePageWindow?.storageOffset ?? 0,
            startingAfterIdentifier:
                stablePageWindow?.startingAfterIdentifier,
            workMeter: options.workMeter,
            configuration: execution.transactionConfiguration,
            transaction: transaction
        )
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: models.count
        )
        for model in models {
            try options.workMeter.consume(at: .resultMaterialization)
            let row = try QueryRowCodec.encode(model)
            let sourceRow = CanonicalSourceRow.fromBaseFields(
                row.fields,
                sourceName: sourceName,
                annotations: row.annotations,
                version: row.version
            )
            try retainedRows.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: sourceRow,
                    workMeter: options.workMeter
                ),
                make: { sourceRow }
            )
        }
        let rows = retainedRows.finish().promoteToOutput()
        let continuationPosition: ByteString?
        if let stablePageWindow,
           models.count > stablePageWindow.visibleCount,
           stablePageWindow.visibleCount > 0 {
            let identity = try identity(
                for: models[stablePageWindow.visibleCount - 1],
                entity: entity
            )
            continuationPosition = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: entity.identifierType
            ).pack()
        } else {
            continuationPosition = nil
        }
        return EntityTableRows(
            rows: rows,
            residualFilter: stablePageWindow == nil ? selectQuery.filter : nil,
            residualOrderBy: stablePageWindow == nil ? selectQuery.orderBy : nil,
            limitPushed: false,
            offsetPushed: false,
            pageWindowPushed: stablePageWindow != nil,
            continuationPosition: continuationPosition,
            stableSnapshotQueryFingerprint:
                stablePageWindow?.queryFingerprint
        )
    }

    private struct SchemaDrivenStablePageWindow {
        let queryFingerprint: ByteString
        let storageOffset: Int
        let fetchLimit: Int
        let visibleCount: Int
        let startingAfterIdentifier: ByteString?
    }

    private static func schemaDrivenStablePageWindow(
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) throws -> SchemaDrivenStablePageWindow? {
        guard options.options.continuationSnapshotIsStable,
              selectQuery.filter == nil,
              selectQuery.orderBy?.isEmpty ?? true,
              !selectQuery.distinct,
              !isCountProjection(selectQuery.projection),
              let pageSize = try options.resolvePageSize() else {
            return nil
        }
        let cursor = try CanonicalQueryPagination
            .validatedStableSnapshotCursor(
                selectQuery: selectQuery,
                options: options
            )
        guard options.continuation == nil || cursor.storagePosition != nil else {
            return nil
        }
        guard let queryOffset = Int(exactly: selectQuery.offset ?? 0) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Query offset exceeds the platform integer range"
            )
        }
        let remainingLimit: Int?
        if let limit = selectQuery.limit {
            guard let limit = Int(exactly: limit) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Query limit exceeds the platform integer range"
                )
            }
            remainingLimit = max(0, limit - cursor.offset)
        } else {
            remainingLimit = nil
        }
        let visibleCount = min(pageSize, remainingLimit ?? pageSize)
        let wantsLookahead = remainingLimit.map { $0 > visibleCount } ?? true
        let (fetchLimit, overflow) = visibleCount.addingReportingOverflow(
            wantsLookahead ? 1 : 0
        )
        return SchemaDrivenStablePageWindow(
            queryFingerprint: cursor.queryFingerprint,
            storageOffset: cursor.storagePosition == nil ? queryOffset : 0,
            fetchLimit: max(1, overflow ? Int.max : fetchLimit),
            visibleCount: visibleCount,
            startingAfterIdentifier: cursor.storagePosition
        )
    }

    private static func isCountProjection(_ projection: Projection) -> Bool {
        guard case .items(let items) = projection, items.count == 1,
              case .aggregate(.count) = items[0].expression else {
            return false
        }
        return true
    }

}

private protocol EntityRuntimeIndexOperationBuilding {}

extension EntityRuntimeDefinition: EntityRuntimeIndexOperationBuilding {}

extension EntityRuntimeIndexOperationBuilding {

    fileprivate static func makeUpdateIndexesOperation(
        entity: Schema.Entity,
        indexDescriptors: [IndexDescriptor],
        providers: [IndexType: any EntityIndexProvider]
    ) -> EntityRuntimeRegistration.UpdateIndexes {
        {
            lifecycleStore,
            violationTracker,
            configurations,
            wallClock,
            oldModel,
            newModel,
            id,
            overrideDescriptors,
            logicalTypeName,
            transaction in
            guard oldModel != nil || newModel != nil else { return }
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
                guard let provider = providers[descriptor.type] else {
                    throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                        indexType: descriptor.type,
                        indexName: descriptor.name
                    )
                }
                let index = makeCanonicalEntityIndex(
                    descriptor,
                    entity: logicalTypeName ?? entity.name
                )
                let subspace = try lifecycleStore.indexSubspace(
                    for: descriptor.name)
                let idExpression = TupleKeyExpression(value: id)
                let maintainer = try provider.makeMaintainer(
                    index: index,
                    subspace: subspace,
                    idExpression: idExpression,
                    configurations: configurations,
                    wallClock: wallClock
                )
                if descriptor.isUnique, let newModel {
                    let uniquenessMaintainer = try provider.makeUniquenessMaintainer(
                        index: index,
                        subspace: subspace,
                        idExpression: idExpression,
                        configurations: configurations
                    )
                    try await IndexUniquenessConstraint.enforce(
                        index: index,
                        item: newModel,
                        id: id,
                        state: state,
                        maintainer: uniquenessMaintainer,
                        violationTracker: violationTracker,
                        transaction: transaction
                    )
                }
                try await maintainer.updateIndex(
                    oldItem: oldModel,
                    newItem: newModel,
                    transaction: transaction
                )
            }
        }
    }

    fileprivate static func makeBuildIndexOperation(
        entity: Schema.Entity,
        providers: [IndexType: any EntityIndexProvider],
        canonicalizeModel: @escaping EntityRuntimeRegistration.CanonicalizeModel
    ) -> EntityRuntimeRegistration.BuildIndex {
        {
            container,
            storeSubspace,
            index,
            lifecycleStore,
            batchSize,
            configurations in
            guard let provider = providers[index.type] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    indexType: index.type,
                    indexName: index.name
                )
            }
            let subspace = try lifecycleStore.indexSubspace(
                for: index.name
            )
            let idExpression = FieldKeyExpression(fieldName: "id")
            let maintainer = try provider.makeMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<PersistedModel>)?
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
            let indexer = try OnlineIndexer<PersistedModel>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: entity.name,
                index: index,
                indexMaintainer: maintainer,
                uniquenessMaintainer: uniquenessMaintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: batchSize,
                decodeItem: {
                    try canonicalizeModel(
                        DataAccess.deserializePersistedModel(
                            $0,
                            expectedEntity: entity.name
                        )
                    )
                }
            )
            try await indexer.buildIndex(clearFirst: false)
        }
    }

    fileprivate static func makeIndexSliceOperation(
        entity: Schema.Entity,
        providers: [IndexType: any EntityIndexProvider],
        canonicalizeModel: @escaping EntityRuntimeRegistration.CanonicalizeModel
    ) -> EntityRuntimeRegistration.RunIndexSlice {
        {
            container,
            storeSubspace,
            index,
            lastProcessedKey,
            maximumWorkUnits,
            transaction in
            guard let provider = providers[index.type] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    indexType: index.type,
                    indexName: index.name
                )
            }
            let subspace = try IndexLifecycleStore(
                container: container,
                subspace: storeSubspace
            ).indexSubspace(for: index.name)
            let idExpression = FieldKeyExpression(fieldName: "id")
            let configurations = container.runtimeConfiguration
                .indexConfigurations(named: index.name)
            let maintainer = try provider.makeMaintainer(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                configurations: configurations,
                wallClock: container.wallClock
            )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<PersistedModel>)?
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
                .subspace(entity.name)
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
            // Work units bound the total slice, while this fixed batch bounds
            // retained decoded models. The two limits have different ownership
            // and must not be reused as one allocation capacity.
            let retainedBatchLimit = min(maximumWorkUnits, 256)
            var batch: [(item: PersistedModel, id: Tuple)] = []
            batch.reserveCapacity(retainedBatchLimit)
            var lastKey: ByteString?
            var hasMore = false
            var processed = 0
            let violationTracker = UniquenessViolationTracker(
                container: container,
                metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
            )
            var iterator = sequence.makeAsyncIterator()
            while let (key, data) = try await iterator.next() {
                if processed == maximumWorkUnits {
                    hasMore = true
                    break
                }
                let item = try canonicalizeModel(
                    DataAccess.deserializePersistedModel(
                        data,
                        expectedEntity: entity.name
                    )
                )
                batch.append((item: item, id: try itemTypeSubspace.unpack(key)))
                lastKey = key
                processed += 1
                if batch.count == retainedBatchLimit {
                    try await OnlineIndexBatchWriter.write(
                        batch,
                        index: index,
                        maintainer: maintainer,
                        uniquenessMaintainer: uniquenessMaintainer,
                        violationTracker: violationTracker,
                        transaction: transaction
                    )
                    batch.removeAll(keepingCapacity: true)
                }
            }
            if !batch.isEmpty {
                try await OnlineIndexBatchWriter.write(
                    batch,
                    index: index,
                    maintainer: maintainer,
                    uniquenessMaintainer: uniquenessMaintainer,
                    violationTracker: violationTracker,
                    transaction: transaction
                )
            }
            return EntityIndexSliceResult(
                processed: UInt64(processed),
                lastProcessedKey: lastKey,
                hasMore: hasMore
            )
        }
    }

    fileprivate static func makeIndexFinalizationOperation(
        providers: [IndexType: any EntityIndexProvider]
    ) -> EntityRuntimeRegistration.FinalizeIndex {
        { container, storeSubspace, index, configurations, transaction in
            guard let provider = providers[index.type] else {
                throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                    indexType: index.type,
                    indexName: index.name
                )
            }
            let maintainer = try provider.makeMaintainer(
                index: index,
                subspace: try IndexLifecycleStore(
                    container: container,
                    subspace: storeSubspace
                ).indexSubspace(for: index.name),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: configurations,
                wallClock: container.wallClock
            )
            try await maintainer.finalizeBuild(transaction: transaction)
        }
    }
}

private func makeCanonicalEntityIndex(
    _ descriptor: IndexDescriptor,
    entity: String
) -> ResolvedIndex {
    ResolvedIndex(
        descriptor: descriptor,
        rootExpression: KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        ),
        itemTypes: Set([entity]),
    )
}

public struct EntityRuntimeRegistration: Sendable {
    public let entity: Schema.Entity
    private let indexProviders: [IndexType: EntityIndexProviderDescriptor]

    private let indexReaders: [IndexType: IndexReader]
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
        _ options: ReadExecutionContext,
        _ transaction: (any TransactionAccess)?
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
            ResolvedIndex,
            IndexLifecycleStore,
        Int,
        [any IndexRuntimeConfiguration]
    ) async throws -> Void

    fileprivate typealias RunIndexSlice = @Sendable (
        DBContainer,
            Subspace,
            ResolvedIndex,
            ByteString?,
        Int,
        any TransactionAccess
    ) async throws -> EntityIndexSliceResult

    fileprivate typealias FinalizeIndex = @Sendable (
        DBContainer,
            Subspace,
            ResolvedIndex,
            [any IndexRuntimeConfiguration],
        any TransactionAccess
    ) async throws -> Void

    fileprivate init(
        entity: Schema.Entity,
        indexReaders: [IndexType: IndexReader],
        indexProviders: [IndexType: any EntityIndexProvider],
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
            index.type == indexScan.indexType
        else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Index access path does not match the validated schema descriptor"
            )
        }
        guard let read = indexReaders[index.type] else {
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

    func hasIndexReader(for indexType: IndexType) -> Bool {
        indexReaders[indexType] != nil
    }

    func hasIndexProvider(for indexType: IndexType) -> Bool {
        indexProviders[indexType] != nil
    }

    func runtimeRequirements(
        for indexType: IndexType
    ) -> IndexRuntimeRequirements? {
        indexProviders[indexType]?.runtimeRequirements
    }

    func supportsUniquenessConstraints(
        for indexType: IndexType
    ) -> Bool? {
        indexProviders[indexType]?.supportsUniquenessConstraints
    }

    package func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        guard let provider = indexProviders[index.type] else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                indexType: index.type,
                indexName: index.name
            )
        }
        return try provider.resolvePhysicalLayout(
            index,
            configurations
        )
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

    @_spi(DatabaseExecution)
    public func identity(
        for model: PersistedModel
    ) throws -> EntityReference {
        try resolveIdentityOperation(model)
    }

    func fetchTableRows(
        context: DatabaseContext,
        sourceName: String,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> EntityTableRows {
        try await fetchTableRowsOperation(
            context,
            sourceName,
            selectQuery,
            options,
            transaction
        )
    }

    func updateIndexes(
        lifecycleStore: IndexLifecycleStore,
        violationTracker: UniquenessViolationTracker,
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
        index: ResolvedIndex,
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
        index: ResolvedIndex,
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
        index: ResolvedIndex,
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

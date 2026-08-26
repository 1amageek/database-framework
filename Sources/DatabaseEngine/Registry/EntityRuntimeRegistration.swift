@_spi(DatabaseExecution) import DatabaseKit
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
    let rows: DatabaseSharedRetainedArray<CanonicalSourceRow>
    let residualFilter: DatabaseKit.Expression?
    let residualOrderBy: [SortKey]?
    let limitPushed: Bool
    let offsetPushed: Bool
    let pageWindowPushed: Bool
    let continuationPosition: ByteString?
    let stableSnapshotQueryFingerprint: ByteString?
}

fileprivate struct EntityIndexReadRuntime: Sendable {
    let additionalRequiredFieldNames: @Sendable (
        IndexScanSource
    ) throws -> Set<String>
    let execute: @Sendable (
        DatabaseReadSession,
        SelectQuery,
        IndexDescriptor,
        IndexScanSource,
        ReadExecutionContext,
        FieldObject
    ) async throws -> IndexReadResult
}

public struct EntityRuntimeDefinition: Sendable {
    public let entity: Schema.Entity

    private var indexReaders: [IndexType: EntityIndexReadRuntime]
    private var indexProviders: [IndexType: any EntityIndexProvider]
    private let canonicalizeStoredModel:
        EntityRuntimeRegistration.CanonicalizeStoredModel
    private let canonicalizeModel: EntityRuntimeRegistration.CanonicalizeModel
    private let makePersistedModel: EntityRuntimeRegistration.MakePersistedModel
    private let resolveIdentity: EntityRuntimeRegistration.ResolveIdentity
    private let fetchTableRows: EntityRuntimeRegistration.FetchTableRows

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
        let canonicalSchemas = entity.fields.sorted(by: {
            ($0.fieldNumber, $0.name) < ($1.fieldNumber, $1.name)
        })
        self.canonicalizeStoredModel = { model, reservation, workMeter, stage in
            try Self.canonicalStoredModel(
                model,
                entity: entity,
                canonicalSchemas: canonicalSchemas,
                reservation: reservation,
                workMeter: workMeter,
                stage: stage
            )
        }
        self.canonicalizeModel = { source in
            let canonicalModel = try Self.canonicalModel(
                source,
                entity: entity,
                canonicalSchemas: canonicalSchemas
            )
            return try PersistedModel(
                canonicalModel.decode(as: Model.self)
            )
        }
        self.makePersistedModel = {
            try PersistedModel(Model.decodePersistedObject($0))
        }
        self.resolveIdentity = { source in
            let canonicalModel = try Self.canonicalModel(
                source,
                entity: entity,
                canonicalSchemas: canonicalSchemas
            )
            return try EntityReferenceEncoder.encode(
                canonicalModel.decode(as: Model.self)
            )
        }
        self.fetchTableRows = {
            context,
            sourceName,
            selectQuery,
            authorizationRequirement,
            options,
            transaction in
            let plan = try SelectQueryPlanner.plan(
                selectQuery,
                as: Model.self,
                indexDescriptors: compiledIndexDescriptors,
                options: options
            )
            let models = try await context.fetchCanonicalPersistedModels(
                plan.typedQuery,
                transaction: transaction,
                listRequirement: authorizationRequirement,
                workMeter: options.workMeter
            )
            let rows = try Self.canonicalTableRows(
                from: models,
                sourceName: sourceName,
                workMeter: options.workMeter
            )
            let continuationPosition: ByteString?
            if let visiblePageSize = plan.visiblePageSize,
               models.count > visiblePageSize,
               visiblePageSize > 0 {
                var resolved: ByteString?
                try models.withEntry(at: visiblePageSize - 1) {
                    entry in
                    guard let entry else {
                        throw CanonicalReadError.unsupportedAccessPath(
                            "Canonical table fetch retained a missing model"
                        )
                    }
                    try entry.withModel { model in
                        let identity = try Self.identity(
                            for: model,
                            entity: entity
                        )
                        resolved = try PersistableIdentifierKeyCodec.tuple(
                            for: identity,
                            expectedType: entity.identifierType
                        ).pack()
                    }
                }
                continuationPosition = resolved
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
        let canonicalSchemas = entity.fields.sorted(by: {
            ($0.fieldNumber, $0.name) < ($1.fieldNumber, $1.name)
        })
        self.entity = entity
        self.indexReaders = [:]
        self.indexProviders = [:]
        self.canonicalizeStoredModel = { model, reservation, workMeter, stage in
            try Self.canonicalStoredModel(
                model,
                entity: entity,
                canonicalSchemas: canonicalSchemas,
                reservation: reservation,
                workMeter: workMeter,
                stage: stage
            )
        }
        self.canonicalizeModel = { model in
            try Self.canonicalModel(
                model,
                entity: entity,
                canonicalSchemas: canonicalSchemas
            )
        }
        self.makePersistedModel = { object in
            try Self.canonicalModel(
                try Self.persistedModel(from: object, entity: entity),
                entity: entity,
                canonicalSchemas: canonicalSchemas
            )
        }
        self.resolveIdentity = { model in
            try Self.identity(
                for: try Self.canonicalModel(
                    model,
                    entity: entity,
                    canonicalSchemas: canonicalSchemas
                ),
                entity: entity
            )
        }
        self.fetchTableRows = {
            context,
            sourceName,
            selectQuery,
            authorizationRequirement,
            options,
            transaction in
            try await Self.fetchSchemaDrivenRows(
                entity: entity,
                context: context,
                sourceName: sourceName,
                selectQuery: selectQuery,
                authorizationRequirement: authorizationRequirement,
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
        indexReaders[executor.indexType] = EntityIndexReadRuntime(
            additionalRequiredFieldNames: { indexScan in
                try executor.additionalRequiredFieldNames(
                    indexScan: indexScan
                )
            },
            execute: {
                session,
                selectQuery,
                index,
                indexScan,
                options,
                partitions in
                try await executor.executeRows(
                    session: session,
                    selectQuery: selectQuery,
                    index: index,
                    indexScan: indexScan,
                    entity: registeredEntity,
                    options: options,
                    partitions: partitions
                )
            }
        )
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
            canonicalizeStoredModel: canonicalizeStoredModel,
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
        entity: Schema.Entity,
        canonicalSchemas: [FieldSchema]? = nil
    ) throws -> PersistedModel {
        guard model.entity == entity.name else {
            throw SchemaDrivenEntityRuntimeError.entityMismatch(
                expected: entity.name,
                actual: model.entity
            )
        }
        for field in model.fields {
            try validateSourceFieldIdentity(field, entity: entity)
        }

        var canonicalFields: [PersistableField] = []
        canonicalFields.reserveCapacity(entity.fields.count)
        for schema in canonicalSchemas ?? entity.fields.sorted(by: {
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
            } else if let defaultValue = schema.defaultValue {
                value = defaultValue
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

    private struct CanonicalSourceLookupSlot: Sendable {
        let name: String
        let index: Int
    }

    private static func canonicalStoredModel(
        _ model: PersistedModel,
        entity: Schema.Entity,
        canonicalSchemas: [FieldSchema],
        reservation: DatabaseIntermediateReservation,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> (model: PersistedModel, retainedByteCount: UInt64) {
        try DatabaseByteProcessingMeter.consume(
            byteCount: UInt64(model.entity.utf8.count + entity.name.utf8.count),
            workMeter: workMeter,
            stage: stage
        )
        guard model.entity == entity.name else {
            throw SchemaDrivenEntityRuntimeError.entityMismatch(
                expected: entity.name,
                actual: model.entity
            )
        }

        let lookupLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(MemoryLayout<[String: Int]>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<CanonicalSourceLookupSlot>.stride)
            )
        )
        let lookupGrowth = try lookupLayout.growth(
            from: 0,
            toFit: model.fields.count
        )
        let scratch = try reservation.reserveChild(
            bytes: try DatabaseIntermediateFootprint(
                bytes: lookupLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: lookupGrowth.additionalByteCount
                )
            ).bytes,
            at: stage
        )
        defer { scratch.release() }
        var sourceByName: [String: Int] = [:]
        sourceByName.reserveCapacity(lookupGrowth.capacity)
        for index in model.fields.indices {
            let field = model.fields[index]
            try workMeter.consume(at: stage)
            try DatabaseByteProcessingMeter.consume(
                byteCount: field.name.utf8.count,
                workMeter: workMeter,
                stage: stage
            )
            try validateSourceFieldIdentity(field, entity: entity)
            precondition(
                sourceByName.updateValue(index, forKey: field.name) == nil,
                "PersistedModel admitted duplicate field names"
            )
        }

        var footprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<PersistedModel>.stride + 64)
                + UInt64(entity.name.utf8.count)
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<PersistableField>.stride + 16)
            ).multiplied(by: UInt64(canonicalSchemas.count))
        )
        for schema in canonicalSchemas {
            guard schema.fieldNumber > 0,
                  UInt32(exactly: schema.fieldNumber) != nil else {
                throw SchemaDrivenEntityRuntimeError.invalidFieldNumber(
                    entity: entity.name,
                    field: schema.name,
                    number: schema.fieldNumber
                )
            }
            try workMeter.consume(at: stage)
            try DatabaseByteProcessingMeter.consume(
                byteCount: schema.name.utf8.count,
                workMeter: workMeter,
                stage: stage
            )
            let value: FieldValue
            if let sourceIndex = sourceByName[schema.name] {
                value = model.fields[sourceIndex].value
            } else if let defaultValue = schema.defaultValue {
                value = defaultValue
            } else {
                throw SchemaDrivenEntityRuntimeError.missingRequiredField(
                    entity: entity.name,
                    field: schema.name
                )
            }
            let valueFootprint = try StorageValueDecoder.retainedFootprint(
                of: value,
                workMeter: workMeter,
                stage: stage
            )
            try DatabaseByteProcessingMeter.consume(
                byteCount: valueFootprint,
                workMeter: workMeter,
                stage: stage
            )
            try validate(value, schema: schema, entity: entity.name)
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(schema.name.utf8.count)
                )
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: valueFootprint
                )
            )
        }
        try reservation.reserveAdditional(bytes: footprint.bytes, at: stage)
        do {
            let constructionScratch = try reservation.reserveChild(
                bytes: try PersistedModelAdmissionFootprint
                    .validationScratchByteCount(
                        fieldCount: canonicalSchemas.count
                    ),
                at: stage
            )
            defer { constructionScratch.release() }
            var canonicalFields: [PersistableField] = []
            canonicalFields.reserveCapacity(canonicalSchemas.count)
            for schema in canonicalSchemas {
                guard let number = UInt32(exactly: schema.fieldNumber) else {
                    preconditionFailure(
                        "Validated canonical field number disappeared"
                    )
                }
                let value: FieldValue
                if let sourceIndex = sourceByName[schema.name] {
                    value = model.fields[sourceIndex].value
                } else if let defaultValue = schema.defaultValue {
                    value = defaultValue
                } else {
                    preconditionFailure(
                        "Validated canonical schema value disappeared"
                    )
                }
                canonicalFields.append(
                    try PersistableField(
                        number: number,
                        name: schema.name,
                        value: value
                    )
                )
            }
            return (
                try PersistedModel(
                    entity: entity.name,
                    fields: canonicalFields
                ),
                footprint.bytes
            )
        } catch {
            reservation.releaseGuaranteedPartial(bytes: footprint.bytes)
            throw error
        }
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

    private static func validateSourceFieldIdentity(
        _ field: PersistableField,
        entity: Schema.Entity
    ) throws {
        guard let schema = entity.fieldMapByName[field.name] else {
            throw SchemaDrivenEntityRuntimeError.unknownField(
                entity: entity.name,
                field: field.name
            )
        }
        guard schema.fieldNumber > 0,
              let expectedNumber = UInt32(exactly: schema.fieldNumber) else {
            throw SchemaDrivenEntityRuntimeError.invalidFieldNumber(
                entity: entity.name,
                field: schema.name,
                number: schema.fieldNumber
            )
        }
        guard field.number == expectedNumber else {
            throw SchemaDrivenEntityRuntimeError.fieldIdentityMismatch(
                entity: entity.name,
                field: field.name,
                expectedNumber: expectedNumber,
                actualNumber: field.number
            )
        }
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
        authorizationRequirement: DatabaseListReadAuthorizationRequirement,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction
    ) async throws -> EntityTableRows {
        guard case .table(let table) = selectQuery.source,
              table.table == entity.name else {
            throw CanonicalReadError.unsupportedSource(
                "Schema-driven runtime expected table '\(entity.name)'"
            )
        }
        let stablePagePlan = try schemaDrivenStablePagePlan(
            selectQuery: selectQuery,
            options: options
        )
        let stablePageWindow = stablePagePlan.window
        let budgetReadLimit = try options.workMeter.storageReadLimitWithSentinel(
            at: .storageRow
        )
        let readLimit = min(
            stablePageWindow?.fetchLimit ?? budgetReadLimit,
            budgetReadLimit
        )
        let models = try await context.scanPersistedModels(
            entity: entity,
            partitions: table.partitions,
            limit: readLimit,
            offset: stablePageWindow?.storageOffset ?? 0,
            startingAfterIdentifier:
                stablePageWindow?.startingAfterIdentifier,
            workMeter: options.workMeter,
            transaction: transaction,
            authorizationRequirement: authorizationRequirement
        )
        let rows = try canonicalTableRows(
            from: models,
            sourceName: sourceName,
            workMeter: options.workMeter
        )
        let continuationPosition: ByteString?
        if let stablePageWindow,
           models.count > stablePageWindow.visibleCount,
           stablePageWindow.visibleCount > 0 {
            var resolved: ByteString?
            try models.withEntry(at: stablePageWindow.visibleCount - 1) {
                entry in
                guard let entry else {
                    throw CanonicalReadError.unsupportedAccessPath(
                        "Canonical table fetch retained a missing model"
                    )
                }
                try entry.withModel { model in
                    let identity = try identity(for: model, entity: entity)
                    resolved = try PersistableIdentifierKeyCodec.tuple(
                        for: identity,
                        expectedType: entity.identifierType
                    ).pack()
                }
            }
            continuationPosition = resolved
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
                stablePagePlan.queryFingerprint
        )
    }

    private static func canonicalTableRows(
        from models: DatabaseRetainedPersistedModels,
        sourceName: String,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSharedRetainedArray<CanonicalSourceRow> {
        guard models.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.forElement(
                CanonicalSourceRow.self
            ),
            expectedCount: models.count
        )
        for index in models.indices {
            try workMeter.consume(at: .resultMaterialization)
            try models.withEntry(at: index) { entry in
                guard let entry else {
                    throw CanonicalReadError.unsupportedAccessPath(
                        "Canonical table fetch retained a missing model"
                    )
                }
                try entry.withModel { model in
                    try retainedRows.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .sourceRowFootprint(
                                of: model,
                                sourceName: sourceName,
                                workMeter: workMeter,
                                stage: .resultMaterialization
                            ),
                        make: {
                            let row = try QueryRowCodec.encode(model)
                            return CanonicalSourceRow.fromBaseFields(
                                row.fields,
                                sourceName: sourceName,
                                annotations: row.annotations,
                                version: row.version
                            )
                        }
                    )
                }
            }
        }
        return try retainedRows.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )
    }

    private struct SchemaDrivenStablePageWindow {
        let storageOffset: Int
        let fetchLimit: Int
        let visibleCount: Int
        let startingAfterIdentifier: ByteString?
    }

    private struct SchemaDrivenStablePagePlan {
        let window: SchemaDrivenStablePageWindow?
        let queryFingerprint: ByteString?
    }

    private static func schemaDrivenStablePagePlan(
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) throws -> SchemaDrivenStablePagePlan {
        guard options.options.continuationSnapshotIsStable,
              selectQuery.filter == nil,
              selectQuery.orderBy?.isEmpty ?? true,
              windowPushdownIsSemanticallySafe(selectQuery),
              let pageSize = try options.resolvePageSize() else {
            return SchemaDrivenStablePagePlan(
                window: nil,
                queryFingerprint: nil
            )
        }
        let cursor = try CanonicalQueryPagination
            .validatedStableSnapshotCursor(
                selectQuery: selectQuery,
                options: options
            )
        guard options.continuation == nil || cursor.storagePosition != nil else {
            return SchemaDrivenStablePagePlan(
                window: nil,
                queryFingerprint: cursor.queryFingerprint
            )
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
        return SchemaDrivenStablePagePlan(
            window: SchemaDrivenStablePageWindow(
                storageOffset: cursor.storagePosition == nil ? queryOffset : 0,
                fetchLimit: visibleCount == 0
                    ? 0
                    : max(1, overflow ? Int.max : fetchLimit),
                visibleCount: visibleCount,
                startingAfterIdentifier: cursor.storagePosition
            ),
            queryFingerprint: cursor.queryFingerprint
        )
    }

    private static func windowPushdownIsSemanticallySafe(
        _ query: SelectQuery
    ) -> Bool {
        guard !query.distinct,
              !canonicalQueryRequiresAggregation(query) else {
            return false
        }
        if case .distinctItems = query.projection {
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

    private let indexReaders: [IndexType: EntityIndexReadRuntime]
    private let fetchTableRowsOperation: FetchTableRows
    private let canonicalizeStoredModelOperation: CanonicalizeStoredModel
    private let canonicalizeModelOperation: CanonicalizeModel
    private let makePersistedModelOperation: MakePersistedModel
    private let resolveIdentityOperation: ResolveIdentity
    private let updateIndexesOperation: UpdateIndexes
    private let buildIndexOperation: BuildIndex
    private let runIndexSliceOperation: RunIndexSlice
    private let finalizeIndexOperation: FinalizeIndex

    fileprivate typealias FetchTableRows = @Sendable (
        _ context: DatabaseContext,
        _ sourceName: String,
        _ selectQuery: SelectQuery,
        _ authorizationRequirement: DatabaseListReadAuthorizationRequirement,
        _ options: ReadExecutionContext,
        _ transaction: DatabaseReadTransaction
    ) async throws -> EntityTableRows

    fileprivate typealias CanonicalizeModel = @Sendable (
        PersistedModel
    ) throws -> PersistedModel

    fileprivate typealias CanonicalizeStoredModel = @Sendable (
        PersistedModel,
        DatabaseIntermediateReservation,
        DatabaseWorkMeter,
        DatabaseWorkStage
    ) throws -> (model: PersistedModel, retainedByteCount: UInt64)

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
        indexReaders: [IndexType: EntityIndexReadRuntime],
        indexProviders: [IndexType: any EntityIndexProvider],
        canonicalizeStoredModel: @escaping CanonicalizeStoredModel,
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
        self.canonicalizeStoredModelOperation = canonicalizeStoredModel
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
        session: DatabaseReadSession,
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
        guard let runtime = indexReaders[index.type] else {
            return nil
        }
        return try await runtime.execute(
            session,
            selectQuery,
            index,
            indexScan,
            options,
            partitions
        )
    }

    func additionalRequiredFieldNames(
        for indexScan: IndexScanSource
    ) throws -> Set<String>? {
        guard let runtime = indexReaders[indexScan.indexType] else {
            return nil
        }
        return try runtime.additionalRequiredFieldNames(indexScan)
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

    package func canonicalizedStoredModel(
        _ model: PersistedModel,
        reservation: DatabaseIntermediateReservation,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> (model: PersistedModel, retainedByteCount: UInt64) {
        try canonicalizeStoredModelOperation(
            model,
            reservation,
            workMeter,
            stage
        )
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
        authorizationRequirement: DatabaseListReadAuthorizationRequirement,
        options: ReadExecutionContext,
        transaction: DatabaseReadTransaction
    ) async throws -> EntityTableRows {
        try await fetchTableRowsOperation(
            context,
            sourceName,
            selectQuery,
            authorizationRequirement,
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

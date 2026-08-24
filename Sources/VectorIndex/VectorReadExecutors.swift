import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum VectorReadParameter {
    static let fieldName = "fieldName"
    static let dimensions = "dimensions"
    static let queryVector = "queryVector"
    static let k = "k"
    static let metric = "metric"
}

public enum VectorReadExecutors {
    public static func polymorphicIndexExecutor(
        graphResourceLimits: HNSWGraphResourceLimits = .default,
        maximumGraphCacheCost: Int = 24 * 1_024 * 1_024
    ) -> any PolymorphicIndexReadExecutor {
        PolymorphicVectorReadExecutor(
            graphResourceLimits: graphResourceLimits,
            maximumGraphCacheCost: maximumGraphCacheCost
        )
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition,
        graphResourceLimits: HNSWGraphResourceLimits = .default,
        maximumGraphCacheCost: Int = 24 * 1_024 * 1_024
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(
            VectorReadExecutor(
                graphResourceLimits: graphResourceLimits,
                maximumGraphCacheCost: maximumGraphCacheCost
            )
        )
    }
}

private enum VectorReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case indexNotFound(String)
    case fetchedItemCountMismatch(expected: Int, actual: Int)
    case missingFetchedEntity(ByteString)
}

private struct VectorReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .vector
    private let graphCache: HNSWGraphCache
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(
        graphResourceLimits: HNSWGraphResourceLimits,
        maximumGraphCacheCost: Int
    ) {
        self.graphResourceLimits = graphResourceLimits
        self.graphCache = HNSWGraphCache(maximumCost: maximumGraphCacheCost)
    }

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloat32Vector(
            VectorReadParameter.queryVector,
            from: indexScan.parameters
        )
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        let specification = try VectorIndexSpecification(
            index.declaration.definition
        )
        guard index.type == indexType,
            index.fieldNames == [fieldName],
              specification.dimensions == dimensions,
              specification.metric.rawValue == metricRawValue,
              queryVector.count == dimensions,
              k > 0 else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        let boundedK = min(
            k,
            try options.workMeter.storageReadLimitWithSentinel()
        )

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        try context.authorizeCanonicalListAccess(
            entity: entity,
            selectQuery: selectQuery
        )
        let search = PolymorphicVectorReadExecutor(
            graphCache: graphCache,
            graphResourceLimits: graphResourceLimits
        )
        let algorithm = try search.resolveAlgorithm(
            indexName: index.name,
            context: context
        )
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction -> IndexReadResult in
            guard let readableIndex else { return .empty }
            let matches = try await search.executeSearch(
                specification: specification,
                indexSubspace: readableIndex.subspace,
                queryVector: queryVector,
                k: boundedK,
                transaction: transaction,
                workMeter: options.workMeter,
                algorithm: algorithm
            )
            let matchReservation = try reserveVectorMatches(
                matches,
                workMeter: options.workMeter
            )
            defer { matchReservation.release() }
            let identifiers = matches.map { Tuple($0.primaryKey) }
            let identifierReservation = try DatabaseIntermediateCollectionMeter
                .reserveTuples(
                    identifiers,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                transaction: transaction,
                snapshot: execution.consistency == .snapshot,
                workMeter: options.workMeter
            )
            guard fetched.count == matches.count else {
                throw VectorReadError.fetchedItemCountMismatch(
                    expected: matches.count,
                    actual: fetched.count
                )
            }
            let validator = VectorCanonicalStateValidator(
                indexSubspace: readableIndex.subspace,
                fieldName: fieldName,
                dimensions: specification.dimensions,
                algorithm: algorithm
            )
            for index in matches.indices {
                guard let model = fetched[index] else {
                    throw VectorReadError.missingFetchedEntity(
                        identifiers[index].pack()
                    )
                }
                try await validator.validate(
                    primaryKey: identifiers[index],
                    model: model,
                    transaction: transaction,
                    workMeter: options.workMeter
                )
            }
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                expectedCount: matches.count
            ) { rows in
                for (match, item) in zip(matches, fetched) {
                    guard let item else {
                        throw VectorReadError.missingFetchedEntity(
                            Tuple(match.primaryKey).pack()
                        )
                    }
                    try rows.append(
                        try IndexReadRow.materializing(
                            item,
                            annotations: [
                                "distance": .float64(match.distance)
                            ]
                        )
                    )
                }
            }
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(key)
        }
        return result
    }

    private func requireFloat32Vector(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Vector {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        return vector
    }
}

private struct PolymorphicVectorReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .vector
    private let graphCache: HNSWGraphCache
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(
        graphResourceLimits: HNSWGraphResourceLimits,
        maximumGraphCacheCost: Int
    ) {
        self.graphResourceLimits = graphResourceLimits
        self.graphCache = HNSWGraphCache(maximumCost: maximumGraphCacheCost)
    }

    fileprivate init(
        graphCache: HNSWGraphCache,
        graphResourceLimits: HNSWGraphResourceLimits
    ) {
        self.graphCache = graphCache
        self.graphResourceLimits = graphResourceLimits
    }

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloat32Vector(
            VectorReadParameter.queryVector,
            from: indexScan.parameters
        )
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        guard VectorMetric(rawValue: metricRawValue) != nil, k > 0 else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }

        let concreteDescriptor = try resolveConcreteDescriptor(
            schema: context.container.schema,
            group: group,
            indexName: index.name
        )
        let specification = try resolveSpecification(
            fieldName: fieldName,
            dimensions: dimensions,
            metricRawValue: metricRawValue,
            groupDescriptor: index,
            concreteDescriptor: concreteDescriptor
        )
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let boundedK = min(
            k,
            try options.workMeter.storageReadLimitWithSentinel()
        )
        let algorithm = try resolveAlgorithm(
            indexName: index.name,
            context: context
        )

        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: try authorizationValue(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try authorizationValue(
                selectQuery.offset,
                parameter: "offset"
            ),
            orderBy: orderByFields
        )

        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction -> IndexReadResult in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return .empty
            }
            let primaryKeysWithDistances = try await executeSearch(
                specification: specification,
                indexSubspace: readableIndex.subspace,
                queryVector: queryVector,
                k: boundedK,
                transaction: transaction,
                workMeter: options.workMeter,
                algorithm: algorithm
            )
            let matchReservation = try reserveVectorMatches(
                primaryKeysWithDistances,
                workMeter: options.workMeter
            )
            defer { matchReservation.release() }
            let tuples = primaryKeysWithDistances.map {
                Tuple($0.primaryKey)
            }
            let tupleReservation = try DatabaseIntermediateCollectionMeter
                .reserveTuples(
                    tuples,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { tupleReservation.release() }
            let entities = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: tuples,
                transaction: transaction,
                workMeter: options.workMeter
            )
            let entityReservation = try DatabaseIntermediateCollectionMeter
                .reservePolymorphicEntities(
                    entities,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { entityReservation.release() }
            let validator = VectorCanonicalStateValidator(
                indexSubspace: readableIndex.subspace,
                fieldName: fieldName,
                dimensions: specification.dimensions,
                algorithm: algorithm
            )
            for index in primaryKeysWithDistances.indices {
                guard let entity = entities[index] else {
                    throw VectorReadError.missingFetchedEntity(
                        tuples[index].pack()
                    )
                }
                try await validator.validate(
                    primaryKey: tuples[index],
                    model: entity.item,
                    transaction: transaction,
                    workMeter: options.workMeter
                )
            }
            return try makeResponse(
                results: primaryKeysWithDistances,
                entities: entities,
                workMeter: options.workMeter
            )
        }
    }

    fileprivate func executeSearch(
        specification: VectorIndexSpecification,
        indexSubspace: Subspace,
        queryVector: Vector,
        k: Int,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        algorithm: VectorAlgorithm
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        switch algorithm {
        case .flat:
            return try await FlatVectorIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                workMeter: workMeter
            )

        case .hnsw(let hnswParams):
            try workMeter.checkpoint(at: .indexScan)
            let parameters = HNSWParameters(
                m: hnswParams.m,
                efConstruction: hnswParams.efConstruction,
                efSearch: hnswParams.efSearch
            )
            let storage = HNSWIndexStorage(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: parameters,
                graphCache: graphCache,
                resourceLimits: graphResourceLimits
            )
            let results = try await HNSWIndexReader(storage: storage).search(
                queryVector: queryVector,
                k: k,
                parameters: HNSWSearchParameters(
                    ef: max(k, hnswParams.efSearch)
                ),
                transaction: transaction,
                workMeter: workMeter
            )
            return results

        case .ivf(let ivfParams):
            return try await IVFIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: IVFParameters(
                    nlist: ivfParams.nlist,
                    nprobe: ivfParams.nprobe,
                    kmeansIterations: ivfParams.kmeansIterations
                )
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                workMeter: workMeter
            )

        case .pq(let pqParams):
            return try await PQIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: PQParameters(
                    m: pqParams.m,
                    ksub: 256,
                    niter: pqParams.niter
                )
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    fileprivate func resolveAlgorithm(
        indexName: String,
        context: DatabaseContext
    ) throws -> VectorAlgorithm {
        let configurations = context.container.runtimeConfiguration
            .indexConfigurations(named: indexName)
        return try VectorRuntimePolicy.resolve(in: configurations)?.algorithm
            ?? .flat
    }

    private func makeResponse(
        results: [(primaryKey: [any TupleElement], distance: Double)],
        entities: [PolymorphicEntity?],
        workMeter: DatabaseWorkMeter
    ) throws -> IndexReadResult {
        guard results.count == entities.count else {
            throw VectorReadError.fetchedItemCountMismatch(
                expected: results.count,
                actual: entities.count
            )
        }

        return try IndexReadResult.build(
            workMeter: workMeter,
            expectedCount: results.count
        ) { rows in
            for (result, entity) in zip(results, entities) {
                guard let entity else {
                    throw VectorReadError.missingFetchedEntity(
                        Tuple(result.primaryKey).pack()
                    )
                }
                try rows.append(
                    try IndexReadRow.materializing(
                        entity.item,
                        annotations: [
                            PolymorphicRowAnnotation.typeName:
                                .string(entity.typeName),
                            PolymorphicRowAnnotation.typeCode:
                                .int64(entity.typeCode),
                            "distance": .float64(result.distance),
                        ]
                    )
                )
            }
        }
    }

    private func resolveSpecification(
        fieldName: String,
        dimensions: Int,
        metricRawValue: String,
        groupDescriptor: IndexDeclaration<String>,
        concreteDescriptor: IndexDescriptor
    ) throws -> VectorIndexSpecification {
        guard
            case .vector(
                let groupField,
                let groupDimensions,
                let groupMetric
            ) = groupDescriptor.definition,
            groupField == fieldName
        else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        let specification = try VectorIndexSpecification(
            concreteDescriptor.declaration.definition
        )
        guard specification.dimensions == groupDimensions,
            specification.metric == groupMetric,
            specification.dimensions == dimensions else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.dimensions
            )
        }
        guard specification.metric.rawValue == metricRawValue else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }
        return specification
    }

    private func resolveConcreteDescriptor(
        schema: Schema,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> IndexDescriptor {
        for memberTypeName in group.memberTypeNames {
            if let descriptor = schema.polymorphicIndexDescriptors(
                identifier: group.identifier,
                memberTypeName: memberTypeName
            ).first(
                where: { $0.name == indexName }
            ) {
                return descriptor
            }
        }
        throw VectorReadError.indexNotFound(indexName)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(key)
        }
        return result
    }

    private func requireFloat32Vector(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Vector {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        return vector
    }

    private func authorizationValue(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else {
            return nil
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(parameter)
        }
        return result
    }
}

private func reserveVectorMatches(
    _ matches: [(primaryKey: [any TupleElement], distance: Double)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: matches.count,
        element: (primaryKey: [any TupleElement], distance: Double).self
    )
    for match in matches {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(Tuple(match.primaryKey).pack().count)
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

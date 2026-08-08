import DatabaseEngine
import DatabaseTypes
import DatabaseKit
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
    let kindIdentifier = "vector"
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

        let specification = try VectorIndexSpecification(index.kind)
        guard index.kindIdentifier == kindIdentifier,
              index.fieldNames == [fieldName],
              specification.dimensions == dimensions,
              specification.metric.rawValue == metricRawValue,
              queryVector.count == dimensions,
              k > 0 else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }

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
        let results = try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            kindIdentifier: kindIdentifier,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction -> [(item: PersistedModel, distance: Double)] in
            guard let readableIndex else { return [] }
            let indexSubspace = try search.resolvedIndexSubspace(
                baseIndexSubspace: readableIndex.subspace,
                context: context,
                indexName: index.name
            )
            let matches = try await search.executeSearch(
                specification: specification,
                indexName: index.name,
                fieldName: fieldName,
                indexSubspace: indexSubspace,
                queryVector: queryVector,
                k: k,
                context: context,
                transaction: transaction
            )
            let identifiers = matches.map { Tuple($0.primaryKey) }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                transaction: transaction
            )
            guard fetched.count == matches.count else {
                throw VectorReadError.fetchedItemCountMismatch(
                    expected: matches.count,
                    actual: fetched.count
                )
            }
            var results: [(item: PersistedModel, distance: Double)] = []
            results.reserveCapacity(matches.count)
            for (match, item) in zip(matches, fetched) {
                guard let item else {
                    throw VectorReadError.missingFetchedEntity(
                        Tuple(match.primaryKey).pack()
                    )
                }
                results.append((item, match.distance))
            }
            return results
        }
        let rows = try results.map { result in
            try IndexReadRow.materializing(
                result.item,
                annotations: ["distance": .float64(result.distance)]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
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
    let kindIdentifier = "vector"
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
        index: PolymorphicIndexMetadata,
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

        guard VectorMetric(rawValue: metricRawValue) != nil else {
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
                return IndexReadResult(
                    rows: [],
                    ordering: .orderedByIndex
                )
            }
            let indexSubspace = try resolvedIndexSubspace(
                baseIndexSubspace: readableIndex.subspace,
                context: context,
                indexName: index.name
            )
            let primaryKeysWithDistances = try await executeSearch(
                specification: specification,
                indexName: index.name,
                fieldName: fieldName,
                indexSubspace: indexSubspace,
                queryVector: queryVector,
                k: k,
                context: context,
                transaction: transaction
            )
            let tuples = primaryKeysWithDistances.map {
                Tuple($0.primaryKey)
            }
            let entities = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: tuples,
                transaction: transaction
            )
            return try makeResponse(
                results: primaryKeysWithDistances,
                entities: entities
            )
        }
    }

    fileprivate func executeSearch(
        specification: VectorIndexSpecification,
        indexName: String,
        fieldName: String,
        indexSubspace: Subspace,
        queryVector: Vector,
        k: Int,
        context: DatabaseContext,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let configs = context.container.indexConfigurations[indexName] ?? []
        let runtimePolicy = try VectorRuntimePolicy.resolve(in: configs)
        let algorithm = runtimePolicy?.algorithm ?? .flat

        switch algorithm {
        case .flat:
            return try await FlatVectorIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction
            )

        case .hnsw(let hnswParams):
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
            return try await HNSWIndexReader(storage: storage).search(
                queryVector: queryVector,
                k: k,
                parameters: HNSWSearchParameters(
                    ef: max(k, hnswParams.efSearch)
                ),
                transaction: transaction
            )

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
                transaction: transaction
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
                transaction: transaction
            )
        }
    }

    private func makeResponse(
        results: [(primaryKey: [any TupleElement], distance: Double)],
        entities: [PolymorphicEntity?]
    ) throws -> IndexReadResult {
        guard results.count == entities.count else {
            throw VectorReadError.fetchedItemCountMismatch(
                expected: results.count,
                actual: entities.count
            )
        }

        var rows: [IndexReadRow] = []
        rows.reserveCapacity(results.count)
        for (result, entity) in zip(results, entities) {
            guard let entity else {
                throw VectorReadError.missingFetchedEntity(
                    Tuple(result.primaryKey).pack()
                )
            }
            rows.append(
                try IndexReadRow.materializing(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode),
                        "distance": .float64(result.distance)
                    ]
                )
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func resolveSpecification(
        fieldName: String,
        dimensions: Int,
        metricRawValue: String,
        groupDescriptor: PolymorphicIndexMetadata,
        concreteDescriptor: IndexDescriptor
    ) throws -> VectorIndexSpecification {
        guard groupDescriptor.kindIdentifier
                == VectorIndexSpecification.identifier,
              groupDescriptor.subspaceStructure == .hierarchical,
              groupDescriptor.fieldNames == [fieldName] else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        let specification = try VectorIndexSpecification(
            concreteDescriptor.kind
        )
        guard specification.metadata.metadata == groupDescriptor.metadata else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        guard specification.dimensions == dimensions else {
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

    fileprivate func resolvedIndexSubspace(
        baseIndexSubspace: Subspace,
        context: DatabaseContext,
        indexName: String
    ) throws -> Subspace {
        let configs = context.container.indexConfigurations[indexName] ?? []
        guard let runtimePolicy = try VectorRuntimePolicy.resolve(in: configs),
              let subspaceKey = runtimePolicy.subspaceKey else {
            return baseIndexSubspace
        }
        return baseIndexSubspace.subspace(subspaceKey)
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

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
        graphResourceLimits: HNSWGraphResourceLimits = .default
    ) -> any PolymorphicIndexReadExecutor {
        PolymorphicVectorReadExecutor(graphResourceLimits: graphResourceLimits)
    }

    public static func register<Model: Persistable>(
        with definition: inout EntityRuntimeDefinition<Model>,
        graphResourceLimits: HNSWGraphResourceLimits = .default
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(
            VectorReadExecutor(graphResourceLimits: graphResourceLimits)
        )
    }
}

private enum VectorReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case indexNotFound(String)
    case duplicateFetchedEntity(ByteString)
    case missingFetchedEntity(ByteString)
}

private struct VectorReadExecutor: IndexReadExecutor {
    let kindIdentifier = "vector"
    private let graphCache = HNSWGraphCache()
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(graphResourceLimits: HNSWGraphResourceLimits) {
        self.graphResourceLimits = graphResourceLimits
    }

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloatArray(VectorReadParameter.queryVector, from: indexScan.parameters)
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        guard let metric = VectorMetric(rawValue: metricRawValue) else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }
        let distanceMetric: VectorDistanceMetric
        switch metric {
        case .cosine:
            distanceMetric = .cosine
        case .euclidean:
            distanceMetric = .euclidean
        case .dotProduct:
            distanceMetric = .dotProduct
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        let builder = VectorQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            dimensions: dimensions,
            graphCache: graphCache,
            graphResourceLimits: graphResourceLimits
        )
            .metric(distanceMetric)
        let configuredBuilder = try builder.query(queryVector, k: k)

        let results: [(item: T, distance: Double)] = try await configuredBuilder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
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
        return Int(value)
    }

    private func requireFloatArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [Float] {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        guard let values = vector.withFloat32Elements({ elements in
            Array(elements)
        }) else {
            throw VectorReadError.invalidParameter(key)
        }
        return values
    }
}

private struct PolymorphicVectorReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "vector"
    private let graphCache = HNSWGraphCache()
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(graphResourceLimits: HNSWGraphResourceLimits) {
        self.graphResourceLimits = graphResourceLimits
    }

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloatArray(VectorReadParameter.queryVector, from: indexScan.parameters)
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        guard VectorMetric(rawValue: metricRawValue) != nil else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }

        let descriptor = resolveDescriptor(
            in: group,
            indexName: indexScan.indexName,
            fieldName: fieldName
        )
        let concreteDescriptor = try resolveConcreteDescriptor(
            schema: context.container.schema,
            group: group,
            indexName: indexScan.indexName
        )
        let specification = try resolveSpecification(
            fieldName: fieldName,
            dimensions: dimensions,
            metricRawValue: metricRawValue,
            groupDescriptor: descriptor,
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

        let polySubspace = try await context.container.resolvePolymorphicDirectory(for: group.identifier)
        let baseIndexSubspace = polySubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)
        let indexSubspace = try resolvedIndexSubspace(
            baseIndexSubspace: baseIndexSubspace,
            context: context,
            indexName: indexScan.indexName
        )

        let primaryKeysWithDistances = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            try await executeSearch(
                specification: specification,
                indexName: indexScan.indexName,
                fieldName: fieldName,
                indexSubspace: indexSubspace,
                queryVector: queryVector,
                k: k,
                context: context,
                transaction: transaction
            )
        }

        let tuples = primaryKeysWithDistances.map { Tuple($0.primaryKey) }
        let entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: tuples,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        return try makeResponse(
            results: primaryKeysWithDistances,
            entities: entities,
            selectQuery: selectQuery,
            options: options
        )
    }

    private func executeSearch(
        specification: VectorIndexSpecification,
        indexName: String,
        fieldName: String,
        indexSubspace: Subspace,
        queryVector: [Float],
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
        entities: [PolymorphicEntity],
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) throws -> IndexReadResult {
        var entityByID: [ByteString: PolymorphicEntity] = [:]
        entityByID.reserveCapacity(entities.count)
        for entity in entities {
            let key = entity.polymorphicIdentifier.pack()
            guard entityByID[key] == nil else {
                throw VectorReadError.duplicateFetchedEntity(key)
            }
            entityByID[key] = entity
        }

        var orderedResults: [(entity: PolymorphicEntity, distance: Double)] = []
        orderedResults.reserveCapacity(results.count)
        for result in results {
            let key = Tuple(result.primaryKey).pack()
            guard let entity = entityByID[key] else {
                throw VectorReadError.missingFetchedEntity(key)
            }
            orderedResults.append(
                (entity: entity, distance: result.distance)
            )
        }

        let rows = try orderedResults.map { result in
            try IndexReadRow.materializing(
                result.entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.entity.typeCode),
                    "distance": .float64(result.distance)
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func resolveSpecification(
        fieldName: String,
        dimensions: Int,
        metricRawValue: String,
        groupDescriptor: PolymorphicIndexMetadata?,
        concreteDescriptor: IndexDescriptor
    ) throws -> VectorIndexSpecification {
        guard let groupDescriptor else {
            throw VectorReadError.indexNotFound(fieldName)
        }
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

    private func resolveDescriptor(
        in group: PolymorphicGroup,
        indexName: String,
        fieldName: String
    ) -> PolymorphicIndexMetadata? {
        if let descriptor = group.indexes.first(where: { $0.name == indexName }) {
            return descriptor
        }
        return group.indexes.first(where: {
            $0.kindIdentifier == kindIdentifier && $0.fieldNames.contains(fieldName)
        })
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

    private func resolvedIndexSubspace(
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
        return Int(value)
    }

    private func requireFloatArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [Float] {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        guard let values = vector.withFloat32Elements({ elements in
            Array(elements)
        }) else {
            throw VectorReadError.invalidParameter(key)
        }
        return values
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

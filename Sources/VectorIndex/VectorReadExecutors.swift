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
    public static var indexExecutor: any IndexReadExecutor { VectorReadExecutor() }
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicVectorReadExecutor()
    }
}

private enum VectorReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case indexNotFound(String)
    case unresolvedAlgorithm
}

private struct VectorReadExecutor: IndexReadExecutor {
    let kindIdentifier = "vector"

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
            dimensions: dimensions
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

private struct PolymorphicVectorPlaceholder: Persistable {
    typealias ID = String

    var id: String = ""

    static var persistableType: String { "_PolymorphicVectorPlaceholder" }
    static var allFields: [String] { ["id"] }

    static func fieldNumber(for fieldName: String) -> Int? {
        fieldName == "id" ? 1 : nil
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<PolymorphicVectorPlaceholder, Value>) -> String {
        if keyPath == \PolymorphicVectorPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: PartialKeyPath<PolymorphicVectorPlaceholder>) -> String {
        if keyPath == \PolymorphicVectorPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PolymorphicVectorPlaceholder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct PolymorphicVectorReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "vector"

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
            context: context,
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
        let indexSubspace = resolvedIndexSubspace(
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
        let index = Index(
            name: indexName,
            kind: specification.metadata,
            rootExpression: FieldKeyExpression(fieldName: fieldName)
        )

        let configs = context.container.indexConfigurations[indexName] ?? []
        let vectorConfig = configs.first { config in
            type(of: config).kindIdentifier
                == VectorIndexSpecification.identifier
        } as? VectorIndexRuntimeConfiguration

        let resolvedAlgorithm: VectorAlgorithm
        if let vectorConfig {
            switch vectorConfig.algorithm {
            case .auto(let autoParams):
                let vectorCount = try await countVectors(
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
                resolvedAlgorithm = autoParams.selectAlgorithm(
                    vectorCount: vectorCount
                )
            case .flat, .hnsw, .ivf, .pq:
                resolvedAlgorithm = vectorConfig.algorithm
            }
        } else {
            resolvedAlgorithm = .flat
        }

        switch resolvedAlgorithm {
        case .auto:
            throw VectorReadError.unresolvedAlgorithm

        case .flat:
            let maintainer = FlatVectorIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: specification.dimensions,
                metric: specification.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )
            return try await maintainer.search(
                queryVector: queryVector,
                k: k,
                transaction: transaction
            )

        case .hnsw(let hnswParams):
            let maintainer = HNSWIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: specification.dimensions,
                metric: specification.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: HNSWParameters(
                    m: hnswParams.m,
                    efConstruction: hnswParams.efConstruction,
                    efSearch: hnswParams.efSearch
                )
            )
            return try await maintainer.search(
                queryVector: queryVector,
                k: k,
                searchParams: HNSWSearchParameters(ef: max(k, hnswParams.efSearch)),
                transaction: transaction
            )

        case .ivf(let ivfParams):
            let maintainer = IVFIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: specification.dimensions,
                metric: specification.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: IVFParameters(
                    nlist: ivfParams.nlist,
                    nprobe: ivfParams.nprobe,
                    kmeansIterations: ivfParams.kmeansIterations
                )
            )
            return try await maintainer.search(
                queryVector: queryVector,
                k: k,
                transaction: transaction
            )

        case .pq(let pqParams):
            let maintainer = PQIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: specification.dimensions,
                metric: specification.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: PQParameters(
                    m: pqParams.m,
                    ksub: 256,
                    niter: pqParams.niter
                )
            )
            return try await maintainer.search(
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
        var entityByID: [String: PolymorphicEntity] = [:]
        entityByID.reserveCapacity(entities.count)
        for entity in entities {
            let identifier = try entity.item.persistableIdentifierTuple()
            let key = stableKey(
                Tuple(entity.typeCode).appending(identifier)
            )
            entityByID[key] = entity
        }

        let orderedResults: [(entity: PolymorphicEntity, distance: Double)] = results.compactMap { result -> (entity: PolymorphicEntity, distance: Double)? in
            let key = stableKey(Tuple(result.primaryKey))
            guard let entity = entityByID[key] else { return nil }
            return (entity: entity, distance: result.distance)
        }

        let rows = try orderedResults.map { result in
            try IndexReadRow.materializing(
                any: result.entity.item,
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
        context: DatabaseContext,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> IndexDescriptor {
        for memberTypeName in group.memberTypeNames {
            guard let memberType = context.container.runtimeConfiguration
                .persistableTypes.type(named: memberTypeName) else {
                continue
            }
            if let descriptor = try memberType.indexDescriptors.first(
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
    ) -> Subspace {
        let configs = context.container.indexConfigurations[indexName] ?? []
        guard let vectorConfig = configs.first(where: {
            type(of: $0).kindIdentifier
                == VectorIndexSpecification.identifier
        }) as? VectorIndexRuntimeConfiguration,
        let subspaceKey = vectorConfig.subspaceKey else {
            return baseIndexSubspace
        }
        return baseIndexSubspace.subspace(subspaceKey)
    }

    private func countVectors(
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Int {
        let (begin, end) = indexSubspace.range()
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var count = 0
        for _ in sequence {
            count += 1
            if count > 100_000 {
                break
            }
        }
        return count
    }

    private func stableKey(_ tuple: Tuple) -> String {
        let packed = tuple.pack()
        return QueryLiteralEncoding.base64(
            ByteString(retaining: packed)
        )
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

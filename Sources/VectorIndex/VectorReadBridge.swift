import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Vector
import StorageKit

enum VectorReadParameter {
    static let fieldName = "fieldName"
    static let dimensions = "dimensions"
    static let queryVector = "queryVector"
    static let k = "k"
    static let metric = "metric"
}

public enum VectorReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(VectorReadExecutor())
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicVectorReadExecutor())
    }
}

private enum VectorReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case indexNotFound(String)
}

private struct VectorReadExecutor: IndexReadExecutor {
    let kindIdentifier = "vector"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloatArray(VectorReadParameter.queryVector, from: indexScan.parameters)
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        guard let metric = VectorMetric(rawValue: metricRawValue) else {
            throw VectorReadBridgeError.invalidParameter(VectorReadParameter.metric)
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
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        let builder = VectorQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            dimensions: dimensions
        )
            .query(queryVector, k: k)
            .metric(distanceMetric)

        let results: [(item: T, distance: Double)] = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        return try makeResponse(results: results, selectQuery: selectQuery, options: options)
    }

    private func makeResponse<T: Persistable>(
        results: [(item: T, distance: Double)],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> QueryResponse {
        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for vector access paths")
        }

        let page = try CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )

        let rows = try page.items.map { result in
            try QueryRowCodec.encode(
                result.item,
                annotations: ["distance": .double(result.distance)]
            )
        }

        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func isCountProjection(_ selectQuery: SelectQuery) -> Bool {
        guard case .items(let items) = selectQuery.projection,
              items.count == 1,
              case .aggregate(.count(let expression, let distinct)) = items[0].expression,
              expression == nil,
              distinct == false else {
            return false
        }
        return true
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        return Int(value)
    }

    private func requireFloatArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [Float] {
        guard let values = parameters[key]?.arrayValue else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        var floats: [Float] = []
        floats.reserveCapacity(values.count)
        for value in values {
            guard let scalar = value.doubleValue else {
                throw VectorReadBridgeError.invalidParameter(key)
            }
            floats.append(Float(scalar))
        }
        return floats
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

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloatArray(VectorReadParameter.queryVector, from: indexScan.parameters)
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        guard VectorMetric(rawValue: metricRawValue) != nil else {
            throw VectorReadBridgeError.invalidParameter(VectorReadParameter.metric)
        }

        let descriptor = resolveDescriptor(
            in: group,
            indexName: indexScan.indexName,
            fieldName: fieldName
        )
        let kind = try makeKind(
            fieldName: fieldName,
            dimensions: dimensions,
            metricRawValue: metricRawValue,
            descriptor: descriptor
        )
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )

        let orderByFields = selectQuery.orderBy?.compactMap { sortKey -> String? in
            guard case .column(let column) = sortKey.expression else { return nil }
            return column.column
        }
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: selectQuery.limit,
            offset: selectQuery.offset,
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
                kind: kind,
                indexName: indexScan.indexName,
                fieldName: fieldName,
                dimensions: dimensions,
                indexSubspace: indexSubspace,
                queryVector: queryVector,
                k: k,
                context: context,
                transaction: transaction
            )
        }

        let tuples = primaryKeysWithDistances.map { Tuple($0.primaryKey) }
        let records = try await context.fetchPolymorphicItems(
            group: group,
            ids: tuples,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        return try makeResponse(
            results: primaryKeysWithDistances,
            records: records,
            selectQuery: selectQuery,
            options: options
        )
    }

    private func executeSearch(
        kind: VectorIndexKind<PolymorphicVectorPlaceholder>,
        indexName: String,
        fieldName: String,
        dimensions: Int,
        indexSubspace: Subspace,
        queryVector: [Float],
        k: Int,
        context: FDBContext,
        transaction: any Transaction
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: fieldName)
        )

        let configs = context.container.indexConfigurations[indexName] ?? []
        let vectorConfig = configs.first { config in
            type(of: config).kindIdentifier == VectorIndexKind<PolymorphicVectorPlaceholder>.identifier
        } as? _VectorIndexConfiguration

        let resolvedAlgorithm: VectorAlgorithm
        if let vectorConfig {
            switch vectorConfig.algorithm {
            case .auto(let autoParams):
                let vectorCount = try await countVectors(
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
                resolvedAlgorithm = autoParams.selectAlgorithm(
                    vectorCount: vectorCount,
                    dimensions: dimensions
                )
            case .flat, .hnsw, .ivf, .pq:
                resolvedAlgorithm = vectorConfig.algorithm
            }
        } else {
            resolvedAlgorithm = .flat
        }

        switch resolvedAlgorithm {
        case .auto:
            let maintainer = FlatVectorIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: dimensions,
                metric: kind.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )
            return try await maintainer.search(
                queryVector: queryVector,
                k: k,
                transaction: transaction
            )

        case .flat:
            let maintainer = FlatVectorIndexMaintainer<PolymorphicVectorPlaceholder>(
                index: index,
                dimensions: dimensions,
                metric: kind.metric,
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
                dimensions: dimensions,
                metric: kind.metric,
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
                dimensions: dimensions,
                metric: kind.metric,
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
                dimensions: dimensions,
                metric: kind.metric,
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
        records: [PolymorphicRecord],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> QueryResponse {
        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for vector access paths")
        }

        let recordByID: [String: PolymorphicRecord] = Dictionary(
            uniqueKeysWithValues: records.map { record in
                (stableKey(Tuple([record.typeCode] + primaryKeyElements(from: record.item))), record)
            }
        )

        let orderedResults: [(record: PolymorphicRecord, distance: Double)] = results.compactMap { result -> (record: PolymorphicRecord, distance: Double)? in
            let key = stableKey(Tuple(result.primaryKey))
            guard let record = recordByID[key] else { return nil }
            return (record: record, distance: result.distance)
        }

        let page = try CanonicalOffsetPagination.window(
            items: orderedResults,
            selectQuery: selectQuery,
            options: options
        )

        let rows = page.items.map { result in
            QueryRowCodec.encodeAny(
                result.record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.record.typeCode),
                    "distance": .double(result.distance)
                ]
            )
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func makeKind(
        fieldName: String,
        dimensions: Int,
        metricRawValue: String,
        descriptor: AnyIndexDescriptor?
    ) throws -> VectorIndexKind<PolymorphicVectorPlaceholder> {
        let resolvedDimensions = descriptor?.kind.metadata["dimensions"]?.intValue ?? dimensions
        let resolvedMetricRawValue = descriptor?.kind.metadata["metric"]?.stringValue ?? metricRawValue
        guard let metric = VectorMetric(rawValue: resolvedMetricRawValue) else {
            throw VectorReadBridgeError.invalidParameter("metric")
        }
        return VectorIndexKind<PolymorphicVectorPlaceholder>(
            fieldNames: descriptor?.fieldNames.isEmpty == false ? descriptor!.fieldNames : [fieldName],
            dimensions: resolvedDimensions,
            metric: metric
        )
    }

    private func resolveDescriptor(
        in group: PolymorphicGroup,
        indexName: String,
        fieldName: String
    ) -> AnyIndexDescriptor? {
        if let descriptor = group.indexes.first(where: { $0.name == indexName }) {
            return descriptor
        }
        return group.indexes.first(where: {
            $0.kindIdentifier == kindIdentifier && $0.fieldNames.contains(fieldName)
        })
    }

    private func resolvedIndexSubspace(
        baseIndexSubspace: Subspace,
        context: FDBContext,
        indexName: String
    ) -> Subspace {
        let configs = context.container.indexConfigurations[indexName] ?? []
        guard let vectorConfig = configs.first(where: {
            type(of: $0).kindIdentifier == VectorIndexKind<PolymorphicVectorPlaceholder>.identifier
        }) as? _VectorIndexConfiguration,
        let subspaceKey = vectorConfig.subspaceKey else {
            return baseIndexSubspace
        }
        return baseIndexSubspace.subspace(subspaceKey)
    }

    private func countVectors(
        indexSubspace: Subspace,
        transaction: any Transaction
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

    private func primaryKeyElements(from item: any Persistable) -> [any TupleElement] {
        guard let raw = item[dynamicMember: "id"] as? any TupleElement else {
            return []
        }
        return [raw]
    }

    private func stableKey(_ tuple: Tuple) -> String {
        Data(tuple.pack()).base64EncodedString()
    }

    private func isCountProjection(_ selectQuery: SelectQuery) -> Bool {
        guard case .items(let items) = selectQuery.projection,
              items.count == 1,
              case .aggregate(.count(let expression, let distinct)) = items[0].expression,
              expression == nil,
              distinct == false else {
            return false
        }
        return true
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        return Int(value)
    }

    private func requireFloatArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [Float] {
        guard let values = parameters[key]?.arrayValue else {
            throw VectorReadBridgeError.missingParameter(key)
        }
        var floats: [Float] = []
        floats.reserveCapacity(values.count)
        for value in values {
            guard let scalar = value.doubleValue else {
                throw VectorReadBridgeError.invalidParameter(key)
            }
            floats.append(Float(scalar))
        }
        return floats
    }
}

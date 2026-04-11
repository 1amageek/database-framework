import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Vector

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
    }
}

private enum VectorReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
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
            let fields = try encodeFields(result.item)
            return QueryRow(
                fields: fields,
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

    private func encodeFields<T: Persistable>(_ item: T) throws -> [String: FieldValue] {
        let data = try JSONEncoder().encode(item)
        let fields = try JSONDecoder().decode([String: FieldValue].self, from: data)
        return fields
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

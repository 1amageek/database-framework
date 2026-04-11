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

private struct VectorContinuationPayload: Codable, Sendable {
    let offset: Int
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

        let continuationOffset = try decodeOffset(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return QueryResponse(rows: [])
        }

        let requestedPageSize: Int = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return results.count
            }
        }()

        let window = Array(results.dropFirst(baseOffset).prefix(requestedPageSize + 1))
        let hasMore = window.count > requestedPageSize
        let visible = hasMore ? Array(window.prefix(requestedPageSize)) : window

        let rows = try visible.map { result in
            let fields = try encodeFields(result.item)
            return QueryRow(
                fields: fields,
                annotations: ["distance": .double(result.distance)]
            )
        }

        let continuation: QueryContinuation?
        if hasMore {
            continuation = try encodeOffset(continuationOffset + visible.count)
        } else {
            continuation = nil
        }

        return QueryResponse(rows: rows, continuation: continuation)
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

    private func decodeOffset(_ continuation: QueryContinuation?) throws -> Int {
        guard let continuation else { return 0 }
        guard let data = Data(base64Encoded: continuation.token) else {
            throw CanonicalReadError.invalidContinuation
        }
        let payload = try JSONDecoder().decode(VectorContinuationPayload.self, from: data)
        return payload.offset
    }

    private func encodeOffset(_ offset: Int) throws -> QueryContinuation {
        let data = try JSONEncoder().encode(VectorContinuationPayload(offset: offset))
        return QueryContinuation(data.base64EncodedString())
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

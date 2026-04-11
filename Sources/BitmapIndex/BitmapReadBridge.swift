import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import StorageKit

enum BitmapReadParameter {
    static let fieldName = "fieldName"
    static let operation = "operation"
    static let values = "values"
    static let valueSets = "valueSets"
    static let limit = "limit"

    static let equalsOperation = "equals"
    static let inOperation = "in"
    static let andOperation = "and"
}

public enum BitmapReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(BitmapReadExecutor())
    }
}

private enum BitmapReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct BitmapReadExecutor: IndexReadExecutor {
    let kindIdentifier = "bitmap"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = BitmapQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )

        if let limit = indexScan.parameters[BitmapReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        switch operation {
        case BitmapReadParameter.equalsOperation:
            let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
            guard let first = values.first else {
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.values)
            }
            builder = builder.equalsAny(first)
        case BitmapReadParameter.inOperation:
            builder = builder.inAny(try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values]))
        case BitmapReadParameter.andOperation:
            builder = builder.allAny(try decodeTupleMatrix(indexScan.parameters[BitmapReadParameter.valueSets]))
        default:
            throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.operation)
        }

        if isCountProjection(selectQuery) {
            let count = try await builder.countDirect(configuration: execution.transactionConfiguration)
            return makeCountResponse(selectQuery: selectQuery, count: count)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = try page.items.map { item in
            let data = try JSONEncoder().encode(item)
            let fields = try JSONDecoder().decode([String: FieldValue].self, from: data)
            return QueryRow(fields: fields)
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func makeCountResponse(
        selectQuery: SelectQuery,
        count: Int
    ) -> QueryResponse {
        let alias: String
        if case .items(let items) = selectQuery.projection,
           let first = items.first {
            alias = first.alias ?? "count"
        } else {
            alias = "count"
        }
        return QueryResponse(rows: [QueryRow(fields: [alias: .int64(Int64(count))])])
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
            throw BitmapReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }

    private func decodeTupleMatrix(
        _ value: QueryParameterValue?
    ) throws -> [[any TupleElement & Sendable]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        }
    }
}

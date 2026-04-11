import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol

enum VersionReadParameter {
    static let primaryKey = "primaryKey"
    static let limit = "limit"
    static let indexName = "indexName"
}

public enum VersionReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(VersionReadExecutor())
    }
}

private enum VersionReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct VersionReadExecutor: IndexReadExecutor {
    let kindIdentifier = "version"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let primaryKeyValues = try requireArray(VersionReadParameter.primaryKey, from: indexScan.parameters)
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = VersionQueryBuilder<T>(
            queryContext: queryContext,
            primaryKey: primaryKey
        )

        if let limit = indexScan.parameters[VersionReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }
        if let indexName = indexScan.parameters[VersionReadParameter.indexName]?.stringValue {
            builder = builder.index(indexName)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration
        )

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: results.count)
        }

        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = try page.items.map { result in
            let data = try JSONEncoder().encode(result.item)
            let fields = try JSONDecoder().decode([String: FieldValue].self, from: data)
            return QueryRow(
                fields: fields,
                annotations: ["version": .data(Data(result.version.bytes))]
            )
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

    private func requireArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [QueryParameterValue] {
        guard let values = parameters[key]?.arrayValue else {
            throw VersionReadBridgeError.missingParameter(key)
        }
        return values
    }
}

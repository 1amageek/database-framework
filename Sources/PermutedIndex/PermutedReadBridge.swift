import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Permuted
import StorageKit

enum PermutedReadParameter {
    static let queryType = "queryType"
    static let values = "values"
    static let permutation = "permutation"
    static let limit = "limit"

    static let prefixQuery = "prefix"
    static let exactQuery = "exact"
    static let allQuery = "all"
}

public enum PermutedReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(PermutedReadExecutor())
    }
}

private enum PermutedReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct PermutedReadExecutor: IndexReadExecutor {
    let kindIdentifier = "permuted"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = PermutedQueryBuilder<T>(
            queryContext: queryContext,
            indexName: indexScan.indexName,
            permutation: try decodePermutation(indexScan.parameters[PermutedReadParameter.permutation])
        )

        if let limit = indexScan.parameters[PermutedReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }

        let queryType = try requireString(PermutedReadParameter.queryType, from: indexScan.parameters)
        switch queryType {
        case PermutedReadParameter.prefixQuery:
            builder = builder.prefix(try decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]))
        case PermutedReadParameter.exactQuery:
            builder = builder.exact(try decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]))
        case PermutedReadParameter.allQuery:
            break
        default:
            throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.queryType)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: results.count)
        }

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
            throw PermutedReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodePermutation(_ value: QueryParameterValue?) throws -> Permutation? {
        guard let values = value?.arrayValue else { return nil }
        let indices = try values.map { parameter in
            guard let intValue = parameter.int64Value else {
                throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.permutation)
            }
            return Int(intValue)
        }
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw PermutedReadBridgeError.missingParameter(PermutedReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }
}

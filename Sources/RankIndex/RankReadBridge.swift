import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Rank

enum RankReadParameter {
    static let fieldName = "fieldName"
    static let mode = "mode"
    static let count = "count"
    static let from = "from"
    static let to = "to"
    static let percentile = "percentile"

    static let topMode = "top"
    static let bottomMode = "bottom"
    static let rangeMode = "range"
    static let percentileMode = "percentile"
}

public enum RankReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(RankReadExecutor())
    }
}

private enum RankReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct RankReadExecutor: IndexReadExecutor {
    let kindIdentifier = "rank"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = RankQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )

        let mode = try requireString(RankReadParameter.mode, from: indexScan.parameters)
        switch mode {
        case RankReadParameter.topMode:
            builder = builder.top(try requireInt(RankReadParameter.count, from: indexScan.parameters))
        case RankReadParameter.bottomMode:
            builder = builder.bottom(try requireInt(RankReadParameter.count, from: indexScan.parameters))
        case RankReadParameter.rangeMode:
            builder = builder.range(
                from: try requireInt(RankReadParameter.from, from: indexScan.parameters),
                to: try requireInt(RankReadParameter.to, from: indexScan.parameters)
            )
        case RankReadParameter.percentileMode:
            builder = builder.percentile(try requireDouble(RankReadParameter.percentile, from: indexScan.parameters))
        default:
            throw RankReadBridgeError.invalidParameter(RankReadParameter.mode)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for rank access paths")
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
                annotations: ["rank": .int64(Int64(result.rank))]
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
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return Int(value)
    }

    private func requireDouble(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Double {
        guard let value = parameters[key]?.doubleValue else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }
}

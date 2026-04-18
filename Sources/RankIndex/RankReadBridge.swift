import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Rank
import StorageKit

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
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicRankReadExecutor())
    }
}

private enum RankReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidRange(from: Int, to: Int)
}

private func validateRankRange(from: Int, to: Int) throws {
    guard from >= 0 else {
        throw RankReadBridgeError.invalidRange(from: from, to: to)
    }
    guard to > from else {
        throw RankReadBridgeError.invalidRange(from: from, to: to)
    }
}

private struct RankReadExecutor: IndexReadExecutor {
    let kindIdentifier = "rank"

    func executeRows<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> BridgedRowSet {
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
            let from = try requireInt(RankReadParameter.from, from: indexScan.parameters)
            let to = try requireInt(RankReadParameter.to, from: indexScan.parameters)
            try validateRankRange(from: from, to: to)
            builder = builder.range(from: from, to: to)
        case RankReadParameter.percentileMode:
            builder = builder.percentile(try requireDouble(RankReadParameter.percentile, from: indexScan.parameters))
        default:
            throw RankReadBridgeError.invalidParameter(RankReadParameter.mode)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        let rows = results.map { result in
            BridgedRow.encoding(
                result.item,
                annotations: ["rank": .int64(Int64(result.rank))]
            )
        }
        return BridgedRowSet(rows: rows, ordering: .indexNative)
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

private struct PolymorphicRankReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "rank"

    func executeRows(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> BridgedRowSet {
        _ = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)
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

        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        let rankedKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            try await scanRanked(
                indexSubspace: indexSubspace,
                transaction: transaction,
                parameters: indexScan.parameters
            )
        }

        let records = try await context.fetchPolymorphicItems(
            group: group,
            ids: rankedKeys.map { $0.primaryKey },
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        let recordByID: [String: PolymorphicRecord] = Dictionary(
            uniqueKeysWithValues: records.map { record in
                (stableKey(Tuple([record.typeCode] + primaryKeyElements(from: record.item))), record)
            }
        )
        let orderedResults: [(record: PolymorphicRecord, rank: Int)] = rankedKeys.compactMap { result in
            let key = stableKey(result.primaryKey)
            guard let record = recordByID[key] else {
                return nil
            }
            return (record: record, rank: result.rank)
        }

        let rows = orderedResults.map { result in
            BridgedRow.encoding(
                any: result.record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.record.typeCode),
                    "rank": .int64(Int64(result.rank))
                ]
            )
        }
        return BridgedRowSet(rows: rows, ordering: .indexNative)
    }

    private func scanRanked(
        indexSubspace: Subspace,
        transaction: any Transaction,
        parameters: [String: QueryParameterValue]
    ) async throws -> [(primaryKey: Tuple, rank: Int)] {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)
        let mode = try requireString(RankReadParameter.mode, from: parameters)

        switch mode {
        case RankReadParameter.topMode:
            let count = try requireInt(RankReadParameter.count, from: parameters)
            let entries = try await scanner.top(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.bottomMode:
            let count = try requireInt(RankReadParameter.count, from: parameters)
            let entries = try await scanner.bottom(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.rangeMode:
            let from = try requireInt(RankReadParameter.from, from: parameters)
            let to = try requireInt(RankReadParameter.to, from: parameters)
            try validateRankRange(from: from, to: to)
            let entries = try await scanner.rangeDescending(from: from, to: to)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: from + $0.offset) }

        case RankReadParameter.percentileMode:
            let percentile = try requireDouble(RankReadParameter.percentile, from: parameters)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
            let totalCount = countBytes.map { Int(ByteConversion.bytesToInt64($0)) } ?? 0
            guard totalCount > 0 else { return [] }
            let targetRank = Int(Double(totalCount) * (1.0 - percentile))
            let safeRank = max(0, min(targetRank, totalCount - 1))
            guard let entry = try await scanner.nthFromTop(safeRank) else { return [] }
            return [(primaryKey: entry.primaryKey, rank: safeRank)]

        default:
            throw RankReadBridgeError.invalidParameter(RankReadParameter.mode)
        }
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

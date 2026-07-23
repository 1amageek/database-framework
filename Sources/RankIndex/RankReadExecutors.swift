#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseValue
import Core
import QueryIR
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

public enum RankReadExecutors {
    public static var indexExecutor: any IndexReadExecutor { RankReadExecutor() }
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicRankReadExecutor()
    }
}

private func validateRankRange(from: Int, to: Int) throws {
    guard from >= 0 else {
        throw RankReadError.invalidRange(from: from, to: to)
    }
    guard to > from else {
        throw RankReadError.invalidRange(from: from, to: to)
    }
}

private func validateRankCount(_ count: Int) throws {
    guard count > 0 else {
        throw RankReadError.invalidCount(count)
    }
}

private func validatePercentile(_ value: Double) throws {
    guard value >= 0.0 && value <= 1.0 else {
        throw RankReadError.invalidPercentile(value)
    }
}

private struct RankReadExecutor: IndexReadExecutor {
    let kindIdentifier = "rank"

    func executeRows<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = RankQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )

        let mode = try requireString(RankReadParameter.mode, from: indexScan.parameters)
        switch mode {
        case RankReadParameter.topMode:
            let count = try requireInt(RankReadParameter.count, from: indexScan.parameters)
            try validateRankCount(count)
            builder = builder.top(count)
        case RankReadParameter.bottomMode:
            let count = try requireInt(RankReadParameter.count, from: indexScan.parameters)
            try validateRankCount(count)
            builder = builder.bottom(count)
        case RankReadParameter.rangeMode:
            let from = try requireInt(RankReadParameter.from, from: indexScan.parameters)
            let to = try requireInt(RankReadParameter.to, from: indexScan.parameters)
            try validateRankRange(from: from, to: to)
            builder = builder.range(from: from, to: to)
        case RankReadParameter.percentileMode:
            let percentile = try requireDouble(RankReadParameter.percentile, from: indexScan.parameters)
            try validatePercentile(percentile)
            builder = builder.percentile(percentile)
        default:
            throw RankReadError.invalidParameter(RankReadParameter.mode)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                result.item,
                annotations: ["rank": .int64(Int64(result.rank))]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw RankReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw RankReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw RankReadError.invalidParameter(key)
        }
        return result
    }

    private func requireDouble(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Double {
        guard let value = parameters[key]?.doubleValue else {
            throw RankReadError.missingParameter(key)
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
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        _ = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try RankReadResultAssembler.orderByFields(
            from: selectQuery.orderBy
        )
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

        let orderedResults = try RankReadResultAssembler.assemble(
            rankedKeys: rankedKeys,
            records: records
        )

        let rows = try orderedResults.map { result in
            try IndexReadRow.materializing(
                any: result.record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.record.typeCode),
                    "rank": .int64(Int64(result.rank))
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
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
            try validateRankCount(count)
            let entries = try await scanner.top(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.bottomMode:
            let count = try requireInt(RankReadParameter.count, from: parameters)
            try validateRankCount(count)
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
            try validatePercentile(percentile)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
            let totalCount: Int
            if let countBytes {
                totalCount = try RankCounterCodec.decodeInt(countBytes)
            } else {
                totalCount = 0
            }
            guard totalCount > 0 else { return [] }
            let targetRank = Int(Double(totalCount) * (1.0 - percentile))
            let safeRank = max(0, min(targetRank, totalCount - 1))
            guard let entry = try await scanner.nthFromTop(safeRank) else {
                throw RankReadError.missingRankEntry(rank: safeRank)
            }
            return [(primaryKey: entry.primaryKey, rank: safeRank)]

        default:
            throw RankReadError.invalidParameter(RankReadParameter.mode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw RankReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw RankReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw RankReadError.invalidParameter(key)
        }
        return result
    }

    private func requireDouble(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Double {
        guard let value = parameters[key]?.doubleValue else {
            throw RankReadError.missingParameter(key)
        }
        return value
    }
}

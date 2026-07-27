import DatabaseEngine
import DatabaseTypes
import DatabaseKit
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
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let parameters = IndexReadParameters(indexScan.parameters)
        let fieldName = try parameters.requireString(
            named: RankReadParameter.fieldName
        )

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = RankQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            selectedIndexName: indexScan.indexName
        )

        let mode = try parameters.requireString(named: RankReadParameter.mode)
        switch mode {
        case RankReadParameter.topMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            builder = builder.top(count)
        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            builder = builder.bottom(count)
        case RankReadParameter.rangeMode:
            let from = try parameters.requireInteger(
                named: RankReadParameter.from
            )
            let to = try parameters.requireInteger(
                named: RankReadParameter.to
            )
            try validateRankRange(from: from, to: to)
            builder = builder.range(from: from, to: to)
        case RankReadParameter.percentileMode:
            let percentile = try parameters.requireFloatingPoint(
                named: RankReadParameter.percentile
            )
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
}

private struct PolymorphicRankReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "rank"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let parameters = IndexReadParameters(indexScan.parameters)
        _ = try parameters.requireString(named: RankReadParameter.fieldName)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: try runtimeInteger(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try runtimeInteger(
                selectQuery.offset,
                parameter: "offset"
            ),
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
                parameters: parameters
            )
        }

        let entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: rankedKeys.map { $0.primaryKey },
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        let orderedResults = try RankReadResultAssembler.assemble(
            rankedKeys: rankedKeys,
            entities: entities
        )

        let rows = try orderedResults.map { result in
            try IndexReadRow.materializing(
                any: result.entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.entity.typeCode),
                    "rank": .int64(Int64(result.rank))
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func scanRanked(
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        parameters: IndexReadParameters
    ) async throws -> [(primaryKey: Tuple, rank: Int)] {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)
        let mode = try parameters.requireString(named: RankReadParameter.mode)

        switch mode {
        case RankReadParameter.topMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let entries = try await scanner.top(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let entries = try await scanner.bottom(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.rangeMode:
            let from = try parameters.requireInteger(
                named: RankReadParameter.from
            )
            let to = try parameters.requireInteger(
                named: RankReadParameter.to
            )
            try validateRankRange(from: from, to: to)
            let entries = try await scanner.rangeDescending(from: from, to: to)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: from + $0.offset) }

        case RankReadParameter.percentileMode:
            let percentile = try parameters.requireFloatingPoint(
                named: RankReadParameter.percentile
            )
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

    private func runtimeInteger(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else {
            return nil
        }
        guard let result = Int(exactly: value) else {
            throw RankReadError.invalidParameter(parameter)
        }
        return result
    }

}

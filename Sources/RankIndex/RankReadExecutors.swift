import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicRankReadExecutor()
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(RankReadExecutor())
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

private struct RankBottomScanPlan {
    let limit: Int
    let startRank: Int
}

private func makeBottomScanPlan(
    requestedCount: Int,
    totalCount: Int
) throws -> RankBottomScanPlan {
    guard totalCount >= 0 else {
        throw RankScannerError.inconsistentCount(
            totalCount: totalCount,
            returnedCount: 0
        )
    }
    guard totalCount > 0 else {
        // Read one entry when the counter says empty so a non-empty index is
        // reported as corruption instead of being silently hidden.
        return RankBottomScanPlan(limit: 1, startRank: 0)
    }

    let startRank = try RankScanner.bottomStartPosition(
        totalCount: totalCount,
        returnedCount: 0
    )
    let limit: Int
    if totalCount == Int.max {
        limit = requestedCount
    } else {
        // When the requested page reaches the recorded tail, one extra bounded
        // row detects an undercount at that boundary without scanning the
        // unrequested remainder.
        limit = min(requestedCount, totalCount + 1)
    }
    return RankBottomScanPlan(limit: limit, startRank: startRank)
}

private func validateBottomScanCount(
    totalCount: Int,
    returnedCount: Int
) throws {
    guard returnedCount <= totalCount else {
        throw RankScannerError.inconsistentCount(
            totalCount: totalCount,
            returnedCount: returnedCount
        )
    }
}

private struct RankReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .rank

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        []
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let transaction = session.transaction
        let parameters = IndexReadParameters(indexScan.parameters)
        let fieldName = try parameters.requireString(
            named: RankReadParameter.fieldName
        )

        guard index.type == indexType,
            index.fieldNames == [fieldName] else {
            throw RankReadError.invalidParameter(RankReadParameter.fieldName)
        }
        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            return .empty
        }
        let rankedEntries = try await scanRanked(
            indexSubspace: readableIndex.subspace,
            transaction: transaction,
            parameters: parameters,
            workMeter: options.workMeter
        )
        let fetched = try await session.fetchRetainedPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: rankedEntries,
            partitions: partitions,
            snapshot: options.consistency == .snapshot
        )
        guard fetched.count == rankedEntries.count else {
            throw RankReadError.fetchedEntityCountMismatch(
                expected: rankedEntries.count,
                actual: fetched.count
            )
        }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: rankedEntries.count
        ) { rows in
            for index in 0..<rankedEntries.count {
                guard let item = fetched[index] else {
                    var missingPrimaryKey: ByteString?
                    rankedEntries.withRetainedPrimaryKey(at: index) {
                        missingPrimaryKey = $0.pack()
                    }
                    guard let missingPrimaryKey else {
                        preconditionFailure(
                            "A missing retained rank entry did not expose its key"
                        )
                    }
                    throw RankReadError.missingFetchedEntity(
                        primaryKey: missingPrimaryKey
                    )
                }
                try rankedEntries.withAnnotation(at: index) { annotation in
                    try item.withModel { model in
                        let rank = FieldValue.int64(Int64(annotation.rank))
                        let footprint = try CanonicalRelationalFootprintMeter
                            .footprint(
                                item.queryRowFootprint,
                                appendingAnnotationNamed: "rank",
                                value: rank,
                                workMeter: options.workMeter
                            )
                        try rows.append(footprint: footprint) {
                            try IndexReadRow.materializing(
                                model,
                                annotations: ["rank": rank]
                            )
                        }
                    }
                }
            }
        }
    }

    private func scanRanked(
        indexSubspace: Subspace,
        transaction: DatabaseReadTransaction,
        parameters: IndexReadParameters,
        workMeter: DatabaseWorkMeter
    ) async throws -> RankScanResult {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(
            scoresSubspace: scoresSubspace,
            transaction: transaction,
            workMeter: workMeter
        )
        let mode = try parameters.requireString(named: RankReadParameter.mode)
        switch mode {
        case RankReadParameter.topMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            return try await scanner.top(k: count)
        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let totalCount = try await readRankCount(
                key: countKey,
                transaction: transaction,
                workMeter: workMeter
            )
            let plan = try makeBottomScanPlan(
                requestedCount: count,
                totalCount: totalCount
            )
            let rankedEntries = try await scanner.bottom(
                k: plan.limit,
                startRank: plan.startRank
            )
            try validateBottomScanCount(
                totalCount: totalCount,
                returnedCount: rankedEntries.count
            )
            return rankedEntries
        case RankReadParameter.rangeMode:
            let from = try parameters.requireInteger(
                named: RankReadParameter.from
            )
            let to = try parameters.requireInteger(
                named: RankReadParameter.to
            )
            try validateRankRange(from: from, to: to)
            return try await scanner.rangeDescending(
                from: from,
                to: to
            )
        case RankReadParameter.percentileMode:
            let percentile = try parameters.requireFloatingPoint(
                named: RankReadParameter.percentile
            )
            try validatePercentile(percentile)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let totalCount = try await readRankCount(
                key: countKey,
                transaction: transaction,
                workMeter: workMeter
            )
            guard totalCount > 0 else { return try await scanner.top(k: 0) }
            let targetRank = Int(Double(totalCount) * (1.0 - percentile))
            let safeRank = max(0, min(targetRank, totalCount - 1))
            let entries = try await scanner.nthFromTop(safeRank)
            guard entries.count == 1 else {
                throw RankReadError.missingRankEntry(rank: safeRank)
            }
            return entries
        default:
            throw RankReadError.invalidParameter(RankReadParameter.mode)
        }
    }
}

private func readRankCount(
    key: ByteString,
    transaction: DatabaseReadTransaction,
    workMeter: DatabaseWorkMeter
) async throws -> Int {
    let bytes = try await transaction.readPointValue(
        for: key,
        snapshot: true,
        workMeter: workMeter,
        at: .indexScan
    )
    return try bytes.map(RankCounterCodec.decodeInt) ?? 0
}

private struct PolymorphicRankReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .rank

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        []
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let transaction = session.transaction
        let parameters = IndexReadParameters(indexScan.parameters)
        let fieldName = try parameters.requireString(
            named: RankReadParameter.fieldName
        )
        guard index.type == indexType,
            index.fieldNames == [fieldName] else {
            throw RankReadError.invalidParameter(
                RankReadParameter.fieldName
            )
        }
        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )

        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            return .empty
        }
        let rankedEntries = try await scanRanked(
            indexSubspace: readableIndex.subspace,
            transaction: transaction,
            parameters: parameters,
            workMeter: options.workMeter
        )
        let entities = try await session.fetchRetainedPolymorphicItemsPreservingOrder(
            group: group,
            ids: rankedEntries,
            snapshot: options.consistency == .snapshot
        )
        guard rankedEntries.count == entities.count else {
            throw RankReadError.fetchedEntityCountMismatch(
                expected: rankedEntries.count,
                actual: entities.count
            )
        }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: rankedEntries.count
        ) { rows in
            for index in 0..<rankedEntries.count {
                var isPresent = false
                var missingPrimaryKey: ByteString?
                try rankedEntries.withAnnotation(at: index) { annotation in
                    isPresent = try entities.appendIndexRow(
                        at: index,
                        to: &rows,
                        additionalAnnotation: (
                            name: "rank",
                            value: .int64(Int64(annotation.rank))
                        )
                    )
                }
                if !isPresent {
                    rankedEntries.withRetainedPrimaryKey(at: index) {
                        missingPrimaryKey = $0.pack()
                    }
                }
                if !isPresent {
                    guard let missingPrimaryKey else {
                        preconditionFailure(
                            "A missing retained rank entry did not expose its key"
                        )
                    }
                    throw RankReadError.missingFetchedEntity(
                        primaryKey: missingPrimaryKey
                    )
                }
            }
        }
}
    private func scanRanked(
        indexSubspace: Subspace,
        transaction: DatabaseReadTransaction,
        parameters: IndexReadParameters,
        workMeter: DatabaseWorkMeter
    ) async throws -> RankScanResult {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(
            scoresSubspace: scoresSubspace,
            transaction: transaction,
            workMeter: workMeter
        )
        let mode = try parameters.requireString(named: RankReadParameter.mode)

        switch mode {
        case RankReadParameter.topMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            return try await scanner.top(k: count)

        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let totalCount = try await readRankCount(
                key: countKey,
                transaction: transaction,
                workMeter: workMeter
            )
            let plan = try makeBottomScanPlan(
                requestedCount: count,
                totalCount: totalCount
            )
            let rankedEntries = try await scanner.bottom(
                k: plan.limit,
                startRank: plan.startRank
            )
            try validateBottomScanCount(
                totalCount: totalCount,
                returnedCount: rankedEntries.count
            )
            return rankedEntries

        case RankReadParameter.rangeMode:
            let from = try parameters.requireInteger(
                named: RankReadParameter.from
            )
            let to = try parameters.requireInteger(
                named: RankReadParameter.to
            )
            try validateRankRange(from: from, to: to)
            return try await scanner.rangeDescending(from: from, to: to)

        case RankReadParameter.percentileMode:
            let percentile = try parameters.requireFloatingPoint(
                named: RankReadParameter.percentile
            )
            try validatePercentile(percentile)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let totalCount = try await readRankCount(
                key: countKey,
                transaction: transaction,
                workMeter: workMeter
            )
            guard totalCount > 0 else { return try await scanner.top(k: 0) }
            let targetRank = Int(Double(totalCount) * (1.0 - percentile))
            let safeRank = max(0, min(targetRank, totalCount - 1))
            let entries = try await scanner.nthFromTop(safeRank)
            guard entries.count == 1 else {
                throw RankReadError.missingRankEntry(rank: safeRank)
            }
            return entries

        default:
            throw RankReadError.invalidParameter(RankReadParameter.mode)
        }
    }

}

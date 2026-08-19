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

private struct RankReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .rank

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
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
        guard index.type == indexType,
            index.fieldNames == [fieldName] else {
            throw RankReadError.invalidParameter(RankReadParameter.fieldName)
        }
        try context.authorizeCanonicalListAccess(
            entity: entity,
            selectQuery: selectQuery
        )
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction -> IndexReadResult in
            guard let readableIndex else { return .empty }
            let rankedKeys = try await scanRanked(
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                parameters: parameters,
                workMeter: options.workMeter
            )
            let rankedKeyReservation = try reserveRankedKeys(
                rankedKeys,
                workMeter: options.workMeter
            )
            defer { rankedKeyReservation.release() }
            let primaryKeys = rankedKeys.map { $0.primaryKey }
            let primaryKeyReservation = try DatabaseIntermediateCollectionMeter
                .reserveTuples(
                    primaryKeys,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { primaryKeyReservation.release() }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: partitions,
                transaction: transaction,
                workMeter: options.workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePersistedModels(
                    fetched,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            guard fetched.count == rankedKeys.count else {
                throw RankReadError.fetchedEntityCountMismatch(
                    expected: rankedKeys.count,
                    actual: fetched.count
                )
            }
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                expectedCount: rankedKeys.count
            ) { rows in
                for (rankedKey, item) in zip(rankedKeys, fetched) {
                    guard let item else {
                        throw RankReadError.missingFetchedEntity(
                            primaryKey: rankedKey.primaryKey.pack()
                        )
                    }
                    try rows.append(
                        try IndexReadRow.materializing(
                            item,
                            annotations: [
                                "rank": .int64(Int64(rankedKey.rank))
                            ]
                        )
                    )
                }
            }
        }
    }

    private func scanRanked(
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        parameters: IndexReadParameters,
        workMeter: DatabaseWorkMeter
    ) async throws -> [(primaryKey: Tuple, rank: Int)] {
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
            return try await scanner.top(k: count).enumerated().map {
                (primaryKey: $0.element.primaryKey, rank: $0.offset)
            }
        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let entries = try await scanner.bottom(k: count)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let countBytes = try await transaction.getValue(
                for: countKey,
                snapshot: true
            )
            let totalCount = try countBytes.map(RankCounterCodec.decodeInt) ?? 0
            let startRank = try RankScanner.bottomStartPosition(
                totalCount: totalCount,
                returnedCount: entries.count
            )
            return entries.enumerated().map {
                (primaryKey: $0.element.primaryKey, rank: startRank - $0.offset)
            }
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
            ).enumerated().map {
                (primaryKey: $0.element.primaryKey, rank: from + $0.offset)
            }
        case RankReadParameter.percentileMode:
            let percentile = try parameters.requireFloatingPoint(
                named: RankReadParameter.percentile
            )
            try validatePercentile(percentile)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let countBytes = try await transaction.getValue(
                for: countKey,
                snapshot: true
            )
            let totalCount = try countBytes.map(RankCounterCodec.decodeInt) ?? 0
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
}

private struct PolymorphicRankReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .rank

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
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

        return try await context
            .executeCanonicalRead(
                configuration: execution.transactionConfiguration
            ) { transaction in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return .empty
            }
            let rankedKeys = try await scanRanked(
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                parameters: parameters,
                workMeter: options.workMeter
            )
            let rankedKeyReservation = try reserveRankedKeys(
                rankedKeys,
                workMeter: options.workMeter
            )
            defer { rankedKeyReservation.release() }
            let primaryKeys = rankedKeys.map { $0.primaryKey }
            let primaryKeyReservation = try DatabaseIntermediateCollectionMeter
                .reserveTuples(
                    primaryKeys,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { primaryKeyReservation.release() }
            let entities = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: primaryKeys,
                transaction: transaction,
                workMeter: options.workMeter
            )
            let entityReservation = try DatabaseIntermediateCollectionMeter
                .reservePolymorphicEntities(
                    entities,
                    workMeter: options.workMeter,
                    stage: .indexScan
                )
            defer { entityReservation.release() }
            guard rankedKeys.count == entities.count else {
                throw RankReadError.fetchedEntityCountMismatch(
                    expected: rankedKeys.count,
                    actual: entities.count
                )
            }
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                expectedCount: rankedKeys.count
            ) { rows in
                for (rankedKey, entity) in zip(rankedKeys, entities) {
                    guard let entity else {
                        throw RankReadError.missingFetchedEntity(
                            primaryKey: rankedKey.primaryKey.pack()
                        )
                    }
                    try rows.append(
                        try IndexReadRow.materializing(
                            entity.item,
                            annotations: [
                                PolymorphicRowAnnotation.typeName:
                                    .string(entity.typeName),
                                PolymorphicRowAnnotation.typeCode:
                                    .int64(entity.typeCode),
                                "rank": .int64(Int64(rankedKey.rank)),
                                ]
                            )
                    )
                }
            }
        }
    }

    private func scanRanked(
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        parameters: IndexReadParameters,
        workMeter: DatabaseWorkMeter
    ) async throws -> [(primaryKey: Tuple, rank: Int)] {
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
            let entries = try await scanner.top(k: count)
            return entries.enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.bottomMode:
            let count = try parameters.requireInteger(
                named: RankReadParameter.count
            )
            try validateRankCount(count)
            let entries = try await scanner.bottom(k: count)
            let countKey = indexSubspace.pack(Tuple("_count"))
            let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
            let totalCount = try countBytes.map(RankCounterCodec.decodeInt) ?? 0
            let startRank = try RankScanner.bottomStartPosition(
                totalCount: totalCount,
                returnedCount: entries.count
            )
            return entries.enumerated().map {
                (primaryKey: $0.element.primaryKey, rank: startRank - $0.offset)
            }

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

private func reserveRankedKeys(
    _ rankedKeys: [(primaryKey: Tuple, rank: Int)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: rankedKeys.count,
        element: (primaryKey: Tuple, rank: Int).self
    )
    for rankedKey in rankedKeys {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(rankedKey.primaryKey.pack().count)
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

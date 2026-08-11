import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

enum VersionReadParameter {
    static let primaryKey = "primaryKey"
    static let limit = "limit"
}

public enum VersionReadExecutors {
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicVersionReadExecutor()
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(VersionReadExecutor())
    }
}

private enum VersionReadError: Error, Sendable {
    case invalidParameter(String)
}

private struct VersionReadExecutor: IndexReadExecutor {
    let kindIdentifier = "version"

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
        let primaryKeyValues = try parameters.requireArray(
            named: VersionReadParameter.primaryKey
        )
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        try context.authorizeCanonicalListAccess(
            entity: entity,
            selectQuery: selectQuery
        )
        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let rawResults = try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            kindIdentifier: kindIdentifier,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction -> [(version: Version, data: ByteString)] in
            guard let readableIndex else { return [] }
            return try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).history(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction,
                workMeter: options.workMeter
            )
        }

        guard let runtime = context.container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw VersionReadError.invalidParameter(
                VersionReadParameter.primaryKey
            )
        }
        let historyReservation = try reserveVersionHistory(
            rawResults,
            workMeter: options.workMeter
        )
        defer { historyReservation.release() }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: rawResults.count
        ) { rows in
            for result in rawResults {
                try options.workMeter.consume(at: .indexScan)
                guard !result.data.isEmpty else { continue }
                let persisted = try DataAccess.deserializePersistedModel(
                    result.data,
                    expectedEntity: entity.name
                )
                let canonical = try runtime.canonicalized(persisted)
                try context.container.securityDelegate?.evaluateGet(
                    persisted,
                    fields: nil
                )
                try rows.append(
                    try IndexReadRow.materializing(
                        canonical,
                        annotations: [
                            "version": .bytes(result.version.bytes)
                        ]
                    )
                )
            }
        }
    }

}

private struct PolymorphicVersionReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "version"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: PolymorphicIndexMetadata,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let parameters = IndexReadParameters(indexScan.parameters)
        let primaryKeyValues = try parameters.requireArray(
            named: VersionReadParameter.primaryKey
        )
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        guard let firstPrimaryKey = primaryKey.first,
              case .signedInteger(let typeCode) = firstPrimaryKey.tupleValue
        else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }
        let runtimesByTypeCode = try context.polymorphicTypeMap(for: group)
        guard let runtime = runtimesByTypeCode[typeCode] else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
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
            orderBy: try selectQuery.requiredOrderByColumnNames()
        )

        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let rawResults = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction -> [(version: Version, data: ByteString)] in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return []
            }
            return try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).history(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction,
                workMeter: options.workMeter
            )
        }
        let historyReservation = try reserveVersionHistory(
            rawResults,
            workMeter: options.workMeter
        )
        defer { historyReservation.release() }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: rawResults.count
        ) { rows in
            for result in rawResults {
                try options.workMeter.consume(at: .indexScan)
                guard !result.data.isEmpty else { continue }
                let persistedModel = try DataAccess.deserializePersistedModel(
                    result.data,
                    expectedEntity: runtime.entity.name
                )
                let item = try runtime.canonicalized(persistedModel)
                try context.container.securityDelegate?.evaluateGet(
                    persistedModel,
                    fields: nil
                )
                try rows.append(
                    try IndexReadRow.materializing(
                        item,
                        annotations: [
                            PolymorphicRowAnnotation.typeName:
                                .string(runtime.entity.name),
                            PolymorphicRowAnnotation.typeCode:
                                .int64(typeCode),
                            "version": .bytes(result.version.bytes)
                        ]
                    )
                )
            }
        }
    }

    private func runtimeInteger(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let result = Int(exactly: value) else {
            throw VersionReadError.invalidParameter(parameter)
        }
        return result
    }

}

private func reserveVersionHistory(
    _ history: [(version: Version, data: ByteString)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: history.count,
        element: (version: Version, data: ByteString).self
    )
    for entry in history {
        let entryBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(entry.version.bytes.count)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(entry.data.count)
            )
        )
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: entryBytes.bytes
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
    let indexType: IndexType = .history

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
        let primaryKeyValues = try parameters.requireArray(
            named: VersionReadParameter.primaryKey
        )
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }

        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )
        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let rawResults: [(version: Version, data: ByteString)]
        if let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) {
            rawResults = try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).history(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )
        } else {
            rawResults = []
        }

        guard let runtime = try session.entityRuntime(named: entity.name) else {
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
    let indexType: IndexType = .history

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
        let primaryKeyValues = try parameters.requireArray(
            named: VersionReadParameter.primaryKey
        )
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        guard let firstPrimaryKey = primaryKey.first,
              case .signedInteger(let typeCode) = firstPrimaryKey.tupleValue
        else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }
        let runtimesByTypeCode = try session.polymorphicTypeMap(for: group)
        guard let runtime = runtimesByTypeCode[typeCode] else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }

        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )

        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let rawResults: [(version: Version, data: ByteString)]
        if let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) {
            rawResults = try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).history(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )
        } else {
            rawResults = []
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
                try rows.append(
                    try IndexReadRow.materializing(
                        item,
                        annotations: [
                            PolymorphicRowAnnotation.typeName:
                                .string(runtime.entity.name),
                            PolymorphicRowAnnotation.typeCode:
                                .int64(typeCode),
                            "version": .bytes(result.version.bytes),
                        ]
                    )
                )
            }
        }
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

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

        let additionalFieldNames = try additionalRequiredFieldNames(
            indexScan: indexScan
        )
        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: additionalFieldNames
        )
        guard let runtime = try session.entityRuntime(named: entity.name) else {
            throw VersionReadError.invalidParameter(
                VersionReadParameter.primaryKey
            )
        }
        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let retainedHistory: VersionRetainedHistory
        if let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) {
            retainedHistory = try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).retainedHistory(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction,
                snapshot: execution.consistency == .snapshot,
                workMeter: options.workMeter
            )
        } else {
            retainedHistory = try VersionRetainedHistory.empty(
                workMeter: options.workMeter
            )
        }

        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: retainedHistory.count
        ) { rows in
            for index in 0..<retainedHistory.count {
                try retainedHistory.withEntry(at: index) { result in
                    try result.withValues { version, data in
                        try options.workMeter.consume(at: .indexScan)
                        guard !data.isEmpty else { return }
                        let retained = try DatabaseRetainedStoredModel.decode(
                            data,
                            entity: entity.name,
                            runtime: runtime,
                            workMeter: options.workMeter,
                            stage: .indexScan
                        )
                        try retained.withModel { model in
                            let version = FieldValue.bytes(version.bytes)
                            var footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    of: model,
                                    workMeter: options.workMeter
                                )
                            footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    footprint,
                                    appendingAnnotationNamed: "version",
                                    value: version,
                                    workMeter: options.workMeter
                                )
                            try rows.append(
                                footprint: footprint
                            ) {
                                try IndexReadRow.materializing(
                                    model,
                                    annotations: [
                                        "version": version
                                    ]
                                )
                            }
                        }
                    }
                }
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
        let primaryKey = try primaryKeyValues.map {
            try DatabaseEngine.CanonicalTupleElementCodec.decode($0)
        }
        guard let firstPrimaryKey = primaryKey.first,
              case .signedInteger(let typeCode) = firstPrimaryKey.tupleValue
        else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }

        let additionalFieldNames = try additionalRequiredFieldNames(
            indexScan: indexScan
        )
        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: additionalFieldNames
        )

        let runtimesByTypeCode = try session.polymorphicTypeMap(for: group)
        guard let runtime = runtimesByTypeCode[typeCode] else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }

        let requestedLimit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw VersionReadError.invalidParameter(VersionReadParameter.limit)
        }
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let retainedHistory: VersionRetainedHistory
        if let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) {
            retainedHistory = try await VersionIndexReader(
                subspace: readableIndex.subspace
            ).retainedHistory(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction,
                snapshot: execution.consistency == .snapshot,
                workMeter: options.workMeter
            )
        } else {
            retainedHistory = try VersionRetainedHistory.empty(
                workMeter: options.workMeter
            )
        }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: retainedHistory.count
        ) { rows in
            for index in 0..<retainedHistory.count {
                try retainedHistory.withEntry(at: index) { result in
                    try result.withValues { version, data in
                        try options.workMeter.consume(at: .indexScan)
                        guard !data.isEmpty else { return }
                        let retained = try DatabaseRetainedStoredModel.decode(
                            data,
                            entity: runtime.entity.name,
                            runtime: runtime,
                            workMeter: options.workMeter,
                            stage: .indexScan
                        )
                        try retained.withModel { model in
                            let typeName = FieldValue.string(runtime.entity.name)
                            let typeCodeValue = FieldValue.int64(typeCode)
                            let version = FieldValue.bytes(version.bytes)
                            var footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    of: model,
                                    workMeter: options.workMeter
                                )
                            footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    footprint,
                                    appendingAnnotationNamed:
                                        PolymorphicRowAnnotation.typeName,
                                    value: typeName,
                                    workMeter: options.workMeter
                                )
                            footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    footprint,
                                    appendingAnnotationNamed:
                                        PolymorphicRowAnnotation.typeCode,
                                    value: typeCodeValue,
                                    workMeter: options.workMeter
                                )
                            footprint = try CanonicalRelationalFootprintMeter
                                .footprint(
                                    footprint,
                                    appendingAnnotationNamed: "version",
                                    value: version,
                                    workMeter: options.workMeter
                                )
                            try rows.append(
                                footprint: footprint
                            ) {
                                try IndexReadRow.materializing(
                                    model,
                                    annotations: [
                                        PolymorphicRowAnnotation.typeName:
                                            typeName,
                                        PolymorphicRowAnnotation.typeCode:
                                            typeCodeValue,
                                        "version": version,
                                    ]
                                )
                            }
                        }
                    }
                }
            }
        }
    }

}

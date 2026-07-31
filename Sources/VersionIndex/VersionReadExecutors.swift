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

    public static func register<Model: Persistable>(
        with definition: inout EntityRuntimeDefinition<Model>
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(VersionReadExecutor())
    }
}

private enum VersionReadError: Error, Sendable {
    case invalidParameter(String)
}

private struct VersionReadExecutor: IndexReadExecutor {
    let kindIdentifier = "version"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        as type: T.Type,
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
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = VersionQueryBuilder<T>(
            queryContext: queryContext,
            primaryKey: primaryKey
        )

        if let limit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        ) {
            builder = builder.limit(limit)
        }
        builder = builder.index(index.name)

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration
        )

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                result.item,
                annotations: [
                    "version": .bytes(
                        result.version.bytes
                    )
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
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

        let limit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
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
                transaction: transaction
            )
        }

        var results: [(version: Version, item: PersistedModel)] = []
        results.reserveCapacity(rawResults.count)
        for result in rawResults {
            guard !result.data.isEmpty else { continue }
            let persistedModel = try DataAccess.deserializePersistedModel(
                result.data,
                expectedEntity: runtime.entity.name
            )
            let item = try runtime.canonicalized(persistedModel)
            try context.container.securityDelegate?.evaluateGet(persistedModel)
            results.append((result.version, item))
        }

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                result.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(runtime.entity.name),
                    PolymorphicRowAnnotation.typeCode: .int64(typeCode),
                    "version": .bytes(
                        result.version.bytes
                    )
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

}

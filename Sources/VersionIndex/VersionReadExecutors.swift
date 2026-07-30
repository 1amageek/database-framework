import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

enum VersionReadParameter {
    static let primaryKey = "primaryKey"
    static let limit = "limit"
    static let indexName = "indexName"
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
        if let indexNameValue = parameters[VersionReadParameter.indexName] {
            guard case .string(let indexName) = indexNameValue else {
                throw IndexReadParameterError.invalid(
                    name: VersionReadParameter.indexName,
                    expected: "string"
                )
            }
            builder = builder.index(indexName)
        }

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

private struct PolymorphicVersionPlaceholder: Persistable {
    typealias ID = String

    var id: String = ""

    static var persistableType: String { "_PolymorphicVersionPlaceholder" }
    static var allFields: [String] { ["id"] }

    static func fieldNumber(for fieldName: String) -> Int? {
        fieldName == "id" ? 1 : nil
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

}

private struct PolymorphicVersionReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "version"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
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
        let indexName: String
        if let value = parameters[VersionReadParameter.indexName] {
            guard case .string(let suppliedIndexName) = value else {
                throw IndexReadParameterError.invalid(
                    name: VersionReadParameter.indexName,
                    expected: "string"
                )
            }
            indexName = suppliedIndexName
        } else {
            indexName = indexScan.indexName
        }
        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)

        let limit = try parameters.optionalInteger(
            named: VersionReadParameter.limit
        )
        let rawResults = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let maintainer = VersionIndexMaintainer<PolymorphicVersionPlaceholder>(
                index: Index(
                    name: indexName,
                    kind: IndexKindMetadata(
                        identifier: "version",
                        subspaceStructure: .hierarchical,
                        fields: [
                            IndexFieldMetadata(
                                identity: FieldIdentity(name: "id", number: 1)
                            )
                        ],
                        metadata: ["strategy": .string("keepAll")]
                    ),
                    rootExpression: EmptyKeyExpression()
                ),
                strategy: .keepAll,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                wallClock: context.container.wallClock
            )
            return try await maintainer.getVersionHistory(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction
            )
        }

        var results: [(version: Version, item: any Persistable)] = []
        results.reserveCapacity(rawResults.count)
        for result in rawResults {
            guard !result.data.isEmpty else { continue }
            let persistedModel = try DataAccess.deserializePersistedModel(
                result.data,
                expectedEntity: runtime.entity.name
            )
            let item = try runtime.decode(persistedModel)
            try context.container.securityDelegate?.evaluateGet(persistedModel)
            results.append((result.version, item))
        }

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                any: result.item,
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

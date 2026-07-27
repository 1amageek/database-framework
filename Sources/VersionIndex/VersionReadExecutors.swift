#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseTypes
import DatabaseKit

enum VersionReadParameter {
    static let primaryKey = "primaryKey"
    static let limit = "limit"
    static let indexName = "indexName"
}

public enum VersionReadExecutors {
    public static var indexExecutor: any IndexReadExecutor { VersionReadExecutor() }
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicVersionReadExecutor()
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
        guard let typeCode = primaryKey.first as? Int64 else {
            throw VersionReadError.invalidParameter(VersionReadParameter.primaryKey)
        }
        guard let runtimeType = resolveRuntimeType(
            typeCode: typeCode,
            group: group,
            context: context
        ) else {
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
                idExpression: FieldKeyExpression(fieldName: "id")
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
            let item = try DataAccess.deserializeAny(result.data, as: runtimeType)
            try context.container.securityDelegate?.evaluateGet(item)
            results.append((result.version, item))
        }

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                any: result.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(runtimeType.persistableType),
                    PolymorphicRowAnnotation.typeCode: .int64(typeCode),
                    "version": .bytes(
                        result.version.bytes
                    )
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func resolveRuntimeType(
        typeCode: Int64,
        group: PolymorphicGroup,
        context: DatabaseContext
    ) -> (any Persistable.Type)? {
        for typeName in group.memberTypeNames {
            guard let type = context.container.runtimeConfiguration
                    .persistableTypes.type(named: typeName),
                  let polymorphicType = type as? any Polymorphable.Type else {
                continue
            }
            if polymorphicType.typeCode(for: type.persistableType) == typeCode {
                return type
            }
        }
        return nil
    }
}

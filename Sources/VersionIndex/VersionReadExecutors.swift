#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseValue
import Core
import QueryIR

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
    case missingParameter(String)
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
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let primaryKeyValues = try requireArray(VersionReadParameter.primaryKey, from: indexScan.parameters)
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

        if let limit = indexScan.parameters[VersionReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }
        if let indexName = indexScan.parameters[VersionReadParameter.indexName]?.stringValue {
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
                        DatabaseBytes(retaining: result.version.bytes)
                    )
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func requireArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [QueryParameterValue] {
        guard let values = parameters[key]?.arrayValue else {
            throw VersionReadError.missingParameter(key)
        }
        return values
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

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<PolymorphicVersionPlaceholder, Value>) -> String {
        if keyPath == \PolymorphicVersionPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: PartialKeyPath<PolymorphicVersionPlaceholder>) -> String {
        if keyPath == \PolymorphicVersionPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PolymorphicVersionPlaceholder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct PolymorphicVersionReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "version"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let primaryKeyValues = try requireArray(VersionReadParameter.primaryKey, from: indexScan.parameters)
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
        let indexName = indexScan.parameters[VersionReadParameter.indexName]?.stringValue ?? indexScan.indexName
        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)

        let limit = indexScan.parameters[VersionReadParameter.limit]?.int64Value.map(Int.init)
        let rawResults = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let maintainer = VersionIndexMaintainer<PolymorphicVersionPlaceholder>(
                index: Index(
                    name: indexName,
                    kind: VersionIndexKind<PolymorphicVersionPlaceholder>(
                        fieldNames: ["id"],
                        strategy: .keepAll
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

        let results: [(version: Version, item: any Persistable)] = try rawResults.compactMap { result in
            guard !result.data.isEmpty else {
                return nil
            }
            let item = try DataAccess.deserializeAny(result.data, as: runtimeType)
            try context.container.securityDelegate?.evaluateGet(item)
            return (result.version, item)
        }

        let rows = try results.map { result in
            try IndexReadRow.materializing(
                any: result.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(runtimeType.persistableType),
                    PolymorphicRowAnnotation.typeCode: .int64(typeCode),
                    "version": .bytes(
                        DatabaseBytes(retaining: result.version.bytes)
                    )
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func requireArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [QueryParameterValue] {
        guard let values = parameters[key]?.arrayValue else {
            throw VersionReadError.missingParameter(key)
        }
        return values
    }

    private func resolveRuntimeType(
        typeCode: Int64,
        group: PolymorphicGroup,
        context: DatabaseContext
    ) -> (any Persistable.Type)? {
        for typeName in group.memberTypeNames {
            guard let type = context.container.schema.entity(named: typeName)?.persistableType,
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

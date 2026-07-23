#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseValue
import Core
import QueryIR
import Permuted
import StorageKit

enum PermutedReadParameter {
    static let queryType = "queryType"
    static let values = "values"
    static let permutation = "permutation"
    static let limit = "limit"

    static let prefixQuery = "prefix"
    static let exactQuery = "exact"
    static let allQuery = "all"
}

public enum PermutedReadExecutors {
    public static var indexExecutor: any IndexReadExecutor { PermutedReadExecutor() }
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicPermutedReadExecutor()
    }
}

private enum PermutedReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct PermutedReadExecutor: IndexReadExecutor {
    let kindIdentifier = "permuted"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = PermutedQueryBuilder<T>(
            queryContext: queryContext,
            indexName: indexScan.indexName,
            permutation: try decodePermutation(indexScan.parameters[PermutedReadParameter.permutation])
        )

        if let limit = indexScan.parameters[PermutedReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }

        let queryType = try requireString(PermutedReadParameter.queryType, from: indexScan.parameters)
        switch queryType {
        case PermutedReadParameter.prefixQuery:
            builder = builder.prefix(try decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]))
        case PermutedReadParameter.exactQuery:
            builder = builder.exact(try decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]))
        case PermutedReadParameter.allQuery:
            break
        default:
            throw PermutedReadError.invalidParameter(PermutedReadParameter.queryType)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        let rows = try results.map { try IndexReadRow.materializing($0) }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw PermutedReadError.missingParameter(key)
        }
        return value
    }

    private func decodePermutation(_ value: QueryParameterValue?) throws -> Permutation? {
        guard let values = value?.arrayValue else { return nil }
        let indices = try values.map { parameter in
            guard let intValue = parameter.int64Value else {
                throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
            }
            return Int(intValue)
        }
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw PermutedReadError.missingParameter(PermutedReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }
}

private struct PolymorphicPermutedPlaceholder: Persistable {
    typealias ID = String

    var id: String = ""

    static var persistableType: String { "_PolymorphicPermutedPlaceholder" }
    static var allFields: [String] { ["id"] }

    static func fieldNumber(for fieldName: String) -> Int? {
        fieldName == "id" ? 1 : nil
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<PolymorphicPermutedPlaceholder, Value>) -> String {
        if keyPath == \PolymorphicPermutedPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: PartialKeyPath<PolymorphicPermutedPlaceholder>) -> String {
        if keyPath == \PolymorphicPermutedPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PolymorphicPermutedPlaceholder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct PolymorphicPermutedReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "permuted"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: selectQuery.limit,
            offset: selectQuery.offset,
            orderBy: orderByFields
        )

        let resolved = try resolveIndex(
            from: indexScan.parameters[PermutedReadParameter.permutation],
            group: group,
            indexName: indexScan.indexName
        )
        let descriptor = resolved.descriptor
        let permutation = resolved.permutation
        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        var primaryKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let maintainer = PermutedIndexMaintainer<PolymorphicPermutedPlaceholder>(
                index: Index(
                    name: indexScan.indexName,
                    kind: descriptor.kind,
                    rootExpression: KeyExpressionFactory.from(keyPaths: descriptor.fieldNames),
                    isUnique: descriptor.unique,
                    storedFieldNames: descriptor.storedFieldNames
                ),
                permutation: permutation,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            let queryType = try requireString(PermutedReadParameter.queryType, from: indexScan.parameters)
            switch queryType {
            case PermutedReadParameter.prefixQuery:
                return try await maintainer.scanByPrefix(
                    prefixValues: decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.exactQuery:
                return try await maintainer.scanByExactMatch(
                    values: decodeTupleArray(indexScan.parameters[PermutedReadParameter.values]),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.allQuery:
                return try await maintainer.scanAll(transaction: transaction).map { Tuple($0.primaryKey) }
            default:
                throw PermutedReadError.invalidParameter(PermutedReadParameter.queryType)
            }
        }

        if let limit = indexScan.parameters[PermutedReadParameter.limit]?.int64Value,
           primaryKeys.count > Int(limit) {
            primaryKeys = Array(primaryKeys.prefix(Int(limit)))
        }

        let entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: primaryKeys,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = try entities.map { entity in
            try IndexReadRow.materializing(
                any: entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw PermutedReadError.missingParameter(key)
        }
        return value
    }

    private func resolveIndex(
        from value: QueryParameterValue?,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> (descriptor: IndexDescriptorMetadata, permutation: Permutation) {
        guard let descriptor = group.indexes.first(where: {
            $0.name == indexName && $0.kindIdentifier == kindIdentifier
        }),
        let indices = descriptor.kind.metadata["permutation"]?.intArrayValue else {
            throw PermutedReadError.missingParameter(PermutedReadParameter.permutation)
        }
        let canonicalPermutation = try Permutation(indices: indices)
        guard canonicalPermutation.size == descriptor.fieldNames.count else {
            throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
        }
        if let requestedPermutation = try decodePermutation(value),
           requestedPermutation != canonicalPermutation {
            throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
        }
        return (descriptor, canonicalPermutation)
    }

    private func decodePermutation(_ value: QueryParameterValue?) throws -> Permutation? {
        guard let values = value?.arrayValue else { return nil }
        let indices = try values.map { parameter in
            guard let intValue = parameter.int64Value else {
                throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
            }
            return Int(intValue)
        }
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw PermutedReadError.missingParameter(PermutedReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }
}

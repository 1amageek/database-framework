import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
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

public enum PermutedReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(PermutedReadExecutor())
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicPermutedReadExecutor())
    }
}

private enum PermutedReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct PermutedReadExecutor: IndexReadExecutor {
    let kindIdentifier = "permuted"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
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
            throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.queryType)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: results.count)
        }

        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = try page.items.map { item in
            try QueryRowCodec.encode(item)
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func makeCountResponse(
        selectQuery: SelectQuery,
        count: Int
    ) -> QueryResponse {
        let alias: String
        if case .items(let items) = selectQuery.projection,
           let first = items.first {
            alias = first.alias ?? "count"
        } else {
            alias = "count"
        }
        return QueryResponse(rows: [QueryRow(fields: [alias: .int64(Int64(count))])])
    }

    private func isCountProjection(_ selectQuery: SelectQuery) -> Bool {
        guard case .items(let items) = selectQuery.projection,
              items.count == 1,
              case .aggregate(.count(let expression, let distinct)) = items[0].expression,
              expression == nil,
              distinct == false else {
            return false
        }
        return true
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw PermutedReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodePermutation(_ value: QueryParameterValue?) throws -> Permutation? {
        guard let values = value?.arrayValue else { return nil }
        let indices = try values.map { parameter in
            guard let intValue = parameter.int64Value else {
                throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.permutation)
            }
            return Int(intValue)
        }
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw PermutedReadBridgeError.missingParameter(PermutedReadParameter.values)
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

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> QueryResponse {
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = selectQuery.orderBy?.compactMap { sortKey -> String? in
            guard case .column(let column) = sortKey.expression else { return nil }
            return column.column
        }
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: selectQuery.limit,
            offset: selectQuery.offset,
            orderBy: orderByFields
        )

        let permutation = try decodeResolvedPermutation(
            from: indexScan.parameters[PermutedReadParameter.permutation],
            group: group,
            indexName: indexScan.indexName
        )
        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        var primaryKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let fieldNames = (0..<permutation.size).map { "field\($0)" }
            let maintainer = PermutedIndexMaintainer<PolymorphicPermutedPlaceholder>(
                index: Index(
                    name: indexScan.indexName,
                    kind: PermutedIndexKind<PolymorphicPermutedPlaceholder>(
                        fieldNames: fieldNames,
                        permutation: permutation
                    ),
                    rootExpression: EmptyKeyExpression(),
                    keyPaths: []
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
                throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.queryType)
            }
        }

        if let limit = indexScan.parameters[PermutedReadParameter.limit]?.int64Value,
           primaryKeys.count > Int(limit) {
            primaryKeys = Array(primaryKeys.prefix(Int(limit)))
        }

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: primaryKeys.count)
        }

        let records = try await context.fetchPolymorphicItems(
            group: group,
            ids: primaryKeys,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: records,
            selectQuery: selectQuery,
            options: options
        )
        let rows = page.items.map { record in
            QueryRowCodec.encodeAny(
                record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(record.typeCode)
                ]
            )
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func makeCountResponse(
        selectQuery: SelectQuery,
        count: Int
    ) -> QueryResponse {
        let alias: String
        if case .items(let items) = selectQuery.projection,
           let first = items.first {
            alias = first.alias ?? "count"
        } else {
            alias = "count"
        }
        return QueryResponse(rows: [QueryRow(fields: [alias: .int64(Int64(count))])])
    }

    private func isCountProjection(_ selectQuery: SelectQuery) -> Bool {
        guard case .items(let items) = selectQuery.projection,
              items.count == 1,
              case .aggregate(.count(let expression, let distinct)) = items[0].expression,
              expression == nil,
              distinct == false else {
            return false
        }
        return true
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw PermutedReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodeResolvedPermutation(
        from value: QueryParameterValue?,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> Permutation {
        if let permutation = try decodePermutation(value) {
            return permutation
        }
        guard let descriptor = group.indexes.first(where: {
            $0.name == indexName && $0.kindIdentifier == kindIdentifier
        }),
        let indices = descriptor.kind.metadata["permutation"]?.intArrayValue else {
            throw PermutedReadBridgeError.missingParameter(PermutedReadParameter.permutation)
        }
        return try Permutation(indices: indices)
    }

    private func decodePermutation(_ value: QueryParameterValue?) throws -> Permutation? {
        guard let values = value?.arrayValue else { return nil }
        let indices = try values.map { parameter in
            guard let intValue = parameter.int64Value else {
                throw PermutedReadBridgeError.invalidParameter(PermutedReadParameter.permutation)
            }
            return Int(intValue)
        }
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw PermutedReadBridgeError.missingParameter(PermutedReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }
}

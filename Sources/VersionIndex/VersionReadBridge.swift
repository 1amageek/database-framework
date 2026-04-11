import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol

enum VersionReadParameter {
    static let primaryKey = "primaryKey"
    static let limit = "limit"
    static let indexName = "indexName"
}

public enum VersionReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(VersionReadExecutor())
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicVersionReadExecutor())
    }
}

private enum VersionReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct VersionReadExecutor: IndexReadExecutor {
    let kindIdentifier = "version"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let primaryKeyValues = try requireArray(VersionReadParameter.primaryKey, from: indexScan.parameters)
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
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

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: results.count)
        }

        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = try page.items.map { result in
            let data = try JSONEncoder().encode(result.item)
            let fields = try JSONDecoder().decode([String: FieldValue].self, from: data)
            return QueryRow(
                fields: fields,
                annotations: ["version": .data(Data(result.version.bytes))]
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

    private func requireArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [QueryParameterValue] {
        guard let values = parameters[key]?.arrayValue else {
            throw VersionReadBridgeError.missingParameter(key)
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

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> QueryResponse {
        let primaryKeyValues = try requireArray(VersionReadParameter.primaryKey, from: indexScan.parameters)
        let primaryKey = try primaryKeyValues.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        guard let typeCode = primaryKey.first as? Int64 else {
            throw VersionReadBridgeError.invalidParameter(VersionReadParameter.primaryKey)
        }
        guard let runtimeType = resolveRuntimeType(
            typeCode: typeCode,
            group: group,
            context: context
        ) else {
            throw VersionReadBridgeError.invalidParameter(VersionReadParameter.primaryKey)
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
                    rootExpression: EmptyKeyExpression(),
                    keyPaths: []
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

        let results: [(version: Version, item: any Persistable)] = rawResults.compactMap { result in
            guard !result.data.isEmpty else {
                return nil
            }
            do {
                let item = try DataAccess.deserializeAny(result.data, as: runtimeType)
                try context.container.securityDelegate?.evaluateGet(item)
                return (result.version, item)
            } catch {
                return nil
            }
        }

        if isCountProjection(selectQuery) {
            return makeCountResponse(selectQuery: selectQuery, count: results.count)
        }

        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = page.items.map { result in
            QueryRowCodec.encodeAny(
                result.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(runtimeType.persistableType),
                    PolymorphicRowAnnotation.typeCode: .int64(typeCode),
                    "version": .data(Data(result.version.bytes))
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

    private func requireArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [QueryParameterValue] {
        guard let values = parameters[key]?.arrayValue else {
            throw VersionReadBridgeError.missingParameter(key)
        }
        return values
    }

    private func resolveRuntimeType(
        typeCode: Int64,
        group: PolymorphicGroup,
        context: FDBContext
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

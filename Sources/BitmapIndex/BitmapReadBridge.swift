import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import StorageKit

enum BitmapReadParameter {
    static let fieldName = "fieldName"
    static let operation = "operation"
    static let values = "values"
    static let valueSets = "valueSets"
    static let limit = "limit"

    static let equalsOperation = "equals"
    static let inOperation = "in"
    static let andOperation = "and"
}

public enum BitmapReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(BitmapReadExecutor())
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicBitmapReadExecutor())
    }
}

private enum BitmapReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct BitmapReadExecutor: IndexReadExecutor {
    let kindIdentifier = "bitmap"

    func executeRows<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> BridgedRowSet {
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = BitmapQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )

        if let limit = indexScan.parameters[BitmapReadParameter.limit]?.int64Value {
            builder = builder.limit(Int(limit))
        }

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        switch operation {
        case BitmapReadParameter.equalsOperation:
            let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
            guard let first = values.first else {
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.values)
            }
            builder = builder.equalsAny(first)
        case BitmapReadParameter.inOperation:
            builder = builder.inAny(try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values]))
        case BitmapReadParameter.andOperation:
            builder = builder.allAny(try decodeTupleMatrix(indexScan.parameters[BitmapReadParameter.valueSets]))
        default:
            throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.operation)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = results.map { BridgedRow.encoding($0) }
        return BridgedRowSet(rows: rows, ordering: .indexNative)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw BitmapReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }

    private func decodeTupleMatrix(
        _ value: QueryParameterValue?
    ) throws -> [[any TupleElement & Sendable]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        }
    }
}

private struct PolymorphicBitmapPlaceholder: Persistable {
    typealias ID = String

    var id: String = ""

    static var persistableType: String { "_PolymorphicBitmapPlaceholder" }
    static var allFields: [String] { ["id"] }

    static func fieldNumber(for fieldName: String) -> Int? {
        fieldName == "id" ? 1 : nil
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<PolymorphicBitmapPlaceholder, Value>) -> String {
        if keyPath == \PolymorphicBitmapPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: PartialKeyPath<PolymorphicBitmapPlaceholder>) -> String {
        if keyPath == \PolymorphicBitmapPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PolymorphicBitmapPlaceholder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct PolymorphicBitmapReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "bitmap"

    func executeRows(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> BridgedRowSet {
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)
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

        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        let primaryKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction -> [Tuple] in
            let maintainer = BitmapIndexMaintainer<PolymorphicBitmapPlaceholder>(
                index: Index(
                    name: indexScan.indexName,
                    kind: BitmapIndexKind<PolymorphicBitmapPlaceholder>(fieldNames: [fieldName]),
                    rootExpression: FieldKeyExpression(fieldName: fieldName)
                ),
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            let bitmap: RoaringBitmap
            switch operation {
            case BitmapReadParameter.equalsOperation:
                let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
                guard let first = values.first else {
                    throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.values)
                }
                bitmap = try await maintainer.getBitmap(for: [first], transaction: transaction)

            case BitmapReadParameter.inOperation:
                let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
                let valueSets = values.map { [$0] as [any TupleElement] }
                bitmap = try await maintainer.orQuery(values: valueSets, transaction: transaction)

            case BitmapReadParameter.andOperation:
                let valueSets = try decodeTupleMatrix(indexScan.parameters[BitmapReadParameter.valueSets])
                let converted = valueSets.map { $0 as [any TupleElement] }
                bitmap = try await maintainer.andQuery(values: converted, transaction: transaction)

            default:
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.operation)
            }

            let limitedBitmap: RoaringBitmap
            if let limit = indexScan.parameters[BitmapReadParameter.limit]?.int64Value {
                let ids = bitmap.toArray()
                if ids.count > limit {
                    var truncated = RoaringBitmap()
                    for id in ids.prefix(Int(limit)) {
                        truncated.add(id)
                    }
                    limitedBitmap = truncated
                } else {
                    limitedBitmap = bitmap
                }
            } else {
                limitedBitmap = bitmap
            }
            return try await maintainer.getPrimaryKeys(from: limitedBitmap, transaction: transaction)
        }

        let records = try await context.fetchPolymorphicItems(
            group: group,
            ids: primaryKeys,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = records.map { record in
            BridgedRow.encoding(
                any: record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(record.typeCode)
                ]
            )
        }
        return BridgedRowSet(rows: rows, ordering: .indexNative)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw BitmapReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func decodeTupleArray(
        _ value: QueryParameterValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }

    private func decodeTupleMatrix(
        _ value: QueryParameterValue?
    ) throws -> [[any TupleElement & Sendable]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadBridgeError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadBridgeError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        }
    }
}

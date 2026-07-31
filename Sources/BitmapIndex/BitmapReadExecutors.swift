import DatabaseEngine
import DatabaseTypes
import DatabaseKit
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

public enum BitmapReadExecutors {
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicBitmapReadExecutor()
    }

    public static func register<Model: Persistable>(
        with definition: inout EntityRuntimeDefinition<Model>
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(BitmapReadExecutor())
    }
}

private enum BitmapReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct BitmapReadExecutor: IndexReadExecutor {
    let kindIdentifier = "bitmap"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = BitmapQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            selectedIndexName: index.name
        )

        if let limit = indexScan.parameters[BitmapReadParameter.limit]?.uint64Value {
            builder = builder.limit(limit)
        }

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        switch operation {
        case BitmapReadParameter.equalsOperation:
            let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
            guard let first = values.first else {
                throw BitmapReadError.invalidParameter(BitmapReadParameter.values)
            }
            builder = builder.equalsAny(first)
        case BitmapReadParameter.inOperation:
            builder = builder.inAny(try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values]))
        case BitmapReadParameter.andOperation:
            builder = builder.allAny(try decodeTupleMatrix(indexScan.parameters[BitmapReadParameter.valueSets]))
        default:
            throw BitmapReadError.invalidParameter(BitmapReadParameter.operation)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = try results.map { try IndexReadRow.materializing($0) }
        // Bitmap iteration order is by internal RoaringBitmap integer ID, which
        // does not correspond to any domain-meaningful field order. Callers must
        // supply explicit orderBy if they want deterministic output.
        return IndexReadResult(rows: rows, ordering: .unordered)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw BitmapReadError.missingParameter(key)
        }
        return value
    }

    private func decodeTupleArray(
        _ value: FieldValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }

    private func decodeTupleMatrix(
        _ value: FieldValue?
    ) throws -> [[any TupleElement & Sendable]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        }
    }
}

private struct PolymorphicBitmapReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "bitmap"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: PolymorphicIndexMetadata,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)
        guard index.kindIdentifier == BitmapIndexSpecification.identifier,
              index.subspaceStructure == .hierarchical,
              index.fieldNames == [fieldName],
              index.metadata.isEmpty else {
            throw BitmapReadError.invalidParameter(
                BitmapReadParameter.fieldName
            )
        }
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: try authorizationValue(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try authorizationValue(
                selectQuery.offset,
                parameter: "offset"
            ),
            orderBy: orderByFields
        )

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        let primaryKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction -> [Tuple] in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return []
            }
            let reader = BitmapIndexReader(
                subspace: readableIndex.subspace
            )

            let bitmap: RoaringBitmap
            switch operation {
            case BitmapReadParameter.equalsOperation:
                let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
                guard let first = values.first else {
                    throw BitmapReadError.invalidParameter(BitmapReadParameter.values)
                }
                bitmap = try await reader.bitmap(for: [first], transaction: transaction)

            case BitmapReadParameter.inOperation:
                let values = try decodeTupleArray(indexScan.parameters[BitmapReadParameter.values])
                let valueSets = values.map { [$0] as [any TupleElement] }
                bitmap = try await reader.union(of: valueSets, transaction: transaction)

            case BitmapReadParameter.andOperation:
                let valueSets = try decodeTupleMatrix(indexScan.parameters[BitmapReadParameter.valueSets])
                let converted = valueSets.map { $0 as [any TupleElement] }
                bitmap = try await reader.intersection(of: converted, transaction: transaction)

            default:
                throw BitmapReadError.invalidParameter(BitmapReadParameter.operation)
            }

            let limitedBitmap: RoaringBitmap
            if let limit = indexScan.parameters[BitmapReadParameter.limit]?.uint64Value {
                let ids = bitmap.toArray()
                if UInt64(ids.count) > limit {
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
            return try await reader.primaryKeys(for: limitedBitmap, transaction: transaction)
        }

        let entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: primaryKeys,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = try entities.map { entity in
            try IndexReadRow.materializing(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
        }
        // Bitmap iteration order is by internal RoaringBitmap integer ID, which
        // does not correspond to any domain-meaningful field order. Callers must
        // supply explicit orderBy if they want deterministic output.
        return IndexReadResult(rows: rows, ordering: .unordered)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw BitmapReadError.missingParameter(key)
        }
        return value
    }

    private func decodeTupleArray(
        _ value: FieldValue?
    ) throws -> [any TupleElement & Sendable] {
        guard let values = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
    }

    private func decodeTupleMatrix(
        _ value: FieldValue?
    ) throws -> [[any TupleElement & Sendable]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
        }
    }

    private func authorizationValue(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else {
            return nil
        }
        guard let result = Int(exactly: value) else {
            throw BitmapReadError.invalidParameter(parameter)
        }
        return result
    }
}

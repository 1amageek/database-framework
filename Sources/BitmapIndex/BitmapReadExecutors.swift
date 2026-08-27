import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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

    public static func register(
        with definition: inout EntityRuntimeDefinition
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(BitmapReadExecutor())
    }
}

private enum BitmapReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case missingFetchedEntity(ByteString)
}

private struct BitmapReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .bitmap

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
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)
        guard index.type == .bitmap,
            index.fieldNames == [fieldName] else {
            throw BitmapReadError.invalidParameter(
                BitmapReadParameter.fieldName
            )
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )
        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let requestedLimit = indexScan.parameters[
            BitmapReadParameter.limit
        ]?.uint64Value.flatMap { Int(exactly: $0) }
        if indexScan.parameters[BitmapReadParameter.limit] != nil,
           requestedLimit == nil {
            throw BitmapReadError.invalidParameter(BitmapReadParameter.limit)
        }
        let resultLimit = min(requestedLimit ?? budgetLimit, budgetLimit)
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            return .empty
        }
        let reader = BitmapIndexReader(subspace: readableIndex.subspace)
        let bitmap: BitmapReadOwner
        switch operation {
        case BitmapReadParameter.equalsOperation:
            let values = try decodeTupleArray(
                indexScan.parameters[BitmapReadParameter.values]
            )
            guard let first = values.first else {
                throw BitmapReadError.invalidParameter(
                    BitmapReadParameter.values
                )
            }
            bitmap = try await reader.bitmap(
                for: [first],
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )
        case BitmapReadParameter.inOperation:
            let values = try decodeTupleArray(
                indexScan.parameters[BitmapReadParameter.values]
            )
            bitmap = try await reader.union(
                of: values.map { [$0] as [any TupleElement] },
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )
        case BitmapReadParameter.andOperation:
            bitmap = try await reader.intersection(
                of: try decodeTupleMatrix(
                    indexScan.parameters[BitmapReadParameter.valueSets]
                ).map { $0 as [any TupleElement] },
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )
        default:
            throw BitmapReadError.invalidParameter(
                BitmapReadParameter.operation
            )
        }

        let retainedPrimaryKeys = try await reader.primaryKeys(
            for: bitmap,
            transaction: transaction.storageTransaction,
            limit: resultLimit,
            workMeter: options.workMeter
        )
        let fetched = try await session.fetchRetainedPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: retainedPrimaryKeys,
            partitions: partitions,
            snapshot: execution.consistency == .snapshot
        )
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            ordering: .unordered,
            expectedCount: fetched.count
        ) { rows in
            for position in 0..<fetched.count {
                guard let retained = fetched[position] else {
                    var primaryKey = ByteString()
                    retainedPrimaryKeys.withRetainedPrimaryKey(
                        at: position
                    ) { key in
                        primaryKey = key.pack()
                    }
                    throw BitmapReadError.missingFetchedEntity(primaryKey)
                }
                try retained.withModel { model in
                    try rows.append(
                        footprint: retained.queryRowFootprint
                    ) {
                        try IndexReadRow.materializing(model)
                    }
                }
            }
        }
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
    ) throws -> [any TupleElement] {
        guard let values = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try $0.toTupleElement() }
    }

    private func decodeTupleMatrix(
        _ value: FieldValue?
    ) throws -> [[any TupleElement]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try $0.toTupleElement() }
        }
    }
}

private struct PolymorphicBitmapReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .bitmap

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
        let fieldName = try requireString(BitmapReadParameter.fieldName, from: indexScan.parameters)
        guard index.type == .bitmap,
            index.fieldNames == [fieldName]
        else {
            throw BitmapReadError.invalidParameter(
                BitmapReadParameter.fieldName
            )
        }
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )

        let operation = try requireString(BitmapReadParameter.operation, from: indexScan.parameters)
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let requestedLimit = indexScan.parameters[
            BitmapReadParameter.limit
        ]?.uint64Value.flatMap { Int(exactly: $0) }
        if indexScan.parameters[BitmapReadParameter.limit] != nil,
           requestedLimit == nil {
            throw BitmapReadError.invalidParameter(BitmapReadParameter.limit)
        }
        let resultLimit = min(requestedLimit ?? budgetLimit, budgetLimit)
        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            return .empty
        }
        let reader = BitmapIndexReader(
            subspace: readableIndex.subspace
        )

        let bitmap: BitmapReadOwner
        switch operation {
        case BitmapReadParameter.equalsOperation:
            let values = try decodeTupleArray(
                indexScan.parameters[BitmapReadParameter.values]
            )
            guard let first = values.first else {
                throw BitmapReadError.invalidParameter(
                    BitmapReadParameter.values
                )
            }
            bitmap = try await reader.bitmap(
                for: [first],
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )

        case BitmapReadParameter.inOperation:
            let values = try decodeTupleArray(
                indexScan.parameters[BitmapReadParameter.values]
            )
            let valueSets = values.map { [$0] as [any TupleElement] }
            bitmap = try await reader.union(
                of: valueSets,
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )

        case BitmapReadParameter.andOperation:
            let valueSets = try decodeTupleMatrix(
                indexScan.parameters[BitmapReadParameter.valueSets]
            )
            let converted = valueSets.map { $0 as [any TupleElement] }
            bitmap = try await reader.intersection(
                of: converted,
                transaction: transaction.storageTransaction,
                workMeter: options.workMeter
            )

        default:
            throw BitmapReadError.invalidParameter(
                BitmapReadParameter.operation
            )
        }

        let retainedPrimaryKeys = try await reader.primaryKeys(
            for: bitmap,
            transaction: transaction.storageTransaction,
            limit: resultLimit,
            workMeter: options.workMeter
        )
        let fetched = try await session.fetchRetainedPolymorphicItemsPreservingOrder(
            group: group,
            ids: retainedPrimaryKeys,
            snapshot: execution.consistency == .snapshot
        )
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            ordering: .unordered,
            expectedCount: fetched.count
        ) { rows in
            for position in 0..<fetched.count {
                guard try fetched.appendIndexRow(
                    at: position,
                    to: &rows
                ) else {
                    var primaryKey: ByteString?
                    retainedPrimaryKeys.withRetainedPrimaryKey(
                        at: position
                    ) { key in
                        primaryKey = key.pack()
                    }
                    guard let primaryKey else {
                        throw BitmapReadError.missingFetchedEntity(
                            ByteString()
                        )
                    }
                    throw BitmapReadError.missingFetchedEntity(primaryKey)
                }
            }
        }
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
    ) throws -> [any TupleElement] {
        guard let values = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.values)
        }
        return try values.map { try $0.toTupleElement() }
    }

    private func decodeTupleMatrix(
        _ value: FieldValue?
    ) throws -> [[any TupleElement]] {
        guard let rows = value?.arrayValue else {
            throw BitmapReadError.missingParameter(BitmapReadParameter.valueSets)
        }
        return try rows.map { row in
            guard let values = row.arrayValue else {
                throw BitmapReadError.invalidParameter(BitmapReadParameter.valueSets)
            }
            return try values.map { try $0.toTupleElement() }
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

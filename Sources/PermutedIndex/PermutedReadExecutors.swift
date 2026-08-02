import DatabaseEngine
import DatabaseTypes
import DatabaseKit
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
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicPermutedReadExecutor()
    }

    public static func register<Model: Persistable>(
        with definition: inout EntityRuntimeDefinition<Model>
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(PermutedReadExecutor())
    }
}

private enum PermutedReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case missingFetchedEntity(ByteString)
}

private struct PermutedReadExecutor: IndexReadExecutor {
    let kindIdentifier = "permuted"

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
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = PermutedQueryBuilder<T>(
            queryContext: queryContext,
            indexName: index.name,
            permutation: try decodePermutation(parameters)
        )

        if let limit = try parameters.optionalInteger(
            named: PermutedReadParameter.limit
        ) {
            guard limit >= 0 else {
                throw PermutedReadError.invalidParameter(
                    PermutedReadParameter.limit
                )
            }
            builder = builder.limit(limit)
        }

        let queryType = try parameters.requireString(
            named: PermutedReadParameter.queryType
        )
        switch queryType {
        case PermutedReadParameter.prefixQuery:
            builder = builder.prefix(try decodeTupleArray(parameters))
        case PermutedReadParameter.exactQuery:
            builder = builder.exact(try decodeTupleArray(parameters))
        case PermutedReadParameter.allQuery:
            break
        default:
            throw PermutedReadError.invalidParameter(PermutedReadParameter.queryType)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration
        )

        let rows = try results.map { try IndexReadRow.materializing($0) }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func decodePermutation(
        _ parameters: IndexReadParameters
    ) throws -> Permutation? {
        guard let values = try parameters.optionalArray(
            named: PermutedReadParameter.permutation
        ) else {
            return nil
        }
        let indices = try integerValues(
            values,
            parameter: PermutedReadParameter.permutation
        )
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ parameters: IndexReadParameters
    ) throws -> [any TupleElement & Sendable] {
        let values = try parameters.requireArray(
            named: PermutedReadParameter.values
        )
        return try values.map { try FieldValueTupleCodec.tupleElement(for: $0) }
    }

    private func integerValues(
        _ values: [FieldValue],
        parameter: String
    ) throws -> [Int] {
        var result: [Int] = []
        result.reserveCapacity(values.count)
        for value in values {
            let element = IndexReadParameters(["value": value])
            do {
                result.append(try element.requireInteger(named: "value"))
            } catch {
                throw PermutedReadError.invalidParameter(parameter)
            }
        }
        return result
    }
}

private struct PolymorphicPermutedReadExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "permuted"

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
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: try runtimeInteger(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try runtimeInteger(
                selectQuery.offset,
                parameter: "offset"
            ),
            orderBy: orderByFields
        )

        let resolved = try resolveIndex(
            parameters: parameters,
            descriptor: index
        )
        let permutation = resolved.permutation

        let entities: [PolymorphicEntity] = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return []
            }
            let reader = PermutedIndexReader(
                permutation: permutation,
                subspace: readableIndex.subspace
            )

            let queryType = try parameters.requireString(
                named: PermutedReadParameter.queryType
            )
            let primaryKeys: [Tuple]
            switch queryType {
            case PermutedReadParameter.prefixQuery:
                primaryKeys = try await reader.primaryKeys(
                    prefixedBy: decodeTupleArray(parameters),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.exactQuery:
                primaryKeys = try await reader.primaryKeys(
                    matching: decodeTupleArray(parameters),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.allQuery:
                primaryKeys = try await reader.entries(
                    transaction: transaction
                ).map { Tuple($0.primaryKey) }
            default:
                throw PermutedReadError.invalidParameter(PermutedReadParameter.queryType)
            }
            let limitedPrimaryKeys: [Tuple]
            if let limit = try parameters.optionalInteger(
                named: PermutedReadParameter.limit
            ) {
                guard limit >= 0 else {
                    throw PermutedReadError.invalidParameter(
                        PermutedReadParameter.limit
                    )
                }
                limitedPrimaryKeys = primaryKeys.count > limit
                    ? Array(primaryKeys.prefix(limit))
                    : primaryKeys
            } else {
                limitedPrimaryKeys = primaryKeys
            }
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: limitedPrimaryKeys,
                transaction: transaction
            )
            var entities: [PolymorphicEntity] = []
            entities.reserveCapacity(fetched.count)
            for (primaryKey, entity) in zip(limitedPrimaryKeys, fetched) {
                guard let entity else {
                    throw PermutedReadError.missingFetchedEntity(
                        primaryKey.pack()
                    )
                }
                entities.append(entity)
            }
            return entities
        }
        let rows = try entities.map { entity in
            try IndexReadRow.materializing(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
    }

    private func resolveIndex(
        parameters: IndexReadParameters,
        descriptor: PolymorphicIndexMetadata
    ) throws -> (
        descriptor: PolymorphicIndexMetadata,
        permutation: Permutation
    ) {
        guard descriptor.kindIdentifier == kindIdentifier,
              case .array(let encodedIndices)? = descriptor.metadata[
                "permutation"
              ] else {
            throw PermutedReadError.missingParameter(PermutedReadParameter.permutation)
        }
        let indices = try integerValues(
            encodedIndices,
            parameter: PermutedReadParameter.permutation
        )
        let canonicalPermutation = try Permutation(indices: indices)
        guard canonicalPermutation.size == descriptor.fieldNames.count else {
            throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
        }
        if let requestedPermutation = try decodePermutation(parameters),
           requestedPermutation != canonicalPermutation {
            throw PermutedReadError.invalidParameter(PermutedReadParameter.permutation)
        }
        return (descriptor, canonicalPermutation)
    }

    private func decodePermutation(
        _ parameters: IndexReadParameters
    ) throws -> Permutation? {
        guard let values = try parameters.optionalArray(
            named: PermutedReadParameter.permutation
        ) else {
            return nil
        }
        let indices = try integerValues(
            values,
            parameter: PermutedReadParameter.permutation
        )
        return try Permutation(indices: indices)
    }

    private func decodeTupleArray(
        _ parameters: IndexReadParameters
    ) throws -> [any TupleElement & Sendable] {
        let values = try parameters.requireArray(
            named: PermutedReadParameter.values
        )
        return try values.map { try FieldValueTupleCodec.tupleElement(for: $0) }
    }

    private func integerValues(
        _ values: [FieldValue],
        parameter: String
    ) throws -> [Int] {
        var result: [Int] = []
        result.reserveCapacity(values.count)
        for value in values {
            let element = IndexReadParameters(["value": value])
            do {
                result.append(try element.requireInteger(named: "value"))
            } catch {
                throw PermutedReadError.invalidParameter(parameter)
            }
        }
        return result
    }

    private func runtimeInteger(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let result = Int(exactly: value) else {
            throw PermutedReadError.invalidParameter(parameter)
        }
        return result
    }
}

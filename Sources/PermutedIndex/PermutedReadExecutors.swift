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
}

private struct PermutedReadExecutor: IndexReadExecutor {
    let kindIdentifier = "permuted"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
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
            indexName: indexScan.indexName,
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
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
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
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
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
            group: group,
            indexName: indexScan.indexName
        )
        let permutation = resolved.permutation
        let indexSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        var primaryKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let reader = PermutedIndexReader(
                permutation: permutation,
                subspace: indexSubspace
            )

            let queryType = try parameters.requireString(
                named: PermutedReadParameter.queryType
            )
            switch queryType {
            case PermutedReadParameter.prefixQuery:
                return try await reader.primaryKeys(
                    prefixedBy: decodeTupleArray(parameters),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.exactQuery:
                return try await reader.primaryKeys(
                    matching: decodeTupleArray(parameters),
                    transaction: transaction
                ).map(Tuple.init)
            case PermutedReadParameter.allQuery:
                return try await reader.entries(
                    transaction: transaction
                ).map { Tuple($0.primaryKey) }
            default:
                throw PermutedReadError.invalidParameter(PermutedReadParameter.queryType)
            }
        }

        if let limit = try parameters.optionalInteger(
            named: PermutedReadParameter.limit
        ) {
            guard limit >= 0 else {
                throw PermutedReadError.invalidParameter(
                    PermutedReadParameter.limit
                )
            }
            if primaryKeys.count > limit {
                primaryKeys = Array(primaryKeys.prefix(limit))
            }
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

    private func resolveIndex(
        parameters: IndexReadParameters,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> (
        descriptor: PolymorphicIndexMetadata,
        permutation: Permutation
    ) {
        guard let descriptor = group.indexes.first(where: {
            $0.name == indexName && $0.kindIdentifier == kindIdentifier
        }),
        case .array(let encodedIndices)? = descriptor.metadata["permutation"] else {
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
        return try values.map { try DatabaseEngine.CanonicalTupleElementCodec.decode($0) }
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

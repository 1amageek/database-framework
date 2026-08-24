import DatabaseKit
import DatabaseTypes

package enum CanonicalPartitionBinding {
    package static func validate(
        _ partitions: FieldObject,
        for entity: Schema.Entity
    ) throws {
        let requiredNames = entity.dynamicFieldNames
        guard !requiredNames.isEmpty else {
            guard partitions.isEmpty else {
                throw CanonicalReadError.invalidPartition(
                    entity: entity.name,
                    reason: "the entity has a static directory"
                )
            }
            return
        }

        guard Set(partitions.fields.map { $0.key }) == Set(requiredNames) else {
            throw CanonicalReadError.invalidPartition(
                entity: entity.name,
                reason: "expected exactly \(requiredNames.sorted())"
            )
        }

        for fieldName in requiredNames {
            guard let value = partitions[fieldName],
                  let field = entity.fields.first(where: {
                      $0.name == fieldName && $0.fieldNumber > 0
                  }) else {
                throw CanonicalReadError.invalidPartition(
                    entity: entity.name,
                    reason: "partition field '\(fieldName)' is missing from the schema"
                )
            }
            guard acceptsPartitionValue(value, field: field) else {
                throw CanonicalReadError.invalidPartition(
                    entity: entity.name,
                    reason: "partition field '\(fieldName)' must be a non-null required scalar matching its schema type"
                )
            }
        }
    }

    package static func makeBinding<T: Persistable>(
        for type: T.Type,
        partitions: FieldObject
    ) throws -> DirectoryPath<T>? {
        let dynamicFieldNames = T.directoryFieldNames
        guard !dynamicFieldNames.isEmpty else {
            guard partitions.isEmpty else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "the entity has a static directory"
                )
            }
            return nil
        }

        let requiredNames = dynamicFieldNames
        guard Set(partitions.fields.map { $0.key }) == Set(requiredNames) else {
            throw CanonicalReadError.invalidPartition(
                entity: T.persistableType,
                reason: "expected exactly \(requiredNames.sorted())"
            )
        }

        var binding = DirectoryPath<T>()
        for fieldName in dynamicFieldNames {
            guard let partitionValue = partitions[fieldName],
                  let fieldSchema = try T.fieldSchemas.first(where: {
                      $0.name == fieldName && $0.fieldNumber > 0
                  }) else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "partition field '\(fieldName)' is missing from the compiled schema"
                )
            }
            guard acceptsPartitionValue(
                partitionValue,
                field: fieldSchema
            ) else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "partition field '\(fieldName)' must be a non-null required scalar matching its schema type"
                )
            }
            binding.fieldValues.append(
                DirectoryFieldBinding(
                    field: FieldIdentity(
                        name: fieldSchema.name,
                        number: fieldSchema.fieldNumber
                    ),
                    value: partitionValue
                )
            )
        }

        _ = try binding.canonicalPartitions()
        return binding
    }

    private static func acceptsPartitionValue(
        _ value: FieldValue,
        field: FieldSchema
    ) -> Bool {
        !field.isOptional
            && !field.isArray
            && field.type != .nested
            && value != .null
            && FieldSchemaValueValidator.accepts(value, as: field.type)
    }

    package static func makeAnyBinding(
        for entity: Schema.Entity,
        partitions: FieldObject
    ) throws -> AnyDirectoryPath? {
        guard entity.hasDynamicDirectory || !partitions.isEmpty else {
            return nil
        }
        return try AnyDirectoryPath(
            entity: entity,
            partitions: partitions
        )
    }

}

// MARK: - DatabaseContext + Typed SelectQuery Adapter

extension DatabaseContext {
    /// Execute a canonical read query and return wire-level rows after validating the typed source.
    public func query<T: Persistable>(
        _ selectQuery: SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default
    ) async throws -> QueryResponse {
        try validateTypedSelectQuery(selectQuery, matches: type)
        return try await query(
            selectQuery,
            options: options
        )
    }

    /// Execute a canonical read query and decode typed models from row fields.
    public func execute<T: Persistable>(
        _ selectQuery: SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default
    ) async throws -> [T] {
        let response = try await query(
            selectQuery,
            as: type,
            options: options
        )
        return try response.rows.map { try QueryRowCodec.decode($0, as: type) }
    }

    private func validateTypedSelectQuery<T: Persistable>(
        _ selectQuery: SelectQuery,
        matches type: T.Type
    ) throws {
        guard case .table(let tableRef) = selectQuery.source else {
            return
        }

        guard tableRef.table == T.persistableType else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Table '\(tableRef.table)' does not match type '\(T.persistableType)'"
            )
        }
    }
}

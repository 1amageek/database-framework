#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import QueryIR

package enum CanonicalPartitionBinding {
    package static func makeBinding<T: Persistable>(
        for type: T.Type,
        partitions: [DatabaseObjectField]
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

        var fieldsByName: [String: DatabaseObjectField] = [:]
        var seenNumbers = Set<UInt32>()
        for partition in partitions {
            guard seenNumbers.insert(partition.number).inserted,
                  fieldsByName.updateValue(partition, forKey: partition.name) == nil else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "partition fields must have unique names and numbers"
                )
            }
        }

        let requiredNames = dynamicFieldNames
        guard Set(fieldsByName.keys) == Set(requiredNames) else {
            throw CanonicalReadError.invalidPartition(
                entity: T.persistableType,
                reason: "expected exactly \(requiredNames.sorted())"
            )
        }

        var binding = DirectoryPath<T>()
        for fieldName in dynamicFieldNames {
            guard let partition = fieldsByName[fieldName],
                  let fieldSchema = T.fieldSchemas.first(where: {
                      $0.name == fieldName && $0.fieldNumber == Int(partition.number)
                  }) else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "partition field '\(fieldName)' does not match the compiled schema"
                )
            }
            guard !fieldSchema.isOptional, !fieldSchema.isArray else {
                throw CanonicalReadError.invalidPartition(
                    entity: T.persistableType,
                    reason: "partition field '\(fieldName)' must be a required scalar"
                )
            }
            binding.fieldValues.append(
                DirectoryFieldBinding(
                    name: fieldName,
                    value: try directoryValue(
                    partition.value,
                    schema: fieldSchema,
                    entity: T.persistableType
                    )
                )
            )
        }

        try binding.validate()
        return binding
    }

    package static func makeAnyBinding(
        for type: any Persistable.Type,
        partitions: [DatabaseObjectField]
    ) throws -> AnyDirectoryPath? {
        func bind<T: Persistable>(_ concreteType: T.Type) throws -> AnyDirectoryPath? {
            try makeBinding(for: concreteType, partitions: partitions).map(AnyDirectoryPath.init)
        }
        return try _openExistential(type, do: bind)
    }

    private static func directoryValue(
        _ value: DatabaseValue,
        schema: FieldSchema,
        entity: String
    ) throws -> any Sendable {
        func mismatch() -> CanonicalReadError {
            .invalidPartition(
                entity: entity,
                reason: "partition field '\(schema.name)' does not match type \(schema.type.rawValue)"
            )
        }

        switch schema.type {
        case .string:
            guard case .string(let scalar) = value else { throw mismatch() }
            return scalar
        case .int:
            guard case .int64(let scalar) = value,
                  let converted = Int(exactly: scalar) else { throw mismatch() }
            return converted
        case .int8:
            guard case .int64(let scalar) = value,
                  let converted = Int8(exactly: scalar) else { throw mismatch() }
            return converted
        case .int16:
            guard case .int64(let scalar) = value,
                  let converted = Int16(exactly: scalar) else { throw mismatch() }
            return converted
        case .int32:
            guard case .int64(let scalar) = value,
                  let converted = Int32(exactly: scalar) else { throw mismatch() }
            return converted
        case .int64:
            guard case .int64(let scalar) = value else { throw mismatch() }
            return scalar
        case .uint:
            guard case .uint64(let scalar) = value,
                  let converted = UInt(exactly: scalar) else { throw mismatch() }
            return converted
        case .uint8:
            guard case .uint64(let scalar) = value,
                  let converted = UInt8(exactly: scalar) else { throw mismatch() }
            return converted
        case .uint16:
            guard case .uint64(let scalar) = value,
                  let converted = UInt16(exactly: scalar) else { throw mismatch() }
            return converted
        case .uint32:
            guard case .uint64(let scalar) = value,
                  let converted = UInt32(exactly: scalar) else { throw mismatch() }
            return converted
        case .uint64:
            guard case .uint64(let scalar) = value else { throw mismatch() }
            return scalar
        case .double:
            guard case .double(let scalar) = value, scalar.isFinite else { throw mismatch() }
            return scalar
        case .float:
            guard case .double(let scalar) = value,
                  scalar.isFinite,
                  let converted = Float(exactly: scalar) else { throw mismatch() }
            return converted
        case .bool:
            guard case .bool(let scalar) = value else { throw mismatch() }
            return scalar
        case .date:
            guard case .timestamp(let scalar) = value else { throw mismatch() }
            return Date(
                timeIntervalSince1970: Double(scalar.secondsSinceUnixEpoch)
                    + Double(scalar.nanoseconds) / 1_000_000_000
            )
        case .uuid:
            guard case .uuid(let scalar) = value else { throw mismatch() }
            let bytes = scalar.bytes
            return UUID(uuid: (
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11],
                bytes[12], bytes[13], bytes[14], bytes[15]
            ))
        case .data:
            guard case .bytes(let scalar) = value else { throw mismatch() }
            return Data(scalar)
        case .rdfTerm:
            guard case .rdfTerm(let term) = value else { throw mismatch() }
            return term
        case .enum:
            switch value {
            case .string(let scalar): return scalar
            case .int64(let scalar): return scalar
            case .uint64(let scalar): return scalar
            default: throw mismatch()
            }
        case .nested:
            throw CanonicalReadError.invalidPartition(
                entity: entity,
                reason: "nested field '\(schema.name)' cannot be a partition"
            )
        case .reference:
            throw CanonicalReadError.invalidPartition(
                entity: entity,
                reason: "reference field '\(schema.name)' cannot be a partition"
            )
        }
    }

}

// MARK: - DatabaseContext + Typed SelectQuery Adapter

extension DatabaseContext {
    /// Execute a canonical read query and return wire-level rows after validating the typed source.
    public func query<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
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
        _ selectQuery: QueryIR.SelectQuery,
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
        _ selectQuery: QueryIR.SelectQuery,
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

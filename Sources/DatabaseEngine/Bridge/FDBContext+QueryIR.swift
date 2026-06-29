import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

enum CanonicalPartitionBinding {
    static func makeBinding<T: Persistable>(
        for type: T.Type,
        partitionValues: [String: String]?
    ) throws -> DirectoryPath<T>? {
        guard let partitionValues else { return nil }

        let allowedFields = Set(T.directoryFieldNames)
        if let unexpectedField = Set(partitionValues.keys).subtracting(allowedFields).sorted().first {
            throw CanonicalReadError.invalidPartitionField(unexpectedField)
        }

        var binding = DirectoryPath<T>()
        for component in T.directoryPathComponents {
            guard let dynamicElement = component as? any DynamicDirectoryElement,
                  let keyPath = dynamicElement.anyKeyPath as? PartialKeyPath<T> else {
                continue
            }

            let fieldName = T.fieldName(for: dynamicElement.anyKeyPath)
            guard let value = partitionValues[fieldName] else { continue }
            binding.fieldValues.append((keyPath, value))
        }

        if T.hasDynamicDirectory || !binding.fieldValues.isEmpty {
            try binding.validate()
            return binding
        }
        return nil
    }
}

// MARK: - FDBContext + Typed SelectQuery Adapter

extension FDBContext {
    /// Execute a canonical read query and return wire-level rows after validating the typed source.
    public func query<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default,
        partitionValues: [String: String]? = nil
    ) async throws -> QueryResponse {
        try validateTypedSelectQuery(selectQuery, matches: type)
        return try await query(
            selectQuery,
            options: options,
            partitionValues: partitionValues
        )
    }

    /// Execute a canonical read query and decode typed models from row fields.
    public func execute<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default,
        partitionValues: [String: String]? = nil
    ) async throws -> [T] {
        let response = try await query(
            selectQuery,
            as: type,
            options: options,
            partitionValues: partitionValues
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

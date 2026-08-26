// DataStore.swift
// DatabaseEngine - Model persistence service contract
//
// Storage backends conform to StorageEngine. DataStore is the higher-level
// model persistence contract consumed by query and statistics services.
//
// Authorization is an implementation-owned operation policy.

import DatabaseKit

/// Model persistence service contract.
///
/// Backend-specific transaction and key-value behavior belongs to `StorageEngine`.
/// The canonical runtime implementation applies schema, security, query, and
/// mutation semantics on top of an injected engine.
///
public protocol DataStore: AnyObject, Sendable {

    // MARK: - Fetch Operations

    /// Fetch models matching a query
    ///
    /// Authorization: LIST access is evaluated before storage execution.
    ///
    /// This method should:
    /// - Evaluate LIST security (limit, offset, orderBy)
    /// - Apply predicates (where clauses)
    /// - Apply sorting (orderBy)
    /// - Apply pagination (limit, offset)
    /// - Use indexes when available for optimization
    ///
    /// - Parameter query: The query to execute
    /// - Returns: Array of matching models
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T]

    /// Fetch a single model by ID
    ///
    /// Authorization: GET access is evaluated before returning a model.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    /// - Returns: The model if found and access is allowed, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetch<T: Persistable>(_ type: T.Type, id: T.ID) async throws -> T?

    /// Fetch all models of a type
    ///
    /// Authorization: LIST access is evaluated before storage execution.
    ///
    /// **Note**: Use with caution for large datasets.
    /// Consider using `fetch(_:Query)` with pagination instead.
    ///
    /// - Parameter type: The model type
    /// - Returns: Array of all models of the type
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T]

    /// Fetch count of models matching a query
    ///
    /// Authorization: LIST access is evaluated before storage execution.
    ///
    /// This method may be optimized to avoid loading full model data.
    ///
    /// - Parameter query: The query to count
    /// - Returns: Count of matching models
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int

    // MARK: - Write Operations

    /// Execute batch save and delete operations
    ///
    /// Authorization:
    /// - CREATE operation is evaluated for new entities
    /// - UPDATE operation is evaluated for existing entities (with old and new values)
    /// - DELETE operation is evaluated for entities being deleted
    ///
    /// All operations are executed atomically in a single transaction.
    /// If any operation fails (including security), all changes are rolled back.
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    /// - Throws: SecurityError if any operation is not allowed, or other errors on failure
    func executeBatch(
        inserts: [PersistedModel],
        deletes: [PersistedModel]
    ) async throws

    // MARK: - Transaction Operations

    /// Execute operations within a transaction
    ///
    /// Security: Each operation within the transaction is evaluated separately
    /// through the transaction's typed database operations.
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (priority, timeout, retry)
    ///   - operation: The closure to execute within the transaction
    /// - Returns: The result of the operation closure
    /// - Throws: SecurityError or other errors from the operation
    func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
    ) async throws -> T
}

public extension DataStore {
    /// Executes a homogeneous typed batch without opening a Persistable
    /// existential. Models cross into the heterogeneous transaction boundary
    /// exactly once as canonical persisted models.
    func executeBatch<Model: Persistable>(
        inserts: [Model],
        deletes: [Model]
    ) async throws {
        var persistedInserts: [PersistedModel] = []
        persistedInserts.reserveCapacity(inserts.count)
        for model in inserts {
            persistedInserts.append(try PersistedModel(model))
        }

        var persistedDeletes: [PersistedModel] = []
        persistedDeletes.reserveCapacity(deletes.count)
        for model in deletes {
            persistedDeletes.append(try PersistedModel(model))
        }

        try await executeBatch(
            inserts: persistedInserts,
            deletes: persistedDeletes
        )
    }
}

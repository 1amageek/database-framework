// DataStore.swift
// DatabaseEngine - Model persistence service contract
//
// Storage backends conform to StorageEngine. DataStore is the higher-level
// model persistence contract consumed by query and statistics services.
//
// Security: DataStore uses DataStoreSecurityDelegate for access control.
// Auth context is obtained via TaskLocal (AuthContextKey.current).

import Core

/// Model persistence service contract.
///
/// Backend-specific transaction and key-value behavior belongs to `StorageEngine`.
/// The canonical runtime implementation applies schema, security, query, and
/// mutation semantics on top of an injected engine.
///
/// **Security**:
/// DataStore holds a security delegate that evaluates permissions.
/// Auth context is obtained via `AuthContextKey.current` (TaskLocal).
///
/// ```swift
/// // Set auth context per request
/// try await AuthContextKey.$current.withValue(userAuth) {
///     let context = container.newContext()
///     try await context.save()  // Security evaluated via delegate
/// }
/// ```
///
public protocol DataStore: AnyObject, Sendable {

    // MARK: - Security

    /// Security delegate for access control evaluation
    ///
    /// If nil, security evaluation is skipped.
    var securityDelegate: (any DataStoreSecurityDelegate)? { get }

    // MARK: - Fetch Operations

    /// Fetch models matching a query
    ///
    /// Security: LIST operation is evaluated via securityDelegate.
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
    /// Security: GET operation is evaluated via securityDelegate after fetch.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    /// - Returns: The model if found and access is allowed, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetch<T: Persistable>(_ type: T.Type, id: T.ID) async throws -> T?

    /// Fetch all models of a type
    ///
    /// Security: LIST operation is evaluated via securityDelegate.
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
    /// Security: LIST operation is evaluated via securityDelegate.
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
    /// Security (evaluated via securityDelegate):
    /// - CREATE operation is evaluated for new records
    /// - UPDATE operation is evaluated for existing records (with old and new values)
    /// - DELETE operation is evaluated for records being deleted
    ///
    /// All operations are executed atomically in a single transaction.
    /// If any operation fails (including security), all changes are rolled back.
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    /// - Throws: SecurityError if any operation is not allowed, or other errors on failure
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
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

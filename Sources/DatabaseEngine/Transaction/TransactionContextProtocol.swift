// TransactionContextProtocol.swift
// DatabaseEngine - Protocol for transactional operations

import Foundation
import FoundationDB
import Core

/// Protocol for transactional operations
///
/// TransactionContextProtocol defines the interface for transactional data access.
/// Implementations are returned by `DataStore.withTransaction()` and include
/// security evaluation within each operation.
///
/// **Design Rationale**:
/// By defining transactions as a protocol, the DataStore can return a security-aware
/// implementation that evaluates permissions on each operation, ensuring no data
/// access can bypass security rules.
///
/// **Read Modes**:
/// - `get(snapshot: false)` (default): Transactional read with conflict tracking.
///   If another transaction writes to this data before commit, retry occurs.
/// - `get(snapshot: true)`: Snapshot read without conflict tracking.
///   May return stale data but avoids conflicts for non-critical reads.
///
/// **Usage**:
/// ```swift
/// try await store.withTransaction(security: securityContext) { tx in
///     // GET security evaluated here
///     let user = try await tx.get(User.self, id: userId)
///
///     // CREATE/UPDATE security evaluated here
///     try await tx.set(updatedUser)
///
///     // DELETE security evaluated here
///     try await tx.delete(oldUser)
/// }
/// ```
public protocol TransactionContextProtocol: Sendable {

    // MARK: - Read Operations

    /// Get a single model by ID
    ///
    /// Security: GET operation is evaluated after fetch.
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The model's identifier
    ///   - snapshot: If `false` (default), adds read conflict for serializable isolation.
    ///               If `true`, performs snapshot read with no conflict (may be stale).
    /// - Returns: The model if found and access is allowed, nil if not found
    /// - Throws: SecurityError if access is denied, or other errors on failure
    func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        snapshot: Bool
    ) async throws -> T?

    /// Get multiple models by IDs
    ///
    /// Security: GET operation is evaluated for each found model.
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - ids: The model identifiers
    ///   - snapshot: If `false` (default), adds read conflict. If `true`, snapshot read.
    /// - Returns: Array of found models (missing IDs are skipped)
    /// - Throws: SecurityError if access is denied for any model
    func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        snapshot: Bool
    ) async throws -> [T]

    // MARK: - Write Operations

    /// Set (insert or update) a model
    ///
    /// Security: CREATE operation is evaluated for new records,
    /// UPDATE operation is evaluated for existing records (with old and new values).
    ///
    /// - Parameter model: The model to save
    /// - Throws: SecurityError if access is denied, or other errors on failure
    func set<T: Persistable>(_ model: T) async throws

    /// Delete a model
    ///
    /// Security: DELETE operation is evaluated with the model being deleted.
    ///
    /// - Parameter model: The model to delete
    /// - Throws: SecurityError if access is denied, or other errors on failure
    func delete<T: Persistable>(_ model: T) async throws

    /// Delete a model by ID
    ///
    /// Fetches the model first to properly clean up indexes and evaluate security.
    ///
    /// Security: GET operation is evaluated first (to fetch the model),
    /// then DELETE operation is evaluated.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - id: The model's identifier
    /// - Throws: SecurityError if access is denied, or other errors on failure
    func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws

    // MARK: - Raw Access

    /// Access the underlying transaction for advanced operations
    ///
    /// **Warning**: Direct transaction access bypasses security evaluation.
    /// Use with caution for operations that don't involve Persistable data.
    var rawTransaction: any TransactionProtocol { get }
}

// MARK: - Default Parameter Values

public extension TransactionContextProtocol {
    /// Get a model with default snapshot = false
    func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement
    ) async throws -> T? {
        try await get(type, id: id, snapshot: false)
    }

    /// Get many models with default snapshot = false
    func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement]
    ) async throws -> [T] {
        try await getMany(type, ids: ids, snapshot: false)
    }
}

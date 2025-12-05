// ModelPersistenceHandler.swift
// DatabaseEngine - Protocol for model persistence operations within transactions

import FoundationDB
import Core

/// Protocol for handling model persistence operations within a transaction
///
/// This protocol abstracts save/delete/load operations for external modules
/// (like RelationshipIndex) that need to modify models without depending
/// on FDBContext internals.
///
/// **Design Rationale**:
/// - DatabaseEngine defines the protocol (contract)
/// - External modules depend only on the protocol
/// - FDBContext provides implementation via `makePersistenceHandler()`
///
/// **Usage**:
/// ```swift
/// // In RelationshipIndex module
/// public func enforceDeleteRules(
///     for model: any Persistable,
///     transaction: any TransactionProtocol,
///     handler: ModelPersistenceHandler
/// ) async throws {
///     try await handler.delete(relatedModel, transaction: transaction)
/// }
/// ```
public protocol ModelPersistenceHandler: Sendable {
    /// Save a model with full index updates within an existing transaction
    ///
    /// - Parameters:
    ///   - model: The model to save
    ///   - transaction: The existing transaction context
    func save(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws

    /// Delete a model with full index cleanup within an existing transaction
    ///
    /// - Parameters:
    ///   - model: The model to delete
    ///   - transaction: The existing transaction context
    func delete(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws

    /// Load a model by type name and ID within an existing transaction
    ///
    /// - Parameters:
    ///   - typeName: The Persistable type name (e.g., "Customer")
    ///   - id: The model's primary key as Tuple
    ///   - transaction: The existing transaction context
    /// - Returns: The loaded model, or nil if not found
    func load(
        _ typeName: String,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> (any Persistable)?
}

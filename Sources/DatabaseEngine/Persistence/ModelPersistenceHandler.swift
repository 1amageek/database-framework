// ModelPersistenceHandler.swift
// DatabaseEngine - Protocol for model persistence operations within transactions

import StorageKit
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
///     transaction: any Transaction,
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
        precondition: WritePrecondition,
        transaction: any Transaction
    ) async throws

    /// Delete a model with full index cleanup within an existing transaction
    ///
    /// - Parameters:
    ///   - model: The model to delete
    ///   - transaction: The existing transaction context
    func delete(
        _ model: any Persistable,
        precondition: WritePrecondition,
        transaction: any Transaction
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
        partition: AnyDirectoryPath?,
        transaction: any Transaction
    ) async throws -> (any Persistable)?

    /// Scan a bounded set of models within an existing transaction.
    ///
    /// This operation is intentionally bounded because it is used by dynamic
    /// statement execution where the concrete Persistable type is discovered
    /// from the compiled schema at runtime.
    func scan(
        _ typeName: String,
        partition: AnyDirectoryPath?,
        limit: Int,
        transaction: any Transaction
    ) async throws -> [any Persistable]

    /// Validates the final transaction state after every primary mutation is visible.
    func validateFinalState(
        of models: [any Persistable],
        transaction: any Transaction
    ) async throws
}

public extension ModelPersistenceHandler {
    func save(
        _ model: any Persistable,
        transaction: any Transaction
    ) async throws {
        try await save(model, precondition: .none, transaction: transaction)
    }

    func delete(
        _ model: any Persistable,
        transaction: any Transaction
    ) async throws {
        try await delete(model, precondition: .none, transaction: transaction)
    }

    func load(
        _ typeName: String,
        id: Tuple,
        transaction: any Transaction
    ) async throws -> (any Persistable)? {
        try await load(typeName, id: id, partition: nil, transaction: transaction)
    }
}

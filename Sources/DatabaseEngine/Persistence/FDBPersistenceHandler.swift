// FDBPersistenceHandler.swift
// DatabaseEngine - ModelPersistenceHandler implementation using FDBContext

import Foundation
import FoundationDB
import Core

/// FDBContext-based implementation of ModelPersistenceHandler
///
/// This struct provides model persistence operations within transactions,
/// used by external modules like RelationshipIndex.
///
/// **Usage**:
/// ```swift
/// let handler = context.makePersistenceHandler()
/// try await handler.save(model, transaction: tx)
/// ```
public struct FDBPersistenceHandler: ModelPersistenceHandler {
    private let context: FDBContext

    internal init(context: FDBContext) {
        self.context = context
    }

    public func save(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)
        let store = try await context.container.store(for: modelType)
        try await store.executeBatchInTransaction(
            inserts: [model],
            deletes: [],
            transaction: transaction
        )
    }

    public func delete(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)
        let store = try await context.container.store(for: modelType)
        try await store.executeBatchInTransaction(
            inserts: [],
            deletes: [model],
            transaction: transaction
        )
    }

    public func load(
        _ typeName: String,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> (any Persistable)? {
        guard let entity = context.container.schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }

        let subspace = try await context.container.resolveDirectory(for: entity.persistableType)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let key = typeSubspace.pack(id)

        guard let data = try await transaction.getValue(for: key, snapshot: false) else {
            return nil
        }

        let decoder = ProtobufDecoder()
        return try decoder.decode(entity.persistableType, from: Data(data))
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Create a ModelPersistenceHandler for use in transactions
    ///
    /// External modules (like RelationshipIndex) use this handler to perform
    /// model persistence operations without depending on FDBContext internals.
    ///
    /// **Usage**:
    /// ```swift
    /// let handler = context.makePersistenceHandler()
    /// try await maintainer.enforceDeleteRules(
    ///     for: model,
    ///     transaction: tx,
    ///     handler: handler
    /// )
    /// ```
    public func makePersistenceHandler() -> ModelPersistenceHandler {
        FDBPersistenceHandler(context: self)
    }
}

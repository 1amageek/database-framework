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
/// **Note**: This handler extracts partition information from model instances
/// for dynamic directory types. For `load()`, it requires static directories
/// since partition info is not available from just a type name and ID.
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

        // For dynamic directory types, extract partition from model instance
        let store: any DataStore
        if hasDynamicDirectory(modelType) {
            let binding = buildAnyDirectoryPath(from: model)
            store = try await context.container.store(for: modelType, path: binding)
        } else {
            store = try await context.container.store(for: modelType)
        }

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

        // For dynamic directory types, extract partition from model instance
        let store: any DataStore
        if hasDynamicDirectory(modelType) {
            let binding = buildAnyDirectoryPath(from: model)
            store = try await context.container.store(for: modelType, path: binding)
        } else {
            store = try await context.container.store(for: modelType)
        }

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

        guard let persistableType = entity.persistableType else {
            throw FDBRuntimeError.internalError("Entity '\(typeName)' has no Persistable type")
        }

        // Dynamic directory types cannot be loaded without partition info
        if hasDynamicDirectory(persistableType) {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: typeName,
                fields: extractDirectoryFieldNames(persistableType)
            )
        }

        let subspace = try await context.container.resolveDirectory(for: persistableType)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let key = typeSubspace.pack(id)

        // Use ItemStorage to properly read ItemEnvelope format
        let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
        guard let data = try await storage.read(for: key) else {
            return nil
        }

        let decoder = ProtobufDecoder()
        return try decoder.decode(persistableType, from: Data(data))
    }

    // MARK: - Private Helpers

    /// Check if a type has dynamic directory (contains Field components)
    private func hasDynamicDirectory(_ type: any Persistable.Type) -> Bool {
        type.directoryPathComponents.contains { $0 is any DynamicDirectoryElement }
    }

    /// Extract directory field names for error messages
    private func extractDirectoryFieldNames(_ type: any Persistable.Type) -> [String] {
        type.directoryPathComponents.compactMap { component -> String? in
            guard let dynamicElement = component as? any DynamicDirectoryElement else { return nil }
            return type.fieldName(for: dynamicElement.anyKeyPath)
        }
    }

    /// Build type-erased partition binding from a model instance
    private func buildAnyDirectoryPath(from model: any Persistable) -> AnyDirectoryPath {
        let modelType = type(of: model)
        var bindings: [(keyPath: AnyKeyPath, value: any Sendable)] = []

        for component in modelType.directoryPathComponents {
            if let dynamicElement = component as? any DynamicDirectoryElement {
                let keyPath = dynamicElement.anyKeyPath
                let fieldName = modelType.fieldName(for: keyPath)
                if let value = model[dynamicMember: fieldName] {
                    bindings.append((keyPath, value))
                }
            }
        }

        return AnyDirectoryPath(fieldValues: bindings, type: modelType)
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

// FDBPersistenceHandler.swift
// DatabaseEngine - ModelPersistenceHandler implementation using FDBContext

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
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
        precondition: WritePrecondition,
        transaction: any Transaction
    ) async throws {
        let modelType = type(of: model)

        // For dynamic directory types, extract partition from model instance
        let store: FDBDataStore
        if hasDynamicDirectory(modelType) {
            let binding = try buildAnyDirectoryPath(from: model)
            store = try await context.container.fdbStore(
                for: modelType,
                path: binding,
                transaction: transaction
            )
        } else {
            store = try await context.container.fdbStore(
                for: modelType,
                transaction: transaction
            )
        }

        let serialized = try await store.executeBatchInTransactionWithPreconditions(
            inserts: [model],
            deletes: [],
            transaction: transaction,
            skipExistingCheck: false,
            insertPreconditions: [precondition],
            deletePreconditions: []
        )
        try await context.processDualWrites(
            serializedInserts: serialized,
            deletes: [],
            transaction: transaction
        )
    }

    public func delete(
        _ model: any Persistable,
        precondition: WritePrecondition,
        transaction: any Transaction
    ) async throws {
        let modelType = type(of: model)

        // For dynamic directory types, extract partition from model instance
        let store: FDBDataStore
        if hasDynamicDirectory(modelType) {
            let binding = try buildAnyDirectoryPath(from: model)
            store = try await context.container.fdbStore(
                for: modelType,
                path: binding,
                transaction: transaction
            )
        } else {
            store = try await context.container.fdbStore(
                for: modelType,
                transaction: transaction
            )
        }

        try await store.executeBatchInTransactionWithPreconditions(
            inserts: [],
            deletes: [model],
            transaction: transaction,
            skipExistingCheck: false,
            insertPreconditions: [],
            deletePreconditions: [precondition]
        )
        try await context.processDualWrites(
            serializedInserts: [],
            deletes: [model],
            transaction: transaction
        )
    }

    public func load(
        _ typeName: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        transaction: any Transaction
    ) async throws -> (any Persistable)? {
        guard let entity = context.container.schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }

        guard let persistableType = entity.persistableType else {
            throw FDBRuntimeError.internalError("Entity '\(typeName)' has no Persistable type")
        }

        let subspace: Subspace
        if hasDynamicDirectory(persistableType), partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: typeName,
                fields: extractDirectoryFieldNames(persistableType)
            )
        }
        if let partition {
            subspace = try await context.container.openDirectory(
                for: persistableType,
                path: partition,
                transaction: transaction
            )
        } else {
            subspace = try await context.container.openDirectory(
                for: persistableType,
                transaction: transaction
            )
        }
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let key = typeSubspace.pack(id)

        // Use ItemStorage to properly read ItemEnvelope format
        let storage = context.container.itemStorageFactory.make(transaction: transaction, blobsSubspace: blobsSubspace)
        guard let data = try await storage.read(for: key) else {
            return nil
        }

        return try DataAccess.deserializeAny(data, as: persistableType)
    }

    public func scan(
        _ typeName: String,
        partition: AnyDirectoryPath?,
        limit: Int,
        transaction: any Transaction
    ) async throws -> [any Persistable] {
        guard limit > 0 else {
            throw FDBRuntimeError.internalError("Transaction-scoped scans require a positive limit")
        }
        guard let entity = context.container.schema.entities.first(where: { $0.name == typeName }) else {
            throw FDBRuntimeError.internalError("Entity '\(typeName)' is not registered in the compiled schema")
        }
        guard let persistableType = entity.persistableType else {
            throw FDBRuntimeError.internalError("Entity '\(typeName)' has no Persistable type")
        }

        if hasDynamicDirectory(persistableType), partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: typeName,
                fields: extractDirectoryFieldNames(persistableType)
            )
        }

        try context.container.securityDelegate?.evaluateList(
            type: persistableType,
            limit: limit,
            offset: nil,
            orderBy: nil
        )

        let subspace: Subspace
        if let partition {
            subspace = try await context.container.openDirectory(
                for: persistableType,
                path: partition,
                transaction: transaction
            )
        } else {
            subspace = try await context.container.openDirectory(
                for: persistableType,
                transaction: transaction
            )
        }

        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let (begin, end) = typeSubspace.range()
        let storage = context.container.itemStorageFactory.make(transaction: transaction, blobsSubspace: blobsSubspace)
        var models: [any Persistable] = []
        models.reserveCapacity(limit)

        for try await (_, data) in storage.scan(
            begin: begin,
            end: end,
            snapshot: false,
            limit: limit
        ) {
            let model = try DataAccess.deserializeAny(data, as: persistableType)
            try context.container.securityDelegate?.evaluateGet(model)
            models.append(model)
        }

        return models
    }

    public func validateFinalState(
        of models: [any Persistable],
        transaction: any Transaction
    ) async throws {
        let service = RecordMutationMaintenanceService(
            container: context.container,
            maintainers: context.container.runtimeConfiguration.recordMutationMaintainers
        )
        try await service.validateFinalState(
            of: models,
            transaction: transaction
        )
    }

    // MARK: - Private Helpers

    /// Check if a type has dynamic directory components.
    private func hasDynamicDirectory(_ type: any Persistable.Type) -> Bool {
        type.hasDynamicDirectory
    }

    /// Extract directory field names for error messages
    private func extractDirectoryFieldNames(_ type: any Persistable.Type) -> [String] {
        type.directoryFieldNames
    }

    /// Build type-erased partition binding from a model instance
    private func buildAnyDirectoryPath(from model: any Persistable) throws -> AnyDirectoryPath {
        let modelType = type(of: model)
        var bindings: [(name: String, value: any Sendable)] = []

        for component in modelType.directoryPathComponents {
            guard case .dynamicField(let fieldName) = component else { continue }
            if let value = model[dynamicMember: fieldName] {
                bindings.append((fieldName, value))
            }
        }

        return try AnyDirectoryPath(fieldValues: bindings, type: modelType)
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

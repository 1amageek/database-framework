// SecureTransactionContext.swift
// DatabaseEngine - Security-aware transaction context

import Foundation
import FoundationDB
import Core

/// Security-aware transaction context
///
/// Implements TransactionContextProtocol with security evaluation on each operation.
/// Created by FDBDataStore.withTransaction() and uses the store's security delegate.
internal final class SecureTransactionContext: TransactionContextProtocol, @unchecked Sendable {

    // MARK: - Properties

    private let transaction: any TransactionProtocol
    private let itemSubspace: Subspace
    private let indexSubspace: Subspace
    private let blobsSubspace: Subspace
    private let indexMaintenanceService: IndexMaintenanceService
    private let securityDelegate: (any DataStoreSecurityDelegate)?

    // MARK: - Initialization

    init(
        transaction: any TransactionProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        blobsSubspace: Subspace,
        indexMaintenanceService: IndexMaintenanceService,
        securityDelegate: (any DataStoreSecurityDelegate)?
    ) {
        self.transaction = transaction
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.blobsSubspace = blobsSubspace
        self.indexMaintenanceService = indexMaintenanceService
        self.securityDelegate = securityDelegate
    }

    // MARK: - TransactionContextProtocol

    public func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        snapshot: Bool = false
    ) async throws -> T? {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let keyTuple = (id as? Tuple) ?? Tuple([id])
        let key = typeSubspace.pack(keyTuple)

        // Use ItemStorage with snapshot semantics properly propagated
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )
        guard let bytes = try await storage.read(for: key, snapshot: snapshot) else {
            return nil
        }

        let result: T = try DataAccess.deserialize(bytes)

        // GET security evaluation
        try securityDelegate?.evaluateGet(result)

        return result
    }

    public func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        snapshot: Bool = false
    ) async throws -> [T] {
        var results: [T] = []
        for id in ids {
            if let model: T = try await get(type, id: id, snapshot: snapshot) {
                results.append(model)
            }
        }
        return results
    }

    public func set<T: Persistable>(_ model: T) async throws {
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )

        // Get existing record for CREATE/UPDATE determination and index updates
        let oldData = try await storage.read(for: key)
        let oldModel: T? = try oldData.map { try DataAccess.deserialize($0) }

        // Security evaluation
        if let old = oldModel {
            try securityDelegate?.evaluateUpdate(old, newResource: model)
        } else {
            try securityDelegate?.evaluateCreate(model)
        }

        // Serialize and save (handles compression + external storage for >90KB)
        let data = try DataAccess.serialize(model)
        try await storage.write(data, for: key)

        // Update indexes
        try await updateScalarIndexes(oldModel: oldModel, newModel: model, id: idTuple)
    }

    public func delete<T: Persistable>(_ model: T) async throws {
        // DELETE security evaluation
        try securityDelegate?.evaluateDelete(model)

        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first
        try await updateScalarIndexes(oldModel: model, newModel: nil as T?, id: idTuple)

        // Delete the record (handles external blob chunks)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )
        try await storage.delete(for: key)
    }

    public func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws {
        guard let model: T = try await get(type, id: id, snapshot: false) else {
            return
        }
        try await delete(model)
    }

    public var rawTransaction: any TransactionProtocol {
        transaction
    }

    // MARK: - Private: Index Maintenance

    /// Update scalar indexes using diff-based approach
    private func updateScalarIndexes<T: Persistable>(
        oldModel: T?,
        newModel: T?,
        id: Tuple
    ) async throws {
        let indexDescriptors = T.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        for descriptor in indexDescriptors {
            // Skip non-scalar indexes
            let kindIdentifier = type(of: descriptor.kind).identifier
            guard kindIdentifier == "scalar" || kindIdentifier == "version" else {
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)
            let keyPathCount = descriptor.keyPaths.count

            // Compute old index keys
            var oldKeys: Set<[UInt8]> = []
            if let old = oldModel {
                let oldValues = try IndexMaintenanceService.extractIndexValues(from: old, keyPaths: descriptor.keyPaths)
                if !oldValues.isEmpty {
                    for key in IndexMaintenanceService.buildIndexKeys(subspace: indexSubspaceForIndex, values: oldValues, id: id, keyPathCount: keyPathCount) {
                        oldKeys.insert(key)
                    }
                }
            }

            // Compute new index keys
            var newKeys: Set<[UInt8]> = []
            if let new = newModel {
                let newValues = try IndexMaintenanceService.extractIndexValues(from: new, keyPaths: descriptor.keyPaths)
                if !newValues.isEmpty {
                    for key in IndexMaintenanceService.buildIndexKeys(subspace: indexSubspaceForIndex, values: newValues, id: id, keyPathCount: keyPathCount) {
                        newKeys.insert(key)
                    }
                }
            }

            // Apply diff
            for key in oldKeys.subtracting(newKeys) {
                transaction.clear(key: key)
            }
            for key in newKeys.subtracting(oldKeys) {
                transaction.setValue([], for: key)
            }
        }
    }
}

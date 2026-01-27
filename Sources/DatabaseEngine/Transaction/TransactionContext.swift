// TransactionContext.swift
// DatabaseEngine - Transactional operations wrapper
//
// Reference: Cloud Firestore Transaction model
// https://firebase.google.com/docs/firestore/manage-data/transactions

import Foundation
import FoundationDB
import Core

// MARK: - TransactionContext

/// Context for transactional operations with configurable isolation level
///
/// TransactionContext provides a Firestore-like API for reading and writing
/// data within a transaction, with control over read conflict tracking.
///
/// **Read Modes**:
/// - `get(snapshot: false)` (default): Transactional read that adds read conflict.
///   If another transaction writes to this data before commit, this transaction
///   will conflict and retry.
/// - `get(snapshot: true)`: Snapshot read with no conflict tracking.
///   May return stale data, but won't cause conflicts. Use for non-critical reads.
///
/// **Usage**:
/// ```swift
/// try await context.withTransaction { tx in
///     // Transactional read - adds read conflict
///     let user = try await tx.get(User.self, id: userID)
///
///     // Snapshot read - no conflict, may be stale
///     let stats = try await tx.get(Stats.self, id: statsID, snapshot: true)
///
///     // Write - adds write conflict
///     try await tx.set(updatedUser)
/// }
/// ```
///
/// **Important**: The closure may be retried on conflict. Avoid side effects
/// (external API calls, etc.) inside the transaction closure.
///
/// **Reference**: FDB snapshot read semantics
public final class TransactionContext: @unchecked Sendable {
    // MARK: - Properties

    /// The underlying FDB transaction
    private let transaction: any TransactionProtocol

    /// The container for directory resolution
    private let container: FDBContainer

    /// Cache of resolved subspaces per cache key (type + partition path)
    private var subspaceCache: [String: ResolvedSubspaces] = [:]

    /// Resolved subspaces for a type
    private struct ResolvedSubspaces {
        let itemSubspace: Subspace
        let indexSubspace: Subspace
        let blobsSubspace: Subspace
    }

    // MARK: - Initialization

    /// Initialize a transaction context
    ///
    /// - Parameters:
    ///   - transaction: The underlying FDB transaction
    ///   - container: The FDBContainer for directory resolution
    init(
        transaction: any TransactionProtocol,
        container: FDBContainer
    ) {
        self.transaction = transaction
        self.container = container
    }

    // MARK: - Directory Resolution

    /// Resolve subspaces for a Persistable type (static directory only)
    ///
    /// - Throws: Error for dynamic directory types without partition binding
    private func resolveSubspaces<T: Persistable>(for type: T.Type) async throws -> ResolvedSubspaces {
        // Check if type has dynamic directory
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }

        let typeName = T.persistableType

        // Check cache first
        if let cached = subspaceCache[typeName] {
            return cached
        }

        // Resolve directory from container
        let subspace = try await container.resolveDirectory(for: type)
        let resolved = ResolvedSubspaces(
            itemSubspace: subspace.subspace(SubspaceKey.items),
            indexSubspace: subspace.subspace(SubspaceKey.indexes),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs)
        )

        // Cache for reuse within this transaction
        subspaceCache[typeName] = resolved

        return resolved
    }

    /// Resolve subspaces for a Persistable type with directory path
    ///
    /// Used for types with dynamic directories (`Field(\.keyPath)` in `#Directory`).
    private func resolveSubspaces<T: Persistable>(
        for type: T.Type,
        partition path: DirectoryPath<T>
    ) async throws -> ResolvedSubspaces {
        // Validate path
        try path.validate()

        // Cache key includes path for uniqueness
        let pathComponents = path.resolve()
        let cacheKey = pathComponents.joined(separator: "/")

        // Check cache first
        if let cached = subspaceCache[cacheKey] {
            return cached
        }

        // Resolve directory from container with path
        let subspace = try await container.resolveDirectory(for: type, path: path)
        let resolved = ResolvedSubspaces(
            itemSubspace: subspace.subspace(SubspaceKey.items),
            indexSubspace: subspace.subspace(SubspaceKey.indexes),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs)
        )

        // Cache for reuse within this transaction
        subspaceCache[cacheKey] = resolved

        return resolved
    }

    /// Resolve subspaces from a model instance (extracts partition values automatically)
    ///
    /// For types with dynamic directories, extracts partition field values from the model.
    private func resolveSubspaces<T: Persistable>(from model: T) async throws -> ResolvedSubspaces {
        if T.hasDynamicDirectory {
            let path = DirectoryPath<T>.from(model)
            return try await resolveSubspaces(for: T.self, partition: path)
        } else {
            return try await resolveSubspaces(for: T.self)
        }
    }

    // MARK: - Read Operations

    /// Get a model by ID with configurable isolation level (static directory types)
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The model's identifier
    ///   - snapshot: If `false` (default), adds read conflict for serializable isolation.
    ///               If `true`, performs snapshot read with no conflict (may be stale).
    /// - Returns: The model if found, nil otherwise
    /// - Throws: `DirectoryPathError.dynamicFieldsRequired` for dynamic directory types
    public func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        snapshot: Bool = false
    ) async throws -> T? {
        let subspaces = try await resolveSubspaces(for: type)
        return try await getWithSubspaces(type, id: id, subspaces: subspaces, snapshot: snapshot)
    }

    /// Get a model by ID from a partitioned directory
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// you must provide partition binding.
    ///
    /// **Example**:
    /// ```swift
    /// var binding = DirectoryPath<Order>()
    /// binding.set(\.tenantID, to: "tenant_123")
    /// let order = try await tx.get(Order.self, id: orderID, partition: path)
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The model's identifier
    ///   - binding: Partition field binding
    ///   - snapshot: If `false` (default), adds read conflict. If `true`, snapshot read.
    /// - Returns: The model if found, nil otherwise
    public func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        partition path: DirectoryPath<T>,
        snapshot: Bool = false
    ) async throws -> T? {
        let subspaces = try await resolveSubspaces(for: type, partition: path)
        return try await getWithSubspaces(type, id: id, subspaces: subspaces, snapshot: snapshot)
    }

    /// Internal: Get with pre-resolved subspaces
    private func getWithSubspaces<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        subspaces: ResolvedSubspaces,
        snapshot: Bool
    ) async throws -> T? {
        let typeSubspace = subspaces.itemSubspace.subspace(T.persistableType)
        let keyTuple = (id as? Tuple) ?? Tuple([id])
        let key = typeSubspace.pack(keyTuple)

        // Use ItemStorage with snapshot semantics properly propagated
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: subspaces.blobsSubspace
        )
        guard let bytes = try await storage.read(for: key, snapshot: snapshot) else {
            return nil
        }

        return try DataAccess.deserialize(bytes)
    }

    /// Get multiple models by IDs with configurable isolation level (static directory types)
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - ids: The model identifiers
    ///   - snapshot: If `false` (default), adds read conflict. If `true`, snapshot read.
    /// - Returns: Array of found models (missing IDs are skipped)
    /// - Throws: `DirectoryPathError.dynamicFieldsRequired` for dynamic directory types
    public func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        snapshot: Bool = false
    ) async throws -> [T] {
        let subspaces = try await resolveSubspaces(for: type)
        return try await getManyWithSubspaces(type, ids: ids, subspaces: subspaces, snapshot: snapshot)
    }

    /// Get multiple models by IDs from a partitioned directory
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - ids: The model identifiers
    ///   - binding: Partition field binding
    ///   - snapshot: If `false` (default), adds read conflict. If `true`, snapshot read.
    /// - Returns: Array of found models (missing IDs are skipped)
    public func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        partition path: DirectoryPath<T>,
        snapshot: Bool = false
    ) async throws -> [T] {
        let subspaces = try await resolveSubspaces(for: type, partition: path)
        return try await getManyWithSubspaces(type, ids: ids, subspaces: subspaces, snapshot: snapshot)
    }

    /// Internal: Get many with pre-resolved subspaces
    private func getManyWithSubspaces<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        subspaces: ResolvedSubspaces,
        snapshot: Bool
    ) async throws -> [T] {
        var results: [T] = []
        results.reserveCapacity(ids.count)

        for id in ids {
            if let model: T = try await getWithSubspaces(type, id: id, subspaces: subspaces, snapshot: snapshot) {
                results.append(model)
            }
        }

        return results
    }

    // MARK: - Write Operations

    /// Set (insert or update) a model
    ///
    /// This operation adds a write conflict on the model's key.
    /// Performs scalar index maintenance only (suitable for transactional operations).
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// partition values are automatically extracted from the model instance.
    ///
    /// **Note**: For full index maintenance including aggregations and uniqueness
    /// constraints, use FDBContext.save() instead.
    ///
    /// - Parameter model: The model to save
    /// - Throws: Error if serialization fails
    public func set<T: Persistable>(_ model: T) async throws {
        // Resolve subspaces from model (extracts partition values automatically)
        let subspaces = try await resolveSubspaces(from: model)
        let idTuple = try IndexMaintenanceService.extractIDTuple(from: model)

        // Serialize model
        let data = try DataAccess.serialize(model)

        // Build key
        let typeSubspace = subspaces.itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: subspaces.blobsSubspace
        )

        // Get existing record for diff-based index update
        let oldData = try await storage.read(for: key)
        let oldModel: T? = try oldData.map { try DataAccess.deserialize($0) }

        // Write record (handles compression + external storage for >90KB)
        try await storage.write(data, for: key)

        // Update scalar indexes using diff-based approach
        try await updateScalarIndexes(
            oldModel: oldModel,
            newModel: model,
            id: idTuple,
            indexSubspace: subspaces.indexSubspace
        )
    }

    /// Delete a model
    ///
    /// This operation adds a write conflict on the model's key.
    /// Performs scalar index cleanup only.
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// partition values are automatically extracted from the model instance.
    ///
    /// - Parameter model: The model to delete
    /// - Throws: Error if index cleanup fails
    public func delete<T: Persistable>(_ model: T) async throws {
        // Resolve subspaces from model (extracts partition values automatically)
        let subspaces = try await resolveSubspaces(from: model)
        let idTuple = try IndexMaintenanceService.extractIDTuple(from: model)

        // Build key
        let typeSubspace = subspaces.itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove scalar index entries
        try await updateScalarIndexes(
            oldModel: model,
            newModel: nil as T?,
            id: idTuple,
            indexSubspace: subspaces.indexSubspace
        )

        // Delete record (handles external blob chunks)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: subspaces.blobsSubspace
        )
        try await storage.delete(for: key)
    }

    // MARK: - Private: Scalar Index Maintenance

    /// Update scalar indexes using diff-based approach
    ///
    /// Only handles scalar indexes. Aggregation indexes (count, sum, min/max)
    /// and uniqueness constraints are not enforced in TransactionContext.
    private func updateScalarIndexes<T: Persistable>(
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        indexSubspace: Subspace
    ) async throws {
        let indexDescriptors = T.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        for descriptor in indexDescriptors {
            // Skip non-scalar indexes (count, sum, min, max)
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

    /// Delete a model by ID (static directory types)
    ///
    /// Fetches the model first to properly clean up index entries.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - id: The model's identifier
    /// - Throws: `DirectoryPathError.dynamicFieldsRequired` for dynamic directory types
    public func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws {
        guard let model: T = try await get(type, id: id, snapshot: false) else {
            return // Model doesn't exist, nothing to delete
        }
        try await delete(model)
    }

    /// Delete a model by ID from a partitioned directory
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// you must provide partition binding.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - id: The model's identifier
    ///   - binding: Partition field binding
    /// - Throws: Error if the model is not found or deletion fails
    public func delete<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        partition path: DirectoryPath<T>
    ) async throws {
        guard let model: T = try await get(type, id: id, partition: path, snapshot: false) else {
            return // Model doesn't exist, nothing to delete
        }
        try await delete(model)
    }

    // MARK: - Raw Transaction Access

    /// Access the underlying transaction for advanced operations
    ///
    /// Use with caution - direct transaction access bypasses the
    /// TransactionContext's abstractions.
    public var rawTransaction: any TransactionProtocol {
        transaction
    }
}

// MARK: - TransactionContextError

/// Errors specific to TransactionContext operations
public enum TransactionContextError: Error, CustomStringConvertible {
    /// Model not found when expected
    case modelNotFound(type: String, id: String)

    /// Serialization failed
    case serializationFailed(type: String, underlyingError: Error)

    /// Deserialization failed
    case deserializationFailed(type: String, underlyingError: Error)

    /// ID is not a valid TupleElement
    case invalidID(type: String)

    public var description: String {
        switch self {
        case .modelNotFound(let type, let id):
            return "TransactionContextError: Model of type '\(type)' with id '\(id)' not found"
        case .serializationFailed(let type, let underlyingError):
            return "TransactionContextError: Failed to serialize '\(type)': \(underlyingError)"
        case .deserializationFailed(let type, let underlyingError):
            return "TransactionContextError: Failed to deserialize '\(type)': \(underlyingError)"
        case .invalidID(let type):
            return "TransactionContextError: ID for '\(type)' must conform to TupleElement"
        }
    }
}

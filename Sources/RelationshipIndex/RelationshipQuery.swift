// RelationshipQuery.swift
// RelationshipIndex - Relationship query API for FDBContext
//
// Provides SwiftData-like relationship loading API with index-optimized queries.

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

// MARK: - FDBContext Get Methods

extension FDBContext {
    /// Get a single item by ID, returning a Snapshot
    ///
    /// This is a convenience method that wraps `model(for:as:)` to return
    /// a `Snapshot<T>` instead of the raw item.
    ///
    /// **Usage**:
    /// ```swift
    /// let snapshot = try await context.get(Order.self, id: "O001")
    /// if let order = snapshot {
    ///     print(order.total)  // Access via dynamicMember
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The item ID
    /// - Returns: A Snapshot containing the item, or nil if not found
    public func get<T: Persistable>(_ type: T.Type, id: String) async throws -> Snapshot<T>? {
        guard let item = try await model(for: id, as: type) else {
            return nil
        }
        return Snapshot(item: item)
    }

    /// Get a single item by ID with to-one relationship loaded
    ///
    /// Fetches the item and its related item in a single operation.
    /// The related item is accessible via `snapshot.ref()`.
    ///
    /// **Usage**:
    /// ```swift
    /// let snapshot = try await context.get(
    ///     Order.self, id: "O001",
    ///     joining: \.customerID, as: Customer.self
    /// )
    /// if let order = snapshot {
    ///     let customer = order.ref(Customer.self, \.customerID)
    ///     print(customer?.name)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The item ID
    ///   - keyPath: The FK field KeyPath to join (to-one, Optional)
    ///   - relatedType: The type of the related item
    /// - Returns: A Snapshot with the related item loaded, or nil if not found
    public func get<T: Persistable, R: Persistable>(
        _ type: T.Type,
        id: String,
        joining keyPath: KeyPath<T, String?>,
        as relatedType: R.Type
    ) async throws -> Snapshot<T>? {
        guard let item = try await model(for: id, as: type) else {
            return nil
        }

        var relations: [AnyKeyPath: any Sendable] = [:]

        // Load the related item if FK is not nil
        if let foreignKeyValue = item[keyPath: keyPath] {
            if let relatedItem = try await model(for: foreignKeyValue, as: R.self) {
                relations[keyPath] = relatedItem
            }
        }

        return Snapshot(item: item, relations: relations)
    }

    /// Get a single item by ID with required to-one relationship loaded
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The item ID
    ///   - keyPath: The FK field KeyPath to join (to-one, required)
    ///   - relatedType: The type of the related item
    /// - Returns: A Snapshot with the related item loaded, or nil if not found
    public func get<T: Persistable, R: Persistable>(
        _ type: T.Type,
        id: String,
        joining keyPath: KeyPath<T, String>,
        as relatedType: R.Type
    ) async throws -> Snapshot<T>? {
        guard let item = try await model(for: id, as: type) else {
            return nil
        }

        var relations: [AnyKeyPath: any Sendable] = [:]

        // Load the related item
        let foreignKeyValue = item[keyPath: keyPath]
        if let relatedItem = try await model(for: foreignKeyValue, as: R.self) {
            relations[keyPath] = relatedItem
        }

        return Snapshot(item: item, relations: relations)
    }

    /// Get a single item by ID with to-many relationship loaded
    ///
    /// **Usage**:
    /// ```swift
    /// let snapshot = try await context.get(
    ///     Customer.self, id: "C001",
    ///     joining: \.orderIDs, as: Order.self
    /// )
    /// if let customer = snapshot {
    ///     let orders = customer.refs(Order.self, \.orderIDs)
    ///     for order in orders {
    ///         print(order.total)
    ///     }
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to fetch
    ///   - id: The item ID
    ///   - keyPath: The FK array KeyPath to join (to-many)
    ///   - relatedType: The type of the related items
    /// - Returns: A Snapshot with the related items loaded, or nil if not found
    public func get<T: Persistable, R: Persistable>(
        _ type: T.Type,
        id: String,
        joining keyPath: KeyPath<T, [String]>,
        as relatedType: R.Type
    ) async throws -> Snapshot<T>? {
        guard let item = try await model(for: id, as: type) else {
            return nil
        }

        var relations: [AnyKeyPath: any Sendable] = [:]

        // Load all related items
        let foreignKeyIds = item[keyPath: keyPath]
        if !foreignKeyIds.isEmpty {
            var relatedItems: [R] = []
            for foreignKeyId in foreignKeyIds {
                if let relatedItem = try await model(for: foreignKeyId, as: R.self) {
                    relatedItems.append(relatedItem)
                }
            }
            relations[keyPath] = relatedItems
        }

        return Snapshot(item: item, relations: relations)
    }
}

// MARK: - FDBContext Relationship Extension

extension FDBContext {
    /// Load a to-one related item by FK field KeyPath
    ///
    /// Loads the related item for a to-one relationship by reading
    /// the foreign key value and fetching the related item by ID.
    ///
    /// **Performance**: O(1) - Direct ID lookup
    ///
    /// **Usage**:
    /// ```swift
    /// let order = try await context.model(for: "O001", as: Order.self)!
    /// let customer = try await context.related(order, \.customerID, as: Customer.self)
    /// // customer is Customer?
    /// ```
    ///
    /// - Parameters:
    ///   - item: The item containing the FK field
    ///   - fkKeyPath: KeyPath to the FK field (e.g., \.customerID)
    ///   - relatedType: Type of the related item
    /// - Returns: The related item, or nil if FK is nil or item not found
    public func related<T: Persistable, R: Persistable>(
        _ item: T,
        _ fkKeyPath: KeyPath<T, String?>,
        as relatedType: R.Type
    ) async throws -> R? {
        // Get the FK value directly from the item
        guard let foreignKeyValue = item[keyPath: fkKeyPath] else {
            return nil  // No foreign key set
        }

        // Use efficient ID lookup
        return try await model(for: foreignKeyValue, as: R.self)
    }

    /// Load a to-one related item by required FK field KeyPath
    ///
    /// Similar to the optional version but for required FK fields.
    ///
    /// - Parameters:
    ///   - item: The item containing the FK field
    ///   - fkKeyPath: KeyPath to the required FK field
    ///   - relatedType: Type of the related item
    /// - Returns: The related item, or nil if item not found
    public func related<T: Persistable, R: Persistable>(
        _ item: T,
        _ fkKeyPath: KeyPath<T, String>,
        as relatedType: R.Type
    ) async throws -> R? {
        let foreignKeyValue = item[keyPath: fkKeyPath]
        return try await model(for: foreignKeyValue, as: R.self)
    }

    /// Load to-many related items by FK array KeyPath
    ///
    /// Loads all related items for a to-many relationship by reading
    /// the FK array and batch loading the related items.
    ///
    /// **Performance**: O(k) where k is number of related items
    ///
    /// **Usage**:
    /// ```swift
    /// let customer = try await context.model(for: "C001", as: Customer.self)!
    /// let orders = try await context.related(customer, \.orderIDs, as: Order.self)
    /// // orders is [Order]
    /// ```
    ///
    /// - Parameters:
    ///   - item: The item containing the FK array field
    ///   - fkArrayKeyPath: KeyPath to the FK array field (e.g., \.orderIDs)
    ///   - relatedType: Type of the related items
    /// - Returns: Array of related items (preserves order of FK array)
    public func related<T: Persistable, R: Persistable>(
        _ item: T,
        _ fkArrayKeyPath: KeyPath<T, [String]>,
        as relatedType: R.Type
    ) async throws -> [R] {
        // Get FK IDs directly from the item
        let foreignKeyIds = item[keyPath: fkArrayKeyPath]

        guard !foreignKeyIds.isEmpty else {
            return []
        }

        // Batch load related items by ID
        var results: [R] = []
        for foreignKeyId in foreignKeyIds {
            if let relatedItem = try await model(for: foreignKeyId, as: R.self) {
                results.append(relatedItem)
            }
        }

        return results
    }

    // MARK: - Delete with Relationship Rules

    /// Delete a model with relationship rule enforcement
    ///
    /// Unlike the basic `delete(_:)` followed by `save()`, this method enforces delete rules
    /// defined in `@Relationship` declarations before deleting.
    ///
    /// **Delete Rules**:
    /// - `.cascade`: Delete all items that reference this item
    /// - `.deny`: Throw error if any items reference this item
    /// - `.nullify`: Set FK field to nil on referencing items
    /// - `.noAction`: Delete without checking references
    ///
    /// **Usage**:
    /// ```swift
    /// // Basic delete (no relationship rules enforced)
    /// context.delete(customer)
    /// try await context.save()
    ///
    /// // Delete with relationship rules enforced
    /// try await context.deleteEnforcingRelationshipRules(customer)
    /// ```
    ///
    /// **Note**: This method executes immediately in a transaction, not batched like `save()`.
    ///
    /// - Parameter model: The model to delete
    /// - Throws: `RelationshipError.deleteRuleDenied` if delete rule is `.deny` and references exist
    public func deleteEnforcingRelationshipRules<T: Persistable>(_ model: T) async throws {
        let handler = makePersistenceHandler()

        try await container.database.withTransaction(configuration: .default) { transaction in
            try await self.deleteEnforcingRelationshipRulesInternal(
                model,
                transaction: transaction,
                handler: handler
            )
        }
    }

    /// Internal implementation of delete with relationship rules
    private func deleteEnforcingRelationshipRulesInternal(
        _ model: any Persistable,
        transaction: any TransactionProtocol,
        handler: ModelPersistenceHandler
    ) async throws {
        // Create relationship maintainer with container for dynamic directory resolution
        // Note: We pass container instead of fixed subspaces because delete rule enforcement
        // needs to scan indexes of OTHER types (owning types), not just the deleted item's type
        let maintainer = RelationshipMaintainer(
            container: container,
            schema: container.schema
        )

        // Recursive deleter for cascade
        let recursiveDeleter: @Sendable (any Persistable, any TransactionProtocol) async throws -> Void = { [self] item, tx in
            try await self.deleteEnforcingRelationshipRulesInternal(item, transaction: tx, handler: handler)
        }

        // Enforce delete rules (may cascade to other items)
        try await maintainer.enforceDeleteRules(
            for: model,
            transaction: transaction,
            handler: handler,
            recursiveDeleter: recursiveDeleter
        )

        // Now delete the item itself
        try await handler.delete(model, transaction: transaction)
    }

    /// Load an item by type name and string ID (public API for relationship loading)
    ///
    /// Used by QueryExecutor for batch loading related items in `joining()`.
    ///
    /// - Parameters:
    ///   - typeName: The Persistable type name (e.g., "Customer")
    ///   - id: The item ID as a string
    /// - Returns: The loaded item, or nil if not found
    public func loadItemByTypeName(
        _ typeName: String,
        id: String
    ) async throws -> (any Persistable)? {
        // Find the entity in schema
        guard let entity = container.schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }

        let persistableType = entity.persistableType
        let subspace = try await container.resolveDirectory(for: persistableType)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let key = typeSubspace.pack(Tuple([id]))

        let result: (any Persistable)? = try await container.database.withTransaction(configuration: .default) { tx in
            guard let data = try await tx.getValue(for: key, snapshot: false) else {
                return nil
            }

            let decoder = ProtobufDecoder()
            return try decoder.decode(persistableType, from: Data(data))
        }

        return result
    }
}

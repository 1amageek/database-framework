// InverseRelationshipResolver.swift
// RelationshipIndex - Resolve inverse relationships using existing scalar indexes

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

/// Resolves inverse relationships using existing scalar indexes
///
/// This utility allows querying "which items reference this target?" without
/// requiring a dedicated inverse relationship index. It leverages the scalar
/// indexes created by the `@Relationship` macro.
///
/// **Use Cases**:
/// - Find all orders for a customer
/// - Find all comments for a post
/// - Find all tasks assigned to a user
///
/// **How It Works**:
/// The `@Relationship` macro creates a scalar index on the foreign key field:
/// ```
/// [fdb]/I/Order_customer/[customerId]/[orderId] = ''
/// ```
///
/// This resolver queries that index to find all referring items.
///
/// **Example**:
/// ```swift
/// let resolver = InverseRelationshipResolver(container: container)
///
/// // Find all orders that reference customer "C001"
/// let orders = try await resolver.referencedBy(
///     Customer.self,
///     id: "C001",
///     from: Order.self,
///     via: "customer",  // The @Relationship property name
///     transaction: transaction
/// )
/// ```
public final class InverseRelationshipResolver: Sendable {

    private let container: FDBContainer

    public init(container: FDBContainer) {
        self.container = container
    }

    // MARK: - Inverse Relationship Queries

    /// Find all items that reference a target via a foreign key relationship
    ///
    /// - Parameters:
    ///   - targetType: The type being referenced (e.g., Customer.self)
    ///   - targetId: ID of the target item
    ///   - owningType: The type that owns the relationship (e.g., Order.self)
    ///   - relationshipPropertyName: Name of the relationship property
    ///   - transaction: FDB transaction
    /// - Returns: Array of IDs of items that reference the target
    public func findReferringItemIds<Target: Persistable, Owner: Persistable>(
        _ targetType: Target.Type,
        id targetId: Target.ID,
        from owningType: Owner.Type,
        via relationshipPropertyName: String,
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        // Build the index name: "{OwnerType}_{propertyName}"
        let ownerTypeName = Owner.persistableType
        let indexName = "\(ownerTypeName)_\(relationshipPropertyName)"

        // Resolve the owning type's directory
        let owningSubspace = try await container.resolveDirectory(for: owningType)
        let indexSubspace = owningSubspace.subspace(SubspaceKey.indexes)
        let relIndexSubspace = indexSubspace.subspace(indexName)

        // Convert targetId to Tuple
        let targetIdTuple = idToTuple(targetId)
        let prefixSubspace = relIndexSubspace.subspace(targetIdTuple)

        // Scan index
        let (begin, end) = prefixSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: false)

        var itemIds: [Tuple] = []
        for try await (key, _) in sequence {
            if prefixSubspace.contains(key) {
                let tuple = try prefixSubspace.unpack(key)
                if tuple.count > 0 {
                    itemIds.append(tuple)
                }
            }
        }

        return itemIds
    }

    /// Find all items that reference a target and load them
    ///
    /// - Parameters:
    ///   - targetType: The type being referenced
    ///   - targetId: ID of the target item
    ///   - owningType: The type that owns the relationship
    ///   - relationshipPropertyName: Name of the relationship property
    ///   - transaction: FDB transaction
    ///   - handler: ModelPersistenceHandler for loading items
    /// - Returns: Array of items that reference the target
    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ targetType: Target.Type,
        id targetId: Target.ID,
        from owningType: Owner.Type,
        via relationshipPropertyName: String,
        transaction: any TransactionProtocol,
        handler: ModelPersistenceHandler
    ) async throws -> [Owner] {
        let ids = try await findReferringItemIds(
            targetType,
            id: targetId,
            from: owningType,
            via: relationshipPropertyName,
            transaction: transaction
        )

        var results: [Owner] = []
        let ownerTypeName = Owner.persistableType

        for ownerId in ids {
            if let item = try await handler.load(ownerTypeName, id: ownerId, transaction: transaction) {
                if let typedItem = item as? Owner {
                    results.append(typedItem)
                }
            }
        }

        return results
    }

    /// Count items that reference a target
    ///
    /// More efficient than loading all items when only count is needed.
    ///
    /// - Parameters:
    ///   - targetType: The type being referenced
    ///   - targetId: ID of the target item
    ///   - owningType: The type that owns the relationship
    ///   - relationshipPropertyName: Name of the relationship property
    ///   - transaction: FDB transaction
    /// - Returns: Count of items referencing the target
    public func countReferences<Target: Persistable, Owner: Persistable>(
        _ targetType: Target.Type,
        id targetId: Target.ID,
        from owningType: Owner.Type,
        via relationshipPropertyName: String,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        // Build the index name
        let ownerTypeName = Owner.persistableType
        let indexName = "\(ownerTypeName)_\(relationshipPropertyName)"

        // Resolve the owning type's directory
        let owningSubspace = try await container.resolveDirectory(for: owningType)
        let indexSubspace = owningSubspace.subspace(SubspaceKey.indexes)
        let relIndexSubspace = indexSubspace.subspace(indexName)

        // Convert targetId to Tuple
        let targetIdTuple = idToTuple(targetId)
        let prefixSubspace = relIndexSubspace.subspace(targetIdTuple)

        // Count entries
        let (begin, end) = prefixSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var count = 0
        for try await _ in sequence {
            count += 1
        }

        return count
    }

    // MARK: - Private Helpers

    /// Convert any ID to a Tuple
    private func idToTuple<T>(_ id: T) -> Tuple {
        if let tupleElement = id as? any TupleElement {
            return Tuple([tupleElement])
        }
        return Tuple([String(describing: id)])
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Create an inverse relationship resolver
    ///
    /// **Usage**:
    /// ```swift
    /// let resolver = context.inverseRelationshipResolver()
    /// let orders = try await resolver.referencedBy(
    ///     Customer.self,
    ///     id: customerId,
    ///     from: Order.self,
    ///     via: "customer",
    ///     transaction: transaction,
    ///     handler: handler
    /// )
    /// ```
    public func inverseRelationshipResolver() -> InverseRelationshipResolver {
        InverseRelationshipResolver(container: container)
    }
}

// RelationshipIndexMaintainer.swift
// RelationshipIndex - Index maintainer for relationship indexes
//
// Maintains indexes that span relationships between Persistable types.
// Similar to FDB Record Layer's "Joined" index types.

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

/// Maintainer for relationship indexes
///
/// Relationship indexes combine fields from a local type with fields from a related type
/// via `@Relationship`. This enables queries that span relationships without joins.
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][relatedField1][relatedField2]...[localField1]...[primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Examples**:
/// ```swift
/// // Relationship index: Order indexed by Customer.name + Order.total
/// Key: [I]/Order_customer_name_total/["Alice"]/[99.99]/["O001"] = ''
/// ```
///
/// **Update Behavior**:
/// - On item save: Load related item, extract fields, update index
/// - On related item change: Find dependent items, update their relationship indexes
///
/// **Configuration**:
/// Relationship indexes require a `RelatedItemLoader` to load related items.
/// This is typically provided through `RelationshipIndexConfiguration` in `FDBConfiguration`.
///
/// **Usage**:
/// ```swift
/// let maintainer = RelationshipIndexMaintainer<Order>(
///     relationshipPropertyName: "customer",
///     relatedTypeName: "Customer",
///     relatedFieldNames: ["name"],
///     localFieldNames: ["total"],
///     index: orderCustomerIndex,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     relatedItemLoader: { typeName, foreignKey, transaction in
///         return try await container.loadItem(typeName: typeName, id: foreignKey, transaction: transaction)
///     }
/// )
/// ```
public struct RelationshipIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Relationship property name (e.g., "customer")
    private let relationshipPropertyName: String

    /// Foreign key field name (e.g., "customerID" for To-One, "orderIDs" for To-Many)
    private let foreignKeyFieldName: String

    /// Related type name (e.g., "Customer")
    private let relatedTypeName: String

    /// Related field names (e.g., ["name"])
    private let relatedFieldNames: [String]

    /// Local field names (e.g., ["total"])
    private let localFieldNames: [String]

    /// Loader for related items (optional)
    ///
    /// When provided, enables relationship index maintenance by loading related items
    /// to extract field values. If nil, relationship index entries will be skipped.
    private let relatedItemLoader: RelatedItemLoader?

    // MARK: - Initialization

    /// Initialize relationship index maintainer
    ///
    /// - Parameters:
    ///   - relationshipPropertyName: Name of the relationship property (e.g., "customer")
    ///   - foreignKeyFieldName: Name of the FK field (e.g., "customerID" or "orderIDs")
    ///   - relatedTypeName: Name of the related Persistable type
    ///   - relatedFieldNames: Field names from the related type
    ///   - localFieldNames: Field names from the local type
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - relatedItemLoader: Optional loader for related items
    public init(
        relationshipPropertyName: String,
        foreignKeyFieldName: String,
        relatedTypeName: String,
        relatedFieldNames: [String],
        localFieldNames: [String],
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        relatedItemLoader: RelatedItemLoader? = nil
    ) {
        self.relationshipPropertyName = relationshipPropertyName
        self.foreignKeyFieldName = foreignKeyFieldName
        self.relatedTypeName = relatedTypeName
        self.relatedFieldNames = relatedFieldNames
        self.localFieldNames = localFieldNames
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.relatedItemLoader = relatedItemLoader
    }

    // MARK: - IndexMaintainer Protocol

    /// Update index when item changes
    ///
    /// **Process**:
    /// 1. Extract foreign key from item (e.g., customerId)
    /// 2. Load related item using foreign key
    /// 3. Extract related fields from related item
    /// 4. Extract local fields from item
    /// 5. Build and update index key
    ///
    /// **Note**: For relationship indexes, we need access to the related item's data.
    /// Since this is called during a transaction, we load the related item inline.
    /// For performance, consider caching related items when doing batch updates.
    ///
    /// - Parameters:
    ///   - oldItem: Previous item (nil for insert)
    ///   - newItem: New item (nil for delete)
    ///   - transaction: FDB transaction
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entry
        if let oldItem = oldItem {
            if let oldKey = try await buildIndexKey(for: oldItem, transaction: transaction) {
                transaction.clear(key: oldKey)
            }
        }

        // Add new index entry
        if let newItem = newItem {
            if let newKey = try await buildIndexKey(for: newItem, transaction: transaction) {
                transaction.setValue([], for: newKey)
            }
        }
    }

    /// Build index entries for an item during batch indexing
    ///
    /// - Parameters:
    ///   - item: Item to index
    ///   - id: The item's unique identifier
    ///   - transaction: FDB transaction
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        if let key = try await buildIndexKey(for: item, id: id, transaction: transaction) {
            transaction.setValue([], for: key)
        }
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the index key that should exist for this item.
    /// Note: This requires loading the related item, which may not be available
    /// in all verification contexts. Returns empty array if related item not found.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Cannot compute without transaction access to load related item
        // Return empty - scrubber will use alternative verification
        return []
    }

    // MARK: - Private Methods

    /// Build index key for an item
    ///
    /// Key structure: [subspace][relatedFieldValues...][localFieldValues...][id]
    ///
    /// - Parameters:
    ///   - item: The item to build key for
    ///   - id: Optional pre-extracted ID (for scanItem)
    ///   - transaction: Transaction for loading related item
    /// - Returns: Index key bytes, or nil if related item not found
    private func buildIndexKey(
        for item: Item,
        id: Tuple? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [UInt8]? {
        // Get foreign key value from item using the stored foreignKeyFieldName
        // (e.g., "customerID" for To-One, "orderIDs" for To-Many)
        guard let foreignKeyValue = item[dynamicMember: foreignKeyFieldName] else {
            // No foreign key set - cannot build relationship index
            return nil
        }

        // Load related item
        let relatedFieldValues = try await loadRelatedFieldValues(
            foreignKey: foreignKeyValue,
            transaction: transaction
        )

        guard let relatedFieldValues = relatedFieldValues else {
            // Related item not found or loader not configured
            return nil
        }

        // Extract local field values
        let localFieldValues = extractLocalFieldValues(from: item)

        // Extract or use provided id
        let itemId: Tuple
        if let providedId = id {
            itemId = providedId
        } else {
            itemId = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Build key: [subspace][relatedFields...][localFields...][id]
        var allElements: [any TupleElement] = []

        // Add related field values
        for value in relatedFieldValues {
            allElements.append(value)
        }

        // Add local field values
        for value in localFieldValues {
            allElements.append(value)
        }

        // Append id elements
        for i in 0..<itemId.count {
            if let element = itemId[i] {
                allElements.append(element)
            }
        }

        let key = subspace.pack(Tuple(allElements))
        try validateKeySize(key)
        return key
    }

    /// Load field values from related item
    ///
    /// Uses the configured `relatedItemLoader` to load the related item and extract
    /// the required field values.
    ///
    /// - Parameters:
    ///   - foreignKey: The foreign key value pointing to related item
    ///   - transaction: Transaction for reading
    /// - Returns: Array of field values, or nil if related item not found or loader not configured
    private func loadRelatedFieldValues(
        foreignKey: any Sendable,
        transaction: any TransactionProtocol
    ) async throws -> [any TupleElement]? {
        // Check if loader is configured
        guard let loader = relatedItemLoader else {
            // No loader configured - relationship index cannot be built
            // This is expected when configuration is not provided
            return nil
        }

        // Load the related item
        guard let relatedItem = try await loader(relatedTypeName, foreignKey, transaction) else {
            // Related item not found
            return nil
        }

        // Extract field values from the related item
        var values: [any TupleElement] = []

        for fieldName in relatedFieldNames {
            if let value = relatedItem[dynamicMember: fieldName] {
                // Try to convert to FieldValue first, then to TupleElement
                if let fieldValue = FieldValue(value) {
                    values.append(fieldValue.toTupleElement())
                } else if let tupleElement = value as? any TupleElement {
                    values.append(tupleElement)
                }
            }
        }

        return values
    }

    /// Extract local field values from item
    ///
    /// - Parameter item: The item to extract from
    /// - Returns: Array of field values as TupleElements
    private func extractLocalFieldValues(from item: Item) -> [any TupleElement] {
        var values: [any TupleElement] = []

        for fieldName in localFieldNames {
            if let value = item[dynamicMember: fieldName] {
                // Try to convert to FieldValue first, then to TupleElement
                if let fieldValue = FieldValue(value) {
                    values.append(fieldValue.toTupleElement())
                } else if let tupleElement = value as? any TupleElement {
                    values.append(tupleElement)
                }
            }
        }

        return values
    }
}

// MARK: - Related Item Update Support

extension RelationshipIndexMaintainer {
    /// Update relationship indexes when related item changes
    ///
    /// This method is called by RelationshipMaintainer when a related item is modified.
    /// It finds all items that reference the changed related item and updates their
    /// relationship index entries.
    ///
    /// **Process**:
    /// 1. For each dependent item, compute the OLD index key using old related values
    /// 2. Clear the old key
    /// 3. Compute the NEW index key using new related values
    /// 4. Set the new key
    ///
    /// **Note**: This approach avoids full index scans by computing exact keys.
    /// The caller MUST provide both old and new related items to enable correct key computation.
    ///
    /// - Parameters:
    ///   - oldRelatedItem: The related item's state BEFORE the change (required for key computation)
    ///   - newRelatedItem: The related item's state AFTER the change
    ///   - changedFields: Set of fields that changed in the related item
    ///   - dependentItemIds: IDs of items that reference this related item
    ///   - itemLoader: Closure to load dependent items by ID
    ///   - transaction: FDB transaction
    public func updateForRelatedChange(
        oldRelatedItem: any Persistable,
        newRelatedItem: any Persistable,
        changedFields: Set<String>,
        dependentItemIds: [Tuple],
        itemLoader: (Tuple, any TransactionProtocol) async throws -> Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Check if any of the changed fields are in our related fields
        let affectedFields = Set(relatedFieldNames).intersection(changedFields)
        guard !affectedFields.isEmpty else {
            // None of our indexed fields changed
            return
        }

        // Extract old and new related field values
        let oldRelatedValues = extractFieldValues(from: oldRelatedItem, fieldNames: relatedFieldNames)
        let newRelatedValues = extractFieldValues(from: newRelatedItem, fieldNames: relatedFieldNames)

        // Update index entries for each dependent item
        for itemId in dependentItemIds {
            guard let item = try await itemLoader(itemId, transaction) else {
                continue
            }

            // Extract local field values (these don't change when related item changes)
            let localFieldValues = extractLocalFieldValues(from: item)

            // Build and clear the OLD index key
            let oldKey = buildIndexKeyWithValues(
                relatedValues: oldRelatedValues,
                localValues: localFieldValues,
                itemId: itemId
            )
            transaction.clear(key: oldKey)

            // Build and set the NEW index key
            let newKey = buildIndexKeyWithValues(
                relatedValues: newRelatedValues,
                localValues: localFieldValues,
                itemId: itemId
            )
            transaction.setValue([], for: newKey)
        }
    }

    /// Build index key using pre-extracted values
    ///
    /// This allows building exact keys without loading related items,
    /// enabling O(1) key computation instead of O(n) scanning.
    ///
    /// - Parameters:
    ///   - relatedValues: Field values from the related item
    ///   - localValues: Field values from the local item
    ///   - itemId: The item's unique identifier
    /// - Returns: Index key bytes
    private func buildIndexKeyWithValues(
        relatedValues: [any TupleElement],
        localValues: [any TupleElement],
        itemId: Tuple
    ) -> [UInt8] {
        var allElements: [any TupleElement] = []

        // Add related field values
        for value in relatedValues {
            allElements.append(value)
        }

        // Add local field values
        for value in localValues {
            allElements.append(value)
        }

        // Append id elements
        for i in 0..<itemId.count {
            if let element = itemId[i] {
                allElements.append(element)
            }
        }

        return subspace.pack(Tuple(allElements))
    }

    /// Extract field values from any Persistable item
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - fieldNames: Names of fields to extract
    /// - Returns: Array of field values as TupleElements
    private func extractFieldValues(
        from item: any Persistable,
        fieldNames: [String]
    ) -> [any TupleElement] {
        var values: [any TupleElement] = []

        for fieldName in fieldNames {
            if let value = item[dynamicMember: fieldName] {
                // Try to convert to FieldValue first, then to TupleElement
                if let fieldValue = FieldValue(value) {
                    values.append(fieldValue.toTupleElement())
                } else if let tupleElement = value as? any TupleElement {
                    values.append(tupleElement)
                }
            }
        }

        return values
    }
}

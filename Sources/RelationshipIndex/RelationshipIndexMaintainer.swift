// RelationshipIndexMaintainer.swift
// RelationshipIndex - Index maintainer for relationship indexes
//
// Maintains indexes that span relationships between Persistable types.

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

/// Maintainer for relationship indexes
///
/// Relationship indexes store fields from a related type, enabling efficient
/// cross-relationship queries without JOINs.
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][relatedField1][relatedField2]...[primaryKey]
/// Value: '' (empty)
/// ```
///
/// **To-One Example**:
/// ```swift
/// // Order.customerID → Customer.name
/// Key: Order_customer_name/["Alice"]/["O001"] = ''
/// ```
///
/// **To-Many Example**:
/// ```swift
/// // Customer.orderIDs → Order.total
/// Key: Customer_orders_total/[99.99]/["C001"] = ''  // O001
/// Key: Customer_orders_total/[50.00]/["C001"] = ''  // O002
/// ```
public struct RelationshipIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Foreign key field name (e.g., "customerID" or "orderIDs")
    private let foreignKeyFieldName: String

    /// Related type name (e.g., "Customer")
    private let relatedTypeName: String

    /// Related field names (e.g., ["name"])
    private let relatedFieldNames: [String]

    /// Whether this is a To-Many relationship
    private let isToMany: Bool

    /// Loader for related items (optional)
    private let relatedItemLoader: RelatedItemLoader?

    // MARK: - Initialization

    public init(
        foreignKeyFieldName: String,
        relatedTypeName: String,
        relatedFieldNames: [String],
        isToMany: Bool,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        relatedItemLoader: RelatedItemLoader? = nil
    ) {
        self.foreignKeyFieldName = foreignKeyFieldName
        self.relatedTypeName = relatedTypeName
        self.relatedFieldNames = relatedFieldNames
        self.isToMany = isToMany
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.relatedItemLoader = relatedItemLoader
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entries
        if let oldItem = oldItem {
            let oldKeys = try await buildIndexKeys(for: oldItem, transaction: transaction)
            for key in oldKeys {
                transaction.clear(key: key)
            }
        }

        // Add new index entries
        if let newItem = newItem {
            let newKeys = try await buildIndexKeys(for: newItem, transaction: transaction)
            for key in newKeys {
                transaction.setValue([], for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let keys = try await buildIndexKeys(for: item, id: id, transaction: transaction)
        for key in keys {
            transaction.setValue([], for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Cannot compute without transaction access to load related item
        return []
    }

    // MARK: - Private Methods

    /// Build index keys for an item
    /// Returns multiple keys for To-Many relationships
    private func buildIndexKeys(
        for item: Item,
        id: Tuple? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [[UInt8]] {
        // Extract item id
        let itemId: Tuple
        if let providedId = id {
            itemId = providedId
        } else {
            itemId = try DataAccess.extractId(from: item, using: idExpression)
        }

        if isToMany {
            return try await buildToManyIndexKeys(for: item, itemId: itemId, transaction: transaction)
        } else {
            if let key = try await buildToOneIndexKey(for: item, itemId: itemId, transaction: transaction) {
                return [key]
            }
            return []
        }
    }

    /// Build index key for To-One relationship
    private func buildToOneIndexKey(
        for item: Item,
        itemId: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [UInt8]? {
        // Get foreign key value (single ID)
        guard let foreignKeyValue = item[dynamicMember: foreignKeyFieldName] else {
            return nil
        }

        // Load related item and extract field values
        guard let relatedFieldValues = try await loadRelatedFieldValues(
            foreignKey: foreignKeyValue,
            transaction: transaction
        ) else {
            return nil
        }

        return buildKey(relatedValues: relatedFieldValues, itemId: itemId)
    }

    /// Build index keys for To-Many relationship
    private func buildToManyIndexKeys(
        for item: Item,
        itemId: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [[UInt8]] {
        // Get foreign key array
        guard let foreignKeyArray = item[dynamicMember: foreignKeyFieldName] else {
            return []
        }

        // Convert to array of strings
        guard let ids = foreignKeyArray as? [String] else {
            return []
        }

        var keys: [[UInt8]] = []

        // Create an index entry for each related item
        for foreignKeyId in ids {
            if let relatedFieldValues = try await loadRelatedFieldValues(
                foreignKey: foreignKeyId,
                transaction: transaction
            ) {
                let key = buildKey(relatedValues: relatedFieldValues, itemId: itemId)
                keys.append(key)
            }
        }

        return keys
    }

    /// Build a single index key from values
    private func buildKey(relatedValues: [any TupleElement], itemId: Tuple) -> [UInt8] {
        var allElements: [any TupleElement] = []

        for value in relatedValues {
            allElements.append(value)
        }

        for i in 0..<itemId.count {
            if let element = itemId[i] {
                allElements.append(element)
            }
        }

        return subspace.pack(Tuple(allElements))
    }

    /// Load field values from a related item
    private func loadRelatedFieldValues(
        foreignKey: any Sendable,
        transaction: any TransactionProtocol
    ) async throws -> [any TupleElement]? {
        guard let loader = relatedItemLoader else {
            return nil
        }

        guard let relatedItem = try await loader(relatedTypeName, foreignKey, transaction) else {
            return nil
        }

        var values: [any TupleElement] = []

        for fieldName in relatedFieldNames {
            if let value = relatedItem[dynamicMember: fieldName] {
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
            return
        }

        // Extract old and new related field values
        let oldRelatedValues = extractFieldValues(from: oldRelatedItem, fieldNames: relatedFieldNames)
        let newRelatedValues = extractFieldValues(from: newRelatedItem, fieldNames: relatedFieldNames)

        // Update index entries for each dependent item
        for itemId in dependentItemIds {
            guard let _ = try await itemLoader(itemId, transaction) else {
                continue
            }

            // Build and clear the OLD index key
            let oldKey = buildKey(relatedValues: oldRelatedValues, itemId: itemId)
            transaction.clear(key: oldKey)

            // Build and set the NEW index key
            let newKey = buildKey(relatedValues: newRelatedValues, itemId: itemId)
            transaction.setValue([], for: newKey)
        }
    }

    private func extractFieldValues(
        from item: any Persistable,
        fieldNames: [String]
    ) -> [any TupleElement] {
        var values: [any TupleElement] = []

        for fieldName in fieldNames {
            if let value = item[dynamicMember: fieldName] {
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

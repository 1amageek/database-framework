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

    /// Loader for related items (required)
    private let relatedItemLoader: RelatedItemLoader

    // MARK: - Initialization

    public init(
        foreignKeyFieldName: String,
        relatedTypeName: String,
        relatedFieldNames: [String],
        isToMany: Bool,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        relatedItemLoader: @escaping RelatedItemLoader
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
            let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
            for key in newKeys {
                transaction.setValue(value, for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let keys = try await buildIndexKeys(for: item, id: id, transaction: transaction)
        let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
        for key in keys {
            transaction.setValue(value, for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // RelationshipIndex requires transaction access to load related items
        // This method should never be called - use computeIndexKeys(for:id:transaction:) instead
        throw RelationshipIndexError.transactionRequired(indexName: index.name)
    }

    /// Compute index keys with transaction access
    ///
    /// RelationshipIndex requires transaction access to load related items
    /// and extract their field values for index key computation.
    ///
    /// - Parameters:
    ///   - item: The item to compute keys for
    ///   - id: The item's unique identifier
    ///   - transaction: Transaction for loading related items
    /// - Returns: Array of index keys that should exist for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [FDB.Bytes] {
        return try await buildIndexKeys(for: item, id: id, transaction: transaction)
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
            // FK field is nil - no index entry needed
            return nil
        }

        // Validate FK type (String for To-One)
        guard let foreignKeyString = foreignKeyValue as? String else {
            throw RelationshipIndexError.invalidForeignKeyType(
                fieldName: foreignKeyFieldName,
                expectedType: "String",
                actualType: String(describing: type(of: foreignKeyValue))
            )
        }

        // Load related item and extract field values
        guard let relatedFieldValues = try await loadRelatedFieldValues(
            foreignKey: foreignKeyString,
            transaction: transaction
        ) else {
            // Related item not found - no index entry needed
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
            // FK field is nil - no index entries needed
            return []
        }

        // FK must be [String] - this is a framework constraint (Persistable.ID is String)
        guard let ids = foreignKeyArray as? [String] else {
            throw RelationshipIndexError.invalidForeignKeyType(
                fieldName: foreignKeyFieldName,
                expectedType: "[String]",
                actualType: String(describing: type(of: foreignKeyArray))
            )
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
            // If related item not found, skip (FK points to non-existent item)
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
    ///
    /// - Returns: Array of field values as TupleElements, or nil if related item not found
    /// - Throws: RelationshipIndexError if field is nil or cannot be converted
    private func loadRelatedFieldValues(
        foreignKey: any Sendable,
        transaction: any TransactionProtocol
    ) async throws -> [any TupleElement]? {
        guard let relatedItem = try await relatedItemLoader(relatedTypeName, foreignKey, transaction) else {
            // Related item not found - this is a valid case (FK points to deleted/non-existent item)
            return nil
        }

        var values: [any TupleElement] = []

        for fieldName in relatedFieldNames {
            guard let value = relatedItem[dynamicMember: fieldName] else {
                // Field is nil - cannot create index entry with partial key
                throw RelationshipIndexError.relatedFieldIsNil(
                    fieldName: fieldName,
                    relatedType: relatedTypeName
                )
            }

            do {
                let tupleElement = try TypeConversion.toTupleElement(value)
                values.append(tupleElement)
            } catch {
                throw RelationshipIndexError.fieldNotConvertibleToTupleElement(
                    fieldName: fieldName,
                    relatedType: relatedTypeName,
                    actualType: String(describing: type(of: value))
                )
            }
        }

        return values
    }
}

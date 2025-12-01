// ScalarIndexMaintainer.swift
// ScalarIndexLayer - Index maintainer for scalar (VALUE) indexes
//
// Maintains standard B-tree-like indexes for ordering and range queries.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for scalar (VALUE) indexes
///
/// **Functionality**:
/// - Single or composite field indexes
/// - Range queries (WHERE price >= 100 AND price <= 500)
/// - Ordering (ORDER BY createdAt DESC)
/// - Uniqueness constraints
/// - Covering indexes (store additional fields in value)
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][field1Value][field2Value]...[primaryKey]
/// Value: '' (non-covering) or Tuple(coveringField1, coveringField2, ...) (covering)
/// ```
///
/// **Examples**:
/// ```swift
/// // Single field index
/// Key: [I]/User_email/["alice@example.com"]/[123] = ''
///
/// // Composite index
/// Key: [I]/Product_category_price/["electronics"]/[999]/[456] = ''
///
/// // Covering index (stores name in value)
/// Key: [I]/User_email/["alice@example.com"]/[123] = Tuple("Alice")
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = ScalarIndexMaintainer<User>(
///     index: emailIndex,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct ScalarIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // MARK: - Initialization

    /// Initialize scalar index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
    ///
    /// **Process**:
    /// 1. Remove old index entry (if oldItem exists)
    /// 2. Add new index entry (if newItem exists)
    ///
    /// **Uniqueness check**: Performed implicitly by FDB
    /// - If unique constraint violated, transaction will fail
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
            let oldKey = try buildIndexKey(for: oldItem)
            transaction.clear(key: oldKey)
        }

        // Add new index entry
        if let newItem = newItem {
            let newKey = try buildIndexKey(for: newItem)
            // Value is empty for scalar indexes
            transaction.setValue([], for: newKey)
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
        let key = try buildIndexKey(for: item, id: id)
        transaction.setValue([], for: key)
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the index key that should exist for this item.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return [try buildIndexKey(for: item, id: id)]
    }

    // MARK: - Private Methods

    /// Build index key for an item
    ///
    /// Key structure: [subspace][fieldValues...][id]
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> [UInt8] {
        // Extract field values using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        // Extract id
        let itemId: Tuple
        if let providedId = id {
            itemId = providedId
        } else {
            itemId = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Build key: [subspace][fieldValues...][id]
        var allElements: [any TupleElement] = []
        for value in fieldValues {
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
}

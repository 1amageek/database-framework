// ScalarIndexMaintainer.swift
// ScalarIndexLayer - Index maintainer for scalar (VALUE) indexes
//
// Maintains standard B-tree-like indexes for ordering and range queries.

import Foundation
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
    /// 1. Remove old index entries (if oldItem exists)
    /// 2. Add new index entries (if newItem exists)
    ///
    /// **Array Field Handling**:
    /// For array-typed fields (e.g., `[String]` in To-Many relationships),
    /// creates one index entry per array element instead of a single entry.
    ///
    /// **Uniqueness check**: Performed by IndexMaintenanceService before index update
    /// - This maintainer only handles index entry creation/deletion
    /// - Uniqueness validation is handled at a higher level (see IndexMaintenanceService)
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
        // Remove old index entries
        if let oldItem = oldItem {
            let oldKeys = try buildIndexKeys(for: oldItem)
            for key in oldKeys {
                transaction.clear(key: key)
            }
        }

        // Add new index entries
        if let newItem = newItem {
            let newKeys = try buildIndexKeys(for: newItem)
            for key in newKeys {
                // Value is empty for scalar indexes
                transaction.setValue([], for: key)
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
        let keys = try buildIndexKeys(for: item, id: id)
        for key in keys {
            transaction.setValue([], for: key)
        }
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the index keys that should exist for this item.
    /// For array fields, returns one key per array element.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return try buildIndexKeys(for: item, id: id)
    }

    // MARK: - Private Methods

    /// Build index keys for an item
    ///
    /// Key structure: [subspace][fieldValue][id]
    ///
    /// **Array Field Handling**:
    /// For single-field indexes on array types (e.g., To-Many FK fields),
    /// creates one key per array element. This enables reverse lookups like
    /// "find all customers who have order O001 in their orderIDs".
    ///
    /// Example:
    /// - Field `orderIDs = ["O001", "O002"]` produces:
    ///   - Key: `[subspace]["O001"]["C001"]`
    ///   - Key: `[subspace]["O002"]["C001"]`
    ///
    /// **Sparse Index Behavior**:
    /// When a field value is nil (e.g., Optional FK fields like `customerID: String? = nil`),
    /// no index entry is created. This is standard "sparse index" behavior where
    /// nil values are simply not indexed rather than causing an error.
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildIndexKeys(for item: Item, id: Tuple? = nil) throws -> [[UInt8]] {
        // Extract field values using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        //
        // Sparse index: if field value is nil, return empty (no index entry)
        // This is standard behavior for Optional FK fields in @Relationship
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try DataAccess.evaluateIndexFields(
                from: item,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index behavior: nil values are not indexed
            return []
        }

        // Extract id
        let itemId: Tuple
        if let providedId = id {
            itemId = providedId
        } else {
            itemId = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Handle empty field values (e.g., empty array) - no index entries
        if fieldValues.isEmpty {
            return []
        }

        // For single-field indexes on array types:
        // If we have multiple field values from a single KeyPath, and only 1 KeyPath,
        // treat it as an array field and create separate keys for each element.
        //
        // For composite indexes (multiple KeyPaths), we concatenate all values
        // into a single key as before.
        //
        // Note: We check isArrayField from Index metadata when available,
        // otherwise use the expression's columnCount to determine field count.
        // Using columnCount ensures composite indexes (e.g., [city, age]) are
        // correctly identified even when keyPaths is nil.
        let indexFieldCount = index.keyPaths?.count ?? index.rootExpression.columnCount
        let isSingleFieldArrayIndex = indexFieldCount == 1 && fieldValues.count > 1

        if isSingleFieldArrayIndex {
            // Array field: create one key per element
            var keys: [[UInt8]] = []
            for value in fieldValues {
                var allElements: [any TupleElement] = [value]

                // Append id elements
                for i in 0..<itemId.count {
                    if let element = itemId[i] {
                        allElements.append(element)
                    }
                }

                let key = subspace.pack(Tuple(allElements))
                try validateKeySize(key)
                keys.append(key)
            }
            return keys
        } else {
            // Scalar/composite field: single key with all values
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
            return [key]
        }
    }
}

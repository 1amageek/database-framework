// PermutedIndexMaintainer.swift
// PermutedIndexLayer - Index maintainer for PERMUTED indexes
//
// Maintains permuted indexes that reorder compound index fields.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for PERMUTED indexes
///
/// **Functionality**:
/// - Reorders compound index fields according to permutation
/// - Stores permuted keys pointing to primary keys
/// - Enables efficient prefix queries on different field orderings
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][permuted_field_0][permuted_field_1]...[permuted_field_n][primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Storage Optimization**:
/// Permuted indexes store only the permuted key ordering pointing to the primary key.
/// The actual entity data is stored once in the main entity storage, not duplicated.
///
/// **Example**:
/// For a compound index on (country, city, name) with permutation [1, 0, 2]:
/// - Original fields: ["Japan", "Tokyo", "Alice"]
/// - Permuted fields: ["Tokyo", "Japan", "Alice"]
/// - Index key: [indexSubspace]["Tokyo"]["Japan"]["Alice"][primaryKey]
///
/// **Usage**:
/// ```swift
/// let maintainer = PermutedIndexMaintainer<Location>(
///     index: permutedIndex,
///     kind: PermutedIndexKind(permutation: try! Permutation(indices: [1, 0, 2])),
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct PermutedIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let permutation: Permutation

    public init(
        index: Index,
        permutation: Permutation,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.permutation = permutation
    }

    /// Update index when item changes
    ///
    /// **Process**:
    /// 1. Extract field values from item
    /// 2. Apply permutation to reorder fields
    /// 3. Build key with permuted fields + primary key
    /// 4. Remove old entry (if exists) and add new entry
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Remove old permuted entry
        if let oldItem = oldItem {
            if let oldKey = try buildPermutedKey(for: oldItem) {
                try transaction.clear(key: oldKey)
            }
        }

        // Add new permuted entry
        if let newItem = newItem {
            if let newKey = try buildPermutedKey(for: newItem) {
                let value = try CoveringValueBuilder.build(for: newItem, index: index)
                try transaction.setValue(value, for: newKey)
            }
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        if let key = try buildPermutedKey(for: item, id: id) {
            let value = try CoveringValueBuilder.build(for: item, index: index)
            try transaction.setValue(value, for: key)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        if let key = try buildPermutedKey(for: item, id: id) {
            return [key]
        }
        return []
    }

    // MARK: - Query Methods

    /// Scan entries matching a prefix in permuted order
    ///
    /// This allows queries on the permuted field ordering.
    /// For example, if base index is (country, city, name) and permutation is [1, 0, 2],
    /// you can efficiently query by city prefix using this method.
    ///
    /// - Parameters:
    ///   - prefixValues: Prefix values in permuted order
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys matching the prefix
    public func scanByPrefix(
        prefixValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        try await PermutedIndexReader(
            permutation: permutation,
            subspace: subspace
        ).primaryKeys(prefixedBy: prefixValues, transaction: transaction)
    }

    /// Scan entries matching exact values in permuted order
    ///
    /// - Parameters:
    ///   - values: Field values in permuted order (must match permutation size)
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys with exact match
    public func scanByExactMatch(
        values: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        try await PermutedIndexReader(
            permutation: permutation,
            subspace: subspace
        ).primaryKeys(matching: values, transaction: transaction)
    }

    /// Get all entries in the permuted index
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: Array of (permutedFields, primaryKey) tuples
    public func scanAll(
        transaction: any TransactionAccess
    ) async throws -> [(permutedFields: [any TupleElement], primaryKey: [any TupleElement])] {
        try await PermutedIndexReader(
            permutation: permutation,
            subspace: subspace
        ).entries(transaction: transaction)
    }

    /// Convert permuted field values back to original order
    ///
    /// - Parameter permutedValues: Values in permuted order
    /// - Returns: Values in original field order
    /// - Throws: PermutedIndexError if value count doesn't match
    public func toOriginalOrder(_ permutedValues: [any TupleElement]) throws -> [any TupleElement] {
        try PermutedIndexReader(
            permutation: permutation,
            subspace: subspace
        ).originalOrder(for: permutedValues)
    }

    // MARK: - Private Methods

    /// Build permuted key for an item
    ///
    /// Key structure: [subspace][permuted_field_0][permuted_field_1]...[permuted_field_n][primaryKey]
    ///
    /// **Sparse index behavior**:
    /// If any field value is nil, returns nil (no index entry).
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildPermutedKey(for item: Item, id: Tuple? = nil) throws -> ByteString? {
        // Evaluate index expression using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        // Sparse index: if any field value is nil, return nil (no index entry)
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try DataAccess.evaluate(
                item: item,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil field values are not indexed
            return nil
        }

        guard !fieldValues.isEmpty else {
            return nil
        }

        // Validate field count matches permutation size
        guard fieldValues.count == permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: fieldValues.count
            )
        }

        // Apply permutation to reorder field values
        let permutedValues = try permutation.apply(fieldValues)

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Build key: [permuted_values...][primaryKey...]
        var allElements: [any TupleElement] = permutedValues
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allElements.append(element)
            }
        }

        return try packAndValidate(Tuple(allElements))
    }
}

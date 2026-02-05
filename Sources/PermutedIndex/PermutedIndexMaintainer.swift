// PermutedIndexMaintainer.swift
// PermutedIndexLayer - Index maintainer for PERMUTED indexes
//
// Maintains permuted indexes that reorder compound index fields.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
/// The actual record data is stored once in the main record storage, not duplicated.
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
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old permuted entry
        if let oldItem = oldItem {
            if let oldKey = try buildPermutedKey(for: oldItem) {
                transaction.clear(key: oldKey)
            }
        }

        // Add new permuted entry
        if let newItem = newItem {
            if let newKey = try buildPermutedKey(for: newItem) {
                let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
                transaction.setValue(value, for: newKey)
            }
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        if let key = try buildPermutedKey(for: item, id: id) {
            let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
            transaction.setValue(value, for: key)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
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
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        let prefixSubspace: Subspace
        if prefixValues.isEmpty {
            prefixSubspace = subspace
        } else {
            // Manually construct prefix to avoid double-wrapping
            // (subspace.subspace(Tuple) would wrap the Tuple as a single element)
            let prefixKey = subspace.prefix + Tuple(prefixValues).pack()
            prefixSubspace = Subspace(prefix: prefixKey)
        }

        let (begin, end) = prefixSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard prefixSubspace.contains(key) else { break }

            // Extract primary key from the key
            // Key structure: [prefix][remaining_fields...][primaryKey]
            let keyTuple = try prefixSubspace.unpack(key)
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }

            // The last element(s) are the primary key
            // We need to know how many fields are in the permutation to extract primary key
            let fieldCount = permutation.size
            let prefixFieldCount = prefixValues.count
            let remainingFieldCount = fieldCount - prefixFieldCount

            if elements.count > remainingFieldCount {
                let primaryKeyElements = Array(elements.suffix(from: remainingFieldCount))
                results.append(primaryKeyElements)
            }
        }

        return results
    }

    /// Scan entries matching exact values in permuted order
    ///
    /// - Parameters:
    ///   - values: Field values in permuted order (must match permutation size)
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys with exact match
    public func scanByExactMatch(
        values: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        guard values.count == permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: values.count
            )
        }

        // Manually construct prefix to avoid double-wrapping
        let valueKey = subspace.prefix + Tuple(values).pack()
        let valueSubspace = Subspace(prefix: valueKey)
        let (begin, end) = valueSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard valueSubspace.contains(key) else { break }

            // The entire remaining key is the primary key
            let keyTuple = try valueSubspace.unpack(key)
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }
            results.append(elements)
        }

        return results
    }

    /// Get all entries in the permuted index
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: Array of (permutedFields, primaryKey) tuples
    public func scanAll(
        transaction: any TransactionProtocol
    ) async throws -> [(permutedFields: [any TupleElement], primaryKey: [any TupleElement])] {
        let (begin, end) = subspace.range()

        var results: [(permutedFields: [any TupleElement], primaryKey: [any TupleElement])] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }

            // Split into permuted fields and primary key
            let fieldCount = permutation.size
            if elements.count > fieldCount {
                let permutedFields = Array(elements.prefix(fieldCount))
                let primaryKey = Array(elements.suffix(from: fieldCount))
                results.append((permutedFields, primaryKey))
            } else if elements.count == fieldCount {
                // No separate primary key? This shouldn't happen in normal use
                results.append((elements, []))
            }
        }

        return results
    }

    /// Convert permuted field values back to original order
    ///
    /// - Parameter permutedValues: Values in permuted order
    /// - Returns: Values in original field order
    /// - Throws: PermutedIndexError if value count doesn't match
    public func toOriginalOrder(_ permutedValues: [any TupleElement]) throws -> [any TupleElement] {
        return try permutation.inverse.apply(permutedValues)
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
    private func buildPermutedKey(for item: Item, id: Tuple? = nil) throws -> [UInt8]? {
        // Evaluate index expression using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        // Sparse index: if any field value is nil, return nil (no index entry)
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try DataAccess.evaluateIndexFields(
                from: item,
                keyPaths: index.keyPaths,
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

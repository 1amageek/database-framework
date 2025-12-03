// CountNotNullIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for COUNT_NOT_NULL aggregation
//
// Tracks counts of non-null values grouped by other fields.
// Reference: FDB Record Layer COUNT_NOT_NULL index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for COUNT_NOT_NULL indexes
///
/// **Functionality**:
/// - Count records where a specific field is not null
/// - Group counts by other fields
/// - Atomic increment/decrement operations
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Int64 (non-null count, 8 bytes little-endian)
/// ```
///
/// **Examples**:
/// ```swift
/// // Count users with phone numbers by country
/// Key: [I]/User_notnull_country_phoneNumber/["USA"] = 1500
/// Key: [I]/User_notnull_country_phoneNumber/["Japan"] = 800
/// ```
///
/// **Behavior**:
/// - Insert with non-null value: Increment count
/// - Insert with null value: No change
/// - Delete with non-null value: Decrement count
/// - Delete with null value: No change
/// - Update null→non-null: Increment count
/// - Update non-null→null: Decrement count
/// - Update non-null→non-null: No change (same group) or decrement/increment (different group)
public struct CountNotNullIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// The field name to check for null
    public let valueFieldName: String

    // MARK: - Initialization

    /// Initialize count not null index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - valueFieldName: The field to check for null
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        valueFieldName: String
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.valueFieldName = valueFieldName
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
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
        // Get grouping values and null status for old item
        let oldGrouping: [any TupleElement]?
        let oldValueIsNull: Bool
        if let oldItem = oldItem {
            oldGrouping = try evaluateGroupingFields(from: oldItem)
            oldValueIsNull = try isValueNull(in: oldItem)
        } else {
            oldGrouping = nil
            oldValueIsNull = true
        }

        // Get grouping values and null status for new item
        let newGrouping: [any TupleElement]?
        let newValueIsNull: Bool
        if let newItem = newItem {
            newGrouping = try evaluateGroupingFields(from: newItem)
            newValueIsNull = try isValueNull(in: newItem)
        } else {
            newGrouping = nil
            newValueIsNull = true
        }

        // Determine count changes
        switch (oldGrouping, newGrouping) {
        case (nil, let newGroup?):
            // Insert
            if !newValueIsNull {
                // Value is not null: increment count
                let key = try packAndValidate(Tuple(newGroup))
                let increment = ByteConversion.int64ToBytes(1)
                transaction.atomicOp(key: key, param: increment, mutationType: .add)
            }

        case (let oldGroup?, nil):
            // Delete
            if !oldValueIsNull {
                // Value was not null: decrement count
                let key = try packAndValidate(Tuple(oldGroup))
                let decrement = ByteConversion.int64ToBytes(-1)
                transaction.atomicOp(key: key, param: decrement, mutationType: .add)
            }

        case (let oldGroup?, let newGroup?):
            // Update
            let oldKey = try packAndValidate(Tuple(oldGroup))
            let newKey = try packAndValidate(Tuple(newGroup))

            // Handle all null transition cases
            switch (oldValueIsNull, newValueIsNull) {
            case (true, true):
                // Both null: no change
                break

            case (true, false):
                // null → non-null: increment
                let increment = ByteConversion.int64ToBytes(1)
                transaction.atomicOp(key: newKey, param: increment, mutationType: .add)

            case (false, true):
                // non-null → null: decrement
                let decrement = ByteConversion.int64ToBytes(-1)
                transaction.atomicOp(key: oldKey, param: decrement, mutationType: .add)

            case (false, false):
                // non-null → non-null
                if oldKey == newKey {
                    // Same group: no change
                } else {
                    // Different group: decrement old, increment new
                    let decrement = ByteConversion.int64ToBytes(-1)
                    transaction.atomicOp(key: oldKey, param: decrement, mutationType: .add)
                    let increment = ByteConversion.int64ToBytes(1)
                    transaction.atomicOp(key: newKey, param: increment, mutationType: .add)
                }
            }

        case (nil, nil):
            // Nothing to do
            break
        }
    }

    /// Build index entries for an item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Only count if value is not null
        guard !(try isValueNull(in: item)) else { return }

        let groupingValues = try evaluateGroupingFields(from: item)
        let key = try packAndValidate(Tuple(groupingValues))

        let increment = ByteConversion.int64ToBytes(1)
        transaction.atomicOp(key: key, param: increment, mutationType: .add)
    }

    /// Compute expected index keys for an item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Only return key if value is not null
        guard !(try isValueNull(in: item)) else { return [] }

        let groupingValues = try evaluateGroupingFields(from: item)
        return [try packAndValidate(Tuple(groupingValues))]
    }

    // MARK: - Private Helpers

    /// Evaluate grouping fields from an item (excluding the value field)
    private func evaluateGroupingFields(from item: Item) throws -> [any TupleElement] {
        // The last keyPath is the value field to check for null
        // All preceding keyPaths are grouping fields
        guard let keyPaths = index.keyPaths else {
            return []
        }
        let groupingKeyPaths = keyPaths.dropLast()

        return try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: Array(groupingKeyPaths),
            expression: index.rootExpression
        )
    }

    /// Check if the value field is null in the item
    private func isValueNull(in item: Item) throws -> Bool {
        // Get the last keyPath (value field)
        guard let keyPaths = index.keyPaths, keyPaths.last != nil else {
            throw IndexError.invalidConfiguration("CountNotNull index requires at least one field")
        }

        // Use Mirror to check for nil
        let mirror = Mirror(reflecting: item)

        // Find the field value
        for child in mirror.children {
            if child.label == valueFieldName {
                // Check if the value is nil
                let valueMirror = Mirror(reflecting: child.value)
                if valueMirror.displayStyle == .optional {
                    // It's an optional - check if nil
                    if valueMirror.children.isEmpty {
                        return true  // nil
                    }
                }
                return false  // Has value
            }
        }

        // Field not found - treat as null
        return true
    }

    // MARK: - Query Methods

    /// Get the non-null count for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The count of non-null values (0 if no entries)
    public func getCount(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let key = try packAndValidate(Tuple(groupingValues))

        guard let bytes = try await transaction.getValue(for: key) else {
            return 0
        }

        return ByteConversion.bytesToInt64(bytes)
    }

    /// Get all non-null counts in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, count) tuples
    public func getAllCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], count: Int64)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let count = ByteConversion.bytesToInt64(value)

            // Only include positive counts
            if count > 0 {
                results.append((grouping: elements, count: count))
            }
        }

        return results
    }
}

// CountNotNullIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT_NOT_NULL aggregation
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
/// **Behavior**:
/// - Insert with non-null value: Increment count
/// - Insert with null value: No change
/// - Delete with non-null value: Decrement count
/// - Delete with null value: No change
/// - Update null→non-null: Increment count
/// - Update non-null→null: Decrement count
public struct CountNotNullIndexMaintainer<Item: Persistable>: CountAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// The field name to check for null
    public let valueFieldName: String

    // MARK: - Initialization

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

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let oldData = try extractNullCheckData(from: oldItem)
        let newData = try extractNullCheckData(from: newItem)

        switch (oldData, newData) {
        case (nil, let new?) where !new.isNull:
            // Insert with non-null value
            incrementCount(key: new.groupingKey, transaction: transaction)

        case (let old?, nil) where !old.isNull:
            // Delete with non-null value
            decrementCount(key: old.groupingKey, transaction: transaction)

        case (let old?, let new?):
            // Update - handle null transitions
            switch (old.isNull, new.isNull) {
            case (true, false):
                // null → non-null: increment
                incrementCount(key: new.groupingKey, transaction: transaction)

            case (false, true):
                // non-null → null: decrement
                decrementCount(key: old.groupingKey, transaction: transaction)

            case (false, false) where old.groupingKey != new.groupingKey:
                // non-null → non-null, different group
                decrementCount(key: old.groupingKey, transaction: transaction)
                incrementCount(key: new.groupingKey, transaction: transaction)

            default:
                // No change needed
                break
            }

        default:
            break
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        guard !(try isValueNull(in: item)) else { return }

        let groupingValues = try evaluateGroupingFields(from: item)
        let key = try buildGroupingKey(groupingValues)
        incrementCount(key: key, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        guard !(try isValueNull(in: item)) else { return [] }

        let groupingValues = try evaluateGroupingFields(from: item)
        return [try buildGroupingKey(groupingValues)]
    }

    // MARK: - Query Methods

    /// Get the non-null count for a specific grouping
    public func getCount(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        try await getCountValue(groupingValues: groupingValues, transaction: transaction)
    }

    /// Get all non-null counts in this index
    public func getAllCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        let allCounts = try await scanAllCounts(transaction: transaction)
        return allCounts.filter { $0.count > 0 }
    }

    // MARK: - Private Helpers

    private struct NullCheckData {
        let groupingKey: FDB.Bytes
        let isNull: Bool
    }

    private func extractNullCheckData(from item: Item?) throws -> NullCheckData? {
        guard let item = item else { return nil }

        let groupingValues = try evaluateGroupingFields(from: item)
        let groupingKey = try buildGroupingKey(groupingValues)
        let isNull = try isValueNull(in: item)

        return NullCheckData(groupingKey: groupingKey, isNull: isNull)
    }

    private func evaluateGroupingFields(from item: Item) throws -> [any TupleElement] {
        guard let keyPaths = index.keyPaths else { return [] }
        let groupingKeyPaths = keyPaths.dropLast()

        return try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: Array(groupingKeyPaths),
            expression: index.rootExpression
        )
    }

    private func isValueNull(in item: Item) throws -> Bool {
        guard index.keyPaths?.last != nil else {
            throw IndexError.invalidConfiguration("CountNotNull index requires at least one field")
        }

        let mirror = Mirror(reflecting: item)

        for child in mirror.children {
            if child.label == valueFieldName {
                let valueMirror = Mirror(reflecting: child.value)
                if valueMirror.displayStyle == .optional {
                    return valueMirror.children.isEmpty
                }
                return false
            }
        }

        return true
    }
}

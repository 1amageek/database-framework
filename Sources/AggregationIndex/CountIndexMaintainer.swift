// CountIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT aggregation
//
// Maintains counts using atomic FDB operations for thread-safe updates.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for COUNT aggregation indexes
///
/// **Functionality**:
/// - Maintain counts of items grouped by field values
/// - Atomic increment/decrement operations
/// - Efficient GROUP BY COUNT queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Int64 (8 bytes little-endian)
/// ```
public struct CountIndexMaintainer<Item: Persistable>: CountAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // MARK: - Initialization

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

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let oldKey = try oldItem.map { try buildGroupingKey(evaluateIndexFields(from: $0)) }
        let newKey = try newItem.map { try buildGroupingKey(evaluateIndexFields(from: $0)) }

        switch (oldKey, newKey) {
        case let (.some(old), .some(new)) where old == new:
            // Same group - no change needed
            break

        case let (.some(old), .some(new)):
            // Different groups - decrement old, increment new
            decrementCount(key: old, transaction: transaction)
            incrementCount(key: new, transaction: transaction)

        case let (nil, .some(new)):
            // Insert - increment new group
            incrementCount(key: new, transaction: transaction)

        case let (.some(old), nil):
            // Delete - decrement old group
            decrementCount(key: old, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let groupingValues = try evaluateIndexFields(from: item)
        let countKey = try buildGroupingKey(groupingValues)
        incrementCount(key: countKey, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let groupingValues = try evaluateIndexFields(from: item)
        return [try buildGroupingKey(groupingValues)]
    }

    // MARK: - Query Methods

    /// Get the count for a specific grouping
    public func getCount(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        try await getCountValue(groupingValues: groupingValues, transaction: transaction)
    }

    /// Get all counts in this index
    public func getAllCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        try await scanAllCounts(transaction: transaction)
    }
}

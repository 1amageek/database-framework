// CountIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT aggregation
//
// Maintains counts with checked transactional read/replace mutations.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for COUNT aggregation indexes
///
/// **Functionality**:
/// - Maintain counts of items grouped by field values
/// - Checked increment/decrement operations in the caller's transaction
/// - Efficient GROUP BY COUNT queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Int64 (8 bytes little-endian)
/// ```
public struct CountIndexMaintainer<Item: PersistedEntityValue>: CountAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var groupingFieldCount: Int {
        index.kind.fieldNames.count
    }

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
        transaction: any TransactionAccess
    ) async throws {
        let oldKey = try oldItem.map { item in
            try buildGroupingKey(
                storedElements: groupingValues(from: item)
            )
        }
        let newKey = try newItem.map { item in
            try buildGroupingKey(
                storedElements: groupingValues(from: item)
            )
        }

        switch (oldKey, newKey) {
        case let (.some(old), .some(new)) where old == new:
            // Same group - no change needed
            break

        case let (.some(old), .some(new)):
            // Different groups - decrement old, increment new
            try await decrementCount(key: old, transaction: transaction)
            try await incrementCount(key: new, transaction: transaction)

        case let (nil, .some(new)):
            // Insert - increment new group
            try await incrementCount(key: new, transaction: transaction)

        case let (.some(old), nil):
            // Delete - decrement old group
            try await decrementCount(key: old, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        let groupingValues = try groupingValues(from: item)
        let countKey = try buildGroupingKey(
            storedElements: groupingValues
        )
        try await incrementCount(key: countKey, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        let groupingValues = try groupingValues(from: item)
        return [try buildGroupingKey(storedElements: groupingValues)]
    }

    // MARK: - Query Methods

    /// Get the count for a specific grouping
    public func getCount(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        try await getCountValue(groupingValues: groupingValues, transaction: transaction)
    }

    /// Get all counts in this index
    public func getAllCounts(
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], count: Int64)] {
        let storedResults = try await scanAllCounts(transaction: transaction)
        return try storedResults.map { result in
            (
                grouping: try AggregationGroupingValueDecoder.decode(
                    result.grouping
                ),
                count: result.count
            )
        }
    }

    private func groupingValues(
        from item: Item
    ) throws -> [any TupleElement] {
        guard index.kind.fieldNames.count
                == index.rootExpression.columnCount else {
            throw AggregationIndexError.invalidStructure(
                "Count index '\(index.name)' has inconsistent field metadata"
            )
        }
        return try AggregationFieldExtractor.grouping(
            from: item,
            fieldNames: index.kind.fieldNames,
            indexName: index.name
        )
    }
}

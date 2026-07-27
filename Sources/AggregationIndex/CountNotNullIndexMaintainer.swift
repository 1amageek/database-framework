// CountNotNullIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT_NOT_NULL aggregation
//
// Tracks counts of non-null values grouped by other fields.
// Reference: FDB Record Layer COUNT_NOT_NULL index type

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for COUNT_NOT_NULL indexes
///
/// **Functionality**:
/// - Count entities where a specific field is not null
/// - Group counts by other fields
/// - Checked increment/decrement operations in the caller's transaction
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
///
/// **Field Access**:
/// Uses the shared aggregation extractor. The value is evaluated first for
/// sparse semantics; nullable grouping fields are canonicalized as null keys.
public struct CountNotNullIndexMaintainer<Item: Persistable>: CountAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// Field names for grouping (supports nested fields via dot notation)
    public let groupByFieldNames: [String]

    /// The field name to check for null (supports nested fields via dot notation)
    public let valueFieldName: String

    public var groupingFieldCount: Int {
        groupByFieldNames.count
    }

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        groupByFieldNames: [String],
        valueFieldName: String
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.groupByFieldNames = groupByFieldNames
        self.valueFieldName = valueFieldName
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        let oldKey = try contributionKey(from: oldItem)
        let newKey = try contributionKey(from: newItem)

        switch (oldKey, newKey) {
        case let (old?, new?) where old == new:
            break
        case let (old?, new?):
            try await decrementCount(key: old, transaction: transaction)
            try await incrementCount(key: new, transaction: transaction)
        case let (nil, new?):
            try await incrementCount(key: new, transaction: transaction)
        case let (old?, nil):
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
        guard let key = try contributionKey(from: item) else { return }
        try await incrementCount(key: key, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        guard let key = try contributionKey(from: item) else { return [] }
        return [key]
    }

    // MARK: - Query Methods

    /// Get the non-null count for a specific grouping
    public func getCount(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        try await getCountValue(groupingValues: groupingValues, transaction: transaction)
    }

    /// Get all non-null counts in this index
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

    // MARK: - Private Helpers

    /// Returns the grouping key only when the aggregate value contributes.
    /// Value extraction deliberately happens before grouping extraction so a
    /// null aggregate value never fails because an unrelated group path is null.
    private func contributionKey(from item: Item?) throws -> Bytes? {
        guard let item,
              let fields = try AggregationFieldExtractor.contribution(
                from: item,
                index: index
              ) else {
            return nil
        }
        guard index.kind.fieldNames.dropLast().elementsEqual(
                  groupByFieldNames
              ),
              fields.grouping.count == groupByFieldNames.count,
              index.kind.fieldNames.last == valueFieldName else {
            throw AggregationIndexError.invalidStructure(
                "Count-not-null index '\(index.name)' has inconsistent field metadata"
            )
        }
        return try buildGroupingKey(storedElements: fields.grouping)
    }
}

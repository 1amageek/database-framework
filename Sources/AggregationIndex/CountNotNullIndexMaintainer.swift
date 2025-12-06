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
///
/// **Field Access**:
/// Uses `DataAccess.extractField` for both grouping fields and value field,
/// which properly supports nested fields (e.g., "address.city", "user.profile.email").
public struct CountNotNullIndexMaintainer<Item: Persistable>: CountAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// Field names for grouping (supports nested fields via dot notation)
    public let groupByFieldNames: [String]

    /// The field name to check for null (supports nested fields via dot notation)
    public let valueFieldName: String

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
        guard !isValueNull(in: item) else { return }

        let groupingValues = try evaluateGroupingFields(from: item)
        let key = try buildGroupingKey(groupingValues)
        incrementCount(key: key, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        guard !isValueNull(in: item) else { return [] }

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
        let isNull = isValueNull(in: item)

        return NullCheckData(groupingKey: groupingKey, isNull: isNull)
    }

    /// Evaluate grouping fields from item using DataAccess
    ///
    /// Uses `DataAccess.extractField` which properly handles:
    /// - Top-level fields (e.g., "category", "status")
    /// - Nested fields (e.g., "address.city", "user.profile.name")
    ///
    /// All grouping fields must be non-null; throws if any is null.
    ///
    /// - Parameter item: The item to extract grouping fields from
    /// - Returns: Array of tuple elements representing grouping values
    /// - Throws: `DataAccessError.nilValueCannotBeIndexed` if any grouping field is null
    private func evaluateGroupingFields(from item: Item) throws -> [any TupleElement] {
        var result: [any TupleElement] = []
        for fieldName in groupByFieldNames {
            let values = try DataAccess.extractField(from: item, keyPath: fieldName)
            result.append(contentsOf: values)
        }
        return result
    }

    /// Check if the value field is null
    ///
    /// Uses `DataAccess.extractField` which properly handles nested fields.
    /// Catches `nilValueCannotBeIndexed` error to determine null status.
    ///
    /// - Parameter item: The item to check
    /// - Returns: `true` if value field is null, `false` otherwise
    private func isValueNull(in item: Item) -> Bool {
        do {
            _ = try DataAccess.extractField(from: item, keyPath: valueFieldName)
            return false  // Value exists (not null)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return true   // Value is null
        } catch {
            // Field not found or other errors - treat as null
            return true
        }
    }
}

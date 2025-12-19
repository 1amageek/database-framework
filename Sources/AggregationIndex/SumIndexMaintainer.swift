// SumIndexMaintainer.swift
// AggregationIndex - Index maintainer for SUM aggregation
//
// Maintains sums using atomic FDB operations for thread-safe updates.
// Type-safe: Integer types stored as Int64, floating-point as scaled Int64.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for SUM aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves numeric type at compile time
/// - Integer types (Int, Int64, Int32): Stored as Int64 bytes (precision preserved)
/// - Floating-point types (Float, Double): Stored as scaled fixed-point Int64
///
/// **Functionality**:
/// - Maintain sums of numeric values grouped by field values
/// - Atomic add/subtract operations
/// - Efficient GROUP BY SUM queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Int64 (8 bytes little-endian) for integers, scaled Int64 for floats
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., sum_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to sum
public struct SumIndexMaintainer<Item: Persistable, Value: Numeric & Codable & Sendable>: NumericAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var isFloatingPointValue: Bool {
        NumericValueExtractor.isFloatingPoint(Value.self)
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
        transaction: any TransactionProtocol
    ) async throws {
        let oldData = try extractAggregationData(from: oldItem)
        let newData = try extractAggregationData(from: newItem)

        try applyDelta(oldData: oldData, newData: newData, transaction: transaction)
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Sparse index: if any field value is nil, skip indexing
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil values are not included in sum
            return
        }

        guard allValues.count >= 2 else {
            throw IndexError.invalidConfiguration(
                "Sum index requires at least 2 fields: [grouping_fields..., sum_field]"
            )
        }

        let groupingValues = Array(allValues.dropLast())
        let valueElement = allValues.last!

        let sumKey = try buildGroupingKey(groupingValues)
        let numericValue = try NumericValueExtractor.extractNumeric(from: valueElement, as: Value.self)

        atomicAdd(
            key: sumKey,
            int64Value: numericValue.int64,
            doubleValue: numericValue.double,
            transaction: transaction
        )
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// If any field value is nil, returns an empty array.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Sparse index: if any field value is nil, no index entry
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }
        guard allValues.count >= 2 else { return [] }

        let groupingValues = Array(allValues.dropLast())
        return [try buildGroupingKey(groupingValues)]
    }

    // MARK: - Query Methods

    /// Get the sum for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The sum as Double (0.0 if no entries)
    public func getSum(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double {
        let sumKey = try buildGroupingKey(groupingValues)

        guard let bytes = try await transaction.getValue(for: sumKey) else {
            return 0.0
        }

        return readNumericValue(bytes)
    }

    /// Get all sums in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSums(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double)] {
        var results: [(grouping: [any TupleElement], sum: Double)] = []

        let sequence = scanAllEntries(transaction: transaction)
        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let sum = readNumericValue(value)

            results.append((grouping: elements, sum: sum))
        }

        return results
    }

    // MARK: - Private Helpers

    private struct AggregationData {
        let groupingKey: FDB.Bytes
        let int64Value: Int64?
        let doubleValue: Double?
    }

    /// Extract aggregation data from an item
    ///
    /// **Sparse index behavior**:
    /// If any field value is nil, returns nil (no aggregation data).
    private func extractAggregationData(from item: Item?) throws -> AggregationData? {
        guard let item = item else { return nil }

        // Sparse index: if any field value is nil, skip aggregation
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return nil
        }
        guard allValues.count >= 2 else { return nil }

        let groupingValues = Array(allValues.dropLast())
        let valueElement = allValues.last!

        let groupingKey = try buildGroupingKey(groupingValues)
        let numericValue = try NumericValueExtractor.extractNumeric(from: valueElement, as: Value.self)

        return AggregationData(
            groupingKey: groupingKey,
            int64Value: numericValue.int64,
            doubleValue: numericValue.double
        )
    }

    private func applyDelta(
        oldData: AggregationData?,
        newData: AggregationData?,
        transaction: any TransactionProtocol
    ) throws {
        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingKey == new.groupingKey:
            // Same group: apply delta only
            if isFloatingPointValue {
                let delta = (new.doubleValue ?? 0) - (old.doubleValue ?? 0)
                if delta != 0 {
                    atomicAddDouble(key: new.groupingKey, value: delta, transaction: transaction)
                }
            } else {
                let delta = (new.int64Value ?? 0) - (old.int64Value ?? 0)
                if delta != 0 {
                    atomicAddInt64(key: new.groupingKey, value: delta, transaction: transaction)
                }
            }

        case let (.some(old), .some(new)):
            // Different groups: subtract from old, add to new
            if isFloatingPointValue {
                atomicAddDouble(key: old.groupingKey, value: -(old.doubleValue ?? 0), transaction: transaction)
                atomicAddDouble(key: new.groupingKey, value: new.doubleValue ?? 0, transaction: transaction)
            } else {
                atomicAddInt64(key: old.groupingKey, value: -(old.int64Value ?? 0), transaction: transaction)
                atomicAddInt64(key: new.groupingKey, value: new.int64Value ?? 0, transaction: transaction)
            }

        case let (nil, .some(new)):
            // Insert: add to new group
            atomicAdd(
                key: new.groupingKey,
                int64Value: new.int64Value,
                doubleValue: new.doubleValue,
                transaction: transaction
            )

        case let (.some(old), nil):
            // Delete: subtract from old group
            if isFloatingPointValue {
                atomicAddDouble(key: old.groupingKey, value: -(old.doubleValue ?? 0), transaction: transaction)
            } else {
                atomicAddInt64(key: old.groupingKey, value: -(old.int64Value ?? 0), transaction: transaction)
            }

        case (nil, nil):
            break
        }
    }
}

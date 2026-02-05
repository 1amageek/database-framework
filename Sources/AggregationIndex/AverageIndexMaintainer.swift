// AverageIndexMaintainer.swift
// AggregationIndex - Index maintainer for AVERAGE aggregation
//
// Maintains averages by storing sum and count separately using atomic operations.
// Type-safe: Sum stored based on value type, result always Double.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for AVERAGE aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves numeric type at compile time
/// - Integer types (Int, Int64, Int32): Sum stored as Int64 bytes (precision preserved)
/// - Floating-point types (Float, Double): Sum stored as scaled fixed-point Int64
/// - Result: Always Double (average = sum / count)
///
/// **Functionality**:
/// - Maintain average values grouped by field values
/// - Store sum and count separately for exact average calculation
/// - Atomic add/subtract operations for both sum and count
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1]...["sum"]
/// Value: Int64 (for integers) or scaled Int64 (for floats)
///
/// Key: [indexSubspace][groupValue1]...["count"]
/// Value: Int64 (8 bytes little-endian)
/// ```
public struct AverageIndexMaintainer<Item: Persistable, Value: Numeric & Codable & Sendable>: NumericAggregationMaintainer {
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
            // Sparse index: nil values are not included in average
            return
        }

        guard allValues.count >= 2,
              let valueElement = allValues.last else {
            throw IndexError.invalidConfiguration(
                "Average index requires at least 2 fields: [grouping_fields..., averaged_field]"
            )
        }

        let groupingValues = Array(allValues.dropLast())
        let groupingTuple = Tuple(groupingValues)
        let sumKey = try buildSumKey(groupingTuple: groupingTuple)
        let countKey = try buildCountKey(groupingTuple: groupingTuple)

        let numericValue = try NumericValueExtractor.extractNumeric(from: valueElement, as: Value.self)

        atomicAdd(
            key: sumKey,
            int64Value: numericValue.int64,
            doubleValue: numericValue.double,
            transaction: transaction
        )
        atomicIncrementCount(key: countKey, transaction: transaction)
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
        let groupingTuple = Tuple(groupingValues)

        return [
            try buildSumKey(groupingTuple: groupingTuple),
            try buildCountKey(groupingTuple: groupingTuple)
        ]
    }

    // MARK: - Query Methods

    /// Get the average for a specific grouping (result always Double)
    public func getAverage(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> (sum: Double, count: Int64, average: Double) {
        let groupingTuple = Tuple(groupingValues)
        let sumKey = try buildSumKey(groupingTuple: groupingTuple)
        let countKey = try buildCountKey(groupingTuple: groupingTuple)

        let sum: Double
        if let sumBytes = try await transaction.getValue(for: sumKey) {
            sum = readNumericValue(sumBytes)
        } else {
            sum = 0.0
        }

        let count: Int64
        if let countBytes = try await transaction.getValue(for: countKey) {
            count = readInt64Value(countBytes)
        } else {
            count = 0
        }

        guard count > 0 else {
            throw IndexError.noData("No values found for AVERAGE aggregate")
        }

        let average = sum / Double(count)
        return (sum: sum, count: count, average: average)
    }

    /// Maximum number of keys to scan for safety (prevents DoS on large indexes)
    private var maxScanKeys: Int { 100_000 }

    /// Get all averages in this index
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    public func getAllAverages(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double, count: Int64, average: Double)] {
        var sumData: [String: (grouping: [any TupleElement], sum: Double)] = [:]
        var countData: [String: Int64] = [:]

        let sequence = scanAllEntries(transaction: transaction)
        var scannedKeys = 0
        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            // Resource limit
            scannedKeys += 1
            if scannedKeys >= maxScanKeys { break }

            let keyTuple = try subspace.unpack(key)
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }

            guard elements.count >= 1, let marker = elements.last as? String else { continue }

            let grouping = Array(elements.dropLast())
            let groupingKey = Data(Tuple(grouping).pack()).base64EncodedString()

            if marker == "sum" {
                sumData[groupingKey] = (grouping: grouping, sum: readNumericValue(value))
            } else if marker == "count" {
                countData[groupingKey] = readInt64Value(value)
            }
        }

        var results: [(grouping: [any TupleElement], sum: Double, count: Int64, average: Double)] = []

        for (groupingKey, sumInfo) in sumData {
            let count = countData[groupingKey] ?? 0
            guard count > 0 else { continue }

            let average = sumInfo.sum / Double(count)
            results.append((grouping: sumInfo.grouping, sum: sumInfo.sum, count: count, average: average))
        }

        return results
    }

    // MARK: - Private Helpers

    private struct AggregationData {
        let groupingTuple: Tuple
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
        guard allValues.count >= 2,
              let valueElement = allValues.last else { return nil }

        let groupingValues = Array(allValues.dropLast())

        let numericValue = try NumericValueExtractor.extractNumeric(from: valueElement, as: Value.self)

        return AggregationData(
            groupingTuple: Tuple(groupingValues),
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
        case let (.some(old), .some(new)) where old.groupingTuple.pack() == new.groupingTuple.pack():
            // Same group: apply sum delta only (count unchanged)
            let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            if isFloatingPointValue {
                let delta = (new.doubleValue ?? 0) - (old.doubleValue ?? 0)
                if delta != 0 {
                    atomicAddDouble(key: sumKey, value: delta, transaction: transaction)
                }
            } else {
                let delta = (new.int64Value ?? 0) - (old.int64Value ?? 0)
                if delta != 0 {
                    atomicAddInt64(key: sumKey, value: delta, transaction: transaction)
                }
            }

        case let (.some(old), .some(new)):
            // Different groups: update both old and new
            let oldSumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let oldCountKey = try buildCountKey(groupingTuple: old.groupingTuple)
            let newSumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let newCountKey = try buildCountKey(groupingTuple: new.groupingTuple)

            // Subtract from old
            if isFloatingPointValue {
                atomicAddDouble(key: oldSumKey, value: -(old.doubleValue ?? 0), transaction: transaction)
            } else {
                atomicAddInt64(key: oldSumKey, value: -(old.int64Value ?? 0), transaction: transaction)
            }
            atomicDecrementCount(key: oldCountKey, transaction: transaction)

            // Add to new
            atomicAdd(key: newSumKey, int64Value: new.int64Value, doubleValue: new.doubleValue, transaction: transaction)
            atomicIncrementCount(key: newCountKey, transaction: transaction)

        case let (nil, .some(new)):
            // Insert
            let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: new.groupingTuple)
            atomicAdd(key: sumKey, int64Value: new.int64Value, doubleValue: new.doubleValue, transaction: transaction)
            atomicIncrementCount(key: countKey, transaction: transaction)

        case let (.some(old), nil):
            // Delete
            let sumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: old.groupingTuple)
            if isFloatingPointValue {
                atomicAddDouble(key: sumKey, value: -(old.doubleValue ?? 0), transaction: transaction)
            } else {
                atomicAddInt64(key: sumKey, value: -(old.int64Value ?? 0), transaction: transaction)
            }
            atomicDecrementCount(key: countKey, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    private func buildSumKey(groupingTuple: Tuple) throws -> FDB.Bytes {
        var elements: [any TupleElement] = []
        for i in 0..<groupingTuple.count {
            if let element = groupingTuple[i] {
                elements.append(element)
            }
        }
        elements.append("sum")
        return try packAndValidate(Tuple(elements))
    }

    private func buildCountKey(groupingTuple: Tuple) throws -> FDB.Bytes {
        var elements: [any TupleElement] = []
        for i in 0..<groupingTuple.count {
            if let element = groupingTuple[i] {
                elements.append(element)
            }
        }
        elements.append("count")
        return try packAndValidate(Tuple(elements))
    }
}

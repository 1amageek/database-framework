// AverageIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for AVERAGE aggregation
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
public struct AverageIndexMaintainer<Item: Persistable, Value: Numeric & Codable & Sendable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Whether the value type is a floating-point type (compile-time known)
    private var isFloatingPoint: Bool {
        Value.self == Double.self || Value.self == Float.self
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

    /// Update index when item changes
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        if isFloatingPoint {
            try await updateIndexDouble(oldItem: oldItem, newItem: newItem, transaction: transaction)
        } else {
            try await updateIndexInt64(oldItem: oldItem, newItem: newItem, transaction: transaction)
        }
    }

    /// Update index for integer types
    private func updateIndexInt64(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let oldData: (groupingTuple: Tuple, avgValue: Int64)?
        if let oldItem = oldItem {
            let values = try evaluateIndexFields(from: oldItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractInt64Value(values.last!)
                oldData = (Tuple(groupingValues), avgValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingTuple: Tuple, avgValue: Int64)?
        if let newItem = newItem {
            let values = try evaluateIndexFields(from: newItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractInt64Value(values.last!)
                newData = (Tuple(groupingValues), avgValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingTuple.pack() == new.groupingTuple.pack():
            let delta = new.avgValue - old.avgValue
            if delta != 0 {
                let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
                addToSumInt64(key: sumKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            let oldSumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let oldCountKey = try buildCountKey(groupingTuple: old.groupingTuple)
            addToSumInt64(key: oldSumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: oldCountKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)

            let newSumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let newCountKey = try buildCountKey(groupingTuple: new.groupingTuple)
            addToSumInt64(key: newSumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: newCountKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)

        case let (nil, .some(new)):
            let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: new.groupingTuple)
            addToSumInt64(key: sumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)

        case let (.some(old), nil):
            let sumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: old.groupingTuple)
            addToSumInt64(key: sumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)

        case (nil, nil):
            break
        }
    }

    /// Update index for floating-point types
    private func updateIndexDouble(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let oldData: (groupingTuple: Tuple, avgValue: Double)?
        if let oldItem = oldItem {
            let values = try evaluateIndexFields(from: oldItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractDoubleValue(values.last!)
                oldData = (Tuple(groupingValues), avgValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingTuple: Tuple, avgValue: Double)?
        if let newItem = newItem {
            let values = try evaluateIndexFields(from: newItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractDoubleValue(values.last!)
                newData = (Tuple(groupingValues), avgValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingTuple.pack() == new.groupingTuple.pack():
            let delta = new.avgValue - old.avgValue
            if delta != 0 {
                let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
                addToSumDouble(key: sumKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            let oldSumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let oldCountKey = try buildCountKey(groupingTuple: old.groupingTuple)
            addToSumDouble(key: oldSumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: oldCountKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)

            let newSumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let newCountKey = try buildCountKey(groupingTuple: new.groupingTuple)
            addToSumDouble(key: newSumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: newCountKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)

        case let (nil, .some(new)):
            let sumKey = try buildSumKey(groupingTuple: new.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: new.groupingTuple)
            addToSumDouble(key: sumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)

        case let (.some(old), nil):
            let sumKey = try buildSumKey(groupingTuple: old.groupingTuple)
            let countKey = try buildCountKey(groupingTuple: old.groupingTuple)
            addToSumDouble(key: sumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: ByteConversion.int64ToBytes(-1), mutationType: .add)

        case (nil, nil):
            break
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let values = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        guard values.count >= 2 else {
            throw IndexError.invalidConfiguration(
                "Average index requires at least 2 fields: [grouping_fields..., averaged_field]"
            )
        }

        let groupingValues = Array(values.dropLast())
        let groupingTuple = Tuple(groupingValues)
        let sumKey = try buildSumKey(groupingTuple: groupingTuple)
        let countKey = try buildCountKey(groupingTuple: groupingTuple)

        if isFloatingPoint {
            let avgValue = try extractDoubleValue(values.last!)
            addToSumDouble(key: sumKey, value: avgValue, transaction: transaction)
        } else {
            let avgValue = try extractInt64Value(values.last!)
            addToSumInt64(key: sumKey, value: avgValue, transaction: transaction)
        }

        transaction.atomicOp(key: countKey, param: ByteConversion.int64ToBytes(1), mutationType: .add)
    }

    /// Compute expected index keys for an item (for scrubber verification)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let values = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        guard values.count >= 2 else {
            return []
        }

        let groupingValues = Array(values.dropLast())
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
        let count: Int64

        if let sumBytes = try await transaction.getValue(for: sumKey) {
            if isFloatingPoint {
                sum = ByteConversion.scaledBytesToDouble(sumBytes)
            } else {
                sum = Double(ByteConversion.bytesToInt64(sumBytes))
            }
        } else {
            sum = 0.0
        }

        if let countBytes = try await transaction.getValue(for: countKey) {
            count = ByteConversion.bytesToInt64(countBytes)
        } else {
            count = 0
        }

        guard count > 0 else {
            throw IndexError.noData("No values found for AVERAGE aggregate")
        }

        let average = sum / Double(count)
        return (sum: sum, count: count, average: average)
    }

    /// Get all averages in this index
    public func getAllAverages(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double, count: Int64, average: Double)] {
        let range = subspace.range()
        var sumData: [String: (grouping: [any TupleElement], sum: Double)] = [:]
        var countData: [String: Int64] = [:]

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())

            guard elements.count >= 1,
                  let marker = elements.last as? String else {
                continue
            }

            let grouping = Array(elements.dropLast())
            let groupingKey = Data(Tuple(grouping).pack()).base64EncodedString()

            if marker == "sum" {
                let sumValue: Double
                if isFloatingPoint {
                    sumValue = ByteConversion.scaledBytesToDouble(value)
                } else {
                    sumValue = Double(ByteConversion.bytesToInt64(value))
                }
                sumData[groupingKey] = (grouping: grouping, sum: sumValue)
            } else if marker == "count" {
                countData[groupingKey] = ByteConversion.bytesToInt64(value)
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

    private func addToSumInt64(
        key: FDB.Bytes,
        value: Int64,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.int64ToBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    private func addToSumDouble(
        key: FDB.Bytes,
        value: Double,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.doubleToScaledBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Extract value from tuple element as Int64 (type-safe for integer Value types)
    private func extractInt64Value(_ element: any TupleElement) throws -> Int64 {
        switch Value.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int64, got \(type(of: element))")
            }
            return value

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int (as Int64), got \(type(of: element))")
            }
            return value

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return value

        default:
            throw IndexError.invalidConfiguration(
                "AVERAGE index (integer mode) requires Int64, Int, or Int32. Got: \(Value.self)"
            )
        }
    }

    /// Extract value from tuple element as Double (type-safe for floating-point Value types)
    private func extractDoubleValue(_ element: any TupleElement) throws -> Double {
        switch Value.self {
        case is Double.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Double, got \(type(of: element))")
            }
            return value

        case is Float.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Float (as Double), got \(type(of: element))")
            }
            return value

        default:
            throw IndexError.invalidConfiguration(
                "AVERAGE index (floating-point mode) requires Double or Float. Got: \(Value.self)"
            )
        }
    }
}

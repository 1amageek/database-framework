// SumIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for SUM aggregation
//
// Maintains sums using atomic FDB operations for thread-safe updates.
// Type-safe: Integer types are stored as Int64, floating-point as scaled Int64.

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
/// - Precision preservation for integer types
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
///
/// **Examples**:
/// ```swift
/// // Sum of sales amount by category (Int64)
/// Key: [I]/Sale_category_amount/["electronics"] = 350000 (Int64)
///
/// // Sum of prices by category (Double)
/// Key: [I]/Sale_category_price/["electronics"] = scaled(350000.50)
/// ```
public struct SumIndexMaintainer<Item: Persistable, Value: Numeric & Codable & Sendable>: SubspaceIndexMaintainer {
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

    /// Initialize sum index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
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
    ///
    /// **Process**:
    /// - Insert (oldItem=nil): Add new sum value
    /// - Delete (newItem=nil): Subtract old sum value
    /// - Update (same group): Apply delta only (1 atomic op instead of 2)
    /// - Update (different group): Subtract from old group, add to new group
    ///
    /// **Type-Aware Storage**:
    /// - Integer types: Stored as Int64 bytes (precision preserved)
    /// - Floating-point: Stored as scaled fixed-point Int64
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
        if isFloatingPoint {
            try await updateIndexDouble(oldItem: oldItem, newItem: newItem, transaction: transaction)
        } else {
            try await updateIndexInt64(oldItem: oldItem, newItem: newItem, transaction: transaction)
        }
    }

    /// Update index for integer types (Int, Int64, Int32)
    private func updateIndexInt64(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Extract grouping and sum values as Int64
        let oldData: (groupingKey: FDB.Bytes, sumValue: Int64)?
        if let oldItem = oldItem {
            let values = try evaluateIndexFields(from: oldItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractInt64Value(values.last!)
                let groupingKey = try packAndValidate(Tuple(groupingValues))
                oldData = (groupingKey, sumValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingKey: FDB.Bytes, sumValue: Int64)?
        if let newItem = newItem {
            let values = try evaluateIndexFields(from: newItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractInt64Value(values.last!)
                let groupingKey = try packAndValidate(Tuple(groupingValues))
                newData = (groupingKey, sumValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        // Apply updates based on case
        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingKey == new.groupingKey:
            let delta = new.sumValue - old.sumValue
            if delta != 0 {
                addToSumInt64(key: new.groupingKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            addToSumInt64(key: old.groupingKey, value: -old.sumValue, transaction: transaction)
            addToSumInt64(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (nil, .some(new)):
            addToSumInt64(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (.some(old), nil):
            addToSumInt64(key: old.groupingKey, value: -old.sumValue, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    /// Update index for floating-point types (Float, Double)
    private func updateIndexDouble(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Extract grouping and sum values as Double
        let oldData: (groupingKey: FDB.Bytes, sumValue: Double)?
        if let oldItem = oldItem {
            let values = try evaluateIndexFields(from: oldItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractDoubleValue(values.last!)
                let groupingKey = try packAndValidate(Tuple(groupingValues))
                oldData = (groupingKey, sumValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingKey: FDB.Bytes, sumValue: Double)?
        if let newItem = newItem {
            let values = try evaluateIndexFields(from: newItem)
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractDoubleValue(values.last!)
                let groupingKey = try packAndValidate(Tuple(groupingValues))
                newData = (groupingKey, sumValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        // Apply updates based on case
        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingKey == new.groupingKey:
            let delta = new.sumValue - old.sumValue
            if delta != 0 {
                addToSumDouble(key: new.groupingKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            addToSumDouble(key: old.groupingKey, value: -old.sumValue, transaction: transaction)
            addToSumDouble(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (nil, .some(new)):
            addToSumDouble(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (.some(old), nil):
            addToSumDouble(key: old.groupingKey, value: -old.sumValue, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    /// Build index entries for an item during batch indexing
    ///
    /// - Parameters:
    ///   - item: Item to index
    ///   - id: The item's unique identifier
    ///   - transaction: FDB transaction
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let values = try evaluateIndexFields(from: item)

        guard values.count >= 2 else {
            throw IndexError.invalidConfiguration(
                "Sum index requires at least 2 fields: [grouping_fields..., sum_field]"
            )
        }

        let groupingValues = Array(values.dropLast())
        let sumKey = try packAndValidate(Tuple(groupingValues))

        // Add to sum based on value type
        if isFloatingPoint {
            let sumValue = try extractDoubleValue(values.last!)
            addToSumDouble(key: sumKey, value: sumValue, transaction: transaction)
        } else {
            let sumValue = try extractInt64Value(values.last!)
            addToSumInt64(key: sumKey, value: sumValue, transaction: transaction)
        }
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the sum key that should be affected by this item.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let values = try evaluateIndexFields(from: item)

        guard values.count >= 2 else {
            return []
        }

        let groupingValues = Array(values.dropLast())
        return [try packAndValidate(Tuple(groupingValues))]
    }

    // MARK: - Query Methods

    /// Get the sum for a specific grouping (type-safe)
    ///
    /// Returns Double for both integer and floating-point Value types.
    /// For integer types, the stored Int64 is converted to Double.
    /// For floating-point types, the scaled value is converted back to Double.
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The sum as Double (0.0 if no entries)
    public func getSum(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double {
        let sumKey = try packAndValidate(Tuple(groupingValues))

        guard let bytes = try await transaction.getValue(for: sumKey) else {
            return 0.0
        }

        if isFloatingPoint {
            return ByteConversion.scaledBytesToDouble(bytes)
        } else {
            return Double(ByteConversion.bytesToInt64(bytes))
        }
    }

    /// Get all sums in this index (type-safe)
    ///
    /// Returns Double for both integer and floating-point Value types.
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSums(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], sum: Double)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let sum: Double
            if isFloatingPoint {
                sum = ByteConversion.scaledBytesToDouble(value)
            } else {
                sum = Double(ByteConversion.bytesToInt64(value))
            }

            results.append((grouping: elements, sum: sum))
        }

        return results
    }

    /// Get the sum for a specific grouping as Int64 (legacy)
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The sum (0 if no entries)
    public func getSumInt64(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let sumKey = try packAndValidate(Tuple(groupingValues))

        guard let bytes = try await transaction.getValue(for: sumKey) else {
            return 0
        }

        return ByteConversion.bytesToInt64(bytes)
    }

    /// Get the sum for a specific grouping as Double (legacy)
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The sum (0.0 if no entries)
    public func getSumDouble(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double {
        let sumKey = try packAndValidate(Tuple(groupingValues))

        guard let bytes = try await transaction.getValue(for: sumKey) else {
            return 0.0
        }

        return ByteConversion.scaledBytesToDouble(bytes)
    }

    /// Get all sums in this index as Int64
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSumsInt64(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Int64)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], sum: Int64)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let sum = ByteConversion.bytesToInt64(value)

            results.append((grouping: elements, sum: sum))
        }

        return results
    }

    /// Get all sums in this index as Double
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSumsDouble(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], sum: Double)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let sum = ByteConversion.scaledBytesToDouble(value)

            results.append((grouping: elements, sum: sum))
        }

        return results
    }

    // MARK: - Private Helpers

    /// Add Int64 value to sum using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation to prevent lost updates.
    /// **Precision**: Exact (no conversion loss for integers).
    private func addToSumInt64(
        key: FDB.Bytes,
        value: Int64,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.int64ToBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Add Double value to sum using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation to prevent lost updates.
    /// **Implementation**: Converts Double to fixed-point Int64 (6 decimal places).
    /// **Precision**: 6 decimal places (e.g., 123456.789012)
    private func addToSumDouble(
        key: FDB.Bytes,
        value: Double,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.doubleToScaledBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Extract value from tuple element as Int64 (type-safe for integer Value types)
    ///
    /// Value type is known at compile time, so we know exactly which conversion to use.
    ///
    /// - Parameter element: Tuple element to extract
    /// - Returns: Int64 value
    /// - Throws: IndexError if extraction fails
    private func extractInt64Value(_ element: any TupleElement) throws -> Int64 {
        // Value type is known at compile time
        switch Value.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration(
                    "Expected Int64, got \(type(of: element))"
                )
            }
            return value

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration(
                    "Expected Int (as Int64), got \(type(of: element))"
                )
            }
            return value

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration(
                    "Expected Int32 (as Int64), got \(type(of: element))"
                )
            }
            return value

        default:
            throw IndexError.invalidConfiguration(
                "SUM index (integer mode) requires Int64, Int, or Int32. Got: \(Value.self)"
            )
        }
    }

    /// Extract value from tuple element as Double (type-safe for floating-point Value types)
    ///
    /// Value type is known at compile time, so we know exactly which conversion to use.
    ///
    /// - Parameter element: Tuple element to extract
    /// - Returns: Double value
    /// - Throws: IndexError if extraction fails
    private func extractDoubleValue(_ element: any TupleElement) throws -> Double {
        // Value type is known at compile time
        switch Value.self {
        case is Double.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration(
                    "Expected Double, got \(type(of: element))"
                )
            }
            return value

        case is Float.Type:
            // Float is stored as Double in FDB Tuple layer
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration(
                    "Expected Float (as Double), got \(type(of: element))"
                )
            }
            return value

        default:
            throw IndexError.invalidConfiguration(
                "SUM index (floating-point mode) requires Double or Float. Got: \(Value.self)"
            )
        }
    }
}

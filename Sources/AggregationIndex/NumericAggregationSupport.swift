// NumericAggregationSupport.swift
// AggregationIndex - Shared utilities for numeric aggregation indexes
//
// Provides common value extraction and atomic operation support for
// Sum, Average, and other numeric aggregation maintainers.
//
// Reference: Consolidates duplicate code from SumIndexMaintainer, AverageIndexMaintainer

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Numeric Value Extraction

/// Utility for extracting numeric values from tuple elements with type safety
///
/// **Purpose**: Centralizes value extraction logic previously duplicated across
/// SumIndexMaintainer, AverageIndexMaintainer, and MinMaxIndexMaintainer.
///
/// **FDB Tuple Layer Behavior**:
/// - Int, Int32, Int64 → stored as Int64
/// - Float, Double → stored as Double
public enum NumericValueExtractor {

    /// Extract Int64 value from tuple element
    ///
    /// Uses `TypeConversion` for unified type extraction.
    ///
    /// - Parameters:
    ///   - element: The tuple element to extract from
    ///   - expectedType: The Swift type being extracted (for error messages)
    /// - Returns: The Int64 value
    /// - Throws: IndexError if extraction fails
    public static func extractInt64(
        from element: any TupleElement,
        expectedType: Any.Type
    ) throws -> Int64 {
        do {
            return try TypeConversion.int64(from: element)
        } catch {
            throw IndexError.invalidConfiguration(
                "Expected \(expectedType) (as Int64), got \(type(of: element))"
            )
        }
    }

    /// Extract Double value from tuple element
    ///
    /// Uses `TypeConversion` for unified type extraction.
    ///
    /// - Parameters:
    ///   - element: The tuple element to extract from
    ///   - expectedType: The Swift type being extracted (for error messages)
    /// - Returns: The Double value
    /// - Throws: IndexError if extraction fails
    public static func extractDouble(
        from element: any TupleElement,
        expectedType: Any.Type
    ) throws -> Double {
        do {
            return try TypeConversion.double(from: element)
        } catch {
            throw IndexError.invalidConfiguration(
                "Expected \(expectedType) (as Double), got \(type(of: element))"
            )
        }
    }

    /// Extract numeric value with type-safe conversion
    ///
    /// Handles FDB's type coercion (Int→Int64, Float→Double).
    ///
    /// - Parameters:
    ///   - element: The tuple element to extract from
    ///   - valueType: The expected value type
    /// - Returns: Tuple of (int64Value, doubleValue, isFloatingPoint)
    /// - Throws: IndexError if type is not supported
    public static func extractNumeric<Value: Numeric & Codable & Sendable>(
        from element: any TupleElement,
        as valueType: Value.Type
    ) throws -> (int64: Int64?, double: Double?, isFloatingPoint: Bool) {
        switch valueType {
        case is Int64.Type, is Int.Type, is Int32.Type:
            let value = try extractInt64(from: element, expectedType: valueType)
            return (int64: value, double: nil, isFloatingPoint: false)

        case is Double.Type, is Float.Type:
            let value = try extractDouble(from: element, expectedType: valueType)
            return (int64: nil, double: value, isFloatingPoint: true)

        default:
            throw IndexError.invalidConfiguration(
                "Unsupported numeric type for aggregation: \(valueType)"
            )
        }
    }

    /// Check if a type is floating-point
    public static func isFloatingPoint<Value>(_ type: Value.Type) -> Bool {
        type == Double.self || type == Float.self
    }
}

// MARK: - Atomic Sum Operations

/// Protocol providing atomic sum operations for aggregation maintainers
///
/// **Purpose**: Centralizes atomic sum operations previously duplicated across
/// SumIndexMaintainer and AverageIndexMaintainer.
///
/// **Atomic Operations**: Uses FDB's `.add` mutation type which:
/// - Performs atomic read-modify-write
/// - Treats value as little-endian Int64
/// - Adds parameter to existing value (or initializes to parameter if key absent)
public protocol AtomicSumSupport: SubspaceIndexMaintainer {
    /// Whether the value type is floating-point
    var isFloatingPointValue: Bool { get }
}

extension AtomicSumSupport {

    /// Add Int64 value to aggregation key using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation.
    /// **Precision**: Exact for integers.
    ///
    /// - Parameters:
    ///   - key: The aggregation key
    ///   - value: The Int64 value to add (can be negative for subtraction)
    ///   - transaction: The FDB transaction
    public func atomicAddInt64(
        key: FDB.Bytes,
        value: Int64,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.int64ToBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Add Double value to aggregation key using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation.
    /// **Implementation**: Converts Double to fixed-point Int64 (6 decimal places).
    /// **Precision**: 6 decimal places (e.g., 123456.789012).
    ///
    /// - Parameters:
    ///   - key: The aggregation key
    ///   - value: The Double value to add (can be negative for subtraction)
    ///   - transaction: The FDB transaction
    public func atomicAddDouble(
        key: FDB.Bytes,
        value: Double,
        transaction: any TransactionProtocol
    ) {
        let bytes = ByteConversion.doubleToScaledBytes(value)
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Add numeric value using the appropriate atomic operation
    ///
    /// - Parameters:
    ///   - key: The aggregation key
    ///   - int64Value: Int64 value (used if isFloatingPointValue is false)
    ///   - doubleValue: Double value (used if isFloatingPointValue is true)
    ///   - transaction: The FDB transaction
    public func atomicAdd(
        key: FDB.Bytes,
        int64Value: Int64? = nil,
        doubleValue: Double? = nil,
        transaction: any TransactionProtocol
    ) {
        if isFloatingPointValue, let value = doubleValue {
            atomicAddDouble(key: key, value: value, transaction: transaction)
        } else if let value = int64Value {
            atomicAddInt64(key: key, value: value, transaction: transaction)
        }
    }

    /// Increment count by 1 using atomic operation
    ///
    /// - Parameters:
    ///   - key: The count key
    ///   - transaction: The FDB transaction
    public func atomicIncrementCount(
        key: FDB.Bytes,
        transaction: any TransactionProtocol
    ) {
        atomicAddInt64(key: key, value: 1, transaction: transaction)
    }

    /// Decrement count by 1 using atomic operation
    ///
    /// - Parameters:
    ///   - key: The count key
    ///   - transaction: The FDB transaction
    public func atomicDecrementCount(
        key: FDB.Bytes,
        transaction: any TransactionProtocol
    ) {
        atomicAddInt64(key: key, value: -1, transaction: transaction)
    }
}

// MARK: - Grouping Key Helpers

/// Protocol for aggregation maintainers that use grouping keys
public protocol GroupingKeySupport: SubspaceIndexMaintainer {
    /// The index definition
    var index: Index { get }
}

extension GroupingKeySupport {

    /// Build a grouping key from values
    ///
    /// - Parameter values: The grouping values
    /// - Returns: Packed key bytes
    /// - Throws: If packing fails
    public func buildGroupingKey(
        _ values: [any TupleElement]
    ) throws -> FDB.Bytes {
        try packAndValidate(Tuple(values))
    }
}

// MARK: - Query Result Helpers

/// Protocol for reading aggregation results
public protocol AggregationQuerySupport: SubspaceIndexMaintainer {
    /// Whether stored values are floating-point
    var isFloatingPointValue: Bool { get }
}

extension AggregationQuerySupport {

    /// Read an Int64 value from stored bytes
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: The Int64 value
    public func readInt64Value(_ bytes: FDB.Bytes) -> Int64 {
        ByteConversion.bytesToInt64(bytes)
    }

    /// Read a Double value from stored bytes (assumes scaled format)
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: The Double value
    public func readDoubleValue(_ bytes: FDB.Bytes) -> Double {
        ByteConversion.scaledBytesToDouble(bytes)
    }

    /// Read numeric value based on value type
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: Double representation of the value
    public func readNumericValue(_ bytes: FDB.Bytes) -> Double {
        if isFloatingPointValue {
            return readDoubleValue(bytes)
        } else {
            return Double(readInt64Value(bytes))
        }
    }

    /// Scan all entries in the index subspace
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: AsyncSequence of (key, value) pairs
    public func scanAllEntries(
        transaction: any TransactionProtocol
    ) -> any AsyncSequence<(FDB.Bytes, FDB.Bytes), Error> {
        let range = subspace.range()
        return transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )
    }
}

// MARK: - Combined Support Protocol

/// Combined protocol for numeric aggregation maintainers
///
/// Provides all common functionality needed by Sum and Average maintainers.
public protocol NumericAggregationMaintainer: AtomicSumSupport, GroupingKeySupport, AggregationQuerySupport {}

// MARK: - Count Aggregation Support

/// Protocol for count-based aggregation maintainers (COUNT, COUNT_NOT_NULL)
///
/// Provides common functionality for maintainers that track counts.
public protocol CountAggregationMaintainer: SubspaceIndexMaintainer, GroupingKeySupport {
    var index: Index { get }
}

extension CountAggregationMaintainer {

    /// Increment count for a grouping key
    public func incrementCount(
        key: FDB.Bytes,
        transaction: any TransactionProtocol
    ) {
        let increment = ByteConversion.int64ToBytes(1)
        transaction.atomicOp(key: key, param: increment, mutationType: .add)
    }

    /// Decrement count for a grouping key
    public func decrementCount(
        key: FDB.Bytes,
        transaction: any TransactionProtocol
    ) {
        let decrement = ByteConversion.int64ToBytes(-1)
        transaction.atomicOp(key: key, param: decrement, mutationType: .add)
    }

    /// Read count value from bytes
    public func readCount(_ bytes: FDB.Bytes) -> Int64 {
        ByteConversion.bytesToInt64(bytes)
    }

    /// Get count for a specific grouping
    public func getCountValue(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let key = try buildGroupingKey(groupingValues)
        guard let bytes = try await transaction.getValue(for: key) else {
            return 0
        }
        return readCount(bytes)
    }

    /// Maximum number of keys to scan for safety (prevents DoS on large indexes)
    private var maxScanKeys: Int { 100_000 }

    /// Scan all count entries
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    public func scanAllCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], count: Int64)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        var scannedKeys = 0
        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            // Resource limit
            scannedKeys += 1
            if scannedKeys >= maxScanKeys { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let count = readCount(value)

            results.append((grouping: elements, count: count))
        }

        return results
    }
}

// MARK: - Value Extraction for Min/Max

/// Utility for extracting comparable values from tuple elements
///
/// Used by MinIndexMaintainer and MaxIndexMaintainer to extract typed values.
public enum ComparableValueExtractor {

    /// Extract a comparable value from tuple element
    ///
    /// Handles FDB's type coercion (Int→Int64, Float→Double).
    ///
    /// - Parameters:
    ///   - element: The tuple element to extract from
    ///   - valueType: The expected value type
    /// - Returns: The extracted value
    /// - Throws: IndexError if extraction fails
    public static func extract<Value: Comparable & Codable & Sendable>(
        from element: any TupleElement,
        as valueType: Value.Type
    ) throws -> Value {
        switch valueType {
        case is Int64.Type:
            guard let value = element as? Int64,
                  let result = value as? Value else {
                throw IndexError.invalidConfiguration("Expected Int64, got \(type(of: element))")
            }
            return result

        case is Int.Type:
            guard let value = element as? Int64,
                  let result = Int(value) as? Value else {
                throw IndexError.invalidConfiguration("Expected Int (as Int64), got \(type(of: element))")
            }
            return result

        case is Int32.Type:
            guard let value = element as? Int64,
                  let result = Int32(value) as? Value else {
                throw IndexError.invalidConfiguration("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return result

        case is Double.Type:
            guard let value = element as? Double,
                  let result = value as? Value else {
                throw IndexError.invalidConfiguration("Expected Double, got \(type(of: element))")
            }
            return result

        case is Float.Type:
            guard let value = element as? Double,
                  let result = Float(value) as? Value else {
                throw IndexError.invalidConfiguration("Expected Float (as Double), got \(type(of: element))")
            }
            return result

        case is String.Type:
            guard let value = element as? String,
                  let result = value as? Value else {
                throw IndexError.invalidConfiguration("Expected String, got \(type(of: element))")
            }
            return result

        default:
            guard let value = element as? Value else {
                throw IndexError.invalidConfiguration(
                    "Cannot convert \(type(of: element)) to \(valueType)"
                )
            }
            return value
        }
    }
}

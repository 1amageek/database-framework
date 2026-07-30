// QueryTypes.swift
// DatabaseEngine - Common types for queries

import DatabaseTypes
import DatabaseKit

// MARK: - Distance Unit

/// Units for distance measurements
public enum DistanceUnit: Sendable {
    case meters
    case kilometers
    case miles

    /// Convert to meters
    public func toMeters(_ value: Double) -> Double {
        switch self {
        case .meters:
            return value
        case .kilometers:
            return value * 1000
        case .miles:
            return value * 1609.344
        }
    }
}

// MARK: - Aggregate Result

/// Result of a GROUP BY aggregation query
///
/// **Type Preservation**:
/// - `groupKey`: Preserves original types via `FieldValue` (int64, double, string, etc.)
/// - `aggregates`: Preserves exact integer kinds for count/sum and integral averages;
///   floating-point inputs remain floating-point; min/max preserve their input type.
///
/// **Empty Results**:
/// - `min`/`max` return `nil` in `aggregates` for empty groups (not zero)
/// - `count` returns `0` for empty groups
/// - `sum`/`avg` return `nil` when every input is null or the group is empty
///
/// An entity count is not implicit metadata. Callers request `count` as an
/// aggregate and read its `FieldValue.int64` result like every other aggregate.
public struct AggregateResult<T: Persistable>: Sendable {
    /// Group key values (field name → typed value)
    public let groupKey: [String: FieldValue]

    /// Aggregation results (aggregation name → typed value)
    /// - count: `FieldValue.int64`
    /// - sum: exact `FieldValue.int64`/`uint64`, or `double` for floating inputs
    /// - avg: exact integer when integral, otherwise an exactly convertible `double`
    /// - min/max: `FieldValue?` (nil for empty groups)
    public let aggregates: [String: FieldValue?]

    public init(
        groupKey: [String: FieldValue],
        aggregates: [String: FieldValue?]
    ) {
        self.groupKey = groupKey
        self.aggregates = aggregates
    }

    // MARK: - Convenience Accessors

    /// Get aggregate value as Double (for sum, avg, or numeric min/max)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: Double value, or nil when the aggregate is absent or null.
    /// - Throws: A typed error for non-numeric, non-finite, or lossy values.
    public func aggregateDouble(
        _ name: String
    ) throws(AggregateResultError) -> Double? {
        guard let stored = aggregates[name], let value = stored else {
            return nil
        }
        switch value {
        case .int8(let integer):
            return Double(integer)
        case .int16(let integer):
            return Double(integer)
        case .int32(let integer):
            return Double(integer)
        case .int64(let integer):
            guard let result = Double(exactly: integer) else {
                throw .notExactlyRepresentableAsDouble(
                    name: name,
                    value: value
                )
            }
            return result
        case .uint8(let integer):
            return Double(integer)
        case .uint16(let integer):
            return Double(integer)
        case .uint32(let integer):
            return Double(integer)
        case .uint64(let integer):
            guard let result = Double(exactly: integer) else {
                throw .notExactlyRepresentableAsDouble(
                    name: name,
                    value: value
                )
            }
            return result
        case .float32(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw .nonFiniteDouble(name: name)
            }
            return Double(floatingPoint)
        case .float64(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw .nonFiniteDouble(name: name)
            }
            return floatingPoint
        case .null:
            return nil
        case .string, .bool, .bytes, .date, .time, .dateTime,
             .timestamp, .timeSpan, .calendarPeriod, .geographicPoint,
             .geographicPosition, .vector, .uuid, .object, .reference,
             .rdfTerm, .array:
            throw .nonNumericValue(name: name, value: value)
        case .decimal:
            throw .notExactlyRepresentableAsDouble(name: name, value: value)
        }
    }

    /// Get aggregate value as Int64 (for count)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: Int64 value, or nil if not found or not integer
    public func aggregateInt64(_ name: String) -> Int64? {
        aggregates[name]??.int64Value
    }

    /// Get aggregate value as String (for string min/max)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: String value, or nil if not found or not string
    public func aggregateString(_ name: String) -> String? {
        aggregates[name]??.stringValue
    }

    /// Get group key value as Int64
    ///
    /// - Parameter name: The field name
    /// - Returns: Int64 value, or nil if not found or not integer
    public func groupKeyInt64(_ name: String) -> Int64? {
        groupKey[name]?.int64Value
    }

    /// Get group key value as String
    ///
    /// - Parameter name: The field name
    /// - Returns: String value, or nil if not found or not string
    public func groupKeyString(_ name: String) -> String? {
        groupKey[name]?.stringValue
    }

    /// Get group key value as Double
    ///
    /// - Parameter name: The field name
    /// - Returns: Double value, or nil if not found or not double
    public func groupKeyDouble(_ name: String) -> Double? {
        groupKey[name]?.float64Value
    }
}

public enum AggregateResultError: Error, Sendable, Equatable {
    case nonNumericValue(name: String, value: FieldValue)
    case notExactlyRepresentableAsDouble(name: String, value: FieldValue)
    case nonFiniteDouble(name: String)
}

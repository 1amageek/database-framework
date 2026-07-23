import Core
import DatabaseValue
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import QueryIR

enum DatabaseValueComparisonError: Error, Sendable, Equatable {
    case incomparable(left: String, right: String)
    case unorderedFloatingPoint
}

enum DatabaseValueComparator {
    static func equal(
        _ lhs: DatabaseValue,
        _ rhs: DatabaseValue
    ) throws(DatabaseValueComparisonError) -> Bool {
        if isNull(lhs) || isNull(rhs) { return false }
        if isNumeric(lhs) || isNumeric(rhs) {
            return try compare(lhs, rhs) == .orderedSame
        }
        return lhs == rhs
    }

    static func compare(
        _ lhs: DatabaseValue,
        _ rhs: DatabaseValue
    ) throws(DatabaseValueComparisonError) -> ComparisonResult {
        if let left = exactNumericLiteral(lhs),
           let right = exactNumericLiteral(rhs),
           let result = left.compareExactNumeric(to: right) {
            return comparisonResult(result)
        }

        if let left = fieldNumericValue(lhs),
           let right = fieldNumericValue(rhs) {
            guard let result = left.compare(to: right) else {
                throw .unorderedFloatingPoint
            }
            return result
        }

        switch (lhs, rhs) {
        case (.string(let left), .string(let right)):
            return ordered(left, right)
        case (.bool(let left), .bool(let right)):
            return ordered(left ? 1 : 0, right ? 1 : 0)
        case (.bytes(let left), .bytes(let right)):
            return lexicographic(left, right)
        case (.date(let left), .date(let right)):
            return ordered(left, right)
        case (.timestamp(let left), .timestamp(let right)):
            return ordered(left, right)
        case (.uuid(let left), .uuid(let right)):
            return ordered(left, right)
        default:
            throw .incomparable(left: kind(lhs), right: kind(rhs))
        }
    }

    private static func exactNumericLiteral(_ value: DatabaseValue) -> Literal? {
        switch value {
        case .int64(let value):
            return .int(value)
        case .uint64(let value):
            return .uint(value)
        case .decimal(let coefficient, let scale):
            return .decimal(coefficient: coefficient, scale: scale)
        default:
            return nil
        }
    }

    private static func fieldNumericValue(_ value: DatabaseValue) -> FieldValue? {
        switch value {
        case .int64(let value): return .int64(value)
        case .uint64(let value): return .uint64(value)
        case .double(let value): return .double(value)
        default: return nil
        }
    }

    private static func ordered<Value: Comparable>(
        _ lhs: Value,
        _ rhs: Value
    ) -> ComparisonResult {
        if lhs < rhs { return .orderedAscending }
        if lhs > rhs { return .orderedDescending }
        return .orderedSame
    }

    private static func lexicographic(
        _ lhs: DatabaseBytes,
        _ rhs: DatabaseBytes
    ) -> ComparisonResult {
        lhs.withUnsafeBytes { left in
            rhs.withUnsafeBytes { right in
                for offset in 0..<min(left.count, right.count) {
                    if left[offset] < right[offset] { return .orderedAscending }
                    if left[offset] > right[offset] { return .orderedDescending }
                }
                return ordered(left.count, right.count)
            }
        }
    }

    private static func comparisonResult(_ value: Int) -> ComparisonResult {
        if value < 0 { return .orderedAscending }
        if value > 0 { return .orderedDescending }
        return .orderedSame
    }

    private static func isNull(_ value: DatabaseValue) -> Bool {
        if case .null = value { return true }
        return false
    }

    private static func isNumeric(_ value: DatabaseValue) -> Bool {
        switch value {
        case .int64, .uint64, .double, .decimal:
            return true
        default:
            return false
        }
    }

    private static func kind(_ value: DatabaseValue) -> String {
        switch value {
        case .null: return "null"
        case .bool: return "bool"
        case .int64: return "int64"
        case .uint64: return "uint64"
        case .double: return "double"
        case .decimal: return "decimal"
        case .string: return "string"
        case .bytes: return "bytes"
        case .date: return "date"
        case .timestamp: return "timestamp"
        case .uuid: return "uuid"
        case .array: return "array"
        case .object: return "object"
        case .reference: return "reference"
        case .rdfTerm: return "rdfTerm"
        }
    }
}

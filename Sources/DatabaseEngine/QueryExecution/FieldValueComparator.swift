import DatabaseKit
import DatabaseTypes

enum FieldValueComparisonError: Error, Sendable, Equatable {
    case incomparable(left: String, right: String)
    case unorderedFloatingPoint
}

enum FieldValueComparator {
    static func equal(
        _ lhs: FieldValue,
        _ rhs: FieldValue
    ) throws(FieldValueComparisonError) -> Bool {
        if isNull(lhs) || isNull(rhs) { return false }
        if lhs.isNumeric || rhs.isNumeric {
            return try compare(lhs, rhs) == .equal
        }
        return lhs == rhs
    }

    static func compare(
        _ lhs: FieldValue,
        _ rhs: FieldValue
    ) throws(FieldValueComparisonError) -> QueryComparison {
        guard let result = lhs.compare(to: rhs) else {
            if lhs.isNumeric && rhs.isNumeric {
                throw .unorderedFloatingPoint
            }
            throw .incomparable(left: kind(lhs), right: kind(rhs))
        }
        return result
    }

    /// Returns the final ordering after applying both direction and NULL
    /// placement. Explicit NULLS FIRST/LAST describes the final order and is
    /// therefore not reversed again for descending keys.
    static func compare(
        _ lhs: FieldValue,
        _ rhs: FieldValue,
        using sortKey: SortKey
    ) throws(FieldValueComparisonError) -> QueryComparison {
        if lhs == .null || rhs == .null {
            if lhs == .null, rhs == .null { return .equal }
            let nullsFirst: Bool
            switch sortKey.nulls {
            case .first:
                nullsFirst = true
            case .last:
                nullsFirst = false
            case nil:
                nullsFirst = sortKey.direction == .ascending
            }
            if lhs == .null {
                return nullsFirst ? .lessThan : .greaterThan
            }
            return nullsFirst ? .greaterThan : .lessThan
        }

        let comparison = try compare(lhs, rhs)
        guard sortKey.direction == .descending else { return comparison }
        switch comparison {
        case .lessThan:
            return .greaterThan
        case .equal:
            return .equal
        case .greaterThan:
            return .lessThan
        }
    }

    private static func isNull(_ value: FieldValue) -> Bool {
        if case .null = value { return true }
        return false
    }

    private static func kind(_ value: FieldValue) -> String {
        switch value {
        case .null: return "null"
        case .bool: return "bool"
        case .int8: return "int8"
        case .int16: return "int16"
        case .int32: return "int32"
        case .int64: return "int64"
        case .uint8: return "uint8"
        case .uint16: return "uint16"
        case .uint32: return "uint32"
        case .uint64: return "uint64"
        case .float32: return "float32"
        case .float64: return "float64"
        case .decimal: return "decimal"
        case .string: return "string"
        case .bytes: return "bytes"
        case .date: return "date"
        case .time: return "time"
        case .dateTime: return "dateTime"
        case .timestamp: return "timestamp"
        case .timeSpan: return "timeSpan"
        case .calendarPeriod: return "calendarPeriod"
        case .geographicPoint: return "geographicPoint"
        case .geographicPosition: return "geographicPosition"
        case .vector: return "vector"
        case .uuid: return "uuid"
        case .array: return "array"
        case .object: return "object"
        case .reference: return "reference"
        case .rdfTerm: return "rdfTerm"
        }
    }
}

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
            guard lhs.isNumeric, rhs.isNumeric else {
                throw .incomparable(left: kind(lhs), right: kind(rhs))
            }
            guard let comparison = RelationalValueIdentity.compareNumeric(
                lhs,
                rhs
            ) else {
                throw .unorderedFloatingPoint
            }
            return comparison == 0
        }
        switch (lhs, rhs) {
        case (.array, .array), (.object, .object):
            return try compare(lhs, rhs) == .equal
        default:
            return lhs == rhs
        }
    }

    static func compare(
        _ lhs: FieldValue,
        _ rhs: FieldValue
    ) throws(FieldValueComparisonError) -> QueryComparison {
        if lhs.isNumeric, rhs.isNumeric {
            guard let comparison = RelationalValueIdentity.compareNumeric(
                lhs,
                rhs
            ) else {
                throw .unorderedFloatingPoint
            }
            if comparison < 0 { return .lessThan }
            if comparison > 0 { return .greaterThan }
            return .equal
        }
        switch (lhs, rhs) {
        case (.bytes(let left), .bytes(let right)):
            return compareBytes(left, right)
        case (.array(let left), .array(let right)):
            return try compareArrays(left, right)
        case (.object(let left), .object(let right)):
            return try compareObjects(left, right)
        default:
            break
        }
        guard let result = lhs.compare(to: rhs) else {
            if lhs.isNumeric && rhs.isNumeric {
                throw .unorderedFloatingPoint
            }
            throw .incomparable(left: kind(lhs), right: kind(rhs))
        }
        return result
    }

    private static func compareBytes(
        _ lhs: ByteString,
        _ rhs: ByteString
    ) -> QueryComparison {
        lhs.withUnsafeBytes { left in
            rhs.withUnsafeBytes { right in
                let sharedCount = min(left.count, right.count)
                for offset in 0..<sharedCount {
                    if left[offset] < right[offset] { return .lessThan }
                    if left[offset] > right[offset] { return .greaterThan }
                }
                if left.count < right.count { return .lessThan }
                if left.count > right.count { return .greaterThan }
                return .equal
            }
        }
    }

    private static func compareArrays(
        _ lhs: [FieldValue],
        _ rhs: [FieldValue]
    ) throws(FieldValueComparisonError) -> QueryComparison {
        for (left, right) in zip(lhs, rhs) {
            let comparison = try compare(left, right)
            if comparison != .equal { return comparison }
        }
        if lhs.count < rhs.count { return .lessThan }
        if lhs.count > rhs.count { return .greaterThan }
        return .equal
    }

    private static func compareObjects(
        _ lhs: FieldObject,
        _ rhs: FieldObject
    ) throws(FieldValueComparisonError) -> QueryComparison {
        let leftFields = lhs.fields
        let rightFields = rhs.fields
        for (left, right) in zip(leftFields, rightFields) {
            if !left.key.utf8.elementsEqual(right.key.utf8) {
                return left.key.utf8.lexicographicallyPrecedes(right.key.utf8)
                    ? .lessThan
                    : .greaterThan
            }
            let comparison = try compare(left.value, right.value)
            if comparison != .equal { return comparison }
        }
        if leftFields.count < rightFields.count { return .lessThan }
        if leftFields.count > rightFields.count { return .greaterThan }
        return .equal
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

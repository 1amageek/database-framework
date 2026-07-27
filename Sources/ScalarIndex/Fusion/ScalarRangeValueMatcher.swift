import DatabaseTypes
import DatabaseKit
import DatabaseEngine

enum ScalarRangeValueMatcher {
    static func matches(
        _ value: FieldValue,
        minimum: FieldValue?,
        maximum: FieldValue?,
        minimumInclusive: Bool,
        maximumInclusive: Bool,
        fieldName: String
    ) throws -> Bool {
        try validateOrdered(value, fieldName: fieldName)

        if let minimum {
            let comparison = try compare(
                value,
                with: minimum,
                fieldName: fieldName
            )
            if minimumInclusive {
                guard comparison != .lessThan else { return false }
            } else {
                guard comparison == .greaterThan else { return false }
            }
        }

        if let maximum {
            let comparison = try compare(
                value,
                with: maximum,
                fieldName: fieldName
            )
            if maximumInclusive {
                guard comparison != .greaterThan else { return false }
            } else {
                guard comparison == .lessThan else { return false }
            }
        }

        return true
    }

    private static func compare(
        _ value: FieldValue,
        with bound: FieldValue,
        fieldName: String
    ) throws -> QueryComparison {
        try validateOrdered(bound, fieldName: fieldName)
        guard let result = value.compare(to: bound) else {
            throw FilterError.incomparableValues(
                fieldName: fieldName,
                valueType: kind(of: value),
                boundType: kind(of: bound)
            )
        }
        return result
    }

    private static func validateOrdered(
        _ value: FieldValue,
        fieldName: String
    ) throws {
        if case .float32(let number) = value, number.isNaN {
            throw FilterError.unorderedFloatingPoint(fieldName: fieldName)
        }
        if case .float64(let number) = value, number.isNaN {
            throw FilterError.unorderedFloatingPoint(fieldName: fieldName)
        }
    }

    private static func kind(of value: FieldValue) -> String {
        switch value {
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
        case .bool: return "bool"
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
        case .object: return "object"
        case .reference: return "reference"
        case .rdfTerm: return "rdfTerm"
        case .null: return "null"
        case .array: return "array"
        }
    }
}

import Core
import DatabaseEngine
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

enum ScalarRangeValueMatcher {
    static func fieldValue(
        from rawValue: Any,
        fieldName: String
    ) throws -> FieldValue {
        let value = try TypeConversion.toFieldValue(rawValue)
        try validateOrdered(value, fieldName: fieldName)
        return value
    }

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
                guard comparison != .orderedAscending else { return false }
            } else {
                guard comparison == .orderedDescending else { return false }
            }
        }

        if let maximum {
            let comparison = try compare(
                value,
                with: maximum,
                fieldName: fieldName
            )
            if maximumInclusive {
                guard comparison != .orderedDescending else { return false }
            } else {
                guard comparison == .orderedAscending else { return false }
            }
        }

        return true
    }

    private static func compare(
        _ value: FieldValue,
        with bound: FieldValue,
        fieldName: String
    ) throws -> ComparisonResult {
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
        if case .double(let number) = value, number.isNaN {
            throw FilterError.unorderedFloatingPoint(fieldName: fieldName)
        }
    }

    private static func kind(of value: FieldValue) -> String {
        switch value {
        case .int64: return "int64"
        case .uint64: return "uint64"
        case .double: return "double"
        case .string: return "string"
        case .bool: return "bool"
        case .data: return "data"
        case .rdfTerm: return "rdfTerm"
        case .null: return "null"
        case .array: return "array"
        }
    }
}

import Foundation

/// Supported field types for dynamic schemas
public enum FieldType: String, Codable, Sendable, CaseIterable {
    case string
    case int
    case double
    case bool
    case date
    case stringArray = "[string]"
    case doubleArray = "[double]"

    /// Parse a field type from CLI input (e.g., "string", "int?")
    /// Returns the type and whether it's optional
    public static func parse(_ input: String) -> (type: FieldType, optional: Bool)? {
        let trimmed = input.trimmingCharacters(in: .whitespaces)
        let isOptional = trimmed.hasSuffix("?")
        let typeString = isOptional ? String(trimmed.dropLast()) : trimmed

        guard let type = FieldType(rawValue: typeString) else {
            return nil
        }
        return (type, isOptional)
    }

    /// Validate a JSON value against this type
    public func validate(_ value: Any?) -> Bool {
        guard let value = value else {
            return false // For non-optional fields, nil is invalid
        }

        switch self {
        case .string:
            return value is String
        case .int:
            return value is Int || value is Int64
        case .double:
            return value is Double || value is Int || value is Int64
        case .bool:
            return value is Bool
        case .date:
            if value is Date {
                return true
            }
            if let str = value as? String {
                return ISO8601DateFormatter().date(from: str) != nil
            }
            return false
        case .stringArray:
            guard let array = value as? [Any] else { return false }
            return array.allSatisfy { $0 is String }
        case .doubleArray:
            guard let array = value as? [Any] else { return false }
            return array.allSatisfy { asDouble($0) != nil }
        }
    }

    /// Convert a JSON value to the appropriate type
    public func coerce(_ value: Any?) throws -> Any? {
        guard let value = value else {
            return nil
        }

        switch self {
        case .string:
            guard let str = value as? String else {
                throw FieldTypeError.typeMismatch(expected: "string", got: "\(type(of: value))")
            }
            return str

        case .int:
            if let i = value as? Int {
                return i
            }
            if let i = value as? Int64 {
                return Int(i)
            }
            if let d = value as? Double {
                return Int(d)
            }
            throw FieldTypeError.typeMismatch(expected: "int", got: "\(type(of: value))")

        case .double:
            if let d = value as? Double {
                return d
            }
            if let i = value as? Int {
                return Double(i)
            }
            if let i = value as? Int64 {
                return Double(i)
            }
            throw FieldTypeError.typeMismatch(expected: "double", got: "\(type(of: value))")

        case .bool:
            guard let b = value as? Bool else {
                throw FieldTypeError.typeMismatch(expected: "bool", got: "\(type(of: value))")
            }
            return b

        case .date:
            // Always return ISO8601 string for JSON serialization compatibility
            if let date = value as? Date {
                return ISO8601DateFormatter().string(from: date)
            }
            if let str = value as? String {
                // Validate it's a valid ISO8601 date, then return the string
                if ISO8601DateFormatter().date(from: str) != nil {
                    return str
                }
                throw FieldTypeError.invalidDateFormat(str)
            }
            throw FieldTypeError.typeMismatch(expected: "date", got: "\(type(of: value))")

        case .stringArray:
            guard let array = value as? [Any] else {
                throw FieldTypeError.typeMismatch(expected: "[string]", got: "\(type(of: value))")
            }
            var result: [String] = []
            for element in array {
                guard let str = element as? String else {
                    throw FieldTypeError.typeMismatch(expected: "string in array", got: "\(type(of: element))")
                }
                result.append(str)
            }
            return result

        case .doubleArray:
            guard let array = value as? [Any] else {
                throw FieldTypeError.typeMismatch(expected: "[double]", got: "\(type(of: value))")
            }
            var result: [Double] = []
            for element in array {
                if let d = asDouble(element) {
                    result.append(d)
                } else {
                    throw FieldTypeError.typeMismatch(expected: "double in array", got: "\(type(of: element))")
                }
            }
            return result
        }
    }
}

/// Helper function to convert numeric values to Double
private func asDouble(_ value: Any) -> Double? {
    if let d = value as? Double { return d }
    if let i = value as? Int { return Double(i) }
    if let i = value as? Int64 { return Double(i) }
    return nil
}

public enum FieldTypeError: Error, CustomStringConvertible {
    case typeMismatch(expected: String, got: String)
    case invalidDateFormat(String)

    public var description: String {
        switch self {
        case .typeMismatch(let expected, let got):
            return "Type mismatch: expected \(expected), got \(got)"
        case .invalidDateFormat(let str):
            return "Invalid date format: '\(str)'. Use ISO8601 format."
        }
    }
}

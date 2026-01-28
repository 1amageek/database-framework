import Foundation

/// JSON parsing utilities for CLI
public struct JSONParser {
    /// Parse a JSON string into a dictionary
    public static func parse(_ jsonString: String) throws -> [String: Any] {
        guard let data = jsonString.data(using: .utf8) else {
            throw CLIError.invalidJSON("Invalid UTF-8 encoding")
        }

        do {
            guard let dict = try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any] else {
                throw CLIError.invalidJSON("Expected a JSON object, got something else")
            }
            return dict
        } catch let error as CLIError {
            throw error
        } catch {
            throw CLIError.invalidJSON(error.localizedDescription)
        }
    }

    /// Convert a dictionary to JSON string
    public static func stringify(_ dict: [String: Any], pretty: Bool = true) throws -> String {
        let options: JSONSerialization.WritingOptions = pretty ? [.prettyPrinted, .sortedKeys] : []
        let data = try JSONSerialization.data(withJSONObject: dict, options: options)
        guard let str = String(data: data, encoding: .utf8) else {
            throw CLIError.invalidJSON("Failed to encode JSON as UTF-8")
        }
        return str
    }
}

/// Parse a simple where clause into a filter function
///
/// Supports: `field op value` where op is =, !=, >, <, >=, <=
public func parseWhereClause(_ clause: String) throws -> (String, @Sendable (Any?) -> Bool) {
    let operators = [">=", "<=", "!=", "=", ">", "<"]

    for op in operators {
        if let range = clause.range(of: op) {
            let field = String(clause[..<range.lowerBound]).trimmingCharacters(in: .whitespaces)
            var value = String(clause[range.upperBound...]).trimmingCharacters(in: .whitespaces)

            // Remove quotes if present
            if value.hasPrefix("\"") && value.hasSuffix("\"") && value.count >= 2 {
                value = String(value.dropFirst().dropLast())
            }

            let filter = makeFilter(op: op, value: value)
            return (field, filter)
        }
    }

    throw CLIError.invalidArguments("Invalid where clause: '\(clause)'. Expected: field op value")
}

private func makeFilter(op: String, value: String) -> @Sendable (Any?) -> Bool {
    // Try to parse as number
    let numValue = Double(value)

    // Capture values for Sendable closure
    let capturedOp = op
    let capturedValue = value
    let capturedNumValue = numValue

    return { @Sendable fieldValue in
        guard let fieldValue = fieldValue else {
            return false
        }

        // String comparison
        if let strValue = fieldValue as? String {
            switch capturedOp {
            case "=": return strValue == capturedValue
            case "!=": return strValue != capturedValue
            case ">": return strValue > capturedValue
            case "<": return strValue < capturedValue
            case ">=": return strValue >= capturedValue
            case "<=": return strValue <= capturedValue
            default: return false
            }
        }

        // Numeric comparison
        if let numFieldValue = asDouble(fieldValue), let numValue = capturedNumValue {
            switch capturedOp {
            case "=": return numFieldValue == numValue
            case "!=": return numFieldValue != numValue
            case ">": return numFieldValue > numValue
            case "<": return numFieldValue < numValue
            case ">=": return numFieldValue >= numValue
            case "<=": return numFieldValue <= numValue
            default: return false
            }
        }

        return false
    }
}

private func asDouble(_ value: Any) -> Double? {
    if let d = value as? Double { return d }
    if let i = value as? Int { return Double(i) }
    if let i = value as? Int64 { return Double(i) }
    return nil
}

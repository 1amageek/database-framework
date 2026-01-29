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

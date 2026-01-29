import Foundation

/// Output formatter for CLI
public struct OutputFormatter: Sendable {
    public init() {}

    /// Print a success message
    public func success(_ message: String) {
        print("OK: \(message)")
    }

    /// Print an error message
    public func error(_ message: String) {
        print("ERROR: \(message)")
    }

    /// Print an info message
    public func info(_ message: String) {
        print(message)
    }

    /// Print a header
    public func header(_ text: String) {
        print("\(text):")
    }

    /// Print a line (with optional indent)
    public func line(_ text: String) {
        print(text)
    }

    /// Print JSON data
    public func json(_ data: [String: Any], compact: Bool = false) {
        do {
            let options: JSONSerialization.WritingOptions = compact ? [] : [.prettyPrinted, .sortedKeys]
            let jsonData = try JSONSerialization.data(withJSONObject: data, options: options)
            if let jsonString = String(data: jsonData, encoding: .utf8) {
                print(jsonString)
            }
        } catch {
            print("{\(data.map { "\"\($0.key)\": \($0.value)" }.joined(separator: ", "))}")
        }
    }

    /// Print a table of records
    public func table(_ records: [(id: String, values: [String: Any])], fields: [String]? = nil) {
        if records.isEmpty {
            info("(no results)")
            return
        }

        for (id, values) in records {
            var displayValues = values
            displayValues["id"] = id
            json(displayValues)
        }
    }

    /// Print raw bytes as hex
    public func hex(_ bytes: [UInt8]) {
        let hexString = bytes.map { String(format: "%02x", $0) }.joined(separator: " ")
        print(hexString)
    }

    /// Print raw bytes as string (if valid UTF-8) or hex
    public func rawValue(_ bytes: [UInt8]) {
        if let str = String(bytes: bytes, encoding: .utf8) {
            print(str)
        } else {
            hex(bytes)
        }
    }
}

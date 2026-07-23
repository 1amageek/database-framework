import Foundation
import StorageKit

/// Read-only handler for bounded raw key-value inspection.
public struct RawCommands {
    private static let defaultRangeLimit = 100
    private static let maximumRangeLimit = 10_000

    private let database: any StorageEngine
    private let output: OutputFormatter

    public init(database: any StorageEngine, output: OutputFormatter) {
        self.database = database
        self.output = output
    }

    /// Execute a raw command
    public func execute(_ command: String, args: [String]) async throws {
        switch command {
        case "get":
            try await get(args: args)
        case "range":
            try await range(args: args)
        default:
            throw CLIError.unknownCommand("raw \(command)")
        }
    }

    // MARK: - Commands

    /// Get a raw key
    /// Usage: raw get <key>
    private func get(args: [String]) async throws {
        guard let keyString = args.first else {
            throw CLIError.invalidArguments("Usage: raw get <key>")
        }

        guard args.count == 1 else {
            throw CLIError.invalidArguments("Usage: raw get <key>")
        }
        let key = try encodeKey(keyString)

        let value = try await database.withTransaction(configuration: .default) { transaction in
            try await transaction.getValue(for: key, snapshot: false)
        }

        if let value = value {
            output.info("Key: \(keyString)")
            output.info("Value (\(value.count) bytes):")
            output.rawValue(value)
        } else {
            output.info("Key not found: \(keyString)")
        }
    }

    /// Scan a range of keys
    /// Usage: raw range <prefix> [limit N]
    private func range(args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: raw range <prefix> [limit N]")
        }

        let prefixString = args[0]
        let limit: Int
        switch args.count {
        case 1:
            limit = Self.defaultRangeLimit
        case 3:
            guard args[1].lowercased() == "limit",
                  let parsedLimit = Int(args[2]),
                  (1...Self.maximumRangeLimit).contains(parsedLimit) else {
                throw CLIError.invalidArguments(
                    "Range limit must be between 1 and \(Self.maximumRangeLimit)"
                )
            }
            limit = parsedLimit
        default:
            throw CLIError.invalidArguments(
                "Usage: raw range <prefix> [limit N]"
            )
        }

        let prefix = try encodeKey(prefixString)
        let subspace = Subspace(prefix: prefix)
        let (begin, end) = subspace.range()

        let results: [(key: Bytes, value: Bytes)] = try await database.withTransaction(configuration: .default) { transaction in
            try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: limit,
                snapshot: true,
                streamingMode: .small
            )
        }

        if results.isEmpty {
            output.info("No keys found with prefix '\(prefixString)'")
        } else {
            output.info("Found \(results.count) key(s):")
            for (key, value) in results {
                let keyDisplay = decodeKey(key)
                output.line("  \(keyDisplay) = \(value.count) bytes")
            }
        }
    }

    // MARK: - Helpers

    /// Encode a key string to bytes
    /// Supports:
    /// - Simple strings: "mykey" -> UTF-8 bytes
    /// - Tuple format: "(\"mykey\", 123)" -> Tuple encoding
    private func encodeKey(_ keyString: String) throws -> Bytes {
        guard !keyString.isEmpty else {
            throw CLIError.invalidArguments("Raw key must not be empty")
        }
        let startsTuple = keyString.hasPrefix("(")
        let endsTuple = keyString.hasSuffix(")")
        if startsTuple || endsTuple {
            guard startsTuple && endsTuple else {
                throw CLIError.invalidArguments("Malformed tuple key")
            }
            return try parseTuple(keyString).pack()
        }

        // Default to simple UTF-8 encoding wrapped in a Tuple
        return Tuple([keyString]).pack()
    }

    /// Decode a key for display
    private func decodeKey(_ key: Bytes) -> String {
        do {
            let elements = try Tuple.unpack(from: key)
            let parts = elements.map { "\($0)" }
            return "(\(parts.joined(separator: ", ")))"
        } catch {
            return "0x\(hexString(key))"
        }
    }

    private func hexString(_ bytes: Bytes) -> String {
        let digits = Array("0123456789abcdef".utf8)
        var output = [UInt8](repeating: 0, count: bytes.count * 2)
        bytes.withUnsafeBytes { source in
            output.withUnsafeMutableBytes { destination in
                for index in source.indices {
                    let byte = source[index]
                    destination[index * 2] = digits[Int(byte >> 4)]
                    destination[index * 2 + 1] = digits[Int(byte & 0x0f)]
                }
            }
        }
        return String(decoding: output, as: UTF8.self)
    }

    /// Parse a tuple string like ("key", 123, true)
    private func parseTuple(_ tupleString: String) throws -> Tuple {
        var inner = tupleString.trimmingCharacters(in: .whitespaces)
        guard inner.hasPrefix("(") && inner.hasSuffix(")") else {
            throw CLIError.invalidArguments("Malformed tuple key")
        }
        inner = String(inner.dropFirst().dropLast())

        if inner.isEmpty {
            return Tuple([])
        }

        var elements: [any TupleElement] = []
        let parts = inner.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespaces) }

        for part in parts {
            if part.hasPrefix("\"") && part.hasSuffix("\"") && part.count >= 2 {
                // String
                let str = String(part.dropFirst().dropLast())
                elements.append(str)
            } else if let intVal = Int(part) {
                // Integer
                elements.append(intVal)
            } else if let doubleVal = Double(part) {
                // Double
                elements.append(doubleVal)
            } else if part.lowercased() == "true" {
                // Bool true
                elements.append(true)
            } else if part.lowercased() == "false" {
                // Bool false
                elements.append(false)
            } else {
                throw CLIError.invalidArguments(
                    "Unsupported tuple element '\(part)'"
                )
            }
        }

        return Tuple(elements)
    }
}

// MARK: - Help

extension RawCommands {
    public static var helpText: String {
        """
        Raw Commands:
          raw get <key>               Get value for a key
          raw range <prefix> [limit N] Scan keys with prefix

        Key Formats:
          Simple string: mykey
          Tuple: ("mykey", 123)

        Examples:
          raw get mykey
          raw range _cli limit 50
        """
    }
}

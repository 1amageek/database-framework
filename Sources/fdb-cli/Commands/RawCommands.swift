import Foundation
import FoundationDB

/// Handler for raw FDB key-value operations
public struct RawCommands {
    private let database: any DatabaseProtocol
    private let output: OutputFormatter

    public init(database: any DatabaseProtocol, output: OutputFormatter) {
        self.database = database
        self.output = output
    }

    /// Execute a raw command
    public func execute(_ command: String, args: [String]) async throws {
        switch command {
        case "get":
            try await get(args: args)
        case "set":
            try await set(args: args)
        case "delete":
            try await delete(args: args)
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

        let key = encodeKey(keyString)

        let value = try await database.withTransaction { transaction in
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

    /// Set a raw key-value
    /// Usage: raw set <key> <value>
    private func set(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: raw set <key> <value>")
        }

        let keyString = args[0]
        let valueString = args.dropFirst().joined(separator: " ")

        let key = encodeKey(keyString)
        let value = Array(valueString.utf8)

        try await database.withTransaction { transaction in
            transaction.setValue(value, for: key)
        }

        output.success("Set key '\(keyString)' (\(value.count) bytes)")
    }

    /// Delete a raw key
    /// Usage: raw delete <key>
    private func delete(args: [String]) async throws {
        guard let keyString = args.first else {
            throw CLIError.invalidArguments("Usage: raw delete <key>")
        }

        let key = encodeKey(keyString)

        try await database.withTransaction { transaction in
            transaction.clear(key: key)
        }

        output.success("Deleted key '\(keyString)'")
    }

    /// Scan a range of keys
    /// Usage: raw range <prefix> [limit N]
    private func range(args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: raw range <prefix> [limit N]")
        }

        let prefixString = args[0]
        var limit = 100

        // Parse limit option
        if args.count >= 3 && args[1].lowercased() == "limit" {
            if let n = Int(args[2]) {
                limit = n
            }
        }

        let prefix = encodeKey(prefixString)
        let subspace = Subspace(prefix: prefix)
        let (begin, end) = subspace.range()

        let results: [(key: FDB.Bytes, value: FDB.Bytes)] = try await database.withTransaction { transaction in
            var collected: [(key: FDB.Bytes, value: FDB.Bytes)] = []

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, value) in sequence {
                collected.append((key: key, value: value))
                if collected.count >= limit { break }
            }

            return collected
        }

        if results.isEmpty {
            output.info("No keys found with prefix '\(prefixString)'")
        } else {
            output.info("Found \(results.count) key(s):")
            for (key, value) in results {
                let keyDisplay = decodeKey(key, prefix: prefix)
                output.line("  \(keyDisplay) = \(value.count) bytes")
            }
        }
    }

    // MARK: - Helpers

    /// Encode a key string to bytes
    /// Supports:
    /// - Simple strings: "mykey" -> UTF-8 bytes
    /// - Tuple format: "(\"mykey\", 123)" -> Tuple encoding
    private func encodeKey(_ keyString: String) -> FDB.Bytes {
        // Try tuple format first
        if keyString.hasPrefix("(") && keyString.hasSuffix(")") {
            if let tuple = parseTuple(keyString) {
                return tuple.pack()
            }
        }

        // Default to simple UTF-8 encoding wrapped in a Tuple
        return Tuple([keyString]).pack()
    }

    /// Decode a key for display
    private func decodeKey(_ key: FDB.Bytes, prefix: FDB.Bytes) -> String {
        do {
            let elements = try Tuple.unpack(from: key)
            let parts = elements.map { "\($0)" }
            return "(\(parts.joined(separator: ", ")))"
        } catch {
            return String(bytes: key, encoding: .utf8) ?? "<binary>"
        }
    }

    /// Parse a tuple string like ("key", 123, true)
    private func parseTuple(_ tupleString: String) -> Tuple? {
        var inner = tupleString.trimmingCharacters(in: .whitespaces)
        guard inner.hasPrefix("(") && inner.hasSuffix(")") else { return nil }
        inner = String(inner.dropFirst().dropLast())

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
                // Treat as string
                elements.append(part)
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
          raw set <key> <value>       Set a key-value pair
          raw delete <key>            Delete a key
          raw range <prefix> [limit N] Scan keys with prefix

        Key Formats:
          Simple string: mykey
          Tuple: ("mykey", 123)

        Examples:
          raw get mykey
          raw set mykey hello world
          raw delete mykey
          raw range _cli limit 50
        """
    }
}

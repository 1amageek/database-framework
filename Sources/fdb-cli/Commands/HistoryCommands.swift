import Foundation
import FoundationDB

/// Handler for history/version-related commands
public struct HistoryCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    /// Execute a history command
    /// Usage: history <Schema> <id> [--at <version>] [--diff <v1> <v2>] [--rollback <version>]
    public func execute(_ args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: history <Schema> <id> [--at <version>] [--diff <v1> <v2>]")
        }

        let schemaName = args[0]
        let id = args[1]
        let queryArgs = Array(args.dropFirst(2))

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Find version index
        guard let indexDef = schema.indexes.first(where: { $0.kind == .version }) else {
            throw CLIError.validationError("No version index found in schema '\(schemaName)'")
        }

        let handler = VersionIndexHandler(indexDefinition: indexDef, schemaName: schemaName)

        // Parse query type
        if let atIdx = queryArgs.firstIndex(of: "--at") {
            guard atIdx + 1 < queryArgs.count,
                  let version = Int64(queryArgs[atIdx + 1].replacingOccurrences(of: "v", with: "")) else {
                throw CLIError.invalidArguments("--at requires a version number (e.g., v2 or 2)")
            }
            try await executeAtVersion(handler: handler, id: id, version: version)

        } else if let diffIdx = queryArgs.firstIndex(of: "--diff") {
            guard diffIdx + 2 < queryArgs.count,
                  let v1 = Int64(queryArgs[diffIdx + 1].replacingOccurrences(of: "v", with: "")),
                  let v2 = Int64(queryArgs[diffIdx + 2].replacingOccurrences(of: "v", with: "")) else {
                throw CLIError.invalidArguments("--diff requires two version numbers (e.g., v1 v2)")
            }
            try await executeDiff(handler: handler, id: id, v1: v1, v2: v2)

        } else if let rollbackIdx = queryArgs.firstIndex(of: "--rollback") {
            guard rollbackIdx + 1 < queryArgs.count,
                  let version = Int64(queryArgs[rollbackIdx + 1].replacingOccurrences(of: "v", with: "")) else {
                throw CLIError.invalidArguments("--rollback requires a version number")
            }
            try await executeRollback(handler: handler, id: id, version: version, schema: schema)

        } else {
            // Default: show history
            try await executeHistory(handler: handler, id: id)
        }
    }

    // MARK: - Query Implementations

    private func executeHistory(handler: VersionIndexHandler, id: String) async throws {
        let query = VersionQuery.history(id: id)

        let versions = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 100, transaction: transaction, storage: self.storage)
        }

        output.info("Version history for '\(id)':")
        if versions.isEmpty {
            output.line("  (no history)")
        } else {
            for version in versions {
                output.line("  \(version)")
            }
        }
    }

    private func executeAtVersion(handler: VersionIndexHandler, id: String, version: Int64) async throws {
        let query = VersionQuery.atVersion(id: id, version: version)

        let result = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1, transaction: transaction, storage: self.storage)
        }

        if let data = result.first {
            if data == "__DELETED__" {
                output.info("Record was deleted at version \(version)")
            } else {
                output.info("Record at version \(version):")
                output.line(data)
            }
        } else {
            output.info("Version \(version) not found for '\(id)'")
        }
    }

    private func executeDiff(handler: VersionIndexHandler, id: String, v1: Int64, v2: Int64) async throws {
        let query = VersionQuery.diff(id: id, v1: v1, v2: v2)

        let diffs = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        output.info("Diff between v\(v1) and v\(v2) for '\(id)':")
        if diffs.isEmpty || diffs.first == "No differences" {
            output.line("  (no differences)")
        } else {
            for diff in diffs {
                output.line("  \(diff)")
            }
        }
    }

    private func executeRollback(handler: VersionIndexHandler, id: String, version: Int64, schema: DynamicSchema) async throws {
        // Get the data at the target version
        let query = VersionQuery.atVersion(id: id, version: version)

        let result = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1, transaction: transaction, storage: self.storage)
        }

        guard let dataStr = result.first, dataStr != "__DELETED__" else {
            throw CLIError.validationError("Cannot rollback to version \(version): record was deleted or version not found")
        }

        // Parse the historical data
        guard let data = dataStr.data(using: .utf8),
              let values = try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any] else {
            throw CLIError.validationError("Could not parse historical data")
        }

        // Validate against schema
        try schema.validate(values)

        // Update current record with historical data
        try await storage.update(schemaName: schema.name, id: id, values: values)

        output.success("Rolled back '\(id)' to version \(version)")
    }
}

// MARK: - Help

extension HistoryCommands {
    public static var helpText: String {
        """
        History Commands:
          history <Schema> <id>                   Show version history
          history <Schema> <id> --at <version>    Get record at specific version
          history <Schema> <id> --diff <v1> <v2>  Show diff between versions
          history <Schema> <id> --rollback <ver>  Rollback to a specific version

        Version Format:
          Versions can be specified as "v1", "v2", etc. or just "1", "2"

        Examples:
          history Document doc123
          history Document doc123 --at v2
          history Document doc123 --diff v1 v2
          history Document doc123 --rollback v2
        """
    }
}

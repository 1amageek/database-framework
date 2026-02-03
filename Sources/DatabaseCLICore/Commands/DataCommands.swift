/// DataCommands - CRUD operations via CatalogDataAccess + DynamicProtobuf codec

import Foundation
import DatabaseEngine

public struct DataCommands {

    private let dataAccess: CatalogDataAccess
    private let output: OutputFormatter

    public init(dataAccess: CatalogDataAccess, output: OutputFormatter) {
        self.dataAccess = dataAccess
        self.output = output
    }

    // MARK: - Insert

    /// Usage: insert <TypeName> <json> [--partition field=value ...]
    public func insert(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: insert <TypeName> <json> [--partition field=value ...]")
        }

        let typeName = args[0]
        let partitionValues = Self.parsePartitionValues(from: args)
        let cleanArgs = Self.removePartitionArgs(from: Array(args.dropFirst()))
        let jsonString = cleanArgs.joined(separator: " ")
        let dict = try JSONParser.parse(jsonString)

        output.info("WARNING: CLI writes do NOT update indexes.")
        try await dataAccess.insert(typeName: typeName, dict: dict, partitionValues: partitionValues)
        output.success("Inserted record into '\(typeName)'")
    }

    // MARK: - Get

    /// Usage: get <TypeName> <id> [--partition field=value ...]
    public func get(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: get <TypeName> <id> [--partition field=value ...]")
        }

        let typeName = args[0]
        let id = args[1]
        let partitionValues = Self.parsePartitionValues(from: args)

        guard let dict = try await dataAccess.get(typeName: typeName, id: id, partitionValues: partitionValues) else {
            throw CLIError.recordNotFound(type: typeName, id: id)
        }

        let jsonString = try JSONParser.stringify(dict)
        output.line(jsonString)
    }

    // MARK: - Update

    /// Usage: update <TypeName> <id> <json> [--partition field=value ...]
    public func update(args: [String]) async throws {
        guard args.count >= 3 else {
            throw CLIError.invalidArguments("Usage: update <TypeName> <id> <json> [--partition field=value ...]")
        }

        let typeName = args[0]
        let id = args[1]
        let partitionValues = Self.parsePartitionValues(from: args)
        let cleanArgs = Self.removePartitionArgs(from: Array(args.dropFirst(2)))
        let jsonString = cleanArgs.joined(separator: " ")

        // Fetch existing
        guard var existingDict = try await dataAccess.get(typeName: typeName, id: id, partitionValues: partitionValues) else {
            throw CLIError.recordNotFound(type: typeName, id: id)
        }

        // Merge update fields
        let updateDict = try JSONParser.parse(jsonString)
        for (key, value) in updateDict where key != "id" {
            existingDict[key] = value
        }
        existingDict["id"] = id

        output.info("WARNING: CLI writes do NOT update indexes.")
        try await dataAccess.insert(typeName: typeName, dict: existingDict, partitionValues: partitionValues)
        output.success("Updated record '\(id)' in '\(typeName)'")
    }

    // MARK: - Delete

    /// Usage: delete <TypeName> <id> [--partition field=value ...]
    public func delete(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: delete <TypeName> <id> [--partition field=value ...]")
        }

        let typeName = args[0]
        let id = args[1]
        let partitionValues = Self.parsePartitionValues(from: args)

        try await dataAccess.delete(typeName: typeName, id: id, partitionValues: partitionValues)
        output.success("Deleted record '\(id)' from '\(typeName)'")
    }

    // MARK: - Partition Parsing

    /// Parse "--partition key=value" arguments into a dictionary
    static func parsePartitionValues(from tokens: [String]) -> [String: String] {
        var values: [String: String] = [:]
        var i = 0
        while i < tokens.count {
            if tokens[i] == "--partition", i + 1 < tokens.count {
                let parts = tokens[i + 1].split(separator: "=", maxSplits: 1)
                if parts.count == 2 {
                    values[String(parts[0])] = String(parts[1])
                }
                i += 2
            } else {
                i += 1
            }
        }
        return values
    }

    /// Remove "--partition key=value" pairs from args
    static func removePartitionArgs(from tokens: [String]) -> [String] {
        var result: [String] = []
        var i = 0
        while i < tokens.count {
            if tokens[i] == "--partition", i + 1 < tokens.count {
                i += 2
            } else {
                result.append(tokens[i])
                i += 1
            }
        }
        return result
    }
}

// MARK: - Help

extension DataCommands {
    static var helpText: String {
        """
        Data Commands:
          insert <TypeName> <json>     Insert a record
          get <TypeName> <id>          Get a record by ID
          update <TypeName> <id> <json> Update a record (merge fields)
          delete <TypeName> <id>       Delete a record

        Options:
          --partition field=value      Specify partition value for dynamic directory types
                                       (can be repeated for multiple fields)

        Note: CLI writes do NOT update indexes.

        Examples:
          insert User {"name": "Alice", "age": 30, "id": "user-001"}
          get User user-001
          get Order order-001 --partition tenantId=tenant_123
          update User user-001 {"age": 31}
          delete User user-001
        """
    }
}

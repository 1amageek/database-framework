// REPL.swift
// DatabaseCLI - Read-Eval-Print Loop for interactive database access
//
// Main loop handling user input, command parsing, and execution.

import Foundation
import Core

/// Read-Eval-Print Loop for the CLI
public final class REPL: Sendable {

    // MARK: - Properties

    private let cliContext: CLIContext
    private let router: CommandRouter
    private let output: Output

    // MARK: - Initialization

    /// Create REPL with context
    ///
    /// - Parameter cliContext: The CLI context
    public init(cliContext: CLIContext) {
        self.cliContext = cliContext
        self.router = CommandRouter()
        self.output = Output()
    }

    // MARK: - Run Loop

    /// Run the REPL
    public func run() async {
        output.welcome()

        while true {
            output.prompt()

            guard let line = readLine() else {
                // EOF - exit gracefully
                output.newline()
                output.goodbye()
                break
            }

            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.isEmpty {
                continue
            }

            let command = router.parse(trimmed)

            if case .quit = command {
                output.goodbye()
                break
            }

            do {
                try await execute(command)
            } catch {
                output.error(error.localizedDescription)
            }
        }
    }

    // MARK: - Command Execution

    private func execute(_ command: Command) async throws {
        switch command {
        // Schema commands
        case .schemaList:
            executeSchemaList()
        case .schemaShow(let typeName):
            executeSchemaShow(typeName)

        // Data commands
        case .get(let typeName, let id):
            try await executeGet(typeName: typeName, id: id)
        case .query(let typeName, let whereClause, let limit):
            try await executeQuery(typeName: typeName, whereClause: whereClause, limit: limit)
        case .count(let typeName, let whereClause):
            try await executeCount(typeName: typeName, whereClause: whereClause)
        case .insert(let typeName, let json):
            try await executeInsert(typeName: typeName, json: json)
        case .delete(let typeName, let id):
            try await executeDelete(typeName: typeName, id: id)

        // Version commands
        case .versions(let typeName, let id, let limit):
            try await executeVersions(typeName: typeName, id: id, limit: limit)
        case .diff(let typeName, let id):
            try await executeDiff(typeName: typeName, id: id)

        // Index commands
        case .indexList:
            executeIndexList()
        case .indexStatus(let name):
            try await executeIndexStatus(name: name)
        case .indexBuild(let name):
            try await executeIndexBuild(name: name)
        case .indexScrub(let name):
            try await executeIndexScrub(name: name)

        // Raw commands
        case .rawGet(let key):
            try await executeRawGet(key: key)
        case .rawRange(let prefix, let limit):
            try await executeRawRange(prefix: prefix, limit: limit)

        // Other commands
        case .help(let cmd):
            executeHelp(command: cmd)
        case .quit:
            break // Handled above
        case .unknown(let input):
            output.error("Unknown command: \(input)")
            output.print("Type 'help' for available commands.")
        case .empty:
            break
        }
    }

    // MARK: - Schema Commands

    private func executeSchemaList() {
        output.header("Entities:")
        for entity in cliContext.schema.entities {
            let indexCount = entity.indexDescriptors.count
            let indexNames = entity.indexDescriptors.map(\.name).joined(separator: ", ")
            if indexCount > 0 {
                output.print("  \(entity.name) (\(indexCount) indexes: \(indexNames))")
            } else {
                output.print("  \(entity.name)")
            }
        }
    }

    private func executeSchemaShow(_ typeName: String) {
        guard let entity = cliContext.entity(named: typeName) else {
            output.error("Unknown type: \(typeName)")
            return
        }

        output.header("Entity: \(entity.name)")
        output.newline()

        output.header("Fields:")
        for field in entity.allFields {
            output.print("  - \(field)")
        }
        output.newline()

        if !entity.indexDescriptors.isEmpty {
            output.header("Indexes:")
            for idx in entity.indexDescriptors {
                let fields = idx.keyPaths.map { "\($0)" }.joined(separator: ", ")
                output.print("  - \(idx.name) [\(fields)]")
            }
        }
    }

    // MARK: - Data Commands

    private func executeGet(typeName: String, id: String) async throws {
        guard let json = try await cliContext.fetchItem(typeName: typeName, id: id) else {
            output.warning("\(typeName) with id '\(id)' not found.")
            return
        }
        output.json(json)
    }

    private func executeQuery(typeName: String, whereClause: String?, limit: Int?) async throws {
        // For now, simple query without where clause parsing
        // Where clause parsing would require a proper expression parser
        if whereClause != nil {
            output.warning("Where clause parsing is not yet implemented. Fetching all items.")
        }

        let items = try await cliContext.fetchItems(typeName: typeName, limit: limit ?? 100)

        if items.isEmpty {
            output.print("No records found.")
        } else {
            output.print("Found \(items.count) record(s):")
            for json in items {
                output.json(json)
                output.newline()
            }
        }
    }

    private func executeCount(typeName: String, whereClause: String?) async throws {
        if whereClause != nil {
            output.warning("Where clause not supported for count. Counting all items.")
        }

        let count = try await cliContext.countItems(typeName: typeName)
        output.print("Total: \(count.formatted()) records")
    }

    private func executeInsert(typeName: String, json: String) async throws {
        // Insert requires type-specific handling
        // For now, show a message explaining the limitation
        output.warning("Insert from CLI is not yet implemented.")
        output.print("To insert data, use the Swift API directly:")
        output.print("  context.insert(MyModel(...))")
        output.print("  try await context.save()")
    }

    private func executeDelete(typeName: String, id: String) async throws {
        let deleted = try await cliContext.deleteItem(typeName: typeName, id: id)
        if deleted {
            output.success("Deleted \(typeName) with id '\(id)'.")
        } else {
            output.warning("\(typeName) with id '\(id)' not found.")
        }
    }

    // MARK: - Version Commands

    private func executeVersions(typeName: String, id: String, limit: Int?) async throws {
        // Version history requires VersionIndex module
        output.warning("Version history is not yet implemented in CLI.")
        output.print("To view versions, import VersionIndex and use:")
        output.print("  let history = try await context.versions(\(typeName).self)")
        output.print("      .forItem(\"\(id)\")")
        output.print("      .execute()")
    }

    private func executeDiff(typeName: String, id: String) async throws {
        output.warning("Diff is not yet implemented in CLI.")
    }

    // MARK: - Index Commands

    private func executeIndexList() {
        let descriptors = cliContext.allIndexDescriptors

        if descriptors.isEmpty {
            output.print("No indexes defined.")
            return
        }

        output.header("Indexes:")

        var rows: [[String]] = []
        for desc in descriptors {
            let fields = desc.keyPaths.map { "\($0)" }.joined(separator: ", ")
            rows.append([desc.name, desc.kindIdentifier, "[\(fields)]"])
        }

        output.table(
            headers: ["Name", "Type", "Fields"],
            rows: rows
        )
    }

    private func executeIndexStatus(name: String) async throws {
        // Index status requires IndexStateManager
        output.info("Checking status for index: \(name)")

        // Find the index descriptor
        guard let _ = cliContext.allIndexDescriptors.first(where: { $0.name == name }) else {
            output.error("Index '\(name)' not found.")
            return
        }

        // Note: Full implementation would check IndexStateManager
        output.print("Status: readable (assumed)")
        output.muted("(Full index state checking not yet implemented)")
    }

    private func executeIndexBuild(name: String) async throws {
        output.warning("Index building from CLI is not yet implemented.")
        output.print("To build indexes, use the OnlineIndexer API:")
        output.print("  let indexer = OnlineIndexer<MyType>(index: ..., container: container)")
        output.print("  try await indexer.build()")
    }

    private func executeIndexScrub(name: String) async throws {
        output.warning("Index scrubbing from CLI is not yet implemented.")
        output.print("To scrub indexes, use the OnlineIndexScrubber API.")
    }

    // MARK: - Raw Commands

    private func executeRawGet(key: String) async throws {
        output.warning("Raw key access from CLI is not yet implemented.")
        output.print("This would require direct FDB transaction access.")
    }

    private func executeRawRange(prefix: String, limit: Int) async throws {
        output.warning("Raw range scan from CLI is not yet implemented.")
    }

    // MARK: - Help

    private func executeHelp(command: String?) {
        if let cmd = command {
            output.helpCommand(cmd)
        } else {
            output.helpAll()
        }
    }
}

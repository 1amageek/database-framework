/// DatabaseREPL - Interactive database shell powered by schema entities
///
/// Connects to a storage engine, loads schema entries, and provides a
/// read-only catalog inspection REPL using the canonical database record codec.
///
/// **Standalone mode** (no compiled types needed):
/// ```swift
/// import DatabaseCLI
/// import FoundationDB
/// import DatabaseEngine
///
/// let database = try FDBClient.openDatabase()
/// let repl = try await DatabaseREPL(database: database)
/// try await repl.run()
/// ```
///
/// **Container mode**:
/// ```swift
/// let container = try await DBContainer(for: schema)
/// let repl = try await DatabaseREPL(container: container)
/// try await repl.run()
/// ```

import Foundation
import StorageKit
import DatabaseEngine
import Core

public final class DatabaseREPL: Sendable {

    private let dataAccess: CatalogDataAccess
    private let entities: [Schema.Entity]

    /// Initialize from a standalone storage engine.
    public init(database: any StorageEngine) async throws {
        let dataAccess = try await CatalogDataAccess.open(database: database)
        self.entities = dataAccess.allEntities
        self.dataAccess = dataAccess
    }

    /// Initialize from DBContainer (embedded mode)
    ///
    /// Loads entities from the SchemaRegistry persisted by DBContainer.
    public init(container: DBContainer) async throws {
        let registry = SchemaRegistry(database: container.engine)
        self.entities = try await registry.loadAll()
        self.dataAccess = try CatalogDataAccess(
            database: container.engine,
            entities: self.entities
        )
    }

    /// Start the interactive REPL loop
    public func run() async throws {
        let output = OutputFormatter()
        let typeNames = entities.map(\.name).sorted()

        output.info("database - FoundationDB Interactive CLI")
        if typeNames.isEmpty {
            output.info("No types found in catalog. Use an app with @Persistable types to populate the catalog.")
        } else {
            output.info("Types: \(typeNames.joined(separator: ", "))")
        }
        output.info("Type 'help' for available commands, 'quit' to exit.")
        output.info("")

        while true {
            print("database> ", terminator: "")
            fflush(stdout)

            guard let line = readLine()?.trimmingCharacters(in: .whitespaces) else {
                break
            }

            guard !line.isEmpty else { continue }

            if line.lowercased() == "quit" || line.lowercased() == "exit" {
                output.info("Goodbye!")
                break
            }

            do {
                try await CommandRouter.execute(
                    line,
                    dataAccess: dataAccess,
                    entities: entities,
                    output: output
                )
            } catch {
                output.error("\(error)")
            }
        }
    }
}

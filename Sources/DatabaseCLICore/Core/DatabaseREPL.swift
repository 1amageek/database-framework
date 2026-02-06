/// DatabaseREPL - Interactive database shell powered by schema entities
///
/// Connects to FDB, loads Schema.Entity entries, and provides a REPL
/// for data operations using dynamic Protobuf codec.
///
/// **Standalone mode** (no compiled types needed):
/// ```swift
/// import DatabaseCLI
/// import FoundationDB
/// import DatabaseEngine
///
/// let database = try FDBClient.openDatabase()
/// let registry = SchemaRegistry(database: database)
/// let entities = try await registry.loadAll()
/// let repl = DatabaseREPL(database: database, entities: entities)
/// try await repl.run()
/// ```
///
/// **Embedded mode** (with FDBContainer for backward compatibility):
/// ```swift
/// let container = try await FDBContainer(for: schema)
/// let repl = try await DatabaseREPL(container: container)
/// try await repl.run()
/// ```

import Foundation
import FoundationDB
import DatabaseEngine
import Core

public final class DatabaseREPL: Sendable {

    private let dataAccess: CatalogDataAccess
    private let entities: [Schema.Entity]

    /// Initialize with database and pre-loaded entities (standalone mode)
    public init(database: any DatabaseProtocol, entities: [Schema.Entity]) {
        self.entities = entities
        self.dataAccess = CatalogDataAccess(database: database, entities: entities)
    }

    /// Initialize from FDBContainer (embedded mode)
    ///
    /// Loads entities from the SchemaRegistry persisted by FDBContainer.
    public init(container: FDBContainer) async throws {
        let registry = SchemaRegistry(database: container.database)
        self.entities = try await registry.loadAll()
        self.dataAccess = CatalogDataAccess(database: container.database, entities: self.entities)
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

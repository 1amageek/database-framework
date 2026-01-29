/// DatabaseREPL - Interactive database shell powered by schema catalog
///
/// Connects to FDB, loads TypeCatalog entries, and provides a REPL
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
/// let catalogs = try await registry.loadAll()
/// let repl = DatabaseREPL(database: database, catalogs: catalogs)
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

public final class DatabaseREPL: Sendable {

    private let dataAccess: CatalogDataAccess
    private let catalogs: [TypeCatalog]

    /// Initialize with database and pre-loaded catalogs (standalone mode)
    public init(database: any DatabaseProtocol, catalogs: [TypeCatalog]) {
        self.catalogs = catalogs
        self.dataAccess = CatalogDataAccess(database: database, catalogs: catalogs)
    }

    /// Initialize from FDBContainer (embedded mode)
    ///
    /// Loads catalogs from the SchemaRegistry persisted by FDBContainer.
    public init(container: FDBContainer) async throws {
        let registry = SchemaRegistry(database: container.database)
        self.catalogs = try await registry.loadAll()
        self.dataAccess = CatalogDataAccess(database: container.database, catalogs: self.catalogs)
    }

    /// Start the interactive REPL loop
    public func run() async throws {
        let output = OutputFormatter()
        let typeNames = catalogs.map(\.typeName).sorted()

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
                    catalogs: catalogs,
                    output: output
                )
            } catch {
                output.error("\(error)")
            }
        }
    }
}

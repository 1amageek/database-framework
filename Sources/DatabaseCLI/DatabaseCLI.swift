// DatabaseCLI.swift
// DatabaseCLI - Entry point for the interactive CLI
//
// Provides the main entry point and public API for running the CLI.

import Foundation
import DatabaseEngine
import Core
import FoundationDB

/// Database CLI public interface
///
/// The CLI is designed to be embedded in user applications. Users create
/// their own executable that imports their models and starts the CLI.
///
/// **Usage**:
/// ```swift
/// // In your MyModels module
/// import Core
///
/// @Persistable
/// struct User {
///     #Directory<User>("app", "users")
///     var id: String = UUID().uuidString
///     var name: String = ""
///     var email: String = ""
/// }
///
/// // In your CLI executable main.swift
/// import DatabaseCLI
/// import MyModels
///
/// @main
/// struct MyCLI {
///     static func main() async throws {
///         let schema = Schema([User.self, Order.self])
///         try await DatabaseCLI.run(schema: schema)
///     }
/// }
/// ```
public enum DatabaseCLI {

    /// Run the interactive CLI
    ///
    /// This starts an interactive REPL that allows users to query and
    /// manipulate the database.
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - clusterFile: Optional path to FDB cluster file
    public static func run(schema: Schema, clusterFile: String? = nil) async throws {
        let output = Output()

        do {
            let context = try CLIContext(schema: schema, clusterFile: clusterFile)
            output.info("Connected to FoundationDB cluster")
            output.newline()

            let repl = REPL(cliContext: context)
            await repl.run()
        } catch {
            output.error("Failed to connect: \(error.localizedDescription)")
            throw error
        }
    }

    /// Run the CLI with an existing container
    ///
    /// Use this when you already have an FDBContainer configured.
    ///
    /// - Parameter container: The FDB container
    public static func run(container: FDBContainer) async {
        let output = Output()
        output.info("Connected to FoundationDB cluster")
        output.newline()

        let context = CLIContext(container: container)
        let repl = REPL(cliContext: context)
        await repl.run()
    }
}

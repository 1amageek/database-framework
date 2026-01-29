import Foundation
import FoundationDB
import DatabaseEngine
import DatabaseCLICore

@main
struct DatabaseCLIApp {
    static func main() async throws {
        try await FDBClient.initialize()
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)
        let catalogs = try await registry.loadAll()
        let repl = DatabaseREPL(database: database, catalogs: catalogs)
        try await repl.run()
    }
}

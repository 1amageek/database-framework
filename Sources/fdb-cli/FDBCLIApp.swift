import Foundation
import FoundationDB

@main
struct FDBCLIApp {
    static func main() async {
        do {
            // Initialize FDB
            try await FDBClient.initialize()

            // Connect to database
            let database = try FDBClient.openDatabase()

            // Create and run CLI
            let cli = CLIMain(database: database)
            await cli.run()

        } catch {
            print("ERROR: Failed to connect to FoundationDB: \(error)")
            print("")
            print("Make sure FoundationDB is installed and running.")
            print("  macOS: brew services start foundationdb")
            print("  Linux: sudo service foundationdb start")
            Foundation.exit(1)
        }
    }
}

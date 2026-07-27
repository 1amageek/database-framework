#if FOUNDATION_DB
import Foundation
import ArgumentParser
import StorageKit
import DatabaseEngine
import DatabaseCLICore

@main
struct DatabaseCLI: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "database",
        abstract: "FoundationDB Interactive CLI",
        discussion: """
        Run without arguments to enter REPL mode.
        Use subcommands for one-shot operations.
        """,
        subcommands: [
            Init.self,
            Status.self,
            Schema.self,
            Raw.self,
        ]
    )

    /// No arguments starts interactive REPL mode.
    mutating func run() async throws {
        let (database, _) = try await ClusterConnection.openDatabase()
        let repl = try await DatabaseREPL(database: database)
        try await repl.run()
    }
}

// MARK: - Cluster Management Subcommands

extension DatabaseCLI {
    struct Init: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "init",
            abstract: "Initialize a local database directory"
        )

        @Option(name: .customLong("port"), help: "Port for fdbserver (default: 4690)")
        var port: UInt16 = LocalCluster.defaultPort

        mutating func run() throws {
            let basePath = FileManager.default.currentDirectoryPath
            let clusterFile = try LocalCluster.create(at: basePath, port: port)
            let dbDir = (basePath as NSString).appendingPathComponent(LocalCluster.directoryName)

            print("Initialized database at \(dbDir)")
            print("  Cluster file: \(clusterFile)")
            print("  Port: \(port)")
        }
    }

    struct Status: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "status",
            abstract: "Show local database status"
        )

        mutating func run() throws {
            let basePath = FileManager.default.currentDirectoryPath
            let dbDir = (basePath as NSString).appendingPathComponent(LocalCluster.directoryName)
            let clusterFile = (dbDir as NSString).appendingPathComponent("fdb.cluster")

            guard FileManager.default.fileExists(atPath: dbDir) else {
                print("No local database found. Run 'database init' to create one.")
                return
            }

            print("Database directory: \(dbDir)")
            print("Cluster file: \(clusterFile)")

            if let port = LocalCluster.parsePort(fromClusterFile: clusterFile) {
                print("Port: \(port)")
            }
        }
    }
}

// MARK: - Schema Subcommand

extension DatabaseCLI {
    struct Schema: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "schema",
            abstract: "Schema management commands",
            subcommands: [List.self, Show.self]
        )

        struct List: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "list",
                abstract: "List all types"
            )

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let dataAccess = try await CatalogDataAccess.open(
                    database: database
                )
                let output = OutputFormatter()
                let cmd = SchemaInfoCommands(
                    entities: dataAccess.allEntities,
                    output: output
                )
                try cmd.execute(["list"])
            }
        }

        struct Show: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "show",
                abstract: "Show type fields, types, and indexes"
            )

            @Argument(help: "Type name to show")
            var typeName: String

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let dataAccess = try await CatalogDataAccess.open(
                    database: database
                )
                let output = OutputFormatter()
                let cmd = SchemaInfoCommands(
                    entities: dataAccess.allEntities,
                    output: output
                )
                try cmd.execute(["show", typeName])
            }
        }

    }
}

// MARK: - Raw FDB Access Subcommands

extension DatabaseCLI {
    struct Raw: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "raw",
            abstract: "Bounded raw FoundationDB inspection",
            subcommands: [RawGet.self, RawRange.self]
        )

        struct RawGet: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "get",
                abstract: "Get raw key"
            )

            @Argument(help: "Key")
            var key: String

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = RawCommands(database: database, output: output)
                try await cmd.execute("get", args: [key])
            }
        }

        struct RawRange: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "range",
                abstract: "Scan keys by prefix"
            )

            @Argument(help: "Key prefix")
            var prefix: String

            @Option(name: .customLong("limit"), help: "Result limit")
            var limit: Int?

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = RawCommands(database: database, output: output)

                var args = [prefix]
                if let limit = limit {
                    args.append("limit")
                    args.append(String(limit))
                }

                try await cmd.execute("range", args: args)
            }
        }
    }
}
#else

@main
struct DatabaseCLI: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "database",
        abstract: "Database CLI (requires FoundationDB trait)"
    )

    mutating func run() throws {
        print("Error: This CLI requires FoundationDB. Build with default traits or --traits FoundationDB.")
    }
}
#endif

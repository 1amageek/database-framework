import Foundation
import ArgumentParser
import FoundationDB
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
            Get.self,
            Insert.self,
            Update.self,
            Delete.self,
            Find.self,
            Graph.self,
            SPARQL.self,
            Clear.self,
            Raw.self,
        ]
    )

    /// 引数なし → REPLモード
    mutating func run() async throws {
        let (database, _) = try await ClusterConnection.openDatabase()
        let registry = SchemaRegistry(database: database)
        let entities = try await registry.loadAll()
        let repl = DatabaseREPL(database: database, entities: entities)
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
            subcommands: [List.self, Show.self, Apply.self, Export.self, Validate.self, Drop.self]
        )

        struct List: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "list",
                abstract: "List all types"
            )

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let registry = SchemaRegistry(database: database)
                let entities = try await registry.loadAll()
                let output = OutputFormatter()
                let cmd = SchemaInfoCommands(entities: entities, output: output)
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
                let registry = SchemaRegistry(database: database)
                let entities = try await registry.loadAll()
                let output = OutputFormatter()
                let cmd = SchemaInfoCommands(entities: entities, output: output)
                try cmd.execute(["show", typeName])
            }
        }

        struct Apply: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "apply",
                abstract: "Apply schema from YAML file or directory"
            )

            @Argument(help: "Path to YAML file or directory")
            var path: String

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = SchemaDefinitionCommands(database: database, output: output)
                try await cmd.apply(fileOrDirectory: path)
            }
        }

        struct Export: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "export",
                abstract: "Export schema to YAML"
            )

            @Argument(help: "Type name to export (or --all)")
            var typeName: String?

            @Option(name: .customLong("output"), help: "Output file path")
            var outputPath: String?

            @Flag(name: .customLong("all"), help: "Export all schemas")
            var exportAll: Bool = false

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = SchemaDefinitionCommands(database: database, output: output)

                if exportAll {
                    try await cmd.exportAll(outputDirectory: outputPath)
                } else if let typeName = typeName {
                    try await cmd.export(typeName: typeName, outputPath: outputPath)
                } else {
                    throw ValidationError("Specify a type name or use --all")
                }
            }
        }

        struct Validate: ParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "validate",
                abstract: "Validate YAML schema file"
            )

            @Argument(help: "Path to YAML schema file")
            var filePath: String

            mutating func run() throws {
                let output = OutputFormatter()
                // Note: validation doesn't need FDB connection
                try SchemaDefinitionCommands.validate(filePath: filePath, output: output)
            }
        }

        struct Drop: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "drop",
                abstract: "Drop schema"
            )

            @Argument(help: "Type name to drop (or use --all)")
            var typeName: String?

            @Flag(name: .customLong("all"), help: "Drop all schemas")
            var dropAll: Bool = false

            @Flag(name: .customLong("force"), help: "Skip confirmation")
            var force: Bool = false

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = SchemaDefinitionCommands(database: database, output: output)

                if dropAll {
                    try await cmd.dropAll(force: force)
                } else if let typeName = typeName {
                    try await cmd.drop(typeName: typeName, force: force)
                } else {
                    throw ValidationError("Specify a type name or use --all")
                }
            }
        }
    }
}

// MARK: - Data Operation Subcommands

extension DatabaseCLI {
    struct Get: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "get",
            abstract: "Get a record by ID"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "Record ID")
        var id: String

        @Option(name: .customLong("partition"), help: "Partition key-value pairs (format: key=value)")
        var partitions: [String] = []

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = DataCommands(dataAccess: dataAccess, output: output)

            var args = [typeName, id]
            for partition in partitions {
                args.append("--partition")
                args.append(partition)
            }
            try await cmd.get(args: args)
        }
    }

    struct Insert: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "insert",
            abstract: "Insert a record"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "JSON data")
        var json: String

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.insert(args: [typeName, json])
        }
    }

    struct Update: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "update",
            abstract: "Update a record"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "Record ID")
        var id: String

        @Argument(help: "JSON data")
        var json: String

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.update(args: [typeName, id, json])
        }
    }

    struct Delete: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "delete",
            abstract: "Delete a record"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "Record ID")
        var id: String

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.delete(args: [typeName, id])
        }
    }
}

// MARK: - Query Subcommands

extension DatabaseCLI {
    struct Find: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "find",
            abstract: "Query records with filters and sorting"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Option(name: .customLong("where"), help: "Filter expression (field op value)")
        var whereConditions: [String] = []

        @Option(name: .customLong("sort"), help: "Sort field [desc]")
        var sortFields: [String] = []

        @Option(name: .customLong("limit"), help: "Result limit")
        var limit: Int?

        @Option(name: .customLong("partition"), help: "Partition key-value pairs")
        var partitions: [String] = []

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = FindCommands(dataAccess: dataAccess, output: output)

            var args = [typeName]
            for condition in whereConditions {
                args.append("--where")
                args.append(condition)
            }
            for sort in sortFields {
                args.append("--sort")
                args.append(sort)
            }
            if let limit = limit {
                args.append("--limit")
                args.append(String(limit))
            }
            for partition in partitions {
                args.append("--partition")
                args.append(partition)
            }

            try await cmd.execute(args)
        }
    }
}

// MARK: - Graph Subcommands

extension DatabaseCLI {
    struct Graph: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "graph",
            abstract: "Query graph triples"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "Graph query parameters (from=value edge=value to=value)")
        var parameters: [String] = []

        @Option(name: .customLong("limit"), help: "Result limit")
        var limit: Int?

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = GraphCommands(dataAccess: dataAccess, output: output)

            var args = [typeName] + parameters
            if let limit = limit {
                args.append("--limit")
                args.append(String(limit))
            }

            try await cmd.execute(args)
        }
    }

    struct SPARQL: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "sparql",
            abstract: "Execute SPARQL query"
        )

        @Argument(help: "Type name")
        var typeName: String

        @Argument(help: "SPARQL query string")
        var query: String

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = GraphCommands(dataAccess: dataAccess, output: output)
            try await cmd.executeSPARQL([typeName, query])
        }
    }
}

// MARK: - Destructive Subcommands

extension DatabaseCLI {
    struct Clear: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "clear",
            abstract: "Clear all data for a type"
        )

        @Argument(help: "Type name (or use --all)")
        var typeName: String?

        @Flag(name: .customLong("all"), help: "Clear all types")
        var clearAll: Bool = false

        @Flag(name: .customLong("force"), help: "Skip confirmation")
        var force: Bool = false

        mutating func run() async throws {
            let (database, _) = try await ClusterConnection.openDatabase()
            let registry = SchemaRegistry(database: database)
            let entities = try await registry.loadAll()
            let dataAccess = CatalogDataAccess(database: database, entities: entities)
            let output = OutputFormatter()
            let cmd = ClearCommand(dataAccess: dataAccess, output: output)

            var args: [String] = []
            if clearAll {
                args.append("--all")
            } else if let typeName = typeName {
                args.append(typeName)
            }
            if force {
                args.append("--force")
            }

            try await cmd.execute(args)
        }
    }
}

// MARK: - Raw FDB Access Subcommands

extension DatabaseCLI {
    struct Raw: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "raw",
            abstract: "Raw FoundationDB key-value operations",
            subcommands: [RawGet.self, RawSet.self, RawDelete.self, RawRange.self]
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

        struct RawSet: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "set",
                abstract: "Set raw key-value"
            )

            @Argument(help: "Key")
            var key: String

            @Argument(help: "Value")
            var value: String

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = RawCommands(database: database, output: output)
                try await cmd.execute("set", args: [key, value])
            }
        }

        struct RawDelete: AsyncParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "delete",
                abstract: "Delete raw key"
            )

            @Argument(help: "Key")
            var key: String

            mutating func run() async throws {
                let (database, _) = try await ClusterConnection.openDatabase()
                let output = OutputFormatter()
                let cmd = RawCommands(database: database, output: output)
                try await cmd.execute("delete", args: [key])
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

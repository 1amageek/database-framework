import Foundation
import FoundationDB

@main
struct CLIMain {
    static func main() async {
        let args = Array(CommandLine.arguments.dropFirst())

        if let command = args.first {
            switch command {
            case "init":
                let port = parsePort(from: args) ?? LocalCluster.defaultPort
                do {
                    try InitCommand.run(port: port)
                } catch {
                    print("ERROR: \(error)")
                    Foundation.exit(1)
                }
                return

            case "start":
                do {
                    try StartCommand.run()
                } catch {
                    print("ERROR: \(error)")
                    Foundation.exit(1)
                }
                return

            case "stop":
                do {
                    let cwd = FileManager.default.currentDirectoryPath
                    try LocalCluster.stopServer(at: cwd)
                    print("Server stopped.")
                } catch {
                    print("ERROR: \(error)")
                    Foundation.exit(1)
                }
                return

            case "status":
                let cwd = FileManager.default.currentDirectoryPath
                if let clusterFile = LocalCluster.findClusterFile(from: cwd) {
                    let dbDir = (clusterFile as NSString).deletingLastPathComponent
                    let port = LocalCluster.parsePort(fromClusterFile: clusterFile)

                    print("Cluster file: \(clusterFile)")
                    if let port {
                        print("Port:         \(port)")
                    }

                    if let pid = LocalCluster.readPID(fromDBDir: dbDir) {
                        if LocalCluster.isProcessAlive(pid) {
                            print("Status:       running (PID: \(pid))")
                        } else {
                            print("Status:       stopped (stale PID file for \(pid))")
                            LocalCluster.removePIDFile(fromDBDir: dbDir)
                        }
                    } else {
                        print("Status:       stopped")
                    }
                } else {
                    print("No .database directory found.")
                    print("Run 'database init' to create one, or use system FoundationDB.")
                }
                return

            case "--help", "-h":
                printUsage()
                return

            default:
                break
            }
        }

        do {
            try await FDBClient.initialize()

            let cwd = FileManager.default.currentDirectoryPath
            let clusterFile = LocalCluster.findClusterFile(from: cwd)

            let database: any DatabaseProtocol
            if let clusterFile {
                database = try FDBClient.openDatabase(clusterFilePath: clusterFile)
            } else {
                database = try FDBClient.openDatabase()
            }

            let storage = SchemaStorage(database: database)
            let output = OutputFormatter()

            output.info("database - FoundationDB Interactive CLI")
            if let clusterFile {
                output.info("Connected to: \(clusterFile)")
            } else {
                output.info("Connected to: system default")
            }
            output.info("Type 'help' for available commands, 'quit' to exit.")
            output.info("")

            await repl(database: database, storage: storage, output: output)

        } catch {
            print("ERROR: Failed to connect to FoundationDB: \(error)")
            print("")
            print("Make sure FoundationDB is installed and running.")
            print("  Local:  database init")
            print("  macOS:  brew services start foundationdb")
            print("  Linux:  sudo service foundationdb start")
            Foundation.exit(1)
        }
    }

    // MARK: - REPL

    private static func repl(
        database: any DatabaseProtocol,
        storage: SchemaStorage,
        output: OutputFormatter
    ) async {
        while true {
            print("database> ", terminator: "")
            fflush(stdout)

            guard let line = readLine()?.trimmingCharacters(in: .whitespaces) else {
                break
            }

            guard !line.isEmpty else {
                continue
            }

            if line.lowercased() == "quit" || line.lowercased() == "exit" {
                output.info("Goodbye!")
                break
            }

            do {
                try await executeCommand(line, database: database, storage: storage, output: output)
            } catch {
                output.error("\(error)")
            }
        }
    }

    // MARK: - Command Execution

    static func executeCommand(
        _ line: String,
        database: any DatabaseProtocol,
        storage: SchemaStorage,
        output: OutputFormatter
    ) async throws {
        let tokens = tokenize(line)
        guard let command = tokens.first?.lowercased() else {
            return
        }

        let args = Array(tokens.dropFirst())

        switch command {
        case "help":
            printHelp(topic: args.first, output: output)

        case "admin":
            let adminCommands = AdminCommands(storage: storage, output: output)
            try await adminCommands.execute(args)

        case "schema":
            let adminCommands = AdminCommands(storage: storage, output: output)
            try await adminCommands.execute(["schema"] + args)

        case "insert":
            let dataCommands = DataCommands(storage: storage, output: output)
            try await dataCommands.insert(args: args)

        case "get":
            let dataCommands = DataCommands(storage: storage, output: output)
            try await dataCommands.get(args: args)

        case "update":
            let dataCommands = DataCommands(storage: storage, output: output)
            try await dataCommands.update(args: args)

        case "delete":
            let dataCommands = DataCommands(storage: storage, output: output)
            try await dataCommands.delete(args: args)

        case "find":
            let findCommands = FindCommands(storage: storage, output: output)
            try await findCommands.execute(args)

        case "query":
            let findCommands = FindCommands(storage: storage, output: output)
            if !args.isEmpty {
                try await findCommands.execute(args)
            } else {
                throw CLIError.invalidArguments("Usage: find <Schema> [where ...] [options]")
            }

        case "graph":
            let graphCommands = GraphCommands(storage: storage, output: output)
            try await graphCommands.execute(args)

        case "history":
            let historyCommands = HistoryCommands(storage: storage, output: output)
            try await historyCommands.execute(args)

        case "raw":
            let rawCommands = RawCommands(database: database, output: output)
            guard let subCommand = args.first else {
                throw CLIError.invalidArguments("Usage: raw <get|set|delete|range> ...")
            }
            try await rawCommands.execute(subCommand, args: Array(args.dropFirst()))

        default:
            throw CLIError.unknownCommand(command)
        }
    }

    // MARK: - Help

    private static func printHelp(topic: String?, output: OutputFormatter) {
        if let topic = topic?.lowercased() {
            switch topic {
            case "admin", "schema":
                output.info(AdminCommands.helpText)
            case "find":
                output.info(FindCommands.helpText)
            case "graph":
                output.info(GraphCommands.helpText)
            case "history":
                output.info(HistoryCommands.helpText)
            case "data", "insert", "get", "update", "delete":
                output.info(DataCommands.helpText)
            case "raw":
                output.info(RawCommands.helpText)
            default:
                output.info("Unknown help topic: '\(topic)'")
                printGeneralHelp(output: output)
            }
        } else {
            printGeneralHelp(output: output)
        }
    }

    private static func printGeneralHelp(output: OutputFormatter) {
        output.info("""
        database - FoundationDB Interactive CLI with Full Index Support

        Schema & Index Management:
          admin schema define <Name> <fields...>  Define a schema with indexes
          admin schema list                        List all schemas
          admin schema show <Name>                 Show schema details
          admin schema drop <Name>                 Drop a schema

          admin index add <Schema> <def>           Add an index
          admin index list <Schema>                List indexes
          admin index drop <Schema> <name>         Drop an index
          admin index rebuild <Schema> <name>      Rebuild an index

        Data Operations:
          insert <Schema> <json>                   Insert a record
          get <Schema> <id>                        Get a record by ID
          update <Schema> <id> <json>              Update a record
          delete <Schema> <id>                     Delete a record

        Search & Query:
          find <Schema> where <condition>          Scalar query
          find <Schema> --vector <field> <vec> --k N  Vector similarity
          find <Schema> --text <field> "<query>"  Full-text search
          find <Schema> --near <lat> <lon> --radius <d>  Spatial search
          find <Schema> --bitmap <field> = <val>  Bitmap filter
          find <Schema> --rank <field> --top N    Rank query
          find <Schema> --leaderboard <idx> --top N  Leaderboard
          find <Schema> --aggregate <idx>         Aggregation
          find <Schema> --join <relation>         Join query

        Graph Operations:
          graph <Schema> from=<node>              Outgoing edges
          graph <Schema> to=<node>                Incoming edges
          graph <Schema> from=<node> --depth N    Traverse graph
          graph <Schema> --path from=<a> to=<b>   Shortest path
          graph <Schema> --pagerank --top N       PageRank

        Version History:
          history <Schema> <id>                   Show version history
          history <Schema> <id> --at <version>    Get at version
          history <Schema> <id> --diff <v1> <v2>  Diff versions
          history <Schema> <id> --rollback <ver>  Rollback

        Raw FDB Access:
          raw get <key>                           Get raw key
          raw set <key> <value>                   Set raw key-value
          raw delete <key>                        Delete raw key
          raw range <prefix> [limit N]            Scan keys

        Other:
          help [topic]                            Show help
          quit                                    Exit CLI

        For detailed help: help <admin|find|graph|history|data|raw>
        """)
    }

    // MARK: - Tokenizer

    private static func tokenize(_ line: String) -> [String] {
        var tokens: [String] = []
        var current = ""
        var inQuotes = false
        var inJSON = 0
        var inArray = 0
        var quoteChar: Character = "\""

        for char in line {
            if inJSON > 0 {
                current.append(char)
                if char == "{" {
                    inJSON += 1
                } else if char == "}" {
                    inJSON -= 1
                }
            } else if inArray > 0 {
                current.append(char)
                if char == "[" {
                    inArray += 1
                } else if char == "]" {
                    inArray -= 1
                }
            } else if inQuotes {
                if char == quoteChar {
                    inQuotes = false
                    current.append(char)
                } else {
                    current.append(char)
                }
            } else if char == "\"" || char == "'" {
                inQuotes = true
                quoteChar = char
                current.append(char)
            } else if char == "{" {
                inJSON = 1
                current.append(char)
            } else if char == "[" {
                inArray = 1
                current.append(char)
            } else if char == " " || char == "\t" {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
            } else {
                current.append(char)
            }
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        return tokens
    }

    // MARK: - Argument Parsing

    private static func parsePort(from args: [String]) -> UInt16? {
        guard let portIdx = args.firstIndex(of: "--port"),
              portIdx + 1 < args.count,
              let port = UInt16(args[portIdx + 1]) else {
            return nil
        }
        return port
    }

    private static func printUsage() {
        print("""
        database - FoundationDB Interactive Database CLI

        Usage:
          database              Start interactive shell
          database init         Initialize a local database in .database/
          database start        Start the local database server
          database stop         Stop the local database server
          database status       Show local cluster status
          database --help       Show this help

        Options (for init):
          --port <port>         Server port (default: 4690)
        """)
    }
}

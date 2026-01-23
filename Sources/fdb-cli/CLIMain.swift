import Foundation
import FoundationDB

/// Main REPL loop for fdb-cli
public final class CLIMain: Sendable {
    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(database: any DatabaseProtocol) {
        self.database = database
        self.storage = SchemaStorage(database: database)
        self.output = OutputFormatter()
    }

    /// Run the REPL
    public func run() async {
        output.info("fdb-cli - FoundationDB Interactive CLI")
        output.info("Type 'help' for available commands, 'quit' to exit.")
        output.info("")

        while true {
            // Print prompt
            print("fdb> ", terminator: "")
            fflush(stdout)

            // Read line
            guard let line = readLine()?.trimmingCharacters(in: .whitespaces) else {
                // EOF
                break
            }

            // Skip empty lines
            guard !line.isEmpty else {
                continue
            }

            // Check for quit
            if line.lowercased() == "quit" || line.lowercased() == "exit" {
                output.info("Goodbye!")
                break
            }

            // Execute command
            do {
                try await executeCommand(line)
            } catch {
                output.error("\(error)")
            }
        }
    }

    /// Execute a single command
    public func executeCommand(_ line: String) async throws {
        let tokens = tokenize(line)
        guard let command = tokens.first?.lowercased() else {
            return
        }

        let args = Array(tokens.dropFirst())

        switch command {
        case "help":
            printHelp(topic: args.first)

        // Admin commands (schema and index management)
        case "admin":
            let adminCommands = AdminCommands(storage: storage, output: output)
            try await adminCommands.execute(args)

        // Legacy schema commands (redirect to admin schema)
        case "schema":
            let adminCommands = AdminCommands(storage: storage, output: output)
            try await adminCommands.execute(["schema"] + args)

        // Data commands
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

        // Unified search command
        case "find":
            let findCommands = FindCommands(storage: storage, output: output)
            try await findCommands.execute(args)

        // Legacy query command (redirect to find)
        case "query":
            let findCommands = FindCommands(storage: storage, output: output)
            // Convert old query syntax to find syntax
            if !args.isEmpty {
                try await findCommands.execute(args)
            } else {
                throw CLIError.invalidArguments("Usage: find <Schema> [where ...] [options]")
            }

        // Graph commands
        case "graph":
            let graphCommands = GraphCommands(storage: storage, output: output)
            try await graphCommands.execute(args)

        // History/version commands
        case "history":
            let historyCommands = HistoryCommands(storage: storage, output: output)
            try await historyCommands.execute(args)

        // Raw FDB commands
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

    private func printHelp(topic: String?) {
        if let topic = topic?.lowercased() {
            switch topic {
            case "admin":
                output.info(AdminCommands.helpText)
            case "schema":
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
                printGeneralHelp()
            }
        } else {
            printGeneralHelp()
        }
    }

    private func printGeneralHelp() {
        output.info("""
        fdb-cli - FoundationDB Interactive CLI with Full Index Support

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

        Index Modifiers:
          #indexed       Scalar index
          #unique        Unique constraint
          #bitmap        Bitmap index
          #rank          Rank index
          #vector(...)   Vector index
          #fulltext(...) Full-text index
          #leaderboard(...) Time-windowed leaderboard
          @relationship(Target,rule) Foreign key

        Examples:
          admin schema define User id:string name:string#indexed email:string#unique age:int#indexed status:string#bitmap
          insert User {"id": "u1", "name": "Alice", "age": 25, "email": "a@x.com", "status": "active"}
          find User where age > 20 limit 10
          find User --bitmap status = active
        """)
    }

    // MARK: - Tokenizer

    /// Tokenize a command line respecting quotes and JSON
    private func tokenize(_ line: String) -> [String] {
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
}

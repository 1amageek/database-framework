/// CommandRouter - Tokenizer and command dispatch for the REPL

import Foundation
import FoundationDB
import DatabaseEngine

enum CommandRouter {

    static func execute(
        _ line: String,
        dataAccess: CatalogDataAccess,
        catalogs: [TypeCatalog],
        output: OutputFormatter
    ) async throws {
        let tokens = tokenize(line)
        guard let command = tokens.first?.lowercased() else { return }
        let args = Array(tokens.dropFirst())

        switch command {
        case "help":
            printHelp(topic: args.first, output: output)

        case "schema":
            let cmd = SchemaInfoCommands(catalogs: catalogs, output: output)
            try cmd.execute(args)

        case "insert":
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.insert(args: args)

        case "get":
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.get(args: args)

        case "update":
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.update(args: args)

        case "delete":
            let cmd = DataCommands(dataAccess: dataAccess, output: output)
            try await cmd.delete(args: args)

        case "find", "query":
            let cmd = FindCommands(dataAccess: dataAccess, output: output)
            try await cmd.execute(args)

        case "graph":
            let cmd = GraphCommands(dataAccess: dataAccess, output: output)
            try await cmd.execute(args)

        case "sparql":
            let cmd = GraphCommands(dataAccess: dataAccess, output: output)
            try await cmd.executeSPARQL(args)

        case "history":
            let cmd = HistoryCommands(output: output)
            try await cmd.execute(args)

        case "clear":
            let cmd = ClearCommand(dataAccess: dataAccess, output: output)
            try await cmd.execute(args)

        case "raw":
            let rawCmd = RawCommands(database: dataAccess.database, output: output)
            guard let sub = args.first else {
                throw CLIError.invalidArguments("Usage: raw <get|set|delete|range> ...")
            }
            try await rawCmd.execute(sub, args: Array(args.dropFirst()))

        default:
            throw CLIError.unknownCommand(command)
        }
    }

    // MARK: - Help

    private static func printHelp(topic: String?, output: OutputFormatter) {
        if let topic = topic?.lowercased() {
            switch topic {
            case "schema":
                output.info(SchemaInfoCommands.helpText)
            case "find":
                output.info(FindCommands.helpText)
            case "graph", "sparql":
                output.info(GraphCommands.helpText)
            case "history":
                output.info(HistoryCommands.helpText)
            case "data", "insert", "get", "update", "delete":
                output.info(DataCommands.helpText)
            case "clear":
                output.info(ClearCommand.helpText)
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
        database - FoundationDB Interactive CLI

        Schema Info:
          schema list                        List all types
          schema show <TypeName>             Show type fields, types, and indexes

        Data Operations:
          insert <TypeName> <json>           Insert a record
          get <TypeName> <id>                Get a record by ID
          update <TypeName> <id> <json>      Update a record
          delete <TypeName> <id>             Delete a record

        Query:
          find <TypeName> [--where field op value] [--sort field [desc]] [--limit N]

        Partition (for dynamic directory types):
          --partition field=value            Specify partition value (repeatable)

        Graph:
          graph <TypeName> [from=<value>] [edge=<value>] [to=<value>] [--limit N]
          sparql <TypeName> <SPARQL query>

        Version History (requires embedded mode):
          history <TypeName> <id> [--limit N]

        Destructive:
          clear <TypeName> [--force]         Clear all data for a type
          clear --all [--force]              Clear all data for all types

        Raw FDB Access:
          raw get <key>                      Get raw key
          raw set <key> <value>              Set raw key-value
          raw delete <key>                   Delete raw key
          raw range <prefix> [limit N]       Scan keys

        Other:
          help [topic]                       Show help
          quit                               Exit CLI

        For detailed help: help <schema|find|graph|data|history|raw>
        """)
    }

    // MARK: - Tokenizer

    static func tokenize(_ line: String) -> [String] {
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

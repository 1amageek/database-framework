/// CommandRouter - Tokenizer and command dispatch for the REPL

import Foundation
import StorageKit
import DatabaseEngine
import Core

enum CommandRouter {

    static func execute(
        _ line: String,
        dataAccess: CatalogDataAccess,
        entities: [Schema.Entity],
        output: OutputFormatter
    ) async throws {
        let tokens = try tokenize(line)
        guard let command = tokens.first?.lowercased() else { return }
        let args = Array(tokens.dropFirst())

        switch command {
        case "help":
            printHelp(topic: args.first, output: output)

        case "schema":
            let cmd = SchemaInfoCommands(entities: entities, output: output)
            try cmd.execute(args)

        case "raw":
            let rawCmd = RawCommands(database: dataAccess.database, output: output)
            guard let sub = args.first else {
                throw CLIError.invalidArguments("Usage: raw <get|range> ...")
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

        Raw Storage Inspection:
          raw get <key>                      Get raw key
          raw range <prefix> [limit N]       Scan keys

        Other:
          help [topic]                       Show help
          quit                               Exit CLI

        Query, mutation, graph, ontology, and job operations are available only
        through the authenticated DatabaseWire client.

        For detailed help: help <schema|raw>
        """)
    }

    // MARK: - Tokenizer

    static func tokenize(_ line: String) throws -> [String] {
        var tokens: [String] = []
        var current = ""
        var quote: Character?
        var escaped = false
        var delimiters: [Character] = []

        for char in line {
            if escaped {
                current.append(char)
                escaped = false
            } else if quote != nil, char == "\\" {
                current.append(char)
                escaped = true
            } else if let activeQuote = quote {
                current.append(char)
                if char == activeQuote {
                    quote = nil
                }
            } else if char == "\"" || char == "'" {
                quote = char
                current.append(char)
            } else if char == "(" || char == "[" || char == "{" {
                delimiters.append(char)
                current.append(char)
            } else if char == ")" || char == "]" || char == "}" {
                guard let opening = delimiters.last,
                      Self.matches(opening: opening, closing: char) else {
                    throw CLIError.invalidArguments(
                        "Unmatched closing delimiter '\(char)'"
                    )
                }
                delimiters.removeLast()
                current.append(char)
            } else if (char == " " || char == "\t") && delimiters.isEmpty {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
            } else {
                current.append(char)
            }
        }

        if escaped {
            throw CLIError.invalidArguments("Dangling escape sequence")
        }
        if let quote {
            throw CLIError.invalidArguments("Unterminated quote '\(quote)'")
        }
        if let opening = delimiters.last {
            throw CLIError.invalidArguments(
                "Unterminated delimiter '\(opening)'"
            )
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        return tokens
    }

    private static func matches(
        opening: Character,
        closing: Character
    ) -> Bool {
        switch (opening, closing) {
        case ("(", ")"), ("[", "]"), ("{", "}"):
            return true
        default:
            return false
        }
    }
}

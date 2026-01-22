// CommandRouter.swift
// DatabaseCLI - Parses user input into Command enum
//
// Handles command parsing with support for quoted strings and various formats.

import Foundation

/// Parses user input into Command enum values
public struct CommandRouter: Sendable {

    public init() {}

    /// Parse a line of input into a Command
    ///
    /// - Parameter input: The raw input string from the user
    /// - Returns: The parsed Command
    public func parse(_ input: String) -> Command {
        let trimmed = input.trimmingCharacters(in: .whitespaces)

        guard !trimmed.isEmpty else {
            return .empty
        }

        let tokens = tokenize(trimmed)
        guard let first = tokens.first?.lowercased() else {
            return .empty
        }

        switch first {
        // Schema commands
        case "schema":
            return parseSchemaCommand(tokens)

        // Data commands
        case "get":
            return parseGetCommand(tokens)
        case "query":
            return parseQueryCommand(tokens)
        case "count":
            return parseCountCommand(tokens)
        case "insert":
            return parseInsertCommand(trimmed, tokens)
        case "delete":
            return parseDeleteCommand(tokens)

        // Version commands
        case "versions":
            return parseVersionsCommand(tokens)
        case "diff":
            return parseDiffCommand(tokens)

        // Index commands
        case "index":
            return parseIndexCommand(tokens)

        // Raw commands
        case "raw":
            return parseRawCommand(tokens)

        // Other commands
        case "help":
            if tokens.count > 1 {
                return .help(command: tokens[1])
            }
            return .help(command: nil)

        case "quit", "exit", "q":
            return .quit

        default:
            return .unknown(input: trimmed)
        }
    }

    // MARK: - Tokenization

    /// Tokenize input, respecting quoted strings
    private func tokenize(_ input: String) -> [String] {
        var tokens: [String] = []
        var current = ""
        var inQuotes = false
        var quoteChar: Character = "\""

        for char in input {
            if inQuotes {
                if char == quoteChar {
                    inQuotes = false
                    tokens.append(current)
                    current = ""
                } else {
                    current.append(char)
                }
            } else {
                if char == "\"" || char == "'" {
                    inQuotes = true
                    quoteChar = char
                    if !current.isEmpty {
                        tokens.append(current)
                        current = ""
                    }
                } else if char.isWhitespace {
                    if !current.isEmpty {
                        tokens.append(current)
                        current = ""
                    }
                } else {
                    current.append(char)
                }
            }
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        return tokens
    }

    // MARK: - Command Parsers

    private func parseSchemaCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 2 else {
            return .unknown(input: "schema requires a subcommand (list, show)")
        }

        switch tokens[1].lowercased() {
        case "list":
            return .schemaList
        case "show":
            if tokens.count >= 3 {
                return .schemaShow(typeName: tokens[2])
            }
            return .unknown(input: "schema show requires a type name")
        default:
            return .unknown(input: "unknown schema subcommand: \(tokens[1])")
        }
    }

    private func parseGetCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 3 else {
            return .unknown(input: "get requires <Type> <id>")
        }
        return .get(typeName: tokens[1], id: tokens[2])
    }

    private func parseQueryCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 2 else {
            return .unknown(input: "query requires <Type>")
        }

        let typeName = tokens[1]
        var whereClause: String? = nil
        var limit: Int? = nil

        // Parse "where" clause
        if let whereIndex = tokens.firstIndex(where: { $0.lowercased() == "where" }) {
            // Find where limit starts (if any)
            let limitIndex = tokens.firstIndex(where: { $0.lowercased() == "limit" }) ?? tokens.endIndex
            if whereIndex + 1 < limitIndex {
                let whereTokens = tokens[(whereIndex + 1)..<limitIndex]
                whereClause = whereTokens.joined(separator: " ")
            }
        }

        // Parse "limit" clause
        if let limitIndex = tokens.firstIndex(where: { $0.lowercased() == "limit" }),
           limitIndex + 1 < tokens.count,
           let n = Int(tokens[limitIndex + 1]) {
            limit = n
        }

        return .query(typeName: typeName, whereClause: whereClause, limit: limit)
    }

    private func parseCountCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 2 else {
            return .unknown(input: "count requires <Type>")
        }

        let typeName = tokens[1]
        var whereClause: String? = nil

        // Parse "where" clause
        if let whereIndex = tokens.firstIndex(where: { $0.lowercased() == "where" }),
           whereIndex + 1 < tokens.count {
            let whereTokens = tokens[(whereIndex + 1)...]
            whereClause = whereTokens.joined(separator: " ")
        }

        return .count(typeName: typeName, whereClause: whereClause)
    }

    private func parseInsertCommand(_ original: String, _ tokens: [String]) -> Command {
        guard tokens.count >= 3 else {
            return .unknown(input: "insert requires <Type> <json>")
        }

        let typeName = tokens[1]

        // Find JSON in original input (preserve formatting)
        if let jsonStart = original.firstIndex(of: "{"),
           let jsonEnd = original.lastIndex(of: "}") {
            let json = String(original[jsonStart...jsonEnd])
            return .insert(typeName: typeName, json: json)
        }

        return .unknown(input: "insert requires valid JSON data")
    }

    private func parseDeleteCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 3 else {
            return .unknown(input: "delete requires <Type> <id>")
        }
        return .delete(typeName: tokens[1], id: tokens[2])
    }

    private func parseVersionsCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 3 else {
            return .unknown(input: "versions requires <Type> <id>")
        }

        let typeName = tokens[1]
        let id = tokens[2]
        var limit: Int? = nil

        // Parse "limit" clause
        if let limitIndex = tokens.firstIndex(where: { $0.lowercased() == "limit" }),
           limitIndex + 1 < tokens.count,
           let n = Int(tokens[limitIndex + 1]) {
            limit = n
        }

        return .versions(typeName: typeName, id: id, limit: limit)
    }

    private func parseDiffCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 3 else {
            return .unknown(input: "diff requires <Type> <id>")
        }
        return .diff(typeName: tokens[1], id: tokens[2])
    }

    private func parseIndexCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 2 else {
            return .unknown(input: "index requires a subcommand (list, status, build, scrub)")
        }

        switch tokens[1].lowercased() {
        case "list":
            return .indexList
        case "status":
            if tokens.count >= 3 {
                return .indexStatus(name: tokens[2])
            }
            return .unknown(input: "index status requires an index name")
        case "build":
            if tokens.count >= 3 {
                return .indexBuild(name: tokens[2])
            }
            return .unknown(input: "index build requires an index name")
        case "scrub":
            if tokens.count >= 3 {
                return .indexScrub(name: tokens[2])
            }
            return .unknown(input: "index scrub requires an index name")
        default:
            return .unknown(input: "unknown index subcommand: \(tokens[1])")
        }
    }

    private func parseRawCommand(_ tokens: [String]) -> Command {
        guard tokens.count >= 2 else {
            return .unknown(input: "raw requires a subcommand (get, range)")
        }

        switch tokens[1].lowercased() {
        case "get":
            if tokens.count >= 3 {
                return .rawGet(key: tokens[2])
            }
            return .unknown(input: "raw get requires a key")
        case "range":
            if tokens.count >= 3 {
                let prefix = tokens[2]
                var limit = 10 // default
                if tokens.count >= 5, tokens[3].lowercased() == "limit",
                   let n = Int(tokens[4]) {
                    limit = n
                } else if tokens.count >= 4, let n = Int(tokens[3]) {
                    limit = n
                }
                return .rawRange(prefix: prefix, limit: limit)
            }
            return .unknown(input: "raw range requires a prefix")
        default:
            return .unknown(input: "unknown raw subcommand: \(tokens[1])")
        }
    }
}

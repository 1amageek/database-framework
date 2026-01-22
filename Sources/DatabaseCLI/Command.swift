// Command.swift
// DatabaseCLI - Command definitions for the interactive REPL
//
// Defines all supported commands and their parameters.

import Foundation

/// All commands supported by the CLI
public enum Command: Sendable, Equatable {
    // MARK: - Schema Commands

    /// List all registered entities
    case schemaList

    /// Show details for a specific entity type
    case schemaShow(typeName: String)

    // MARK: - Data Commands

    /// Get a single item by ID
    case get(typeName: String, id: String)

    /// Query items with optional where clause
    case query(typeName: String, whereClause: String?, limit: Int?)

    /// Count items with optional where clause
    case count(typeName: String, whereClause: String?)

    /// Insert a new item from JSON
    case insert(typeName: String, json: String)

    /// Delete an item by ID
    case delete(typeName: String, id: String)

    // MARK: - Version Commands

    /// Show version history for an item
    case versions(typeName: String, id: String, limit: Int?)

    /// Show diff from previous version
    case diff(typeName: String, id: String)

    // MARK: - Index Commands

    /// List all indexes
    case indexList

    /// Show status of a specific index
    case indexStatus(name: String)

    /// Build/rebuild an index
    case indexBuild(name: String)

    /// Scrub (verify) an index
    case indexScrub(name: String)

    // MARK: - Raw Commands

    /// Get raw value by key
    case rawGet(key: String)

    /// Range scan with prefix
    case rawRange(prefix: String, limit: Int)

    // MARK: - Other Commands

    /// Show help
    case help(command: String?)

    /// Quit the CLI
    case quit

    /// Unknown/invalid command
    case unknown(input: String)

    /// Empty input (no-op)
    case empty
}

// MARK: - Command Description

extension Command {
    /// Human-readable description of the command
    public var description: String {
        switch self {
        case .schemaList:
            return "schema list"
        case .schemaShow(let typeName):
            return "schema show \(typeName)"
        case .get(let typeName, let id):
            return "get \(typeName) \(id)"
        case .query(let typeName, let whereClause, let limit):
            var desc = "query \(typeName)"
            if let w = whereClause { desc += " where \(w)" }
            if let l = limit { desc += " limit \(l)" }
            return desc
        case .count(let typeName, let whereClause):
            var desc = "count \(typeName)"
            if let w = whereClause { desc += " where \(w)" }
            return desc
        case .insert(let typeName, _):
            return "insert \(typeName) {...}"
        case .delete(let typeName, let id):
            return "delete \(typeName) \(id)"
        case .versions(let typeName, let id, _):
            return "versions \(typeName) \(id)"
        case .diff(let typeName, let id):
            return "diff \(typeName) \(id)"
        case .indexList:
            return "index list"
        case .indexStatus(let name):
            return "index status \(name)"
        case .indexBuild(let name):
            return "index build \(name)"
        case .indexScrub(let name):
            return "index scrub \(name)"
        case .rawGet(let key):
            return "raw get \(key)"
        case .rawRange(let prefix, let limit):
            return "raw range \(prefix) limit \(limit)"
        case .help(let command):
            if let cmd = command {
                return "help \(cmd)"
            }
            return "help"
        case .quit:
            return "quit"
        case .unknown(let input):
            return "unknown: \(input)"
        case .empty:
            return "(empty)"
        }
    }
}

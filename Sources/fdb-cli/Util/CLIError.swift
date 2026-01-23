import Foundation

/// CLI errors
public enum CLIError: Error, CustomStringConvertible {
    case unknownCommand(String)
    case invalidArguments(String)
    case schemaNotFound(String)
    case schemaExists(String)
    case recordNotFound(schema: String, id: String)
    case invalidJSON(String)
    case validationError(String)
    case connectionFailed(String)

    public var description: String {
        switch self {
        case .unknownCommand(let cmd):
            return "Unknown command: '\(cmd)'. Type 'help' for available commands."
        case .invalidArguments(let usage):
            return "Invalid arguments. \(usage)"
        case .schemaNotFound(let name):
            return "Schema '\(name)' not found"
        case .schemaExists(let name):
            return "Schema '\(name)' already exists"
        case .recordNotFound(let schema, let id):
            return "Record '\(id)' not found in '\(schema)'"
        case .invalidJSON(let message):
            return "Invalid JSON: \(message)"
        case .validationError(let message):
            return "Validation error: \(message)"
        case .connectionFailed(let message):
            return "Connection failed: \(message)"
        }
    }
}

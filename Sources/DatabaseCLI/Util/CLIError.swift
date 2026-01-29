import Foundation

/// CLI errors
public enum CLIError: Error, CustomStringConvertible {
    case unknownCommand(String)
    case invalidArguments(String)
    case entityNotFound(String)
    case recordNotFound(type: String, id: String)
    case invalidJSON(String)
    case connectionFailed(String)
    case alreadyInitialized(String)
    case serverNotFound(String)
    case initializationFailed(String)
    case portInUse(UInt16)
    case missingPartition(String)

    public var description: String {
        switch self {
        case .unknownCommand(let cmd):
            return "Unknown command: '\(cmd)'. Type 'help' for available commands."
        case .invalidArguments(let usage):
            return "Invalid arguments. \(usage)"
        case .entityNotFound(let name):
            return "Type '\(name)' not found in schema. Use 'schema list' to see registered types."
        case .recordNotFound(let type, let id):
            return "Record '\(id)' not found in '\(type)'"
        case .invalidJSON(let message):
            return "Invalid JSON: \(message)"
        case .connectionFailed(let message):
            return "Connection failed: \(message)"
        case .alreadyInitialized(let path):
            return "Already initialized: \(path) already exists"
        case .serverNotFound(let message):
            return "fdbserver not found: \(message)"
        case .initializationFailed(let message):
            return "Initialization failed: \(message)"
        case .portInUse(let port):
            return "Port \(port) is already in use. Use --port <port> or stop the process using that port."
        case .missingPartition(let field):
            return "Missing --partition value for dynamic directory field: '\(field)'. Use --partition \(field)=<value>"
        }
    }
}

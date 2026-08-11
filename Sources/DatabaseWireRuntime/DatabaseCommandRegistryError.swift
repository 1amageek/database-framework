import DatabaseKit

public enum DatabaseCommandRegistryError: Error, Sendable, CustomStringConvertible {
    case duplicate(CommandIdentifier)
    case commandNotFound(CommandIdentifier)

    public var description: String {
        switch self {
        case .duplicate(let identifier):
            return "Duplicate database command identifier: \(identifier.rawValue)"
        case .commandNotFound(let identifier):
            return "Database command is not registered: \(identifier.rawValue)"
        }
    }
}

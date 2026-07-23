public enum DatabaseCommandRegistryError: Error, Sendable, CustomStringConvertible {
    case emptyIdentifier
    case duplicate(String)
    case commandNotFound(String)

    public var description: String {
        switch self {
        case .emptyIdentifier:
            return "Database command identifiers must not be empty"
        case .duplicate(let identifier):
            return "Duplicate database command identifier: \(identifier)"
        case .commandNotFound(let identifier):
            return "Database command is not registered: \(identifier)"
        }
    }
}

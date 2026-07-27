@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseOperationRegistryError: Error, Sendable, CustomStringConvertible {
    case duplicate(DatabaseOperationIdentifier)
    case missing([DatabaseOperationIdentifier])

    public var description: String {
        switch self {
        case .duplicate(let identifier):
            return "Database operation \(identifier) was registered more than once"
        case .missing(let identifiers):
            return "Database runtime is missing handlers for: \(identifiers)"
        }
    }
}

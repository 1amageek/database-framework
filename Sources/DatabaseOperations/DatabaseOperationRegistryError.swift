import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

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

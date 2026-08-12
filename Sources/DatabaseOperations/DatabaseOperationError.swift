import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public enum DatabaseOperationError: Error, Sendable, CustomStringConvertible {
    case responseOperationMismatch(
        expected: DatabaseOperationIdentifier,
        actual: DatabaseOperationIdentifier
    )
    case responseRequestIDMismatch(expected: UInt64, actual: UInt64)
    case missingHandler(DatabaseOperationIdentifier)
    case targetKindNotAccepted(DatabaseOperationTarget)
    case dataRootLeaseMismatch
    case migrationRequired

    public var description: String {
        switch self {
        case .responseOperationMismatch(let expected, let actual):
            return "Database response operation \(actual) does not match request \(expected)"
        case .responseRequestIDMismatch(let expected, let actual):
            return "Database response request ID \(actual) does not match request \(expected)"
        case .missingHandler(let identifier):
            return "Database operation \(identifier) has no registered handler"
        case .targetKindNotAccepted(let target):
            return "Database operation does not accept target \(target)"
        case .dataRootLeaseMismatch:
            return "The selected data root does not match the active operation lease"
        case .migrationRequired:
            return "The database layout must be migrated before this operation can run"
        }
    }
}

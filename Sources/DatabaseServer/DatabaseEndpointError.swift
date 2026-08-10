@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseEndpointError: Error, Sendable, CustomStringConvertible {
    case invalidRequestFrame(DatabaseWireError)
    case responseEncodingFailed(DatabaseWireError)
    case responseOperationMismatch(
        expected: DatabaseOperationIdentifier,
        actual: DatabaseOperationIdentifier
    )
    case responseRequestIDMismatch(expected: UInt64, actual: UInt64)
    case missingHandler(DatabaseOperationIdentifier)
    case targetKindNotAccepted(DatabaseOperationTarget)
    case migrationRequired

    public var description: String {
        switch self {
        case .invalidRequestFrame(let error):
            return "Database request frame is invalid: \(error)"
        case .responseEncodingFailed(let error):
            return "Database response frame could not be encoded: \(error)"
        case .responseOperationMismatch(let expected, let actual):
            return "Database response operation \(actual) does not match request \(expected)"
        case .responseRequestIDMismatch(let expected, let actual):
            return "Database response request ID \(actual) does not match request \(expected)"
        case .missingHandler(let identifier):
            return "Database operation \(identifier) has no registered handler"
        case .targetKindNotAccepted(let target):
            return "Database operation does not accept target \(target)"
        case .migrationRequired:
            return "The database layout must be migrated before this operation can run"
        }
    }
}

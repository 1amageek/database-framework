import DatabaseWire

public enum DatabaseResumableOperationRegistryError: Error, CustomStringConvertible {
    case duplicateOperation(DatabaseJobOperationIdentifier)
    case unsupportedOperation(DatabaseJobOperationIdentifier)

    public var description: String {
        switch self {
        case .duplicateOperation(let operation):
            return "Duplicate resumable operation: \(operation.family.rawValue):\(operation.kind)"
        case .unsupportedOperation(let operation):
            return "Operation is not resumable: \(operation.family.rawValue):\(operation.kind)"
        }
    }
}

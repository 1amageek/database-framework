/// Failure raised when a data-root transaction capability is used outside its
/// admitted mutation, root, lifetime, or transaction-control contract.
public enum DatabaseReadTransactionError: Error, Sendable, Equatable {
    case mutationRequiresWriteAccess
    case transactionControlUnavailable
    case versionstampUnavailable
    case invalidDataRoot
    case keyOutsideDataRoot
    case rangeOutsideDataRoot
    case unsupportedKeySelector
    case backendReturnedKeyOutsideDataRoot
    case snapshotClosed
    case readOperationCountOverflow
}

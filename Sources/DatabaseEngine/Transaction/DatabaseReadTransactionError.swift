/// Failure raised when a read-authorized transaction capability is used for
/// a storage mutation.
public enum DatabaseReadTransactionError: Error, Sendable, Equatable {
    case mutationRequiresWriteAccess
}

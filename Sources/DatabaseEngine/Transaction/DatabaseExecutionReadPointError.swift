/// Failure to restore or capture a storage read point without exposing raw
/// transaction-control capability.
@_spi(DatabaseExecution)
public enum DatabaseExecutionReadPointError: Error, Sendable, Equatable {
    case domainMismatch
    case opaquePositionCannotBeRestored
    case historicalReadUnsupported
    case invalidVersion(UInt64)
    case restoreRequiresIndependentTransaction
    case backendReturnedInvalidVersion(Int64)
}

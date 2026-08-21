@_spi(DatabaseExecution)
public enum DatabaseTransactionMutationError: Error, Sendable, Equatable {
    case workMeterMismatch
    case workMeterBoundAfterMutation
}

@_spi(DatabaseExecution)
public enum DatabaseEntityMutationLimitsError: Error, Sendable, Equatable {
    case nonPositiveMaximumChanges(Int)
    case nonPositiveMaximumPreconditions(Int)
}

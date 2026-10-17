/// Failures of the logical transaction checkpoint scope.
public enum DatabaseTransactionCheckpointError: Error, Sendable, Equatable {
    case unavailable
    case failed
    case invalidToken
    case barrierInsideOperation
    case scopeIdentifierExhausted
}

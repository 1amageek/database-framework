/// A query connection could not admit another invocation.
public enum QueryConnectionError: Error, Sendable, Equatable {
    case closed
    case invalidConcurrencyLimit
    case concurrentExecutionLimitReached(Int)
    case requestIdentifierExhausted
    case graphPartitionsRequireGraphQuery
}

/// Terminal result produced by one branch of a database execution deadline race.
enum DatabaseExecutionTimeoutOutcome<Value: Sendable>: Sendable {
    case value(Value)
    case failure(any Error)
    case timedOut
    case cancelled
}

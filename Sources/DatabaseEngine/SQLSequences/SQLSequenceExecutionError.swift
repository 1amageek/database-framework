/// Typed sequence failures; allocator control demands are private to SQLBlock.
public enum SQLSequenceExecutionError: Error, Sendable, Equatable {
    case requestRequired
    case unknownSequence(String)
    case currentValueUnavailable(String)
    case lastValueUnavailable
    case exhausted(String)
    case invalidArguments(String)
    case invalidPersistedState
    case replayDiverged
    case sequenceAlreadyExists(String)
    case dependentIdentity(String)
    case invalidRestart(Int64)
}

struct SQLSequenceAllocationDemand: Error, Sendable {
    let event: Int
}

@_spi(DatabaseExecution)
public struct SQLSequenceRequestCleanupError: Error, Sendable {
    public let operationError: any Error
    public let cleanupError: any Error
}

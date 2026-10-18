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

struct SQLSequenceRequestCleanupError: Error, Sendable {
    let operationError: any Error
    let cleanupError: any Error
}

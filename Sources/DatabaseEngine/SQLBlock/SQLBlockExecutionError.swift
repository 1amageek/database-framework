/// Failures specific to semantic SQL block admission and checkpoint lookup.
public enum SQLBlockExecutionError: Error, Sendable, Equatable {
    case unknownCheckpoint(String)
    case unsupportedStatement
    case unsupportedMutationMapping(String)
    case mutationLimitOutOfRange(UInt32)
}

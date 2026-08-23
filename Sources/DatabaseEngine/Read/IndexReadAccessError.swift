/// A physical read attempted to escape its schema-admitted index boundary.
public enum IndexReadAccessError: Error, Sendable, Equatable {
    case indexPartitionAbsent
    case invalidReadableIndexSubspace
    case keyOutsideReadableIndex
    case rangeOutsideReadableIndex
    case unsupportedKeySelector
    case backendReturnedKeyOutsideReadableIndex
    case queryContextUnavailable
}

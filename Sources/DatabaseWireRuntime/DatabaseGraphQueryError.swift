#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
public enum DatabaseGraphQueryError: Error, Sendable, CustomStringConvertible {
    case invalidContinuation
    case continuationDoesNotMatchRequest
    case continuationSnapshotChanged
    case pageLimitExceedsMaximum(requested: UInt32, maximum: UInt32)
    case pageLimitExceedsPlatformCapacity(requested: UInt32)
    case continuationOffsetOutOfRange(offset: UInt64, count: Int)

    public var description: String {
        switch self {
        case .invalidContinuation:
            return "Graph query continuation is malformed"
        case .continuationDoesNotMatchRequest:
            return "Graph query continuation belongs to a different request"
        case .continuationSnapshotChanged:
            return "Graph query snapshot changed before pagination completed"
        case .pageLimitExceedsMaximum(let requested, let maximum):
            return "Graph page limit \(requested) exceeds the maximum row budget \(maximum)"
        case .pageLimitExceedsPlatformCapacity(let requested):
            return "Graph page limit \(requested) exceeds the platform capacity"
        case .continuationOffsetOutOfRange(let offset, let count):
            return "Graph continuation offset \(offset) is outside the result count \(count)"
        }
    }
}

#endif

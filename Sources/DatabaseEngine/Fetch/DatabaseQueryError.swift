public enum DatabaseQueryError: Error, Sendable, Equatable {
    case invalidLimit(Int)
    case invalidOffset(Int)
}

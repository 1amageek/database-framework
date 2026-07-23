public enum PersistentUnionFindError: Error, Sendable, Equatable {
    case invalidParentUTF8(individual: String)
    case parentCycle(start: String, repeated: String)
    case parentTraversalLimitExceeded(start: String, limit: Int)
    case malformedMemberKey(reason: String)
    case unexpectedMemberComponent(position: Int, actual: String)
    case invalidRankByteCount(actual: Int)
    case invalidRankValue(Int64)
    case rankOverflow(Int)
}

extension PersistentUnionFindError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .invalidParentUTF8(let individual):
            return "Union-find parent for '\(individual)' is not valid UTF-8"
        case .parentCycle(let start, let repeated):
            return "Union-find parent cycle from '\(start)' repeats '\(repeated)'"
        case .parentTraversalLimitExceeded(let start, let limit):
            return "Union-find parent traversal from '\(start)' exceeds \(limit) nodes"
        case .malformedMemberKey(let reason):
            return "Malformed union-find member key: \(reason)"
        case .unexpectedMemberComponent(let position, let actual):
            return "Union-find member component \(position) must be String, got \(actual)"
        case .invalidRankByteCount(let actual):
            return "Union-find rank must contain exactly 8 bytes, got \(actual)"
        case .invalidRankValue(let value):
            return "Union-find rank must be a non-negative Int, got \(value)"
        case .rankOverflow(let value):
            return "Union-find rank cannot be incremented beyond \(value)"
        }
    }
}

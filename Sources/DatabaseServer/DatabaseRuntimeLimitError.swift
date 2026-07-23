public enum DatabaseRuntimeLimitError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidMaximumRows(requested: UInt32, maximum: UInt32)
    case invalidMaximumWorkUnits(requested: UInt64, maximum: UInt64)
    case invalidMaximumIntermediateRows(requested: UInt32, maximum: UInt32)
    case invalidMaximumIntermediateBytes(requested: UInt64, maximum: UInt64)
    case invalidTimeout(requested: UInt32, maximum: UInt32)
    case executionTimedOut(UInt32)
    case resultLimitExceeded(actual: Int, maximum: UInt32)

    public var description: String {
        switch self {
        case .invalidMaximumRows(let requested, let maximum):
            return "Requested row budget \(requested) is outside 1...\(maximum)"
        case .invalidMaximumWorkUnits(let requested, let maximum):
            return "Requested work budget \(requested) is outside 1...\(maximum)"
        case .invalidMaximumIntermediateRows(let requested, let maximum):
            return "Requested intermediate row budget \(requested) is outside 1...\(maximum)"
        case .invalidMaximumIntermediateBytes(let requested, let maximum):
            return "Requested intermediate byte budget \(requested) is outside 1...\(maximum)"
        case .invalidTimeout(let requested, let maximum):
            return "Requested timeout \(requested) ms is outside 1...\(maximum) ms"
        case .executionTimedOut(let milliseconds):
            return "Database execution exceeded \(milliseconds) ms"
        case .resultLimitExceeded(let actual, let maximum):
            return "Database result contains \(actual) values, exceeding the limit of \(maximum)"
        }
    }
}

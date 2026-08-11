/// A malformed permuted-index request or persisted key.
public enum PermutedIndexError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case fieldCountMismatch(expected: Int, got: Int)
    case invalidLimit(Int)
    case corruptedEntry(
        expectedMinimumElementCount: Int,
        actual: Int
    )

    public var description: String {
        switch self {
        case .fieldCountMismatch(let expected, let got):
            return "Permuted index expected \(expected) field values, got \(got)"
        case .invalidLimit(let limit):
            return "Permuted index limit must be nonnegative, got \(limit)"
        case .corruptedEntry(let expected, let actual):
            return "Permuted index entry has \(actual) elements; expected at least \(expected)"
        }
    }
}

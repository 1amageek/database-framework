import StorageKit

public enum RangeSetError: Error, Sendable, Equatable {
    case invalidRangeIndex(index: Int, count: Int)
    case progressKeyOutsideRange(index: Int, key: Bytes)
    case progressRegression(index: Int, previous: Bytes, next: Bytes)
}

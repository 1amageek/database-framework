import DatabaseTypes
import StorageKit

public enum RangeSetError: Error, Sendable, Equatable {
    case invalidRangeIndex(index: Int, count: Int)
    case progressKeyOutsideRange(index: Int, key: ByteString)
    case progressRegression(index: Int, previous: ByteString, next: ByteString)
}

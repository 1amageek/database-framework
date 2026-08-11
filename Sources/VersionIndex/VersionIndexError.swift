import DatabaseTypes

public enum VersionIndexError: Error, Sendable, Equatable {
    case invalidHistoryLimit(Int)
    case malformedVersionKey(expectedByteCount: Int, actualByteCount: Int)
    case versionKeyTooLong(byteCount: Int)
    case malformedVersionValue(byteCount: Int)
    case invalidTimestamp(TimestampError)
    case timestampArithmeticOverflow
}

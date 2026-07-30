import DatabaseTypes

public enum VersionIndexError: Error, Sendable, Equatable {
    case versionKeyTooLong(byteCount: Int)
    case malformedVersionValue(byteCount: Int)
    case invalidTimestamp(TimestampError)
    case timestampArithmeticOverflow
}

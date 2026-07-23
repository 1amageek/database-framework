public enum StatisticsStorageError: Error, Sendable, Equatable {
    case invalidMagic
    case unsupportedVersion(UInt8)
    case unexpectedEntryKind(expected: UInt8, actual: UInt8)
    case invalidFieldValue
    case integerOutOfRange
    case invalidTimestamp
    case malformedKey(expectedElementCount: Int, actual: Int)
    case malformedKeyElement
}

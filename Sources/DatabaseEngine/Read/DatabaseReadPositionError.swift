/// Failure to select or verify a requested database read position.
public enum DatabaseReadPositionError: Error, Sendable, Equatable {
    case positionIsNotRestorable
    case versionExceedsStorageRange(UInt64)
    case invalidStorageVersion(Int64)
    case restoredPositionChanged(
        expected: DatabaseReadPosition,
        actual: DatabaseReadPosition
    )
}

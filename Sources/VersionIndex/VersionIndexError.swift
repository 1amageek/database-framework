public enum VersionIndexError: Error, Sendable, Equatable {
    case versionKeyTooLong(byteCount: Int)
}

#if !os(WASI)
#if FOUNDATION_DB && !DATABASE_MULTIPLE_BASES
public enum FDBDatabaseConfigurationError: Error, Sendable, Equatable {
    case emptyDirectoryPath
    case emptyDirectoryComponent(index: Int)
}
#endif
#endif

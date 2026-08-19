#if !os(WASI)
#if FOUNDATION_DB && !DATABASE_MULTI_BASE
public enum FDBDatabaseConfigurationError: Error, Sendable, Equatable {
    case emptyDirectoryPath
    case emptyDirectoryComponent(index: Int)
}
#endif
#endif

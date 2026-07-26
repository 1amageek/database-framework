#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import FDBStorage

extension FDBStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration {
        DBConfiguration(
            backend: .fdb(self),
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif

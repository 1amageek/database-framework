#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import FDBStorage
import StorageKit

extension FDBStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try await FDBStorageEngine(configuration: self)
        return DBConfiguration(
            storageEngine: engine,
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif

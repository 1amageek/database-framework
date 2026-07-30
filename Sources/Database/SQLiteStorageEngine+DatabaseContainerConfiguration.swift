#if !os(WASI)
#if SQLITE
import DatabaseEngine
import SQLiteStorage
import StorageKit

extension SQLiteStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try SQLiteStorageEngine(configuration: self)
        return DBConfiguration(
            backend: .custom(engine),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif

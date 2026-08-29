#if !os(WASI)
#if SQLITE && !DATABASE_MULTI_BASE
import DatabaseEngine
import SQLiteStorage
import StorageKit

extension SQLiteStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock
    ) async throws -> DBConfiguration {
        let engine = try SQLiteStorageEngine(configuration: self)
        return try DBConfiguration(
            storageEngine: engine,
            monotonicClock: monotonicClock,
            wallClock: wallClock
        )
    }
}
#endif

#endif

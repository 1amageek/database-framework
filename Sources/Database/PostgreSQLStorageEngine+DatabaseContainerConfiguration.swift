#if !os(WASI)
#if POSTGRESQL && !DATABASE_MULTI_BASE
import DatabaseEngine
import PostgreSQLStorage
import StorageKit

extension PostgreSQLStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock
    ) async throws -> DBConfiguration {
        let engine = try await PostgreSQLStorageEngine(configuration: self)
        return try DBConfiguration(
            storageEngine: engine,
            monotonicClock: monotonicClock,
            wallClock: wallClock
        )
    }
}
#endif

#endif

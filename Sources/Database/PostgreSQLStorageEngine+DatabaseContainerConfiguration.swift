#if !os(WASI)
#if POSTGRESQL && !DATABASE_MULTIPLE_BASES
import DatabaseEngine
import PostgreSQLStorage
import StorageKit

extension PostgreSQLStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try await PostgreSQLStorageEngine(configuration: self)
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

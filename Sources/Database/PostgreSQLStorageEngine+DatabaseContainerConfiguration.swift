#if !os(WASI)
#if POSTGRESQL
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
            backend: .custom(engine),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif

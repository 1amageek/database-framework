#if POSTGRESQL
import DatabaseEngine
import PostgreSQLStorage

extension PostgreSQLStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        indexConfigurations: [any IndexConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try await PostgreSQLStorageEngine(configuration: self)
        return DBConfiguration(
            backend: .custom(engine),
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#if SQLITE
import DatabaseEngine
import SQLiteStorage

extension SQLiteStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        indexConfigurations: [any IndexConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try SQLiteStorageEngine(configuration: self)
        return DBConfiguration(
            backend: .custom(engine),
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

import DatabaseKit

/// Opens the canonical model persistence layer for framework benchmarks.
///
/// Application code should use `DatabaseContext`. This probe exists only so
/// benchmark targets can measure the DataStore layer independently without
/// widening `DBContainer`'s package-scoped execution API.
@_spi(Benchmarking)
public enum DataStoreBenchmarkProbe {
    /// Opens the store for a statically partitioned model in the container's
    /// current database operation scope.
    public static func openDataStore<Model: Persistable>(
        for type: Model.Type,
        in container: DBContainer
    ) async throws -> any DataStore {
        try await container.store(for: type)
    }

    /// Opens the store for a dynamically partitioned model at one resolved path.
    public static func openDataStore<Model: Persistable>(
        for type: Model.Type,
        in container: DBContainer,
        path: DirectoryPath<Model>
    ) async throws -> any DataStore {
        try await container.store(for: type, path: path)
    }
}

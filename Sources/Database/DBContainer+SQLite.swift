#if !os(WASI)
#if SQLITE
import DatabaseEngine
import SQLiteStorage
import StorageKit

extension DBContainer {
    /// Create a container backed by SQLite for on-device use.
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - path: File path for the SQLite database
    ///   - security: Security configuration (default: enabled)
    /// - Returns: A DBContainer backed by SQLite
    public static func sqlite(
        for schema: Schema,
        path: String,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        return try await DBContainer.open(
            for: schema,
            configuration: SQLiteStorageEngine.Configuration.file(path),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    /// Create a container backed by in-memory SQLite (for testing).
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - security: Security configuration (default: enabled)
    /// - Returns: A DBContainer backed by in-memory SQLite
    public static func inMemory(
        for schema: Schema,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        return try await DBContainer.open(
            for: schema,
            configuration: SQLiteStorageEngine.Configuration.inMemory,
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif

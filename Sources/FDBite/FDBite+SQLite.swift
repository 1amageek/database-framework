import DatabaseEngine
import SQLiteStorage
import StorageKit

extension FDBContainer {
    /// Create a container backed by SQLite for on-device use.
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - path: File path for the SQLite database
    ///   - security: Security configuration (default: enabled)
    /// - Returns: An FDBContainer backed by SQLite
    public static func sqlite(
        for schema: Schema,
        path: String,
        security: SecurityConfiguration = .enabled()
    ) async throws -> FDBContainer {
        let engine = try SQLiteStorageEngine(path: path)
        return try await FDBContainer(
            for: schema,
            engine: engine,
            security: security
        )
    }

    /// Create a container backed by in-memory SQLite (for testing).
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - security: Security configuration (default: enabled)
    /// - Returns: An FDBContainer backed by in-memory SQLite
    public static func inMemory(
        for schema: Schema,
        security: SecurityConfiguration = .enabled()
    ) async throws -> FDBContainer {
        let engine = try SQLiteStorageEngine()
        return try await FDBContainer(
            for: schema,
            engine: engine,
            security: security
        )
    }
}

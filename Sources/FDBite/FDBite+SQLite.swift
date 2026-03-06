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
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .file(path))
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: security
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
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: security
        )
    }
}

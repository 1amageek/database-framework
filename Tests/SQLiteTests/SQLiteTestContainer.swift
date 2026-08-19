#if SQLITE
import Foundation
import Database
import DatabaseEngine
import TestSupport
import Testing

struct SQLiteTestDatabase {
    let directory: URL
    let path: String

    init(prefix: String) throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "\(prefix)-\(UUID().uuidString)",
                isDirectory: true
            )
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        self.directory = directory
        self.path = directory.appendingPathComponent("database.sqlite").path
    }

    func remove() {
        do {
            try FileManager.default.removeItem(at: directory)
        } catch {
            Issue.record("Failed to remove SQLite test database: \(error)")
        }
    }
}

extension DBContainer {
    static func open(
        for schema: Schema,
        configuration: SQLiteStorageEngine.Configuration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: configuration)
        return try await open(
            for: schema,
            configuration: try .testing(
                storageEngine: engine),
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }

    static func open<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: SQLiteStorageEngine.Configuration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: configuration)
        return try await open(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: try .testing(
                storageEngine: engine),
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }

    static func inMemory(
        for schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        return try await open(
            for: schema,
            configuration: try .testing(
                storageEngine: engine),
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }

    static func sqlite(
        for schema: Schema,
        path: String,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .file(path))
        return try await open(
            for: schema,
            configuration: try .testing(
                storageEngine: engine),
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }
}
#endif

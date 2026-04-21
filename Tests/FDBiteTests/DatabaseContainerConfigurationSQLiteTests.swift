#if SQLITE
import Foundation
import Database
import Testing
import TestHeartbeat

@Persistable(type: "SQLiteFacadeUser")
struct SQLiteFacadeUserV1 {
    #Directory<SQLiteFacadeUserV1>("sqlite", "facade", "users")

    var name: String
}

@Persistable(type: "SQLiteFacadeUser")
struct SQLiteFacadeUserV2 {
    #Directory<SQLiteFacadeUserV2>("sqlite", "facade", "users")

    var name: String
    var age: Int = 0
}

enum SQLiteFacadeSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [SQLiteFacadeUserV1.self]
}

enum SQLiteFacadeSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [SQLiteFacadeUserV2.self]
}

enum SQLiteFacadeMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteFacadeSchemaV1.self, SQLiteFacadeSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteFacadeSchemaV1.self,
                toVersion: SQLiteFacadeSchemaV2.self
            )
        ]
    }
}

@Suite("Database Container Configuration SQLite Tests", .serialized, .heartbeat)
struct DatabaseContainerConfigurationSQLiteTests {
    @Test("Database facade accepts SQLite configuration through the common label")
    func sqliteConfigurationRoundTrip() async throws {
        let schema = Schema([SQLiteFacadeUserV1.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: SQLiteStorageEngine.Configuration.inMemory,
            security: .disabled
        )

        let context = container.newContext()
        var user = SQLiteFacadeUserV1(name: "Alice")
        user.id = "sqlite-facade-user"
        context.insert(user)
        try await context.save()

        let fetched = try await context.fetch(SQLiteFacadeUserV1.self).execute()
        #expect(fetched.count == 1)
        #expect(fetched.first?.id == "sqlite-facade-user")
        #expect(fetched.first?.name == "Alice")
    }

    @Test("Database facade migration initializer accepts the same configuration label")
    func sqliteMigrationConfigurationRoundTrip() async throws {
        let dbPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-facade-\(UUID().uuidString).sqlite")
            .path
        defer { try? FileManager.default.removeItem(atPath: dbPath) }

        let initialContainer = try await DBContainer(
            for: SQLiteFacadeSchemaV1.makeSchema(),
            configuration: SQLiteStorageEngine.Configuration.file(dbPath),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()
        var user = SQLiteFacadeUserV1(name: "Bob")
        user.id = "sqlite-facade-migration"
        initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(SQLiteFacadeSchemaV1.versionIdentifier)

        let migratedContainer = try await DBContainer(
            for: SQLiteFacadeSchemaV2.self,
            migrationPlan: SQLiteFacadeMigrationPlan.self,
            configuration: SQLiteStorageEngine.Configuration.file(dbPath)
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer(
            for: SQLiteFacadeSchemaV2.makeSchema(),
            configuration: SQLiteStorageEngine.Configuration.file(dbPath),
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()
        let fetched = try await verificationContext.fetch(SQLiteFacadeUserV2.self).execute()

        #expect(fetched.count == 1)
        #expect(fetched.first?.id == "sqlite-facade-migration")
        #expect(fetched.first?.name == "Bob")
        #expect(fetched.first?.age == 0)
    }
}
#endif

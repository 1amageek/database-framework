#if SQLITE
import Foundation
import Database
import Testing
import TestHeartbeat
import DatabaseRuntime

@Persistable(type: "SQLiteFacadeUser")
struct SQLiteFacadeUserV1 {
    #Directory<SQLiteFacadeUserV1>("sqlite", "facade", "users")

    var id: String = ""
    var name: String
}

@Persistable(type: "SQLiteFacadeUser")
struct SQLiteFacadeUserV2 {
    #Directory<SQLiteFacadeUserV2>("sqlite", "facade", "users")

    var id: String = ""
    var name: String
    var age: Int64 = 0
}

enum SQLiteFacadeSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteFacadeUserV1.schemaEntity]
        }
    }
}

enum SQLiteFacadeSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteFacadeUserV2.schemaEntity]
        }
    }
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
        let schema = try Schema(
            entities: [try SQLiteFacadeUserV1.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: SQLiteStorageEngine.Configuration.inMemory,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [SQLiteFacadeUserV1.self]
            ),
            security: .disabled
        )

        let context = container.newContext()
        var user = SQLiteFacadeUserV1(name: "Alice")
        user.id = "sqlite-facade-user"
        try context.insert(user)
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
        defer {
            do {
                try FileManager.default.removeItem(atPath: dbPath)
            } catch {
                Issue.record("Failed to remove SQLite migration fixture: \(error)")
            }
        }

        let initialContainer = try await DBContainer.open(
            for: SQLiteFacadeSchemaV1.makeSchema(),
            configuration: SQLiteStorageEngine.Configuration.file(dbPath),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteFacadeUserV1.self]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()
        var user = SQLiteFacadeUserV1(name: "Bob")
        user.id = "sqlite-facade-migration"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: SQLiteFacadeSchemaV1.versionIdentifier)

        let migratedContainer = try await DBContainer.open(
            for: SQLiteFacadeSchemaV2.self,
            migrationPlan: SQLiteFacadeMigrationPlan.self,
            configuration: SQLiteStorageEngine.Configuration.file(dbPath),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteFacadeUserV2.self])
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteFacadeSchemaV2.makeSchema(),
            configuration: SQLiteStorageEngine.Configuration.file(dbPath),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteFacadeUserV2.self]),
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

#if SQLITE
import Testing
import Foundation
import Database
import TestHeartbeat
import DatabaseRuntime

@Persistable(type: "SQLiteDirectoryMigrationUser")
struct SQLiteDirectoryMigrationUserV1 {
    #Directory<SQLiteDirectoryMigrationUserV1>("sqlite-dir-migration", "legacy")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteDirectoryMigrationUser")
struct SQLiteDirectoryMigrationUserV2 {
    #Directory<SQLiteDirectoryMigrationUserV2>("sqlite-dir-migration", "current")

    var id: String = ""
    var name: String
    var email: String
}

enum SQLiteDirectoryMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteDirectoryMigrationUserV1.schemaEntity]
        }
    }
}

enum SQLiteDirectoryMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteDirectoryMigrationUserV2.schemaEntity]
        }
    }
}

enum SQLiteDirectoryMigrationCopyPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteDirectoryMigrationSchemaV1.self, SQLiteDirectoryMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLiteDirectoryMigrationSchemaV1.self,
                toVersion: SQLiteDirectoryMigrationSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [SQLiteDirectoryMigrationUserV2] = []
        for try await legacyUser in context.enumerate(SQLiteDirectoryMigrationUserV1.self) {
            var newUser = SQLiteDirectoryMigrationUserV2(
                name: legacyUser.name,
                email: legacyUser.email
            )
            newUser.id = legacyUser.id
            copied.append(newUser)
        }
        guard !copied.isEmpty else { return }
        try await context.batchUpdate(copied, batchSize: 100)
    }

    static func purgeLegacyDirectory(context: MigrationContext) async throws {
        try await context.purgeSourceSchemaStorage(SQLiteDirectoryMigrationUserV1.self)
    }
}

@Suite("Directory Migration SQLite Tests", .serialized, .heartbeat)
struct DirectoryMigrationSQLiteTests {
    @Test("Custom migration copies data across changed #Directory paths on SQLite")
    func customMigrationCopiesAcrossDirectoryChange() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let seededID = "sqlite-dir-migration-\(UUID().uuidString)"

        let initialContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteDirectoryMigrationUserV1.self]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()
        var seededUser = SQLiteDirectoryMigrationUserV1(name: "Alice", email: "alice@example.com")
        seededUser.id = seededID
        try initialContext.insert(seededUser)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV2.self,
            migrationPlan: SQLiteDirectoryMigrationCopyPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteDirectoryMigrationUserV2.self])
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SQLiteDirectoryMigrationUserV2.self]),
            security: .disabled
        )
        let rows = try await verificationContainer.newContext()
            .fetch(SQLiteDirectoryMigrationUserV2.self)
            .execute()

        #expect(rows.count == 1)
        let migrated = try #require(rows.first { $0.id == seededID })
        #expect(migrated.name == "Alice")
        #expect(migrated.email == "alice@example.com")
    }
}
#endif

#if SQLITE
import Testing
import TestSupport
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
        let database = try SQLiteTestDatabase(prefix: "directory-migration")
        defer { database.remove() }
        let seededID = "sqlite-dir-migration-\(UUID().uuidString)"

        let initialContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteDirectoryMigrationUserV1.self)]),
            security: .testingDisabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.testBaseContext()
        var seededUser = SQLiteDirectoryMigrationUserV1(name: "Alice", email: "alice@example.com")
        seededUser.id = seededID
        try initialContext.insert(seededUser)
        try await initialContext.save()
        try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV2.self,
            migrationPlan: SQLiteDirectoryMigrationCopyPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteDirectoryMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.testBaseAdmin().migrateIfNeeded()
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteDirectoryMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteDirectoryMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await verificationContainer.shutdown() }
        let rows = try await verificationContainer.testBaseContext()
            .fetch(SQLiteDirectoryMigrationUserV2.self)
            .execute()

        #expect(rows.count == 1)
        let migrated = try #require(rows.first { $0.id == seededID })
        #expect(migrated.name == "Alice")
        #expect(migrated.email == "alice@example.com")
    }
}
#endif

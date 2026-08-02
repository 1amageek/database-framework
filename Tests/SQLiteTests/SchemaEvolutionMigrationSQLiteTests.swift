#if SQLITE
import Testing
import TestSupport
import Foundation
import Database
import TestHeartbeat
import DatabaseRuntime

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserV2 {
    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserReordered {
    var id: String = ""
    var email: String
    var name: String
}

enum SQLiteSchemaEvolutionSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteSchemaEvolutionUserV1.schemaEntity]
        }
    }
}

enum SQLiteSchemaEvolutionSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteSchemaEvolutionUserV2.schemaEntity]
        }
    }
}

@Persistable(type: "SQLiteMigratedUser")
struct SQLiteMigratedUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteMigratedUser")
struct SQLiteMigratedUserV2 {
    #Index(
        .scalar,
        fields: [\SQLiteMigratedUserV2.fullName],
        name: "SQLiteMigratedUser_fullName"
    )

    var id: String = ""
    var fullName: String
    var email: String
}

enum SQLiteAppendOnlyMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteSchemaEvolutionSchemaV1.self, SQLiteSchemaEvolutionSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteSchemaEvolutionSchemaV1.self,
                toVersion: SQLiteSchemaEvolutionSchemaV2.self
            )
        ]
    }
}

enum SQLiteMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteMigratedUserV1.schemaEntity]
        }
    }
}

enum SQLiteMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteMigratedUserV2.schemaEntity]
        }
    }
}

enum SQLiteCustomMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteMigrationSchemaV1.self, SQLiteMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLiteMigrationSchemaV1.self,
                toVersion: SQLiteMigrationSchemaV2.self,
                willMigrate: migrateLegacyUsers,
                didMigrate: nil
            )
        ]
    }

    static func migrateLegacyUsers(context: MigrationContext) async throws {
        var migratedUsers: [SQLiteMigratedUserV2] = []

        for try await legacyUser in context.enumerate(SQLiteMigratedUserV1.self) {
            var migratedUser = SQLiteMigratedUserV2(
                fullName: legacyUser.name,
                email: legacyUser.email
            )
            migratedUser.id = legacyUser.id
            migratedUsers.append(migratedUser)
        }

        guard !migratedUsers.isEmpty else {
            return
        }

        try await context.batchUpdate(migratedUsers, batchSize: 100)
    }
}

@Suite("Schema Evolution Migration SQLite Tests", .serialized, .heartbeat)
struct SchemaEvolutionMigrationSQLiteTests {
    @Test("Lightweight migration keeps existing SQLite data readable end-to-end")
    func lightweightMigrationPreservesExistingDataEndToEnd() async throws {
        let database = try SQLiteTestDatabase(prefix: "schema-evolution-lightweight")
        defer { database.remove() }

        let initialContainer = try await DBContainer.open(
            for: SQLiteSchemaEvolutionSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSchemaEvolutionUserV1.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()
        var user = SQLiteSchemaEvolutionUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-lightweight-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteSchemaEvolutionSchemaV2.self,
            migrationPlan: SQLiteAppendOnlyMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSchemaEvolutionUserV2.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteSchemaEvolutionSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSchemaEvolutionUserV2.self)]),
            security: .disabled
        )
        defer { await verificationContainer.shutdown() }
        let migratedContext = verificationContainer.newContext()
        let migratedUsers = try await migratedContext.fetch(SQLiteSchemaEvolutionUserV2.self).execute()

        #expect(migratedUsers.count == 1)
        #expect(migratedUsers.first?.id == "sqlite-lightweight-user")
        #expect(migratedUsers.first?.name == "Alice")
        #expect(migratedUsers.first?.email == "alice@example.com")
        #expect(migratedUsers.first?.age == 0)
    }

    @Test("SchemaRegistry accepts append-only fields on SQLite")
    func schemaRegistryAcceptsAppendOnlyFields() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.waitUntilShutdown() }
        let registry = SchemaRegistry(database: engine, clock: TestProcessMonotonicClock())

        try await registry.persist(Schema(entities: [try SQLiteSchemaEvolutionUserV1.schemaEntity]))
        try await registry.persist(Schema(entities: [try SQLiteSchemaEvolutionUserV2.schemaEntity]))

        let entity = try await registry.load(typeName: SQLiteSchemaEvolutionUserV1.persistableType)
        #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
        #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
        #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
    }

    @Test("SchemaRegistry rejects reordered fields on SQLite")
    func schemaRegistryRejectsReorderedFields() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.waitUntilShutdown() }
        let registry = SchemaRegistry(database: engine, clock: TestProcessMonotonicClock())
        let typeName = SQLiteSchemaEvolutionUserV1.persistableType

        try await registry.persist(Schema(entities: [try SQLiteSchemaEvolutionUserV1.schemaEntity]))

        do {
            try await registry.persist(Schema(entities: [try SQLiteSchemaEvolutionUserReordered.schemaEntity]))
            Issue.record("Expected incompatibleEntityEvolution error")
        } catch let error as SchemaRegistryError {
            if case .incompatibleEntityEvolution(let entityName, let issues) = error {
                #expect(entityName == typeName)
                #expect(
                    issues.contains(
                        .renumberedField(
                            entityName: typeName,
                            fieldName: "email",
                            expected: 3,
                            actual: 2
                        )
                    )
                )
            } else {
                Issue.record("Unexpected schema registry error: \(error)")
            }
        }
    }

    @Test("Custom migration persists breaking schema changes on SQLite")
    func customMigrationPersistsBreakingSchemaChanges() async throws {
        let database = try SQLiteTestDatabase(prefix: "schema-evolution-breaking")
        defer { database.remove() }
        let seededID = "sqlite-breaking-\(UUID().uuidString)"

        let initialContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV1.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()
        var seededUser = SQLiteMigratedUserV1(name: "Charlie", email: "charlie@example.com")
        seededUser.id = seededID
        try initialContext.insert(seededUser)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV2.self,
            migrationPlan: SQLiteCustomMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV2.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()

        let registry = SchemaRegistry(
            database: migratedContainer.engine,
            clock: TestProcessMonotonicClock()
        )
        let entity = try await registry.load(typeName: SQLiteMigratedUserV1.persistableType)
        let version = try await migratedContainer.getCurrentSchemaVersion()

        #expect(version == Schema.Version(2, 0, 0))
        #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
        #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
        #expect(entity?.fieldMapByName["name"] == nil)
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV2.self)]),
            security: .disabled
        )
        defer { await verificationContainer.shutdown() }
        let migratedUsers = try await verificationContainer.newContext()
            .fetch(SQLiteMigratedUserV2.self)
            .execute()
        let migratedUser = migratedUsers.first { $0.id == seededID }

        #expect(migratedUsers.count == 1)
        #expect(migratedUser?.fullName == "Charlie")
        #expect(migratedUser?.email == "charlie@example.com")
    }

    @Test("Custom migration transforms SQLite data end-to-end")
    func customMigrationTransformsDataEndToEnd() async throws {
        let database = try SQLiteTestDatabase(prefix: "schema-evolution-transform")
        defer { database.remove() }

        let initialContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV1.self)]),
            security: .disabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.newContext()

        var firstUser = SQLiteMigratedUserV1(name: "Alice", email: "alice@example.com")
        firstUser.id = "sqlite-migrated-user-1"
        try initialContext.insert(firstUser)

        var secondUser = SQLiteMigratedUserV1(name: "Bob", email: "bob@example.com")
        secondUser.id = "sqlite-migrated-user-2"
        try initialContext.insert(secondUser)

        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV2.self,
            migrationPlan: SQLiteCustomMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV2.self)]),
            security: .disabled
        )
        defer { await migratedContainer.shutdown() }
        try await migratedContainer.migrateIfNeeded()
        await migratedContainer.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteMigratedUserV2.self)]),
            security: .disabled
        )
        defer { await verificationContainer.shutdown() }
        let migratedContext = verificationContainer.newContext()
        let migratedUsers = try await migratedContext
            .fetch(SQLiteMigratedUserV2.self)
            .orderBy(SQLiteMigratedUserV2.fields.fullName)
            .execute()

        #expect(migratedUsers.count == 2)
        #expect(migratedUsers.map { $0.id } == ["sqlite-migrated-user-1", "sqlite-migrated-user-2"])
        #expect(migratedUsers.map { $0.fullName } == ["Alice", "Bob"])
        #expect(migratedUsers.map { $0.email } == ["alice@example.com", "bob@example.com"])
    }
}
#endif

#if SQLITE
import Testing
import Database
import TestHeartbeat

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserV1 {
    var name: String
    var email: String
}

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserV2 {
    var name: String
    var email: String
    var age: Int = 0
}

@Persistable(type: "SQLiteSchemaEvolutionUser")
struct SQLiteSchemaEvolutionUserReordered {
    var email: String
    var name: String
}

enum SQLiteSchemaEvolutionSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [SQLiteSchemaEvolutionUserV1.self]
}

enum SQLiteSchemaEvolutionSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [SQLiteSchemaEvolutionUserV2.self]
}

@Persistable(type: "SQLiteMigratedUser")
struct SQLiteMigratedUserV1 {
    var name: String
    var email: String
}

@Persistable(type: "SQLiteMigratedUser")
struct SQLiteMigratedUserV2 {
    #Index(ScalarIndexKind<SQLiteMigratedUserV2>(fields: [\.fullName]), name: "SQLiteMigratedUser_fullName")

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
    static let models: [any Persistable.Type] = [SQLiteMigratedUserV1.self]
}

enum SQLiteMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [SQLiteMigratedUserV2.self]
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let initialContainer = try await DBContainer(
            for: SQLiteSchemaEvolutionSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()
        var user = SQLiteSchemaEvolutionUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-lightweight-user"
        initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: SQLiteSchemaEvolutionSchemaV2.self,
            migrationPlan: SQLiteAppendOnlyMigrationPlan.self,
            configuration: .init(backend: .custom(engine))
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer(
            for: SQLiteSchemaEvolutionSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
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
        let registry = SchemaRegistry(database: engine)

        try await registry.persist(Schema([SQLiteSchemaEvolutionUserV1.self]))
        try await registry.persist(Schema([SQLiteSchemaEvolutionUserV2.self]))

        let entity = try await registry.load(typeName: SQLiteSchemaEvolutionUserV1.persistableType)
        #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
        #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
        #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
    }

    @Test("SchemaRegistry rejects reordered fields on SQLite")
    func schemaRegistryRejectsReorderedFields() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let registry = SchemaRegistry(database: engine)
        let typeName = SQLiteSchemaEvolutionUserV1.persistableType

        try await registry.persist(Schema([SQLiteSchemaEvolutionUserV1.self]))

        do {
            try await registry.persist(Schema([SQLiteSchemaEvolutionUserReordered.self]))
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let initialContainer = try await DBContainer(
            for: SQLiteMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: SQLiteMigrationSchemaV2.self,
            migrationPlan: SQLiteCustomMigrationPlan.self,
            configuration: .init(backend: .custom(engine))
        )
        try await migratedContainer.migrateIfNeeded()

        let registry = SchemaRegistry(database: engine)
        let entity = try await registry.load(typeName: SQLiteMigratedUserV1.persistableType)
        let version = try await migratedContainer.getCurrentSchemaVersion()

        #expect(version == Schema.Version(2, 0, 0))
        #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
        #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
        #expect(entity?.fieldMapByName["name"] == nil)
    }

    @Test("Custom migration transforms SQLite data end-to-end")
    func customMigrationTransformsDataEndToEnd() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let initialContainer = try await DBContainer(
            for: SQLiteMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var firstUser = SQLiteMigratedUserV1(name: "Alice", email: "alice@example.com")
        firstUser.id = "sqlite-migrated-user-1"
        initialContext.insert(firstUser)

        var secondUser = SQLiteMigratedUserV1(name: "Bob", email: "bob@example.com")
        secondUser.id = "sqlite-migrated-user-2"
        initialContext.insert(secondUser)

        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: SQLiteMigrationSchemaV2.self,
            migrationPlan: SQLiteCustomMigrationPlan.self,
            configuration: .init(backend: .custom(engine))
        )
        try await migratedContainer.migrateIfNeeded()

        let verificationContainer = try await DBContainer(
            for: SQLiteMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let migratedContext = verificationContainer.newContext()
        let migratedUsers = try await migratedContext
            .fetch(SQLiteMigratedUserV2.self)
            .orderBy(\.fullName)
            .execute()

        #expect(migratedUsers.count == 2)
        #expect(migratedUsers.map(\.id) == ["sqlite-migrated-user-1", "sqlite-migrated-user-2"])
        #expect(migratedUsers.map(\.fullName) == ["Alice", "Bob"])
        #expect(migratedUsers.map(\.email) == ["alice@example.com", "bob@example.com"])
    }
}
#endif

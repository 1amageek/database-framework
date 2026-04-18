#if POSTGRESQL
import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import Core
@testable import ScalarIndex
import TestSupport
import TestHeartbeat

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserV1 {
    #Directory<PGSchemaEvolutionUserV1>("test", "pg-migration", "schema-evolution")

    var name: String
    var email: String
}

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserV2 {
    #Directory<PGSchemaEvolutionUserV2>("test", "pg-migration", "schema-evolution")

    var name: String
    var email: String
    var age: Int = 0
}

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserReordered {
    #Directory<PGSchemaEvolutionUserReordered>("test", "pg-migration", "schema-evolution")

    var email: String
    var name: String
}

enum PGSchemaEvolutionSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [PGSchemaEvolutionUserV1.self]
}

enum PGSchemaEvolutionSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [PGSchemaEvolutionUserV2.self]
}

enum PGAppendOnlyMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PGSchemaEvolutionSchemaV1.self, PGSchemaEvolutionSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: PGSchemaEvolutionSchemaV1.self,
                toVersion: PGSchemaEvolutionSchemaV2.self
            )
        ]
    }
}

@Persistable(type: "PGMigratedUser")
struct PGMigratedUserV1 {
    #Directory<PGMigratedUserV1>("test", "pg-migration", "migrated-user")

    var name: String
    var email: String
}

@Persistable(type: "PGMigratedUser")
struct PGMigratedUserV2 {
    #Directory<PGMigratedUserV2>("test", "pg-migration", "migrated-user")
    #Index(ScalarIndexKind<PGMigratedUserV2>(fields: [\.fullName]), name: "PGMigratedUser_fullName")

    var fullName: String
    var email: String
}

enum PGMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [PGMigratedUserV1.self]
}

enum PGMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [PGMigratedUserV2.self]
}

enum PGCustomMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PGMigrationSchemaV1.self, PGMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: PGMigrationSchemaV1.self,
                toVersion: PGMigrationSchemaV2.self,
                willMigrate: migrateLegacyUsers,
                didMigrate: nil
            )
        ]
    }

    static func migrateLegacyUsers(context: MigrationContext) async throws {
        var migratedUsers: [PGMigratedUserV2] = []

        for try await legacyUser in context.enumerate(PGMigratedUserV1.self) {
            var migratedUser = PGMigratedUserV2(
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

@Suite("Schema Evolution Migration PostgreSQL Tests", .serialized, .heartbeat)
struct SchemaEvolutionMigrationPostgreSQLTests {
    @Test("Lightweight migration keeps existing PostgreSQL data readable end-to-end")
    func lightweightMigrationPreservesExistingDataEndToEnd() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine

            let initialContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGSchemaEvolutionSchemaV1.makeSchema()
            )
            let initialContext = initialContainer.newContext()

            var user = PGSchemaEvolutionUserV1(name: "Alice", email: "alice@example.com")
            user.id = "pg-lightweight-user"
            initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: PGSchemaEvolutionSchemaV2.self,
                migrationPlan: PGAppendOnlyMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGSchemaEvolutionSchemaV2.makeSchema()
            )
            let migratedContext = verificationContainer.newContext()
            let migratedUsers = try await migratedContext
                .fetch(PGSchemaEvolutionUserV2.self)
                .execute()

            #expect(migratedUsers.count == 1)
            #expect(migratedUsers.first?.id == "pg-lightweight-user")
            #expect(migratedUsers.first?.name == "Alice")
            #expect(migratedUsers.first?.email == "alice@example.com")
            #expect(migratedUsers.first?.age == 0)
        }
    }

    @Test("SchemaRegistry accepts append-only fields on PostgreSQL")
    func schemaRegistryAcceptsAppendOnlyFields() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine
            let registry = SchemaRegistry(database: engine)

            try await registry.persist(Schema([PGSchemaEvolutionUserV1.self]))
            try await registry.persist(Schema([PGSchemaEvolutionUserV2.self]))

            let entity = try await registry.load(typeName: PGSchemaEvolutionUserV1.persistableType)
            #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        }
    }

    @Test("SchemaRegistry rejects reordered fields on PostgreSQL")
    func schemaRegistryRejectsReorderedFields() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine
            let registry = SchemaRegistry(database: engine)
            let typeName = PGSchemaEvolutionUserV1.persistableType

            try await registry.persist(Schema([PGSchemaEvolutionUserV1.self]))

            do {
                try await registry.persist(Schema([PGSchemaEvolutionUserReordered.self]))
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
    }

    @Test("Custom migration persists breaking schema changes on PostgreSQL")
    func customMigrationPersistsBreakingSchemaChanges() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine
            let seededID = "pg-breaking-\(UUID().uuidString)"

            let initialContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGMigrationSchemaV1.makeSchema()
            )
            let initialContext = initialContainer.newContext()
            var seededUser = PGMigratedUserV1(name: "Charlie", email: "charlie@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: PGMigrationSchemaV2.self,
                migrationPlan: PGCustomMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let registry = SchemaRegistry(database: engine)
            let entity = try await registry.load(typeName: PGMigratedUserV1.persistableType)
            let version = try await migratedContainer.getCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["name"] == nil)

            let verificationContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGMigrationSchemaV2.makeSchema()
            )
            let migratedUsers = try await verificationContainer.newContext()
                .fetch(PGMigratedUserV2.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == seededID }

            #expect(migratedUsers.count == 1)
            #expect(migratedUser?.fullName == "Charlie")
            #expect(migratedUser?.email == "charlie@example.com")
        }
    }

    @Test("Custom migration transforms PostgreSQL data end-to-end")
    func customMigrationTransformsDataEndToEnd() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine

            let initialContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGMigrationSchemaV1.makeSchema()
            )
            let initialContext = initialContainer.newContext()

            var firstUser = PGMigratedUserV1(name: "Alice", email: "alice@example.com")
            firstUser.id = "pg-migrated-user-1"
            initialContext.insert(firstUser)

            var secondUser = PGMigratedUserV1(name: "Bob", email: "bob@example.com")
            secondUser.id = "pg-migrated-user-2"
            initialContext.insert(secondUser)

            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: PGMigrationSchemaV2.self,
                migrationPlan: PGCustomMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGMigrationSchemaV2.makeSchema()
            )
            let migratedContext = verificationContainer.newContext()
            let migratedUsers = try await migratedContext
                .fetch(PGMigratedUserV2.self)
                .orderBy(\.fullName)
                .execute()

            #expect(migratedUsers.count == 2)
            #expect(migratedUsers.map(\.id) == ["pg-migrated-user-1", "pg-migrated-user-2"])
            #expect(migratedUsers.map(\.fullName) == ["Alice", "Bob"])
            #expect(migratedUsers.map(\.email) == ["alice@example.com", "bob@example.com"])
        }
    }
}
#endif

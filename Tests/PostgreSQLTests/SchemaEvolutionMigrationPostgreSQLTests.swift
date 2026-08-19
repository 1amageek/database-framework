#if POSTGRESQL
import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import DatabaseKit
@testable import ScalarIndex
import TestSupport
import TestHeartbeat
import DatabaseRuntime

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserV1 {
    #Directory<PGSchemaEvolutionUserV1>("test", "pg-migration", "schema-evolution")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserV2 {
    #Directory<PGSchemaEvolutionUserV2>("test", "pg-migration", "schema-evolution")

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "PGSchemaEvolutionUser")
struct PGSchemaEvolutionUserReordered {
    #Directory<PGSchemaEvolutionUserReordered>("test", "pg-migration", "schema-evolution")

    var id: String = ""
    var email: String
    var name: String
}

enum PGSchemaEvolutionSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGSchemaEvolutionUserV1.schemaEntity]
        }
    }
}

enum PGSchemaEvolutionSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGSchemaEvolutionUserV2.schemaEntity]
        }
    }
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

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "PGMigratedUser")
struct PGMigratedUserV2 {
    #Directory<PGMigratedUserV2>("test", "pg-migration", "migrated-user")
    #Index(
        .ordered(
            name: "PGMigratedUser_fullName", keys: [.ascending(\PGMigratedUserV2.fullName)],
            unique: false))

    var id: String = ""
    var fullName: String
    var email: String
}

enum PGMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGMigratedUserV1.schemaEntity]
        }
    }
}

enum PGMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGMigratedUserV2.schemaEntity]
        }
    }
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

@Suite("Schema Evolution Migration PostgreSQL Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct SchemaEvolutionMigrationPostgreSQLTests {
    @Test("Lightweight migration keeps existing PostgreSQL data readable end-to-end")
    func lightweightMigrationPreservesExistingDataEndToEnd() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGSchemaEvolutionSchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGSchemaEvolutionUserV1.self)]
            )
            let initialContext = initialContainer.testBaseContext()

            var user = PGSchemaEvolutionUserV1(name: "Alice", email: "alice@example.com")
            user.id = "pg-lightweight-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGSchemaEvolutionSchemaV2.self,
                migrationPlan: PGAppendOnlyMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGSchemaEvolutionUserV2.self)])
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGSchemaEvolutionSchemaV2.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGSchemaEvolutionUserV2.self)]
            )
            let migratedContext = verificationContainer.testBaseContext()
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
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine
            let registry = SchemaRegistry(
                database: engine,
                root: Subspace(),
                clock: TestProcessMonotonicClock()
            )

            try await registry.persist(Schema(entities: [try PGSchemaEvolutionUserV1.schemaEntity]))
            try await registry.persist(Schema(entities: [try PGSchemaEvolutionUserV2.schemaEntity]))

            let entity = try await registry.load(typeName: PGSchemaEvolutionUserV1.persistableType)
            #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        }
    }

    @Test("SchemaRegistry rejects reordered fields on PostgreSQL")
    func schemaRegistryRejectsReorderedFields() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine
            let registry = SchemaRegistry(
                database: engine,
                root: Subspace(),
                clock: TestProcessMonotonicClock()
            )
            let typeName = PGSchemaEvolutionUserV1.persistableType

            try await registry.persist(Schema(entities: [try PGSchemaEvolutionUserV1.schemaEntity]))

            do {
                try await registry.persist(Schema(entities: [try PGSchemaEvolutionUserReordered.schemaEntity]))
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
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine
            let seededID = "pg-breaking-\(UUID().uuidString)"

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGMigrationSchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV1.self)]
            )
            let initialContext = initialContainer.testBaseContext()
            var seededUser = PGMigratedUserV1(name: "Charlie", email: "charlie@example.com")
            seededUser.id = seededID
            try initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGMigrationSchemaV2.self,
                migrationPlan: PGCustomMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV2.self)])
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let entity = try await migratedContainer
                .testBaseSchemaDefinition()?
                .entity(named: PGMigratedUserV1.persistableType)
            let version = try await migratedContainer.testBaseCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["name"] == nil)

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGMigrationSchemaV2.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV2.self)]
            )
            let migratedUsers = try await verificationContainer.testBaseContext()
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
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGMigrationSchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV1.self)]
            )
            let initialContext = initialContainer.testBaseContext()

            var firstUser = PGMigratedUserV1(name: "Alice", email: "alice@example.com")
            firstUser.id = "pg-migrated-user-1"
            try initialContext.insert(firstUser)

            var secondUser = PGMigratedUserV1(name: "Bob", email: "bob@example.com")
            secondUser.id = "pg-migrated-user-2"
            try initialContext.insert(secondUser)

            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGMigrationSchemaV2.self,
                migrationPlan: PGCustomMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV2.self)])
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGMigrationSchemaV2.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGMigratedUserV2.self)]
            )
            let migratedContext = verificationContainer.testBaseContext()
            let migratedUsers = try await migratedContext
                .fetch(PGMigratedUserV2.self)
                .orderBy(PGMigratedUserV2.fields.fullName)
                .execute()

            #expect(migratedUsers.count == 2)
            #expect(migratedUsers.map(\.id) == ["pg-migrated-user-1", "pg-migrated-user-2"])
            #expect(migratedUsers.map(\.fullName) == ["Alice", "Bob"])
            #expect(migratedUsers.map(\.email) == ["alice@example.com", "bob@example.com"])
        }
    }
}
#endif

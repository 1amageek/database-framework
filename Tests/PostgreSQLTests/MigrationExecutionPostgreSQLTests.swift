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

private actor PGMigrationEventRecorder {
    private var events: [String] = []

    func reset() {
        events = []
    }

    func record(_ event: String) {
        events.append(event)
    }

    func snapshot() -> [String] {
        events
    }
}

private let pgMigrationEventRecorder = PGMigrationEventRecorder()

private enum PGMigrationExecutionError: Error {
    case expectedFailure
}

private func pgVersionLabel(_ version: Schema.Version?) -> String {
    version.map(\.description) ?? "nil"
}

@Persistable(type: "PGStageBoundaryUser")
struct PGStageBoundaryUserV1 {
    #Directory<PGStageBoundaryUserV1>("test", "pg-migration", "stage-boundary")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "PGStageBoundaryUser")
struct PGStageBoundaryUserV2 {
    #Directory<PGStageBoundaryUserV2>("test", "pg-migration", "stage-boundary")

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "PGStageBoundaryUser")
struct PGStageBoundaryUserV3 {
    #Directory<PGStageBoundaryUserV3>("test", "pg-migration", "stage-boundary")
    #Index(
        .scalar,
        fields: [\PGStageBoundaryUserV3.fullName],
        name: "PGStageBoundaryUser_fullName"
    )

    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum PGStageBoundarySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageBoundaryUserV1.schemaEntity]
        }
    }
}

enum PGStageBoundarySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageBoundaryUserV2.schemaEntity]
        }
    }
}

enum PGStageBoundarySchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageBoundaryUserV3.schemaEntity]
        }
    }
}

enum PGStageBoundaryMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PGStageBoundarySchemaV1.self, PGStageBoundarySchemaV2.self, PGStageBoundarySchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: PGStageBoundarySchemaV1.self,
                toVersion: PGStageBoundarySchemaV2.self
            ),
            .custom(
                fromVersion: PGStageBoundarySchemaV2.self,
                toVersion: PGStageBoundarySchemaV3.self,
                willMigrate: migrateUsers,
                didMigrate: auditStage
            )
        ]
    }

    static func migrateUsers(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await pgMigrationEventRecorder.record("will:\(pgVersionLabel(currentVersion))")

        var migratedUsers: [PGStageBoundaryUserV3] = []
        for try await legacyUser in context.enumerate(PGStageBoundaryUserV2.self) {
            var migratedUser = PGStageBoundaryUserV3(
                fullName: legacyUser.name,
                email: legacyUser.email,
                age: legacyUser.age
            )
            migratedUser.id = legacyUser.id
            migratedUsers.append(migratedUser)
        }

        guard !migratedUsers.isEmpty else {
            return
        }

        try await context.batchUpdate(migratedUsers, batchSize: 100)
    }

    static func auditStage(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await pgMigrationEventRecorder.record("did:\(pgVersionLabel(currentVersion))")
    }
}

@Persistable(type: "PGStageFailureUser")
struct PGStageFailureUserV1 {
    #Directory<PGStageFailureUserV1>("test", "pg-migration", "stage-failure")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "PGStageFailureUser")
struct PGStageFailureUserV2 {
    #Directory<PGStageFailureUserV2>("test", "pg-migration", "stage-failure")

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "PGStageFailureUser")
struct PGStageFailureUserV3 {
    #Directory<PGStageFailureUserV3>("test", "pg-migration", "stage-failure")

    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum PGStageFailureSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageFailureUserV1.schemaEntity]
        }
    }
}

enum PGStageFailureSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageFailureUserV2.schemaEntity]
        }
    }
}

enum PGStageFailureSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try PGStageFailureUserV3.schemaEntity]
        }
    }
}

enum PGStageFailureMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PGStageFailureSchemaV1.self, PGStageFailureSchemaV2.self, PGStageFailureSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: PGStageFailureSchemaV1.self,
                toVersion: PGStageFailureSchemaV2.self
            ),
            .custom(
                fromVersion: PGStageFailureSchemaV2.self,
                toVersion: PGStageFailureSchemaV3.self,
                willMigrate: failStage,
                didMigrate: nil
            )
        ]
    }

    static func failStage(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await pgMigrationEventRecorder.record("fail:\(pgVersionLabel(currentVersion))")
        throw PGMigrationExecutionError.expectedFailure
    }
}

@Suite("Migration Execution PostgreSQL Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct MigrationExecutionPostgreSQLTests {
    @Test("Multi-stage migration executes in order and persists stage boundaries on PostgreSQL")
    func multiStageMigrationExecutesInOrderAndPersistsBetweenStages() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            await pgMigrationEventRecorder.reset()
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageBoundarySchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV1.self)]
            )
            let initialContext = initialContainer.newContext()

            var user = PGStageBoundaryUserV1(name: "Alice", email: "alice@example.com")
            user.id = "pg-stage-boundary-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGStageBoundarySchemaV3.self,
                migrationPlan: PGStageBoundaryMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV3.self)])
            )
            try await migratedContainer.migrateIfNeeded()

            let events = await pgMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageBoundarySchemaV3.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV3.self)]
            )
            let migratedUsers = try await verificationContainer.newContext()
                .fetch(PGStageBoundaryUserV3.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "pg-stage-boundary-user" }

            #expect(events == ["will:2.0.0", "did:2.0.0"])
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(migratedUser?.fullName == "Alice")
            #expect(migratedUser?.email == "alice@example.com")
            #expect(migratedUser?.age == 0)
        }
    }

    @Test("Failed later stage keeps earlier stage committed on PostgreSQL")
    func failedLaterStageKeepsEarlierStageCommitted() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            await pgMigrationEventRecorder.reset()
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageFailureSchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageFailureUserV1.self)]
            )
            let initialContext = initialContainer.newContext()

            var user = PGStageFailureUserV1(name: "Alice", email: "alice@example.com")
            user.id = "pg-stage-failure-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGStageFailureSchemaV3.self,
                migrationPlan: PGStageFailureMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageFailureUserV3.self)])
            )

            do {
                try await migratedContainer.migrateIfNeeded()
                Issue.record("Expected migration failure")
            } catch let error as PGMigrationExecutionError {
                #expect(error == .expectedFailure)
            }

            let events = await pgMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
            let registry = SchemaRegistry(
                database: migratedContainer.engine,
                clock: TestProcessMonotonicClock()
            )
            let entity = try await registry.load(typeName: PGStageFailureUserV1.persistableType)

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageFailureSchemaV2.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageFailureUserV2.self)]
            )
            let migratedUsers = try await verificationContainer.newContext()
                .fetch(PGStageFailureUserV2.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "pg-stage-failure-user" }

            #expect(events == ["fail:2.0.0"])
            #expect(currentVersion == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
            #expect(entity?.fieldMapByName["fullName"] == nil)
            #expect(migratedUser?.name == "Alice")
            #expect(migratedUser?.age == 0)
        }
    }

    @Test("Empty database bootstraps to latest schema without executing stages on PostgreSQL")
    func emptyDatabaseBootstrapsWithoutExecutingStages() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            await pgMigrationEventRecorder.reset()
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let migratedContainer = try await DBContainer.open(
                for: PGStageBoundarySchemaV3.self,
                migrationPlan: PGStageBoundaryMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV3.self)])
            )
            try await migratedContainer.migrateIfNeeded()

            let events = await pgMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
            let registry = SchemaRegistry(
                database: migratedContainer.engine,
                clock: TestProcessMonotonicClock()
            )
            let entity = try await registry.load(typeName: PGStageBoundaryUserV1.persistableType)

            #expect(events.isEmpty)
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["name"] == nil)
        }
    }

    @Test("Re-entrant migrateIfNeeded is idempotent on PostgreSQL")
    func reEntrantMigrateIsIdempotent() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            await pgMigrationEventRecorder.reset()
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let initialContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageBoundarySchemaV1.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV1.self)]
            )
            let initialContext = initialContainer.newContext()

            var user = PGStageBoundaryUserV1(name: "Alice", email: "alice@example.com")
            user.id = "pg-reentrant-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: PGStageBoundarySchemaV3.self,
                migrationPlan: PGStageBoundaryMigrationPlan.self,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV3.self)])
            )

            try await migratedContainer.migrateIfNeeded()
            let eventsAfterFirst = await pgMigrationEventRecorder.snapshot()

            try await migratedContainer.migrateIfNeeded()
            let eventsAfterSecond = await pgMigrationEventRecorder.snapshot()

            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()

            let verificationContainer = try await PostgreSQLScenarioCoordinator.shared.makeContainer(
                schema: PGStageBoundarySchemaV3.makeSchema(),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStageBoundaryUserV3.self)]
            )
            let migratedUsers = try await verificationContainer.newContext()
                .fetch(PGStageBoundaryUserV3.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "pg-reentrant-user" }

            #expect(eventsAfterFirst == ["will:2.0.0", "did:2.0.0"])
            #expect(eventsAfterSecond == eventsAfterFirst)
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(migratedUser?.fullName == "Alice")
        }
    }
}
#endif

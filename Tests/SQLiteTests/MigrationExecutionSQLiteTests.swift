#if SQLITE
import DatabaseTypes
import TestSupport
import Testing
import Foundation
import Database
import TestHeartbeat
import DatabaseRuntime

private actor SQLiteMigrationEventRecorder {
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

private let sqliteMigrationEventRecorder = SQLiteMigrationEventRecorder()

private enum SQLiteMigrationExecutionError: Error {
    case expectedFailure
}

private func versionLabel(_ version: Schema.Version?) -> String {
    version.map(\.description) ?? "nil"
}

private func countKeys(
    in subspace: Subspace,
    engine: any StorageEngine
) async throws -> Int {
    let range = subspace.range()
    return try await engine.withTransaction { transaction in
        var count = 0
        for _ in try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            snapshot: true
        ) {
            count += 1
        }
        return count
    }
}

private func value(
    for key: ByteString,
    engine: any StorageEngine
) async throws -> ByteString? {
    try await engine.withTransaction { transaction in
        try await transaction.getValue(for: key, snapshot: true)
    }
}

@Persistable(type: "SQLiteStageBoundaryUser")
struct SQLiteStageBoundaryUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteStageBoundaryUser")
struct SQLiteStageBoundaryUserV2 {
    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "SQLiteStageBoundaryUser")
struct SQLiteStageBoundaryUserV3 {
    #Index(
        .scalar,
        fields: [\SQLiteStageBoundaryUserV3.fullName],
        name: "SQLiteStageBoundaryUser_fullName"
    )

    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum SQLiteStageBoundarySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageBoundaryUserV1.schemaEntity]
        }
    }
}

enum SQLiteStageBoundarySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageBoundaryUserV2.schemaEntity]
        }
    }
}

enum SQLiteStageBoundarySchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageBoundaryUserV3.schemaEntity]
        }
    }
}

enum SQLiteStageBoundaryMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            SQLiteStageBoundarySchemaV1.self,
            SQLiteStageBoundarySchemaV2.self,
            SQLiteStageBoundarySchemaV3.self
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteStageBoundarySchemaV1.self,
                toVersion: SQLiteStageBoundarySchemaV2.self
            ),
            .custom(
                fromVersion: SQLiteStageBoundarySchemaV2.self,
                toVersion: SQLiteStageBoundarySchemaV3.self,
                willMigrate: migrateUsers,
                didMigrate: auditStage
            )
        ]
    }

    static func migrateUsers(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await sqliteMigrationEventRecorder.record("will:\(versionLabel(currentVersion))")

        var migratedUsers: [SQLiteStageBoundaryUserV3] = []
        for try await legacyUser in context.enumerate(SQLiteStageBoundaryUserV2.self) {
            var migratedUser = SQLiteStageBoundaryUserV3(
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
        await sqliteMigrationEventRecorder.record("did:\(versionLabel(currentVersion))")
    }
}

@Persistable(type: "SQLiteIndexLifecycleUser")
struct SQLiteIndexLifecycleUserV2 {
    #Index(
        .scalar,
        fields: [\SQLiteIndexLifecycleUserV2.email],
        name: "SQLiteIndexLifecycleUser_email"
    )
    #Index(
        .scalar,
        fields: [\SQLiteIndexLifecycleUserV2.age],
        name: "SQLiteIndexLifecycleUser_age"
    )

    var id: String = ""
    var name: String
    var email: String
    var age: Int64
}

@Persistable(type: "SQLiteIndexLifecycleUser")
struct SQLiteIndexLifecycleUserV3 {
    #Index(
        .scalar,
        fields: [\SQLiteIndexLifecycleUserV3.email],
        name: "SQLiteIndexLifecycleUser_email"
    )
    #Index(
        .scalar,
        fields: [\SQLiteIndexLifecycleUserV3.createdAt],
        name: "SQLiteIndexLifecycleUser_createdAt"
    )

    var id: String = ""
    var name: String
    var email: String
    var age: Int64
    var createdAt: Double = 0
}

enum SQLiteIndexLifecycleSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteIndexLifecycleUserV2.schemaEntity]
        }
    }
}

enum SQLiteIndexLifecycleSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteIndexLifecycleUserV3.schemaEntity]
        }
    }
}

enum SQLiteIndexLifecycleMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteIndexLifecycleSchemaV2.self, SQLiteIndexLifecycleSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteIndexLifecycleSchemaV2.self,
                toVersion: SQLiteIndexLifecycleSchemaV3.self
            )
        ]
    }
}

@Persistable(type: "SQLiteStageFailureUser")
struct SQLiteStageFailureUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteStageFailureUser")
struct SQLiteStageFailureUserV2 {
    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "SQLiteStageFailureUser")
struct SQLiteStageFailureUserV3 {
    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum SQLiteStageFailureSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageFailureUserV1.schemaEntity]
        }
    }
}

enum SQLiteStageFailureSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageFailureUserV2.schemaEntity]
        }
    }
}

enum SQLiteStageFailureSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteStageFailureUserV3.schemaEntity]
        }
    }
}

enum SQLiteStageFailureMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [
            SQLiteStageFailureSchemaV1.self,
            SQLiteStageFailureSchemaV2.self,
            SQLiteStageFailureSchemaV3.self
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SQLiteStageFailureSchemaV1.self,
                toVersion: SQLiteStageFailureSchemaV2.self
            ),
            .custom(
                fromVersion: SQLiteStageFailureSchemaV2.self,
                toVersion: SQLiteStageFailureSchemaV3.self,
                willMigrate: failStage,
                didMigrate: nil
            )
        ]
    }

    static func failStage(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await sqliteMigrationEventRecorder.record("fail:\(versionLabel(currentVersion))")
        throw SQLiteMigrationExecutionError.expectedFailure
    }
}

@Suite("Migration Execution SQLite Tests", .serialized, .heartbeat)
struct MigrationExecutionSQLiteTests {
    @Test("Multi-stage migration executes in order and persists stage boundaries")
    func multiStageMigrationExecutesInOrderAndPersistsBetweenStages() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        await sqliteMigrationEventRecorder.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteStageBoundarySchemaV1.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageBoundaryUserV1.self)]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var user = SQLiteStageBoundaryUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-stage-boundary-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer.open(
            for: SQLiteStageBoundarySchemaV3.self,
            migrationPlan: SQLiteStageBoundaryMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageBoundaryUserV3.self)])
        )
        try await migratedContainer.migrateIfNeeded()

        let events = await sqliteMigrationEventRecorder.snapshot()
        let currentVersion = try await migratedContainer.getCurrentSchemaVersion()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteStageBoundarySchemaV3.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageBoundaryUserV3.self)]),
            security: .disabled
        )
        let migratedUsers = try await verificationContainer.newContext()
            .fetch(SQLiteStageBoundaryUserV3.self)
            .execute()
        let migratedUser = migratedUsers.first { $0.id == "sqlite-stage-boundary-user" }

        #expect(events == ["will:2.0.0", "did:2.0.0"])
        #expect(currentVersion == Schema.Version(3, 0, 0))
        #expect(migratedUser?.fullName == "Alice")
        #expect(migratedUser?.email == "alice@example.com")
        #expect(migratedUser?.age == 0)
    }

    @Test("Lightweight migration adds and removes indexes end-to-end")
    func lightweightMigrationAddsAndRemovesIndexesEndToEnd() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let initialContainer = try await DBContainer.open(
            for: SQLiteIndexLifecycleSchemaV2.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteIndexLifecycleUserV2.self)]),
            security: .disabled
        )
        let subspace = try await initialContainer.resolveDirectory(for: SQLiteIndexLifecycleUserV2.self)
        let ageIndexSubspace = subspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLiteIndexLifecycleUser_age")
        let createdAtIndexSubspace = subspace
            .subspace(SubspaceKey.indexes)
            .subspace("SQLiteIndexLifecycleUser_createdAt")

        let initialContext = initialContainer.newContext()
        var user = SQLiteIndexLifecycleUserV2(
            name: "Alice",
            email: "alice@example.com",
            age: 42
        )
        user.id = "sqlite-index-lifecycle-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(2, 0, 0))

        #expect(try await countKeys(in: ageIndexSubspace, engine: engine) > 0)

        let migratedContainer = try await DBContainer.open(
            for: SQLiteIndexLifecycleSchemaV3.self,
            migrationPlan: SQLiteIndexLifecycleMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteIndexLifecycleUserV3.self)])
        )
        try await migratedContainer.migrateIfNeeded()

        let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
        let registry = SchemaRegistry(database: engine, clock: TestProcessMonotonicClock())
        let entity = try await registry.load(typeName: SQLiteIndexLifecycleUserV2.persistableType)
        let formerIndexKey = subspace
            .subspace("storeInfo")
            .subspace("formerIndexes")
            .pack(Tuple("SQLiteIndexLifecycleUser_age"))
        let formerIndexValue = try await value(for: formerIndexKey, engine: engine)
        let indexRegistry = DatabaseIndexRegistry(container: migratedContainer, subspace: subspace)
        let removedIndexState = try await indexRegistry.state(of: "SQLiteIndexLifecycleUser_age")

        let verificationContainer = try await DBContainer.open(
            for: SQLiteIndexLifecycleSchemaV3.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteIndexLifecycleUserV3.self)]),
            security: .disabled
        )
        let migratedUsers = try await verificationContainer.newContext()
            .fetch(SQLiteIndexLifecycleUserV3.self)
            .execute()
        let migratedUser = migratedUsers.first { $0.id == "sqlite-index-lifecycle-user" }

        #expect(currentVersion == Schema.Version(3, 0, 0))
        #expect(entity?.fieldMapByName["createdAt"]?.fieldNumber == 5)
        #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        #expect(formerIndexValue != nil)
        #expect(try await countKeys(in: ageIndexSubspace, engine: engine) == 0)
        #expect(try await countKeys(in: createdAtIndexSubspace, engine: engine) > 0)
        #expect(removedIndexState == .disabled)
        #expect(migratedUser?.age == 42)
        #expect(migratedUser?.createdAt == 0)
    }

    @Test("Failed later stage keeps earlier stage committed")
    func failedLaterStageKeepsEarlierStageCommitted() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        await sqliteMigrationEventRecorder.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteStageFailureSchemaV1.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageFailureUserV1.self)]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var user = SQLiteStageFailureUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-stage-failure-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer.open(
            for: SQLiteStageFailureSchemaV3.self,
            migrationPlan: SQLiteStageFailureMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageFailureUserV3.self)])
        )

        do {
            try await migratedContainer.migrateIfNeeded()
            Issue.record("Expected migration failure")
        } catch let error as SQLiteMigrationExecutionError {
            #expect(error == .expectedFailure)
        }

        let events = await sqliteMigrationEventRecorder.snapshot()
        let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
        let registry = SchemaRegistry(database: engine, clock: TestProcessMonotonicClock())
        let entity = try await registry.load(typeName: SQLiteStageFailureUserV1.persistableType)

        let verificationContainer = try await DBContainer.open(
            for: SQLiteStageFailureSchemaV2.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageFailureUserV2.self)]),
            security: .disabled
        )
        let migratedUsers = try await verificationContainer.newContext()
            .fetch(SQLiteStageFailureUserV2.self)
            .execute()
        let migratedUser = migratedUsers.first { $0.id == "sqlite-stage-failure-user" }

        #expect(events == ["fail:2.0.0"])
        #expect(currentVersion == Schema.Version(2, 0, 0))
        #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        #expect(entity?.fieldMapByName["fullName"] == nil)
        #expect(migratedUser?.name == "Alice")
        #expect(migratedUser?.age == 0)
    }

    @Test("Empty database bootstraps to latest schema without executing stages")
    func emptyDatabaseBootstrapsWithoutExecutingStages() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        await sqliteMigrationEventRecorder.reset()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteStageBoundarySchemaV3.self,
            migrationPlan: SQLiteStageBoundaryMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStageBoundaryUserV3.self)])
        )
        try await migratedContainer.migrateIfNeeded()

        let events = await sqliteMigrationEventRecorder.snapshot()
        let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
        let registry = SchemaRegistry(database: engine, clock: TestProcessMonotonicClock())
        let entity = try await registry.load(typeName: SQLiteStageBoundaryUserV1.persistableType)

        #expect(events.isEmpty)
        #expect(currentVersion == Schema.Version(3, 0, 0))
        #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
        #expect(entity?.fieldMapByName["name"] == nil)
    }
}
#endif

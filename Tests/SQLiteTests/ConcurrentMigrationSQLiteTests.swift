#if SQLITE
import Testing
import TestSupport
import Foundation
import Database
import TestHeartbeat
import DatabaseRuntime

private actor ConcurrentMigrationCounter {
    private var willCount: Int = 0
    private var didCount: Int = 0

    func reset() {
        willCount = 0
        didCount = 0
    }

    func incrementWill() {
        willCount += 1
    }

    func incrementDid() {
        didCount += 1
    }

    func snapshot() -> (will: Int, did: Int) {
        (willCount, didCount)
    }
}

private let concurrentMigrationCounter = ConcurrentMigrationCounter()

@Persistable(type: "SQLiteConcurrentMigrationUser")
struct SQLiteConcurrentMigrationUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SQLiteConcurrentMigrationUser")
struct SQLiteConcurrentMigrationUserV2 {
    var id: String = ""
    var fullName: String
    var email: String
}

enum SQLiteConcurrentMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteConcurrentMigrationUserV1.schemaEntity]
        }
    }
}

enum SQLiteConcurrentMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try SQLiteConcurrentMigrationUserV2.schemaEntity]
        }
    }
}

enum SQLiteConcurrentMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SQLiteConcurrentMigrationSchemaV1.self, SQLiteConcurrentMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SQLiteConcurrentMigrationSchemaV1.self,
                toVersion: SQLiteConcurrentMigrationSchemaV2.self,
                willMigrate: migrateUsers,
                didMigrate: audit
            )
        ]
    }

    static func migrateUsers(context: MigrationContext) async throws {
        await concurrentMigrationCounter.incrementWill()

        var migratedUsers: [SQLiteConcurrentMigrationUserV2] = []
        for try await legacy in context.enumerate(SQLiteConcurrentMigrationUserV1.self) {
            var migrated = SQLiteConcurrentMigrationUserV2(
                fullName: legacy.name,
                email: legacy.email
            )
            migrated.id = legacy.id
            migratedUsers.append(migrated)
        }

        guard !migratedUsers.isEmpty else { return }
        try await context.batchUpdate(migratedUsers, batchSize: 100)
    }

    static func audit(context: MigrationContext) async throws {
        await concurrentMigrationCounter.incrementDid()
    }
}

@Suite("Concurrent Migration SQLite Tests", .serialized, .heartbeat)
struct ConcurrentMigrationSQLiteTests {
    @Test("Re-entrant migrateIfNeeded is idempotent")
    func reEntrantMigrateIsIdempotent() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        await concurrentMigrationCounter.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV1.self)]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()
        var user = SQLiteConcurrentMigrationUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-reentrant-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let container = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)])
        )

        try await container.migrateIfNeeded()
        let afterFirst = await concurrentMigrationCounter.snapshot()

        try await container.migrateIfNeeded()
        let afterSecond = await concurrentMigrationCounter.snapshot()

        let version = try await container.getCurrentSchemaVersion()

        #expect(afterFirst == (1, 1))
        #expect(afterSecond == afterFirst)
        #expect(version == Schema.Version(2, 0, 0))
    }

    @Test("Concurrent migrateIfNeeded preserves final state correctness")
    func concurrentMigrateConvergesToTarget() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        await concurrentMigrationCounter.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV1.self)]),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        for i in 0..<5 {
            var user = SQLiteConcurrentMigrationUserV1(
                name: "User\(i)",
                email: "user\(i)@example.com"
            )
            user.id = "sqlite-concurrent-user-\(i)"
            try initialContext.insert(user)
        }
        try await initialContext.save()
        try await initialContainer.installSchemaSnapshot(for: Schema.Version(1, 0, 0))

        let containerA = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)])
        )
        let containerB = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)])
        )

        async let migrationA: Void = containerA.migrateIfNeeded()
        async let migrationB: Void = containerB.migrateIfNeeded()
        _ = try await (migrationA, migrationB)

        let versionA = try await containerA.getCurrentSchemaVersion()
        let versionB = try await containerB.getCurrentSchemaVersion()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.makeSchema(),
            configuration: .testing(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)]),
            security: .disabled
        )
        let users = try await verificationContainer.newContext()
            .fetch(SQLiteConcurrentMigrationUserV2.self)
            .orderBy(SQLiteConcurrentMigrationUserV2.fields.fullName)
            .execute()

        #expect(versionA == Schema.Version(2, 0, 0))
        #expect(versionB == Schema.Version(2, 0, 0))
        #expect(users.count == 5)
        #expect(users.map { $0.fullName } == ["User0", "User1", "User2", "User3", "User4"])
        #expect(users.map { $0.email } == [
            "user0@example.com", "user1@example.com", "user2@example.com",
            "user3@example.com", "user4@example.com"
        ])
    }
}
#endif

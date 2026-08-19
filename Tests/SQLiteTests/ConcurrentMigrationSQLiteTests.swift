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
        let database = try SQLiteTestDatabase(prefix: "concurrent-migration-reentrant")
        defer { database.remove() }
        await concurrentMigrationCounter.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV1.self)]),
            security: .testingDisabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.testBaseContext()
        var user = SQLiteConcurrentMigrationUserV1(name: "Alice", email: "alice@example.com")
        user.id = "sqlite-reentrant-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let container = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        try await container.testBaseAdmin().migrateIfNeeded()
        let afterFirst = await concurrentMigrationCounter.snapshot()

        try await container.testBaseAdmin().migrateIfNeeded()
        let afterSecond = await concurrentMigrationCounter.snapshot()

        let version = try await container.testBaseCurrentSchemaVersion()

        #expect(afterFirst == (1, 1))
        #expect(afterSecond == afterFirst)
        #expect(version == Schema.Version(2, 0, 0))
    }

    @Test("Concurrent migrateIfNeeded preserves final state correctness")
    func concurrentMigrateConvergesToTarget() async throws {
        let database = try SQLiteTestDatabase(prefix: "concurrent-migration")
        defer { database.remove() }
        await concurrentMigrationCounter.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV1.self)]),
            security: .testingDisabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.testBaseContext()

        for i in 0..<5 {
            var user = SQLiteConcurrentMigrationUserV1(
                name: "User\(i)",
                email: "user\(i)@example.com"
            )
            user.id = "sqlite-concurrent-user-\(i)"
            try initialContext.insert(user)
        }
        try await initialContext.save()
        try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))
        await initialContainer.shutdown()

        let containerA = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await containerA.shutdown() }
        let containerB = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await containerB.shutdown() }

        async let migrationA: Void = containerA.testBaseAdmin().migrateIfNeeded()
        async let migrationB: Void = containerB.testBaseAdmin().migrateIfNeeded()
        _ = try await (migrationA, migrationB)

        let versionA = try await containerA.testBaseCurrentSchemaVersion()
        let versionB = try await containerB.testBaseCurrentSchemaVersion()
        await containerA.shutdown()
        await containerB.shutdown()

        let verificationContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteConcurrentMigrationUserV2.self)]),
            security: .testingDisabled
        )
        defer { await verificationContainer.shutdown() }
        let users = try await verificationContainer.testBaseContext()
            .fetch(SQLiteConcurrentMigrationUserV2.self)
            .orderBy(SQLiteConcurrentMigrationUserV2.fields.fullName)
            .execute()

        #expect(versionA == Schema.Version(2, 0, 0))
        #expect(versionB == Schema.Version(2, 0, 0))
        #expect(users.count == 5)
        #expect(users.map { $0.fullName } == ["User0", "User1", "User2", "User3", "User4"])
        #expect(users.map { $0.email } == [
            "user0@example.com", "user1@example.com", "user2@example.com",
            "user3@example.com", "user4@example.com",
            ])
    }

    @Test("Interrupted migration skips rows already encoded for the target")
    func interruptedMigrationResumesPastTargetRows() async throws {
        let database = try SQLiteTestDatabase(
            prefix: "concurrent-migration-partial"
        )
        defer { database.remove() }
        await concurrentMigrationCounter.reset()

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SQLiteConcurrentMigrationUserV1.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.testBaseContext()
        for index in 0..<2 {
            var user = SQLiteConcurrentMigrationUserV1(
                name: "User\(index)",
                email: "user\(index)@example.com"
            )
            user.id = "sqlite-partial-user-\(index)"
            try initialContext.insert(user)
        }
        try await initialContext.save()
        try await initialContainer.installTestBaseSchemaSnapshot(
            for: Schema.Version(1, 0, 0)
        )

        let subspace = try await initialContainer.testBaseDirectory(
            for: SQLiteConcurrentMigrationUserV1.self
        )
        var alreadyMigrated = SQLiteConcurrentMigrationUserV2(
            fullName: "User0",
            email: "user0@example.com"
        )
        alreadyMigrated.id = "sqlite-partial-user-0"
        let alreadyMigratedKey = subspace
            .subspace(SubspaceKey.items)
            .subspace(SQLiteConcurrentMigrationUserV1.persistableType)
            .pack(try alreadyMigrated.persistableIdentifierTuple())
        let alreadyMigratedBytes = try DataAccess.serialize(alreadyMigrated)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        try await initialContainer.withTestBaseOperation {
            try await initialContainer.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: initialContainer.monotonicClock
            ) { transaction in
                let storage = initialContainer.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )
                try await storage.write(
                    alreadyMigratedBytes,
                    for: alreadyMigratedKey
                )
            }
        }
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SQLiteConcurrentMigrationUserV2.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await migratedContainer.shutdown() }

        try await migratedContainer.testBaseAdmin().migrateIfNeeded()
        let users = try await migratedContainer.testBaseContext()
            .fetch(SQLiteConcurrentMigrationUserV2.self)
            .orderBy(SQLiteConcurrentMigrationUserV2.fields.fullName)
            .execute()

        #expect(users.map(\.fullName) == ["User0", "User1"])
        #expect(
            try await migratedContainer.testBaseCurrentSchemaVersion()
                == Schema.Version(2, 0, 0)
        )
    }

    @Test("Migration never treats malformed source bytes as target data")
    func malformedSourceDataFailsMigration() async throws {
        let database = try SQLiteTestDatabase(
            prefix: "concurrent-migration-corruption"
        )
        defer { database.remove() }

        let initialContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV1.makeSchema(),
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SQLiteConcurrentMigrationUserV1.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await initialContainer.shutdown() }
        let initialContext = initialContainer.testBaseContext()
        var user = SQLiteConcurrentMigrationUserV1(
            name: "Corrupt",
            email: "corrupt@example.com"
        )
        user.id = "sqlite-corrupt-migration-user"
        try initialContext.insert(user)
        try await initialContext.save()
        try await initialContainer.installTestBaseSchemaSnapshot(
            for: Schema.Version(1, 0, 0)
        )

        let subspace = try await initialContainer.testBaseDirectory(
            for: SQLiteConcurrentMigrationUserV1.self
        )
        let itemKey = subspace
            .subspace(SubspaceKey.items)
            .subspace(SQLiteConcurrentMigrationUserV1.persistableType)
            .pack(try user.persistableIdentifierTuple())
        let malformedBytes = ByteString([0xFF])
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        try await initialContainer.withTestBaseOperation {
            try await initialContainer.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: initialContainer.monotonicClock
            ) { transaction in
                let storage = initialContainer.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )
                try await storage.write(malformedBytes, for: itemKey)
            }
        }
        await initialContainer.shutdown()

        let migratedContainer = try await DBContainer.open(
            for: SQLiteConcurrentMigrationSchemaV2.self,
            migrationPlan: SQLiteConcurrentMigrationPlan.self,
            configuration: .file(database.path),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SQLiteConcurrentMigrationUserV2.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await migratedContainer.shutdown() }

        do {
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()
            Issue.record("Expected malformed persisted data to fail migration")
        } catch let error as DatabaseRuntimeError {
            guard case .internalError(let message) = error else {
                Issue.record("Unexpected migration error: \(error)")
                return
            }
            #expect(message.contains("Failed to decode a persisted"))
        }

        #expect(
            try await migratedContainer.testBaseCurrentSchemaVersion()
                == Schema.Version(1, 0, 0)
        )
        let persistedPayload = try await migratedContainer.transactionExecutor
            .withTransaction(
                configuration: .readOnly,
                clock: migratedContainer.monotonicClock
            ) { transaction in
                let storage = migratedContainer.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )
                return try await storage.read(for: itemKey, snapshot: true)
            }
        #expect(persistedPayload == malformedBytes)
    }
}
#endif

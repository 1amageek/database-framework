#if !os(WASI)
#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime

private actor FDBMigrationEventRecorder {
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

private let fdbMigrationEventRecorder = FDBMigrationEventRecorder()

private enum FDBMigrationExecutionError: Error {
    case expectedFailure
}

private func fdbVersionLabel(_ version: Schema.Version?) -> String {
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

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV1 {
    #Directory<FDBStageBoundaryUserV1>("test", "migration", "stage-boundary")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV2 {
    #Directory<FDBStageBoundaryUserV2>("test", "migration", "stage-boundary")

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV3 {
    #Directory<FDBStageBoundaryUserV3>("test", "migration", "stage-boundary")
    #Index(
        .ordered(
            name: "FDBStageBoundaryUser_fullName",
            keys: [.ascending(\FDBStageBoundaryUserV3.fullName)], unique: false))

    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum FDBStageBoundarySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageBoundaryUserV1.schemaEntity] }
    }
}

enum FDBStageBoundarySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageBoundaryUserV2.schemaEntity] }
    }
}

enum FDBStageBoundarySchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageBoundaryUserV3.schemaEntity] }
    }
}

enum FDBStageBoundaryMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBStageBoundarySchemaV1.self, FDBStageBoundarySchemaV2.self, FDBStageBoundarySchemaV3.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBStageBoundarySchemaV1.self,
                toVersion: FDBStageBoundarySchemaV2.self
            ),
            .custom(
                fromVersion: FDBStageBoundarySchemaV2.self,
                toVersion: FDBStageBoundarySchemaV3.self,
                willMigrate: migrateUsers,
                didMigrate: auditStage
            ),
        ]
    }

    static func migrateUsers(context: MigrationContext) async throws {
        let currentVersion = try await context.container.testBaseCurrentSchemaVersion()
        await fdbMigrationEventRecorder.record("will:\(fdbVersionLabel(currentVersion))")

        var migratedUsers: [FDBStageBoundaryUserV3] = []
        for try await legacyUser in context.enumerate(FDBStageBoundaryUserV2.self) {
            var migratedUser = FDBStageBoundaryUserV3(
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
        let currentVersion = try await context.container.testBaseCurrentSchemaVersion()
        await fdbMigrationEventRecorder.record("did:\(fdbVersionLabel(currentVersion))")
    }
}

@Persistable(type: "FDBIndexLifecycleUser")
struct FDBIndexLifecycleUserV2 {
    #Directory<FDBIndexLifecycleUserV2>("test", "migration", "index-lifecycle")
    #Index(
        .ordered(
            name: "FDBIndexLifecycleUser_email",
            keys: [.ascending(\FDBIndexLifecycleUserV2.email)],
            unique: false))
    #Index(
        .ordered(
            name: "FDBIndexLifecycleUser_age",
            keys: [.ascending(\FDBIndexLifecycleUserV2.age)],
            unique: false))

    var id: String = ""
    var name: String
    var email: String
    var age: Int64
}

@Persistable(type: "FDBIndexLifecycleUser")
struct FDBIndexLifecycleUserV3 {
    #Directory<FDBIndexLifecycleUserV3>("test", "migration", "index-lifecycle")
    #Index(
        .ordered(
            name: "FDBIndexLifecycleUser_email",
            keys: [.ascending(\FDBIndexLifecycleUserV3.email)],
            unique: false))
    #Index(
        .ordered(
            name: "FDBIndexLifecycleUser_createdAt",
            keys: [.ascending(\FDBIndexLifecycleUserV3.createdAt)], unique: false))

    var id: String = ""
    var name: String
    var email: String
    var age: Int64
    var createdAt: Double = 0
}

enum FDBIndexLifecycleSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBIndexLifecycleUserV2.schemaEntity] }
    }
}

enum FDBIndexLifecycleSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBIndexLifecycleUserV3.schemaEntity] }
    }
}

enum FDBIndexLifecycleMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBIndexLifecycleSchemaV2.self, FDBIndexLifecycleSchemaV3.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBIndexLifecycleSchemaV2.self,
                toVersion: FDBIndexLifecycleSchemaV3.self
            )
        ]
    }
}

@Persistable(type: "FDBStageFailureUser")
struct FDBStageFailureUserV1 {
    #Directory<FDBStageFailureUserV1>("test", "migration", "stage-failure")

    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "FDBStageFailureUser")
struct FDBStageFailureUserV2 {
    #Directory<FDBStageFailureUserV2>("test", "migration", "stage-failure")

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

@Persistable(type: "FDBStageFailureUser")
struct FDBStageFailureUserV3 {
    #Directory<FDBStageFailureUserV3>("test", "migration", "stage-failure")

    var id: String = ""
    var fullName: String
    var email: String
    var age: Int64 = 0
}

enum FDBStageFailureSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageFailureUserV1.schemaEntity] }
    }
}

enum FDBStageFailureSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageFailureUserV2.schemaEntity] }
    }
}

enum FDBStageFailureSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try FDBStageFailureUserV3.schemaEntity] }
    }
}

enum FDBStageFailureMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBStageFailureSchemaV1.self, FDBStageFailureSchemaV2.self, FDBStageFailureSchemaV3.self,
        ]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: FDBStageFailureSchemaV1.self,
                toVersion: FDBStageFailureSchemaV2.self
            ),
            .custom(
                fromVersion: FDBStageFailureSchemaV2.self,
                toVersion: FDBStageFailureSchemaV3.self,
                willMigrate: failStage,
                didMigrate: nil
            ),
        ]
    }

    static func failStage(context: MigrationContext) async throws {
        let currentVersion = try await context.container.testBaseCurrentSchemaVersion()
        await fdbMigrationEventRecorder.record("fail:\(fdbVersionLabel(currentVersion))")
        throw FDBMigrationExecutionError.expectedFailure
    }
}

@Suite("Migration Execution FDB Tests", .foundationDBScenario, .serialized, .heartbeat)
struct MigrationExecutionFDBTests {
    private func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FoundationDBScenarioCoordinator.shared.makeSystemPriorityEngine()
    }

    @Test("Multi-stage migration executes in order and persists stage boundaries on FDB")
    func multiStageMigrationExecutesInOrderAndPersistsBetweenStages() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-execution-stage-boundaries"
            await fdbMigrationEventRecorder.reset()

            let initialContainer = try await DBContainer.open(
                for: FDBStageBoundarySchemaV1.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageBoundaryUserV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var user = FDBStageBoundaryUserV1(name: "Alice", email: "alice@example.com")
            user.id = "fdb-stage-boundary-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: FDBStageBoundarySchemaV3.self,
                migrationPlan: FDBStageBoundaryMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageBoundaryUserV3.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.testBaseCurrentSchemaVersion()

            let verificationContainer = try await DBContainer.open(
                for: FDBStageBoundarySchemaV3.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageBoundaryUserV3.self)]),
                security: .testingDisabled
            )
            let migratedUsers = try await verificationContainer.testBaseContext()
                .fetch(FDBStageBoundaryUserV3.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "fdb-stage-boundary-user" }

            #expect(events == ["will:2.0.0", "did:2.0.0"])
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(migratedUser?.fullName == "Alice")
            #expect(migratedUser?.email == "alice@example.com")
            #expect(migratedUser?.age == 0)
        }
    }

    @Test("Lightweight migration adds and removes indexes end-to-end on FDB")
    func lightweightMigrationAddsAndRemovesIndexesEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-execution-index-lifecycle"
            let initialContainer = try await DBContainer.open(
                for: FDBIndexLifecycleSchemaV2.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBIndexLifecycleUserV2.self)]),
                security: .testingDisabled
            )
            let subspace = try await initialContainer.testBaseDirectory(for: FDBIndexLifecycleUserV2.self)
            let ageIndexSubspace = subspace
                .subspace(SubspaceKey.indexes)
                .subspace("FDBIndexLifecycleUser_age")
            let ageIndexStateSubspace =
                subspace
                .subspace("state")
                .subspace("FDBIndexLifecycleUser_age")
            let createdAtIndexSubspace = subspace
                .subspace(SubspaceKey.indexes)
                .subspace("FDBIndexLifecycleUser_createdAt")

            let initialContext = initialContainer.testBaseContext()
            var user = FDBIndexLifecycleUserV2(name: "Alice", email: "alice@example.com", age: 42)
            user.id = "fdb-index-lifecycle-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(2, 0, 0))

            #expect(try await countKeys(in: ageIndexSubspace, engine: engine) > 0)

            let migratedContainer = try await DBContainer.open(
                for: FDBIndexLifecycleSchemaV3.self,
                migrationPlan: FDBIndexLifecycleMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBIndexLifecycleUserV3.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let currentVersion = try await migratedContainer.testBaseCurrentSchemaVersion()
            let entity = try await migratedContainer
                .testPersistedControlSchemaEntities()
                .first { $0.name == FDBIndexLifecycleUserV2.persistableType }
            let formerIndexKey = subspace
                .subspace("storeInfo")
                .subspace("formerIndexes")
                .pack(Tuple("FDBIndexLifecycleUser_age"))
            let formerIndexValue = try await value(for: formerIndexKey, engine: engine)
            let verificationContainer = try await DBContainer.open(
                for: FDBIndexLifecycleSchemaV3.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBIndexLifecycleUserV3.self)]),
                security: .testingDisabled
            )
            let migratedUsers = try await verificationContainer.testBaseContext()
                .fetch(FDBIndexLifecycleUserV3.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "fdb-index-lifecycle-user" }

            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(entity?.fieldMapByName["createdAt"]?.fieldNumber == 5)
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
            #expect(formerIndexValue != nil)
            #expect(try await countKeys(in: ageIndexSubspace, engine: engine) == 0)
            #expect(try await countKeys(
                    in: ageIndexStateSubspace,
                    engine: engine
                ) == 0
            )
            #expect(try await countKeys(in: createdAtIndexSubspace, engine: engine) > 0)
            #expect(migratedUser?.age == 42)
            #expect(migratedUser?.createdAt == 0)
        }
    }

    @Test("Failed later stage keeps earlier stage committed on FDB")
    func failedLaterStageKeepsEarlierStageCommitted() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-execution-stage-failure"
            await fdbMigrationEventRecorder.reset()

            let initialContainer = try await DBContainer.open(
                for: FDBStageFailureSchemaV1.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageFailureUserV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var user = FDBStageFailureUserV1(name: "Alice", email: "alice@example.com")
            user.id = "fdb-stage-failure-user"
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: FDBStageFailureSchemaV3.self,
                migrationPlan: FDBStageFailureMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageFailureUserV3.self)]),
            )

            do {
                try await migratedContainer.testBaseAdmin().migrateIfNeeded()
                Issue.record("Expected migration failure")
            } catch let error as FDBMigrationExecutionError {
                #expect(error == .expectedFailure)
            }

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.testBaseCurrentSchemaVersion()
            let entity = try await migratedContainer
                .testPersistedControlSchemaEntities()
                .first { $0.name == FDBStageFailureUserV1.persistableType }

            let verificationContainer = try await DBContainer.open(
                for: FDBStageFailureSchemaV2.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageFailureUserV2.self)]),
                security: .testingDisabled
            )
            let migratedUsers = try await verificationContainer.testBaseContext()
                .fetch(FDBStageFailureUserV2.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "fdb-stage-failure-user" }

            #expect(events == ["fail:2.0.0"])
            #expect(currentVersion == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
            #expect(entity?.fieldMapByName["fullName"] == nil)
            #expect(migratedUser?.name == "Alice")
            #expect(migratedUser?.age == 0)
        }
    }

    @Test("Empty database bootstraps to latest schema without executing stages on FDB")
    func emptyDatabaseBootstrapsWithoutExecutingStages() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-execution-empty-bootstrap"
            await fdbMigrationEventRecorder.reset()

            let migratedContainer = try await DBContainer.open(
                for: FDBStageBoundarySchemaV3.self,
                migrationPlan: FDBStageBoundaryMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: engine
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(FDBStageBoundaryUserV3.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.testBaseCurrentSchemaVersion()
            let entity = try await migratedContainer
                .testPersistedControlSchemaEntities()
                .first { $0.name == FDBStageBoundaryUserV1.persistableType }

            #expect(events.isEmpty)
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["name"] == nil)
        }
    }
}
#endif

#endif

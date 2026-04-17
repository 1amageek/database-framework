#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import TestSupport
@testable import DatabaseEngine

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
    for key: Bytes,
    engine: any StorageEngine
) async throws -> Bytes? {
    try await engine.withTransaction { transaction in
        try await transaction.getValue(for: key, snapshot: true)
    }
}

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV1 {
    #Directory<FDBStageBoundaryUserV1>("test", "migration", "stage-boundary")

    var name: String
    var email: String
}

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV2 {
    #Directory<FDBStageBoundaryUserV2>("test", "migration", "stage-boundary")

    var name: String
    var email: String
    var age: Int = 0
}

@Persistable(type: "FDBStageBoundaryUser")
struct FDBStageBoundaryUserV3 {
    #Directory<FDBStageBoundaryUserV3>("test", "migration", "stage-boundary")
    #Index(ScalarIndexKind<FDBStageBoundaryUserV3>(fields: [\.fullName]), name: "FDBStageBoundaryUser_fullName")

    var fullName: String
    var email: String
    var age: Int = 0
}

enum FDBStageBoundarySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageBoundaryUserV1.self]
}

enum FDBStageBoundarySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageBoundaryUserV2.self]
}

enum FDBStageBoundarySchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageBoundaryUserV3.self]
}

enum FDBStageBoundaryMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBStageBoundarySchemaV1.self, FDBStageBoundarySchemaV2.self, FDBStageBoundarySchemaV3.self]
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
            )
        ]
    }

    static func migrateUsers(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
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
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await fdbMigrationEventRecorder.record("did:\(fdbVersionLabel(currentVersion))")
    }
}

@Persistable(type: "FDBIndexLifecycleUser")
struct FDBIndexLifecycleUserV2 {
    #Directory<FDBIndexLifecycleUserV2>("test", "migration", "index-lifecycle")
    #Index(ScalarIndexKind<FDBIndexLifecycleUserV2>(fields: [\.email]), name: "FDBIndexLifecycleUser_email")
    #Index(ScalarIndexKind<FDBIndexLifecycleUserV2>(fields: [\.age]), name: "FDBIndexLifecycleUser_age")

    var name: String
    var email: String
    var age: Int
}

@Persistable(type: "FDBIndexLifecycleUser")
struct FDBIndexLifecycleUserV3 {
    #Directory<FDBIndexLifecycleUserV3>("test", "migration", "index-lifecycle")
    #Index(ScalarIndexKind<FDBIndexLifecycleUserV3>(fields: [\.email]), name: "FDBIndexLifecycleUser_email")
    #Index(ScalarIndexKind<FDBIndexLifecycleUserV3>(fields: [\.createdAt]), name: "FDBIndexLifecycleUser_createdAt")

    var name: String
    var email: String
    var age: Int
    var createdAt: Double = 0
}

enum FDBIndexLifecycleSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [FDBIndexLifecycleUserV2.self]
}

enum FDBIndexLifecycleSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [FDBIndexLifecycleUserV3.self]
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

    var name: String
    var email: String
}

@Persistable(type: "FDBStageFailureUser")
struct FDBStageFailureUserV2 {
    #Directory<FDBStageFailureUserV2>("test", "migration", "stage-failure")

    var name: String
    var email: String
    var age: Int = 0
}

@Persistable(type: "FDBStageFailureUser")
struct FDBStageFailureUserV3 {
    #Directory<FDBStageFailureUserV3>("test", "migration", "stage-failure")

    var fullName: String
    var email: String
    var age: Int = 0
}

enum FDBStageFailureSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageFailureUserV1.self]
}

enum FDBStageFailureSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageFailureUserV2.self]
}

enum FDBStageFailureSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [FDBStageFailureUserV3.self]
}

enum FDBStageFailureMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [FDBStageFailureSchemaV1.self, FDBStageFailureSchemaV2.self, FDBStageFailureSchemaV3.self]
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
            )
        ]
    }

    static func failStage(context: MigrationContext) async throws {
        let currentVersion = try await context.container.getCurrentSchemaVersion()
        await fdbMigrationEventRecorder.record("fail:\(fdbVersionLabel(currentVersion))")
        throw FDBMigrationExecutionError.expectedFailure
    }
}

@Suite("Migration Execution FDB Tests", .serialized, .heartbeat)
struct MigrationExecutionFDBTests {
    private func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let engine = try await FDBTestSetup.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func clearState(
        in database: any StorageEngine,
        typeNames: [String]
    ) async throws {
        do {
            try await database.directoryService.remove(path: ["test", "migration"])
        } catch {
        }

        do {
            try await database.directoryService.remove(path: ["_metadata"])
        } catch {
        }

        try await database.withTransaction { transaction in
            for typeName in typeNames {
                transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    @Test("Multi-stage migration executes in order and persists stage boundaries on FDB")
    func multiStageMigrationExecutesInOrderAndPersistsBetweenStages() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            await fdbMigrationEventRecorder.reset()

            try await clearState(in: engine, typeNames: [FDBStageBoundaryUserV1.persistableType])

            let initialContainer = try await DBContainer(
                for: FDBStageBoundarySchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var user = FDBStageBoundaryUserV1(name: "Alice", email: "alice@example.com")
            user.id = "fdb-stage-boundary-user"
            initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: FDBStageBoundarySchemaV3.self,
                migrationPlan: FDBStageBoundaryMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()

            let verificationContainer = try await DBContainer(
                for: FDBStageBoundarySchemaV3.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let migratedUsers = try await verificationContainer.newContext()
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
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            try await clearState(in: engine, typeNames: [FDBIndexLifecycleUserV2.persistableType])

            let initialContainer = try await DBContainer(
                for: FDBIndexLifecycleSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let subspace = try await initialContainer.resolveDirectory(for: FDBIndexLifecycleUserV2.self)
            let ageIndexSubspace = subspace
                .subspace(SubspaceKey.indexes)
                .subspace("FDBIndexLifecycleUser_age")
            let createdAtIndexSubspace = subspace
                .subspace(SubspaceKey.indexes)
                .subspace("FDBIndexLifecycleUser_createdAt")

            let initialContext = initialContainer.newContext()
            var user = FDBIndexLifecycleUserV2(name: "Alice", email: "alice@example.com", age: 42)
            user.id = "fdb-index-lifecycle-user"
            initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

            #expect(try await countKeys(in: ageIndexSubspace, engine: engine) > 0)

            let migratedContainer = try await DBContainer(
                for: FDBIndexLifecycleSchemaV3.self,
                migrationPlan: FDBIndexLifecycleMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
            let registry = SchemaRegistry(database: engine)
            let entity = try await registry.load(typeName: FDBIndexLifecycleUserV2.persistableType)
            let formerIndexKey = subspace
                .subspace("storeInfo")
                .subspace("formerIndexes")
                .pack(Tuple("FDBIndexLifecycleUser_age"))
            let formerIndexValue = try await value(for: formerIndexKey, engine: engine)
            let indexManager = IndexManager(container: migratedContainer, subspace: subspace)
            let removedIndexState = try await indexManager.state(of: "FDBIndexLifecycleUser_age")

            let verificationContainer = try await DBContainer(
                for: FDBIndexLifecycleSchemaV3.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let migratedUsers = try await verificationContainer.newContext()
                .fetch(FDBIndexLifecycleUserV3.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == "fdb-index-lifecycle-user" }

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
    }

    @Test("Failed later stage keeps earlier stage committed on FDB")
    func failedLaterStageKeepsEarlierStageCommitted() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            await fdbMigrationEventRecorder.reset()

            try await clearState(in: engine, typeNames: [FDBStageFailureUserV1.persistableType])

            let initialContainer = try await DBContainer(
                for: FDBStageFailureSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var user = FDBStageFailureUserV1(name: "Alice", email: "alice@example.com")
            user.id = "fdb-stage-failure-user"
            initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: FDBStageFailureSchemaV3.self,
                migrationPlan: FDBStageFailureMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )

            do {
                try await migratedContainer.migrateIfNeeded()
                Issue.record("Expected migration failure")
            } catch let error as FDBMigrationExecutionError {
                #expect(error == .expectedFailure)
            }

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
            let registry = SchemaRegistry(database: engine)
            let entity = try await registry.load(typeName: FDBStageFailureUserV1.persistableType)

            let verificationContainer = try await DBContainer(
                for: FDBStageFailureSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
            )
            let migratedUsers = try await verificationContainer.newContext()
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
        try await FDBTestSetup.shared.withSerializedAccess {
            let engine = try await makeSystemPriorityEngine()
            await fdbMigrationEventRecorder.reset()

            try await clearState(in: engine, typeNames: [FDBStageBoundaryUserV1.persistableType])

            let migratedContainer = try await DBContainer(
                for: FDBStageBoundarySchemaV3.self,
                migrationPlan: FDBStageBoundaryMigrationPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let events = await fdbMigrationEventRecorder.snapshot()
            let currentVersion = try await migratedContainer.getCurrentSchemaVersion()
            let registry = SchemaRegistry(database: engine)
            let entity = try await registry.load(typeName: FDBStageBoundaryUserV1.persistableType)

            #expect(events.isEmpty)
            #expect(currentVersion == Schema.Version(3, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["name"] == nil)
        }
    }
}
#endif

#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import ScalarIndex
import TestSupport
import TestHeartbeat
@testable import DatabaseEngine

// MARK: - Schema Versions With Different #Directory Paths

@Persistable(type: "DirectoryMigrationUser")
struct DirectoryMigrationUserV1 {
    #Directory<DirectoryMigrationUserV1>("directory_migration_test_legacy")

    var name: String
    var email: String
}

@Persistable(type: "DirectoryMigrationUser")
struct DirectoryMigrationUserV2 {
    #Directory<DirectoryMigrationUserV2>("directory_migration_test_current")

    var name: String
    var email: String
}

enum DirectoryMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryMigrationUserV1.self]
}

enum DirectoryMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryMigrationUserV2.self]
}

// MARK: - Directory change with #Index on both sides

@Persistable(type: "DirectoryIndexedUser")
struct DirectoryIndexedUserV1 {
    #Directory<DirectoryIndexedUserV1>("directory_indexed_migration_test_legacy")
    #Index(ScalarIndexKind<DirectoryIndexedUserV1>(fields: [\.email]), name: "DirectoryIndexedUser_email")

    var name: String
    var email: String
}

@Persistable(type: "DirectoryIndexedUser")
struct DirectoryIndexedUserV2 {
    #Directory<DirectoryIndexedUserV2>("directory_indexed_migration_test_current")
    #Index(ScalarIndexKind<DirectoryIndexedUserV2>(fields: [\.email]), name: "DirectoryIndexedUser_email")

    var name: String
    var email: String
}

enum DirectoryIndexedSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryIndexedUserV1.self]
}

enum DirectoryIndexedSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryIndexedUserV2.self]
}

enum DirectoryIndexedCopyPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DirectoryIndexedSchemaV1.self, DirectoryIndexedSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: DirectoryIndexedSchemaV1.self,
                toVersion: DirectoryIndexedSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [DirectoryIndexedUserV2] = []
        for try await legacy in context.enumerate(DirectoryIndexedUserV1.self) {
            var user = DirectoryIndexedUserV2(name: legacy.name, email: legacy.email)
            user.id = legacy.id
            copied.append(user)
        }
        guard !copied.isEmpty else { return }
        try await context.batchUpdate(copied, batchSize: 100)
    }

    static func purgeLegacyDirectory(context: MigrationContext) async throws {
        try await context.purgeLegacyStorage(DirectoryIndexedUserV1.self)
        try await context.rebuildIndex(indexName: "DirectoryIndexedUser_email")
    }
}

// MARK: - Directory change + addIndex in same stage

@Persistable(type: "DirectoryAddIdxUser")
struct DirectoryAddIdxUserV1 {
    #Directory<DirectoryAddIdxUserV1>("directory_add_idx_test_legacy")

    var name: String
    var score: Int
}

@Persistable(type: "DirectoryAddIdxUser")
struct DirectoryAddIdxUserV2 {
    #Directory<DirectoryAddIdxUserV2>("directory_add_idx_test_current")
    #Index(ScalarIndexKind<DirectoryAddIdxUserV2>(fields: [\.score]), name: "DirectoryAddIdxUser_score")

    var name: String
    var score: Int
}

enum DirectoryAddIdxSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryAddIdxUserV1.self]
}

enum DirectoryAddIdxSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryAddIdxUserV2.self]
}

enum DirectoryAddIdxPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DirectoryAddIdxSchemaV1.self, DirectoryAddIdxSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: DirectoryAddIdxSchemaV1.self,
                toVersion: DirectoryAddIdxSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [DirectoryAddIdxUserV2] = []
        for try await legacy in context.enumerate(DirectoryAddIdxUserV1.self) {
            var user = DirectoryAddIdxUserV2(name: legacy.name, score: legacy.score)
            user.id = legacy.id
            copied.append(user)
        }
        guard !copied.isEmpty else { return }
        try await context.batchUpdate(copied, batchSize: 100)
    }

    static func purgeLegacyDirectory(context: MigrationContext) async throws {
        try await context.purgeLegacyStorage(DirectoryAddIdxUserV1.self)
    }
}

// MARK: - Directory change + removeIndex in same stage

@Persistable(type: "DirectoryRemIdxUser")
struct DirectoryRemIdxUserV1 {
    #Directory<DirectoryRemIdxUserV1>("directory_rem_idx_test_legacy")
    #Index(ScalarIndexKind<DirectoryRemIdxUserV1>(fields: [\.tag]), name: "DirectoryRemIdxUser_tag")

    var name: String
    var tag: String
}

@Persistable(type: "DirectoryRemIdxUser")
struct DirectoryRemIdxUserV2 {
    #Directory<DirectoryRemIdxUserV2>("directory_rem_idx_test_current")

    var name: String
    var tag: String
}

enum DirectoryRemIdxSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryRemIdxUserV1.self]
}

enum DirectoryRemIdxSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryRemIdxUserV2.self]
}

enum DirectoryRemIdxPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DirectoryRemIdxSchemaV1.self, DirectoryRemIdxSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: DirectoryRemIdxSchemaV1.self,
                toVersion: DirectoryRemIdxSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [DirectoryRemIdxUserV2] = []
        for try await legacy in context.enumerate(DirectoryRemIdxUserV1.self) {
            var user = DirectoryRemIdxUserV2(name: legacy.name, tag: legacy.tag)
            user.id = legacy.id
            copied.append(user)
        }
        guard !copied.isEmpty else { return }
        try await context.batchUpdate(copied, batchSize: 100)
    }

    static func purgeLegacyDirectory(context: MigrationContext) async throws {
        try await context.purgeLegacyStorage(DirectoryRemIdxUserV1.self)
    }
}

// MARK: - Lightweight-with-directory-change Schemas (must be rejected)

@Persistable(type: "DirectoryLightweightUser")
struct DirectoryLightweightUserV1 {
    #Directory<DirectoryLightweightUserV1>("directory_lightweight_test_legacy")

    var name: String
}

@Persistable(type: "DirectoryLightweightUser")
struct DirectoryLightweightUserV2 {
    #Directory<DirectoryLightweightUserV2>("directory_lightweight_test_current")

    var name: String
}

enum DirectoryLightweightSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryLightweightUserV1.self]
}

enum DirectoryLightweightSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DirectoryLightweightUserV2.self]
}

enum DirectoryLightweightPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DirectoryLightweightSchemaV1.self, DirectoryLightweightSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: DirectoryLightweightSchemaV1.self,
                toVersion: DirectoryLightweightSchemaV2.self
            )
        ]
    }
}

enum DirectoryMigrationCopyPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DirectoryMigrationSchemaV1.self, DirectoryMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: DirectoryMigrationSchemaV1.self,
                toVersion: DirectoryMigrationSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [DirectoryMigrationUserV2] = []
        for try await legacyUser in context.enumerate(DirectoryMigrationUserV1.self) {
            var newUser = DirectoryMigrationUserV2(
                name: legacyUser.name,
                email: legacyUser.email
            )
            newUser.id = legacyUser.id
            copied.append(newUser)
        }
        guard !copied.isEmpty else { return }
        try await context.batchUpdate(copied, batchSize: 100)
    }

    static func purgeLegacyDirectory(context: MigrationContext) async throws {
        try await context.purgeLegacyStorage(DirectoryMigrationUserV1.self)
    }
}

// MARK: - Tests

@Suite("Directory Migration Tests", .serialized, .heartbeat)
struct DirectoryMigrationTests {
    private func makeSystemPriorityEngine() async throws -> any StorageEngine {
        let engine = try await FDBTestSetup.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func cleanDirectories(engine: any StorageEngine) async throws {
        for path in [["directory_migration_test_legacy"], ["directory_migration_test_current"]] {
            if try await engine.directoryService.exists(path: path) {
                try await engine.directoryService.remove(path: path)
            }
        }
    }

    @Test("Custom migration copies data across changed #Directory paths")
    func customMigrationCopiesAcrossDirectoryChange() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            try await cleanDirectories(engine: engine)

            let seededID = "dir-migration-\(UUID().uuidString)"

            // 1. Insert V1 data into the legacy directory.
            let initialContainer = try await DBContainer(
                testing: DirectoryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let initialContext = initialContainer.newContext()
            var seededUser = DirectoryMigrationUserV1(name: "Alice", email: "alice@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            // Sanity check: data is physically under the V1 directory.
            let legacySubspace = try await initialContainer.resolveDirectory(for: DirectoryMigrationUserV1.self)
            let legacyItemsPrefix = legacySubspace.subspace(SubspaceKey.items).subspace(DirectoryMigrationUserV1.persistableType)
            let (legacyBegin, legacyEnd) = legacyItemsPrefix.range()
            let legacyCountBefore = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyBegin),
                    to: .firstGreaterOrEqual(legacyEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyCountBefore == 1)

            // 2. Run the migration plan that copies V1 → V2 directory and purges V1.
            let migratedContainer = try await DBContainer(
                for: DirectoryMigrationSchemaV2.self,
                migrationPlan: DirectoryMigrationCopyPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            // 3. Data must be readable via the V2 schema (new directory).
            let verificationContainer = try await DBContainer(
                testing: DirectoryMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let verificationContext = verificationContainer.newContext()
            let migratedUsers = try await verificationContext
                .fetch(DirectoryMigrationUserV2.self)
                .execute()

            #expect(migratedUsers.count == 1)
            let migratedUser = try #require(migratedUsers.first { $0.id == seededID })
            #expect(migratedUser.name == "Alice")
            #expect(migratedUser.email == "alice@example.com")

            // 4. Legacy directory must be empty after purgeLegacyStorage.
            let legacyCountAfter = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyBegin),
                    to: .firstGreaterOrEqual(legacyEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyCountAfter == 0)

            // 5. New directory physically holds the row.
            let targetSubspace = try await verificationContainer.resolveDirectory(for: DirectoryMigrationUserV2.self)
            let targetItemsPrefix = targetSubspace.subspace(SubspaceKey.items).subspace(DirectoryMigrationUserV2.persistableType)
            let (targetBegin, targetEnd) = targetItemsPrefix.range()
            let targetCount = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(targetBegin),
                    to: .firstGreaterOrEqual(targetEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(targetCount == 1)

            try await cleanDirectories(engine: engine)
        }
    }

    @Test("Running migration twice leaves data consistent (idempotent)")
    func rerunningMigrationIsIdempotent() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            try await cleanDirectories(engine: engine)

            let seededID = "dir-migration-idempotent-\(UUID().uuidString)"

            let initialContainer = try await DBContainer(
                testing: DirectoryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let initialContext = initialContainer.newContext()
            var seededUser = DirectoryMigrationUserV1(name: "Bob", email: "bob@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            // First run: migrates.
            let migratedContainer = try await DBContainer(
                for: DirectoryMigrationSchemaV2.self,
                migrationPlan: DirectoryMigrationCopyPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            // Second run: already at V2, should be a no-op.
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await DBContainer(
                testing: DirectoryMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let rows = try await verificationContainer.newContext()
                .fetch(DirectoryMigrationUserV2.self)
                .execute()

            let target = try #require(rows.first { $0.id == seededID })
            #expect(target.name == "Bob")
            #expect(target.email == "bob@example.com")

            try await cleanDirectories(engine: engine)
        }
    }

    @Test("purgeLegacyStorage clears both items and index keys from the legacy directory")
    func purgeLegacyStorageClearsIndexKeys() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            for path in [["directory_indexed_migration_test_legacy"], ["directory_indexed_migration_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }

            let seededID = "dir-indexed-\(UUID().uuidString)"

            let initialContainer = try await DBContainer(
                testing: DirectoryIndexedSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let initialContext = initialContainer.newContext()
            var seededUser = DirectoryIndexedUserV1(name: "Alice", email: "alice@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            // Sanity: V1 index subspace must hold one entry before migration.
            let legacySubspace = try await initialContainer.resolveDirectory(for: DirectoryIndexedUserV1.self)
            let legacyIndexPrefix = legacySubspace.subspace(SubspaceKey.indexes).subspace("DirectoryIndexedUser_email")
            let (legacyIndexBegin, legacyIndexEnd) = legacyIndexPrefix.range()
            let legacyIndexBefore = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyIndexBegin),
                    to: .firstGreaterOrEqual(legacyIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyIndexBefore == 1)

            let migratedContainer = try await DBContainer(
                for: DirectoryIndexedSchemaV2.self,
                migrationPlan: DirectoryIndexedCopyPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            // V1 index subspace must be empty after purgeLegacyStorage.
            let legacyIndexAfter = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyIndexBegin),
                    to: .firstGreaterOrEqual(legacyIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyIndexAfter == 0)

            // V2 index subspace must be populated by the copy.
            let targetSubspace = try await migratedContainer.resolveDirectory(for: DirectoryIndexedUserV2.self)
            let targetIndexPrefix = targetSubspace.subspace(SubspaceKey.indexes).subspace("DirectoryIndexedUser_email")
            let (targetIndexBegin, targetIndexEnd) = targetIndexPrefix.range()
            let targetIndexCount = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(targetIndexBegin),
                    to: .firstGreaterOrEqual(targetIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(targetIndexCount == 1)

            for path in [["directory_indexed_migration_test_legacy"], ["directory_indexed_migration_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }
        }
    }

    @Test("Custom stage builds a newly added index in the target directory after copying data")
    func addIndexRunsAfterDirectoryChange() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            for path in [["directory_add_idx_test_legacy"], ["directory_add_idx_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }

            let seededID = "dir-add-idx-\(UUID().uuidString)"

            let initialContainer = try await DBContainer(
                testing: DirectoryAddIdxSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let initialContext = initialContainer.newContext()
            var seededUser = DirectoryAddIdxUserV1(name: "Alice", score: 42)
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: DirectoryAddIdxSchemaV2.self,
                migrationPlan: DirectoryAddIdxPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            // Framework should have called addIndex for the new score index,
            // which builds it in the *target* (V2) directory against the rows
            // that willMigrate just copied there.
            let targetSubspace = try await migratedContainer.resolveDirectory(for: DirectoryAddIdxUserV2.self)
            let targetIndexPrefix = targetSubspace.subspace(SubspaceKey.indexes).subspace("DirectoryAddIdxUser_score")
            let (targetIndexBegin, targetIndexEnd) = targetIndexPrefix.range()
            let targetIndexCount = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(targetIndexBegin),
                    to: .firstGreaterOrEqual(targetIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(targetIndexCount == 1)

            for path in [["directory_add_idx_test_legacy"], ["directory_add_idx_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }
        }
    }

    @Test("Custom stage clears the dropped index from the legacy directory")
    func removeIndexRunsAfterDirectoryChange() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            for path in [["directory_rem_idx_test_legacy"], ["directory_rem_idx_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }

            let seededID = "dir-rem-idx-\(UUID().uuidString)"

            let initialContainer = try await DBContainer(
                testing: DirectoryRemIdxSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let initialContext = initialContainer.newContext()
            var seededUser = DirectoryRemIdxUserV1(name: "Alice", tag: "hot")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            // Sanity: legacy index subspace holds one entry before migration.
            let legacySubspace = try await initialContainer.resolveDirectory(for: DirectoryRemIdxUserV1.self)
            let legacyIndexPrefix = legacySubspace.subspace(SubspaceKey.indexes).subspace("DirectoryRemIdxUser_tag")
            let (legacyIndexBegin, legacyIndexEnd) = legacyIndexPrefix.range()
            let legacyIndexBefore = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyIndexBegin),
                    to: .firstGreaterOrEqual(legacyIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyIndexBefore == 1)

            let migratedContainer = try await DBContainer(
                for: DirectoryRemIdxSchemaV2.self,
                migrationPlan: DirectoryRemIdxPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            // removeIndex targets the *source* registry (the legacy directory).
            // Combined with didMigrate purgeLegacyStorage, the legacy index
            // subspace must be empty after migration.
            let legacyIndexAfter = try await engine.withTransaction { transaction in
                let pairs = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(legacyIndexBegin),
                    to: .firstGreaterOrEqual(legacyIndexEnd),
                    limit: 1000,
                    snapshot: true,
                    streamingMode: .wantAll
                )
                return pairs.count
            }
            #expect(legacyIndexAfter == 0)

            // V2 data must exist in the current directory.
            let verificationContainer = try await DBContainer(
                testing: DirectoryRemIdxSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            let rows = try await verificationContainer.newContext()
                .fetch(DirectoryRemIdxUserV2.self)
                .execute()
            #expect(rows.count == 1)

            for path in [["directory_rem_idx_test_legacy"], ["directory_rem_idx_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }
        }
    }

    @Test("Lightweight stage with changed #Directory is rejected with actionable error")
    func lightweightStageRejectsDirectoryChange() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let engine = try await makeSystemPriorityEngine()
            for path in [["directory_lightweight_test_legacy"], ["directory_lightweight_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }

            let initialContainer = try await DBContainer(
                testing: DirectoryLightweightSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled,
            )
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: DirectoryLightweightSchemaV2.self,
                migrationPlan: DirectoryLightweightPlan.self,
                configuration: .init(backend: .custom(engine))
            )

            await #expect(throws: MigrationPlanError.self) {
                try await migratedContainer.migrateIfNeeded()
            }

            for path in [["directory_lightweight_test_legacy"], ["directory_lightweight_test_current"]] {
                if try await engine.directoryService.exists(path: path) {
                    try await engine.directoryService.remove(path: path)
                }
            }
        }
    }
}
#endif

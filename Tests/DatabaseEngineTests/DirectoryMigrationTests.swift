#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import TestSupport
import TestHeartbeat
@testable import DatabaseEngine

// MARK: - Schema Versions With Different #Directory Paths

@Persistable(type: "DirectoryMigrationUser")
struct DirectoryMigrationUserV1 {
    #Directory<DirectoryMigrationUserV1>("test", "directory-migration", "legacy")

    var name: String
    var email: String
}

@Persistable(type: "DirectoryMigrationUser")
struct DirectoryMigrationUserV2 {
    #Directory<DirectoryMigrationUserV2>("test", "directory-migration", "current")

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
        for segment in ["legacy", "current"] {
            do {
                try await engine.directoryService.remove(path: ["test", "directory-migration", segment])
            } catch {
            }
        }
        do {
            try await engine.directoryService.remove(path: ["_metadata"])
        } catch {
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
                for: DirectoryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
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
                for: DirectoryMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
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
                for: DirectoryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
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
                for: DirectoryMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(engine)),
                security: .disabled
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
}
#endif

#if POSTGRESQL
import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import Core
import TestSupport
import TestHeartbeat

@Persistable(type: "PGDirectoryMigrationUser")
struct PGDirectoryMigrationUserV1 {
    #Directory<PGDirectoryMigrationUserV1>("test", "pg-directory-migration", "legacy")

    var name: String
    var email: String
}

@Persistable(type: "PGDirectoryMigrationUser")
struct PGDirectoryMigrationUserV2 {
    #Directory<PGDirectoryMigrationUserV2>("test", "pg-directory-migration", "current")

    var name: String
    var email: String
}

enum PGDirectoryMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [PGDirectoryMigrationUserV1.self]
}

enum PGDirectoryMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [PGDirectoryMigrationUserV2.self]
}

enum PGDirectoryMigrationCopyPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PGDirectoryMigrationSchemaV1.self, PGDirectoryMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: PGDirectoryMigrationSchemaV1.self,
                toVersion: PGDirectoryMigrationSchemaV2.self,
                willMigrate: copyLegacyUsers,
                didMigrate: purgeLegacyDirectory
            )
        ]
    }

    static func copyLegacyUsers(context: MigrationContext) async throws {
        var copied: [PGDirectoryMigrationUserV2] = []
        for try await legacyUser in context.enumerate(PGDirectoryMigrationUserV1.self) {
            var newUser = PGDirectoryMigrationUserV2(
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
        try await context.purgeLegacyStorage(PGDirectoryMigrationUserV1.self)
    }
}

@Suite("Directory Migration PostgreSQL Tests", .serialized, .heartbeat)
struct DirectoryMigrationPostgreSQLTests {
    @Test("Custom migration copies data across changed #Directory paths on PostgreSQL")
    func customMigrationCopiesAcrossDirectoryChange() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine
            let seededID = "pg-dir-migration-\(UUID().uuidString)"

            let initialContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGDirectoryMigrationSchemaV1.makeSchema()
            )
            let initialContext = initialContainer.newContext()
            var seededUser = PGDirectoryMigrationUserV1(name: "Alice", email: "alice@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: PGDirectoryMigrationSchemaV2.self,
                migrationPlan: PGDirectoryMigrationCopyPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGDirectoryMigrationSchemaV2.makeSchema()
            )
            let rows = try await verificationContainer.newContext()
                .fetch(PGDirectoryMigrationUserV2.self)
                .execute()

            #expect(rows.count == 1)
            let migrated = try #require(rows.first { $0.id == seededID })
            #expect(migrated.name == "Alice")
            #expect(migrated.email == "alice@example.com")
        }
    }

    @Test("Running migration twice leaves data consistent on PostgreSQL (idempotent)")
    func rerunningMigrationIsIdempotent() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            try await PostgreSQLTestSetup.shared.cleanAllData()
            let engine = try await PostgreSQLTestSetup.shared.engine
            let seededID = "pg-dir-migration-idempotent-\(UUID().uuidString)"

            let initialContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGDirectoryMigrationSchemaV1.makeSchema()
            )
            let initialContext = initialContainer.newContext()
            var seededUser = PGDirectoryMigrationUserV1(name: "Bob", email: "bob@example.com")
            seededUser.id = seededID
            initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: PGDirectoryMigrationSchemaV2.self,
                migrationPlan: PGDirectoryMigrationCopyPlan.self,
                configuration: .init(backend: .custom(engine))
            )
            try await migratedContainer.migrateIfNeeded()
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await PostgreSQLTestSetup.shared.makeContainer(
                schema: PGDirectoryMigrationSchemaV2.makeSchema()
            )
            let rows = try await verificationContainer.newContext()
                .fetch(PGDirectoryMigrationUserV2.self)
                .execute()

            let target = try #require(rows.first { $0.id == seededID })
            #expect(target.name == "Bob")
            #expect(target.email == "bob@example.com")
        }
    }
}
#endif

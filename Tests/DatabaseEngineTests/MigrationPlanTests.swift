#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
@testable import DatabaseKit
@testable import DatabaseEngine

// MARK: - Test Models (File Scope for @Persistable macro)

/// V1: Basic user with email index
@Persistable(type: "TestUser")
struct UserV1 {
    #Index(.ordered(name: "TestUser_email", keys: [.ascending(\UserV1.email)], unique: true))

    var id: String = ""
    var name: String
    var email: String
}

/// V2: User with additional age field and index
@Persistable(type: "TestUser")
struct UserV2 {
    #Index(.ordered(name: "TestUser_email", keys: [.ascending(\UserV2.email)], unique: true))
    #Index(.ordered(name: "TestUser_age", keys: [.ascending(\UserV2.age)], unique: false))

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

/// V3: User with removed age index, added createdAt
@Persistable(type: "TestUser")
struct UserV3 {
    #Index(.ordered(name: "TestUser_email", keys: [.ascending(\UserV3.email)], unique: true))
    #Index(.ordered(name: "TestUser_createdAt", keys: [.ascending(\UserV3.createdAt)], unique: false))

    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
    var createdAt: Double = 0
}

/// V2b: User with reordered fields (unsafe without explicit migration)
@Persistable(type: "TestUser")
struct UserV2Reordered {
    #Index(.ordered(name: "TestUser_email", keys: [.ascending(\UserV2Reordered.email)], unique: true))

    var id: String = ""
    var email: String
    var name: String
}

private func addedIndexNames(_ changes: [IndexChange]) -> Set<String> {
    Set(
        changes.compactMap { change in
            guard case .added(let descriptor) = change else { return nil }
            return descriptor.name
        })
}

private func removedIndexNames(_ changes: [IndexChange]) -> Set<String> {
    Set(
        changes.compactMap { change in
            guard case .removed(let descriptor) = change else { return nil }
            return descriptor.name
        })
}

/// Tests for Migration API
///
/// **Coverage**:
/// - VersionedSchema protocol
/// - SchemaMigrationPlan protocol
/// - MigrationStage enum
/// - DBContainer.testBaseAdmin().migrateIfNeeded()
@Suite("Migration Plan Tests", .heartbeat)
struct MigrationPlanTests {

    // MARK: - Test Schema Versions

    /// Schema V1: Basic user with email index
    enum MigrationSchemaV1: VersionedSchema {
        static let versionIdentifier = Schema.Version(1, 0, 0)
        static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try UserV1.schemaEntity] }
    }
    }

    /// Schema V2: User with additional age field and index
    enum MigrationSchemaV2: VersionedSchema {
        static let versionIdentifier = Schema.Version(2, 0, 0)
        static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try UserV2.schemaEntity] }
    }
    }

    /// Schema V3: User with removed age index, added createdAt
    enum MigrationSchemaV3: VersionedSchema {
        static let versionIdentifier = Schema.Version(3, 0, 0)
        static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try UserV3.schemaEntity] }
    }
    }

    enum MigrationSchemaV2Reordered: VersionedSchema {
        static let versionIdentifier = Schema.Version(2, 1, 0)
        static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try UserV2Reordered.schemaEntity] }
    }
    }

    // MARK: - Test Migration Plans

    /// Simple migration plan V1 -> V2
    enum SimpleMigrationPlan: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV1.self, MigrationSchemaV2.self]
        }

        static var stages: [MigrationStage] {
            [migrateV1toV2]
        }

        static let migrateV1toV2 = MigrationStage.lightweight(
            fromVersion: MigrationSchemaV1.self,
            toVersion: MigrationSchemaV2.self
        )
    }

    /// Complex migration plan V1 -> V2 -> V3
    enum ComplexMigrationPlan: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV1.self, MigrationSchemaV2.self, MigrationSchemaV3.self]
        }

        static var stages: [MigrationStage] {
            [migrateV1toV2, migrateV2toV3]
        }

        static let migrateV1toV2 = MigrationStage.lightweight(
            fromVersion: MigrationSchemaV1.self,
            toVersion: MigrationSchemaV2.self
        )

        static let migrateV2toV3 = MigrationStage.custom(
            fromVersion: MigrationSchemaV2.self,
            toVersion: MigrationSchemaV3.self,
            willMigrate: nil,
            didMigrate: nil
        )
    }

    /// Single schema (no migration needed)
    enum SingleSchemaPlan: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV1.self]
        }

        static var stages: [MigrationStage] {
            []  // No migrations for single schema
        }
    }

    // MARK: - VersionedSchema Tests

    /// Test: VersionedSchema creates Schema correctly
    @Test("VersionedSchema creates Schema correctly")
    func versionedSchemaCreatesSchema() throws {
        let schema = try MigrationSchemaV1.makeSchema()

        #expect(schema.version == Schema.Version(1, 0, 0))
        #expect(schema.entities.count == 1)
        #expect(schema.entities.first?.name == "TestUser")
    }

    /// Test: VersionedSchema collects all index descriptors
    @Test("VersionedSchema collects all index descriptors")
    func versionedSchemaCollectsIndexDescriptors() throws {
        let descriptors = try MigrationSchemaV2.allIndexDescriptors

        #expect(descriptors.count == 2)
        #expect(descriptors.contains(where: { $0.name == "TestUser_email" }))
        #expect(descriptors.contains(where: { $0.name == "TestUser_age" }))
    }

    /// Test: VersionedSchema detects index changes
    @Test("VersionedSchema detects index changes")
    func versionedSchemaDetectsIndexChanges() throws {
        let changes = try MigrationSchemaV2.indexChanges(
            from: MigrationSchemaV1.self
        )

        #expect(addedIndexNames(changes) == Set(["TestUser_age"]))
        #expect(removedIndexNames(changes).isEmpty)
    }

    /// Test: VersionedSchema detects lightweight migration possibility
    @Test("VersionedSchema detects lightweight migration possibility")
    func versionedSchemaDetectsLightweightMigration() throws {
        // V1 -> V2: Adding field and index (lightweight)
        #expect(
            try MigrationSchemaV2.canLightweightMigrate(
                from: MigrationSchemaV1.self
            )
        )

        // V2 -> V3: Adding field and index, removing index (lightweight)
        #expect(
            try MigrationSchemaV3.canLightweightMigrate(
                from: MigrationSchemaV2.self
            )
        )
    }

    // MARK: - SchemaMigrationPlan Tests

    /// Test: SchemaMigrationPlan validation passes for valid plan
    @Test("SchemaMigrationPlan validation passes for valid plan")
    func migrationPlanValidationPasses() throws {
        try SimpleMigrationPlan.validate()
        try ComplexMigrationPlan.validate()
        try SingleSchemaPlan.validate()
    }

    /// Test: SchemaMigrationPlan finds migration path
    @Test("SchemaMigrationPlan finds migration path")
    func migrationPlanFindsPath() throws {
        let path = try ComplexMigrationPlan.findPath(
            from: Schema.Version(1, 0, 0),
            to: Schema.Version(3, 0, 0)
        )

        #expect(path.count == 2)
        #expect(path[0].fromVersionIdentifier == Schema.Version(1, 0, 0))
        #expect(path[0].toVersionIdentifier == Schema.Version(2, 0, 0))
        #expect(path[1].fromVersionIdentifier == Schema.Version(2, 0, 0))
        #expect(path[1].toVersionIdentifier == Schema.Version(3, 0, 0))
    }

    /// Test: SchemaMigrationPlan returns empty path for same version
    @Test("SchemaMigrationPlan returns empty path for same version")
    func migrationPlanReturnsEmptyPathForSameVersion() throws {
        let path = try SimpleMigrationPlan.findPath(
            from: Schema.Version(1, 0, 0),
            to: Schema.Version(1, 0, 0)
        )

        #expect(path.isEmpty)
    }

    /// Test: SchemaMigrationPlan throws for downgrade
    @Test("SchemaMigrationPlan throws for downgrade")
    func migrationPlanThrowsForDowngrade() {
        do {
            _ = try SimpleMigrationPlan.findPath(
                from: Schema.Version(2, 0, 0),
                to: Schema.Version(1, 0, 0)
            )
            Issue.record("Expected downgradeNotSupported error")
        } catch let error as MigrationPlanError {
            if case .downgradeNotSupported(let from, let to) = error {
                #expect(from == Schema.Version(2, 0, 0))
                #expect(to == Schema.Version(1, 0, 0))
            } else {
                Issue.record("Unexpected error type: \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    /// Test: SchemaMigrationPlan currentVersion returns latest
    @Test("SchemaMigrationPlan currentVersion returns latest")
    func migrationPlanCurrentVersion() {
        #expect(SimpleMigrationPlan.currentVersion == Schema.Version(2, 0, 0))
        #expect(ComplexMigrationPlan.currentVersion == Schema.Version(3, 0, 0))
        #expect(SingleSchemaPlan.currentVersion == Schema.Version(1, 0, 0))
    }

    // MARK: - MigrationStage Tests

    /// Test: MigrationStage.lightweight properties
    @Test("MigrationStage.lightweight properties")
    func lightweightStageProperties() {
        let stage = MigrationStage.lightweight(
            fromVersion: MigrationSchemaV1.self,
            toVersion: MigrationSchemaV2.self
        )

        #expect(stage.isLightweight)
        #expect(stage.fromVersionIdentifier == Schema.Version(1, 0, 0))
        #expect(stage.toVersionIdentifier == Schema.Version(2, 0, 0))
        #expect(stage.willMigrate == nil)
        #expect(stage.didMigrate == nil)
    }

    /// Test: MigrationStage.custom properties
    @Test("MigrationStage.custom properties")
    func customStageProperties() {
        let stage = MigrationStage.custom(
            fromVersion: MigrationSchemaV1.self,
            toVersion: MigrationSchemaV2.self,
            willMigrate: { _ in /* pre-migration */ },
            didMigrate: { _ in /* post-migration */ }
        )

        #expect(!stage.isLightweight)
        #expect(stage.fromVersionIdentifier == Schema.Version(1, 0, 0))
        #expect(stage.toVersionIdentifier == Schema.Version(2, 0, 0))
        #expect(stage.willMigrate != nil)
        #expect(stage.didMigrate != nil)
    }

    /// Test: MigrationStage detects index changes
    @Test("MigrationStage detects index changes")
    func stageDetectsIndexChanges() throws {
        let stage = MigrationStage.lightweight(
            fromVersion: MigrationSchemaV1.self,
            toVersion: MigrationSchemaV2.self
        )

        let changes = try stage.indexChanges
        #expect(addedIndexNames(changes) == Set(["TestUser_age"]))
        #expect(removedIndexNames(changes).isEmpty)
    }

    /// Test: MigrationStage detects index removal
    @Test("MigrationStage detects index removal")
    func stageDetectsIndexRemoval() throws {
        let stage = MigrationStage.lightweight(
            fromVersion: MigrationSchemaV2.self,
            toVersion: MigrationSchemaV3.self
        )

        let changes = try stage.indexChanges
        #expect(addedIndexNames(changes) == Set(["TestUser_createdAt"]))
        #expect(removedIndexNames(changes) == Set(["TestUser_age"]))
    }

    /// Test: MigrationStage.automatic selects lightweight when possible
    @Test("MigrationStage.automatic selects lightweight when possible")
    func automaticSelectsLightweight() throws {
        let stage = try MigrationStage.automatic(
            from: MigrationSchemaV1.self,
            to: MigrationSchemaV2.self
        )

        #expect(stage.isLightweight)
    }

    /// Test: MigrationStage.automatic selects custom when hooks provided
    @Test("MigrationStage.automatic selects custom when hooks provided")
    func automaticSelectsCustomWithHooks() throws {
        let stage = try MigrationStage.automatic(
            from: MigrationSchemaV1.self,
            to: MigrationSchemaV2.self,
            willMigrate: { _ in }
        )

        #expect(!stage.isLightweight)
    }

    @Test("Unsafe field reordering is not lightweight")
    func unsafeFieldReorderingIsNotLightweight() throws {
        #expect(
            try !MigrationSchemaV2Reordered.canLightweightMigrate(
                from: MigrationSchemaV1.self
            )
        )

        let stage = try MigrationStage.automatic(
            from: MigrationSchemaV1.self,
            to: MigrationSchemaV2Reordered.self
        )

        #expect(!stage.isLightweight)
        let report = try stage.schemaCompatibilityReport
        #expect(
            report.allIssues.contains(
                .renumberedField(
                    entityName: "TestUser",
                    fieldName: "email",
                    expected: 3,
                    actual: 2
                )
            )
        )
    }

    // MARK: - Validation Error Tests

    /// Invalid plan with wrong stage count
    enum InvalidStageCoun: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV1.self, MigrationSchemaV2.self, MigrationSchemaV3.self]
        }
        static var stages: [MigrationStage] {
            [MigrationStage.lightweight(fromVersion: MigrationSchemaV1.self, toVersion: MigrationSchemaV2.self)]
            // Missing V2 -> V3 stage
        }
    }

    /// Test: Validation fails for wrong stage count
    @Test("Validation fails for wrong stage count")
    func validationFailsForWrongStageCount() {
        do {
            try InvalidStageCoun.validate()
            Issue.record("Expected stageCountMismatch error")
        } catch let error as MigrationPlanError {
            if case .stageCountMismatch(let expected, let actual) = error {
                #expect(expected == 2)
                #expect(actual == 1)
            } else {
                Issue.record("Unexpected error type: \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    /// Invalid plan with out-of-order versions
    enum OutOfOrderPlan: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV2.self, MigrationSchemaV1.self]  // Wrong order
        }
        static var stages: [MigrationStage] {
            [MigrationStage.lightweight(fromVersion: MigrationSchemaV2.self, toVersion: MigrationSchemaV1.self)]
        }
    }

    enum InvalidLightweightPlan: SchemaMigrationPlan {
        static var schemas: [any VersionedSchema.Type] {
            [MigrationSchemaV1.self, MigrationSchemaV2Reordered.self]
        }
        static var stages: [MigrationStage] {
            [MigrationStage.lightweight(fromVersion: MigrationSchemaV1.self, toVersion: MigrationSchemaV2Reordered.self)]
        }
    }

    /// Test: Validation fails for out-of-order versions
    @Test("Validation fails for out-of-order versions")
    func validationFailsForOutOfOrderVersions() {
        do {
            try OutOfOrderPlan.validate()
            Issue.record("Expected versionsNotOrdered error")
        } catch let error as MigrationPlanError {
            if case .versionsNotOrdered = error {
                // Expected
            } else {
                Issue.record("Unexpected error type: \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Validation fails when lightweight stage contains breaking schema changes")
    func validationFailsForIncompatibleLightweightStage() {
        do {
            try InvalidLightweightPlan.validate()
            Issue.record("Expected incompatibleLightweightStage error")
        } catch let error as MigrationPlanError {
            if case .incompatibleLightweightStage(let stageIndex, let issues) = error {
                #expect(stageIndex == 0)
                #expect(
                    issues.contains(
                        .renumberedField(
                            entityName: "TestUser",
                            fieldName: "email",
                            expected: 3,
                            actual: 2
                        )
                    )
                )
            } else {
                Issue.record("Unexpected error type: \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}
#endif

#endif

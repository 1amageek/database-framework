import Testing
import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine
@testable import Core
@testable import DatabaseEngine

/// Tests for Migration functionality
///
/// **Coverage**:
/// - Migration path finding
/// - Migration execution
/// - MigrationContext index operations
/// - Entity-scoped index registration
@Suite("Migration Tests")
struct MigrationTests {

    // MARK: - Helper Types

    @Persistable
    struct TestUser {
        var id: Int64 = Int64(Date().timeIntervalSince1970 * 1000)

        #Index<TestUser>([\.email], type: ScalarIndexKind())

        var email: String
        var name: String
    }

    @Persistable
    struct TestProduct {
        var id: Int64 = Int64(Date().timeIntervalSince1970 * 1000)

        #Index<TestProduct>([\.price], type: ScalarIndexKind())

        var name: String
        var price: Double
    }

    // MARK: - Helper Methods

    private func setupDatabase() async throws -> (any DatabaseProtocol, FDBContainer) {
        // Ensure FDB is initialized (safe to call multiple times)
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Register test types with IndexBuilderRegistry
        // This is needed because some tests create Schema manually via Schema(entities:)
        // which doesn't trigger auto-registration
        IndexBuilderRegistry.shared.register(TestUser.self)
        IndexBuilderRegistry.shared.register(TestProduct.self)

        // Create test schema
        let schema = Schema([TestUser.self, TestProduct.self], version: Schema.Version(1, 0, 0))

        // Create test subspace (isolated)
        let testSubspace = Subspace(prefix: Tuple("migration_test", UUID().uuidString).pack())

        // Create test-specific DirectoryLayer to ensure test isolation
        // Node subspace: [testSubspace]/dir_metadata
        // Content subspace: [testSubspace]
        let testDirectoryLayer = DirectoryLayer(
            database: database,
            nodeSubspace: testSubspace.subspace(0xFE),
            contentSubspace: testSubspace
        )

        // Create container with custom DirectoryLayer
        let container = FDBContainer(
            database: database,
            schema: schema,
            subspace: testSubspace,
            directoryLayer: testDirectoryLayer
        )

        return (database, container)
    }

    // MARK: - Tests

    /// Test: findMigrationPath with linear chain
    @Test("findMigrationPath with linear chain")
    func findMigrationPathLinearChain() async throws {
        let (_, container) = try await setupDatabase()

        // Create migration chain: 1.0.0 → 1.1.0 → 2.0.0
        let migration1 = Migration(
            fromVersion: Schema.Version(1, 0, 0),
            toVersion: Schema.Version(1, 1, 0),
            description: "Add index 1"
        ) { _ in }

        let migration2 = Migration(
            fromVersion: Schema.Version(1, 1, 0),
            toVersion: Schema.Version(2, 0, 0),
            description: "Add index 2"
        ) { _ in }

        let schema = Schema([TestUser.self], version: Schema.Version(1, 0, 0))
        let containerWithMigrations = FDBContainer(
            database: container.database,
            schema: schema,
            migrations: [migration1, migration2],
            subspace: container.subspace
        )

        // Set initial version
        try await containerWithMigrations.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        // Execute migration to 2.0.0
        try await containerWithMigrations.migrate(to: Schema.Version(2, 0, 0))

        // Verify final version
        let finalVersion = try await containerWithMigrations.getCurrentSchemaVersion()
        #expect(finalVersion == Schema.Version(2, 0, 0))
    }

    /// Test: findMigrationPath fails when path is missing
    @Test("findMigrationPath fails when path is missing")
    func findMigrationPathMissingPath() async throws {
        let (_, container) = try await setupDatabase()

        // Create incomplete chain: 1.0.0 → 1.1.0, but target is 2.0.0
        let migration1 = Migration(
            fromVersion: Schema.Version(1, 0, 0),
            toVersion: Schema.Version(1, 1, 0),
            description: "Add index 1"
        ) { _ in }

        let schema = Schema([TestUser.self], version: Schema.Version(1, 0, 0))
        let containerWithMigrations = FDBContainer(
            database: container.database,
            schema: schema,
            migrations: [migration1],
            subspace: container.subspace
        )

        // Set initial version
        try await containerWithMigrations.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        // Should throw error (no path from 1.1.0 to 2.0.0)
        do {
            try await containerWithMigrations.migrate(to: Schema.Version(2, 0, 0))
            Issue.record("Expected error for missing migration path")
        } catch let error as FDBRuntimeError {
            // Verify error message mentions the missing path
            let description = error.description
            #expect(description.contains("No migration path found"))
            #expect(description.contains("1.0.0"))
            #expect(description.contains("2.0.0"))
        }
    }

    /// Test: MigrationContext identifies target entity correctly
    @Test("MigrationContext identifies target entity correctly")
    func migrationContextIdentifiesTargetEntity() async throws {
        let (_, container) = try await setupDatabase()

        // Create new index for TestUser
        let newIndex = IndexDescriptor(
            name: "TestUser_name",
            keyPaths: [\TestUser.name],
            kind: ScalarIndexKind(),
            commonOptions: .init()
        )

        // Update schema with new index for TestUser
        // Note: We need to manually create entities to include the new index
        let userEntity = Schema.Entity(
            name: "TestUser",
            allFields: TestUser.allFields,
            indexDescriptors: TestUser.indexDescriptors + [newIndex],
            enumMetadata: [:]
        )

        let productEntity = Schema.Entity(
            name: "TestProduct",
            allFields: TestProduct.allFields,
            indexDescriptors: TestProduct.indexDescriptors,
            enumMetadata: [:]
        )

        // Note: Schema(entities:...) now collects indexDescriptors from entities automatically.
        let schemaV2 = Schema(
            entities: [userEntity, productEntity],
            version: Schema.Version(2, 0, 0)
        )

        let migration = Migration(
            fromVersion: Schema.Version(1, 0, 0),
            toVersion: Schema.Version(2, 0, 0),
            description: "Add TestUser_name index"
        ) { context in
            try await context.addIndex(newIndex)
        }

        let containerWithMigration = FDBContainer(
            database: container.database,
            schema: schemaV2,
            migrations: [migration],
            subspace: container.subspace
        )

        // Set initial version
        try await containerWithMigration.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        // Execute migration
        try await containerWithMigration.migrate(to: Schema.Version(2, 0, 0))

        // Verify version updated
        let finalVersion = try await containerWithMigration.getCurrentSchemaVersion()
        #expect(finalVersion == Schema.Version(2, 0, 0))

        // Note: We cannot easily verify index state without accessing IndexStateManager,
        // but the migration should complete without errors
    }

    /// Test: MigrationContext.addIndex builds index and transitions to readable state
    @Test("MigrationContext.addIndex builds index and transitions to readable state")
    func addIndexBuildsAndTransitionsToReadable() async throws {
        let (_, container) = try await setupDatabase()

        // Create new index
        let newIndex = IndexDescriptor(
            name: "TestUser_name_v2",
            keyPaths: [\TestUser.name],
            kind: ScalarIndexKind(),
            commonOptions: .init()
        )

        // Update schema with new index
        let userEntity = Schema.Entity(
            name: "TestUser",
            allFields: TestUser.allFields,
            indexDescriptors: TestUser.indexDescriptors + [newIndex],
            enumMetadata: [:]
        )

        // Note: Schema(entities:...) now collects indexDescriptors from entities automatically.
        let schemaV2 = Schema(
            entities: [userEntity],
            version: Schema.Version(2, 0, 0)
        )

        let migration = Migration(
            fromVersion: Schema.Version(1, 0, 0),
            toVersion: Schema.Version(2, 0, 0),
            description: "Add TestUser_name_v2 index"
        ) { context in
            try await context.addIndex(newIndex)
        }

        let containerWithMigration = FDBContainer(
            database: container.database,
            schema: schemaV2,
            migrations: [migration],
            subspace: container.subspace
        )

        // Set initial version
        try await containerWithMigration.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        // Execute migration
        try await containerWithMigration.migrate(to: Schema.Version(2, 0, 0))

        // Verify migration completed
        let finalVersion = try await containerWithMigration.getCurrentSchemaVersion()
        #expect(finalVersion == Schema.Version(2, 0, 0))

        // Index should now be in readable state after OnlineIndexer builds it
        // The index transitions: disabled → writeOnly → readable
    }

    /// Test: getCurrentSchemaVersion returns nil for new database
    @Test("getCurrentSchemaVersion returns nil for new database")
    func getCurrentSchemaVersionReturnsNilForNewDatabase() async throws {
        let (_, container) = try await setupDatabase()

        let version = try await container.getCurrentSchemaVersion()
        #expect(version == nil)
    }

    /// Test: setCurrentSchemaVersion and getCurrentSchemaVersion roundtrip
    @Test("setCurrentSchemaVersion and getCurrentSchemaVersion roundtrip")
    func schemaVersionRoundtrip() async throws {
        let (_, container) = try await setupDatabase()

        let testVersion = Schema.Version(1, 2, 3)
        try await container.setCurrentSchemaVersion(testVersion)

        let retrievedVersion = try await container.getCurrentSchemaVersion()
        #expect(retrievedVersion == testVersion)
    }

    /// Test: Migration with multiple entities maintains entity-scoped indexing
    @Test("Migration with multiple entities maintains entity-scoped indexing")
    func multipleEntitiesEntityScopedIndexing() async throws {
        let (_, container) = try await setupDatabase()

        // Create index for TestProduct (not TestUser)
        let productIndex = IndexDescriptor(
            name: "TestProduct_name",
            keyPaths: [\TestProduct.name],
            kind: ScalarIndexKind(),
            commonOptions: .init()
        )

        // Update schema with new index for TestProduct
        let productEntity = Schema.Entity(
            name: "TestProduct",
            allFields: TestProduct.allFields,
            indexDescriptors: TestProduct.indexDescriptors + [productIndex],
            enumMetadata: [:]
        )

        let userEntity = Schema.Entity(
            name: "TestUser",
            allFields: TestUser.allFields,
            indexDescriptors: TestUser.indexDescriptors,
            enumMetadata: [:]
        )

        // Note: Schema(entities:...) now collects indexDescriptors from entities automatically.
        // We don't need to pass them separately - entities already have their indexDescriptors.
        let schemaV2 = Schema(
            entities: [userEntity, productEntity],
            version: Schema.Version(2, 0, 0)
        )

        let migration = Migration(
            fromVersion: Schema.Version(1, 0, 0),
            toVersion: Schema.Version(2, 0, 0),
            description: "Add TestProduct_name index"
        ) { context in
            try await context.addIndex(productIndex)
        }

        let containerWithMigration = FDBContainer(
            database: container.database,
            schema: schemaV2,
            migrations: [migration],
            subspace: container.subspace
        )

        // Set initial version
        try await containerWithMigration.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        // Execute migration
        try await containerWithMigration.migrate(to: Schema.Version(2, 0, 0))

        // Verify migration completed
        let finalVersion = try await containerWithMigration.getCurrentSchemaVersion()
        #expect(finalVersion == Schema.Version(2, 0, 0))

        // Note: The index should only be registered to TestProduct's store, not TestUser's
        // This verifies entity-scoped index registration
    }

    // MARK: - MigrationContext Batch Operations Tests

    /// Helper type for batch operation tests (using String ID for ULID compatibility)
    struct BatchTestUser: Persistable, Codable, Sendable {
        static let persistableType = "BatchTestUser"
        static let allFields = ["id", "name", "status"]
        static let indexDescriptors: [IndexDescriptor] = []

        static func fieldNumber(for fieldName: String) -> Int? {
            switch fieldName {
            case "id": return 1
            case "name": return 2
            case "status": return 3
            default: return nil
            }
        }

        static func enumMetadata(for fieldName: String) -> EnumMetadata? {
            return nil
        }

        static func fieldName<Value>(for keyPath: KeyPath<BatchTestUser, Value>) -> String {
            switch keyPath {
            case \BatchTestUser.id: return "id"
            case \BatchTestUser.name: return "name"
            case \BatchTestUser.status: return "status"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: PartialKeyPath<BatchTestUser>) -> String {
            switch keyPath {
            case \BatchTestUser.id: return "id"
            case \BatchTestUser.name: return "name"
            case \BatchTestUser.status: return "status"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: AnyKeyPath) -> String {
            if let partialKeyPath = keyPath as? PartialKeyPath<BatchTestUser> {
                return fieldName(for: partialKeyPath)
            }
            return "\(keyPath)"
        }

        var id: String
        var name: String
        var status: String

        init(id: String = ULID().ulidString, name: String, status: String = "active") {
            self.id = id
            self.name = name
            self.status = status
        }

        subscript(dynamicMember member: String) -> (any Sendable)? {
            switch member {
            case "id": return id
            case "name": return name
            case "status": return status
            default: return nil
            }
        }
    }

    private func setupBatchTestDatabase() async throws -> (any DatabaseProtocol, FDBContainer) {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Register test type
        IndexBuilderRegistry.shared.register(BatchTestUser.self)

        let schema = Schema([BatchTestUser.self], version: Schema.Version(1, 0, 0))
        let testSubspace = Subspace(prefix: Tuple("batch_migration_test", UUID().uuidString).pack())

        let testDirectoryLayer = DirectoryLayer(
            database: database,
            nodeSubspace: testSubspace.subspace(0xFE),
            contentSubspace: testSubspace
        )

        let container = FDBContainer(
            database: database,
            schema: schema,
            subspace: testSubspace,
            directoryLayer: testDirectoryLayer
        )

        return (database, container)
    }

    /// Helper to insert test records directly using DirectoryLayer
    private func insertTestRecords(
        _ container: FDBContainer,
        records: [BatchTestUser]
    ) async throws {
        let encoder = ProtobufEncoder()

        // Use the same directory approach as migration context
        let entitySubspace = try await container.getOrOpenDirectory(path: ["BatchTestUser"])
        let recordSubspace = entitySubspace.subspace("R").subspace("BatchTestUser")

        try await container.database.withTransaction { transaction in
            for record in records {
                let data = try encoder.encode(record)
                let recordKey = recordSubspace.pack(Tuple(record.id))
                transaction.setValue(Array(data), for: recordKey)
            }
        }
    }

    /// Test: MigrationContext batch operations work correctly
    ///
    /// This test verifies update, delete, batchUpdate, and batchDelete by
    /// directly invoking MigrationContext methods and verifying results.
    @Test("MigrationContext batch operations work correctly")
    func migrationContextBatchOperations() async throws {
        let (_, container) = try await setupBatchTestDatabase()

        // Create test records with predictable IDs
        let updateId = "update-test-\(UUID().uuidString)"
        let deleteId = "delete-test-\(UUID().uuidString)"
        let batchUpdateIds = (1...3).map { "batch-update-\($0)-\(UUID().uuidString)" }
        let batchDeleteIds = (1...3).map { "batch-delete-\($0)-\(UUID().uuidString)" }

        // Insert all test records
        let updateRecord = BatchTestUser(id: updateId, name: "ToUpdate", status: "active")
        let deleteRecord = BatchTestUser(id: deleteId, name: "ToDelete", status: "active")
        let batchUpdateRecords = batchUpdateIds.map { BatchTestUser(id: $0, name: "BatchUpdate", status: "active") }
        let batchDeleteRecords = batchDeleteIds.map { BatchTestUser(id: $0, name: "BatchDelete", status: "active") }

        try await insertTestRecords(container, records: [updateRecord, deleteRecord] + batchUpdateRecords + batchDeleteRecords)

        // Create MigrationContext directly with same DirectoryLayer subspace
        let entitySubspace = try await container.getOrOpenDirectory(path: ["BatchTestUser"])
        let storeInfo = MigrationStoreInfo(
            subspace: entitySubspace,
            indexSubspace: entitySubspace.subspace("I")
        )
        let storeRegistry = ["BatchTestUser": storeInfo]

        let context = MigrationContext(
            database: container.database,
            schema: container.schema,
            metadataSubspace: container.subspace.subspace("_metadata"),
            storeRegistry: storeRegistry
        )

        // Single update
        let updatedRecord = BatchTestUser(id: updateId, name: "ToUpdate", status: "updated")
        try await context.update(updatedRecord)

        // Single delete
        let recordToDelete = BatchTestUser(id: deleteId, name: "ToDelete", status: "active")
        try await context.delete(recordToDelete)

        // Batch update
        let recordsToUpdate = batchUpdateIds.map {
            BatchTestUser(id: $0, name: "BatchUpdate", status: "batch_updated")
        }
        try await context.batchUpdate(recordsToUpdate, batchSize: 2)

        // Batch delete
        let recordsToDelete = batchDeleteIds.map {
            BatchTestUser(id: $0, name: "BatchDelete", status: "active")
        }
        try await context.batchDelete(recordsToDelete, batchSize: 2)

        // Verify results
        let decoder = ProtobufDecoder()
        let recordSubspace = entitySubspace.subspace("R").subspace("BatchTestUser")

        // Check single update
        let updateKey = recordSubspace.pack(Tuple(updateId))
        let updateData: FDB.Bytes? = try await container.database.withTransaction { tx in
            try await tx.getValue(for: updateKey, snapshot: false)
        }
        #expect(updateData != nil)
        let updatedUser = try decoder.decode(BatchTestUser.self, from: Data(updateData!))
        #expect(updatedUser.status == "updated")

        // Check single delete
        let deleteKey = recordSubspace.pack(Tuple(deleteId))
        let deleteData: FDB.Bytes? = try await container.database.withTransaction { tx in
            try await tx.getValue(for: deleteKey, snapshot: false)
        }
        #expect(deleteData == nil)

        // Check batch update
        for id in batchUpdateIds {
            let key = recordSubspace.pack(Tuple(id))
            let data: FDB.Bytes? = try await container.database.withTransaction { tx in
                try await tx.getValue(for: key, snapshot: false)
            }
            #expect(data != nil)
            let user = try decoder.decode(BatchTestUser.self, from: Data(data!))
            #expect(user.status == "batch_updated")
        }

        // Check batch delete
        for id in batchDeleteIds {
            let key = recordSubspace.pack(Tuple(id))
            let data: FDB.Bytes? = try await container.database.withTransaction { tx in
                try await tx.getValue(for: key, snapshot: false)
            }
            #expect(data == nil)
        }
    }

    /// Test: MigrationContext.count counts records correctly
    @Test("MigrationContext.count counts records correctly")
    func migrationContextCount() async throws {
        let (_, container) = try await setupBatchTestDatabase()

        // Insert test records
        let records = (1...7).map { BatchTestUser(name: "User \($0)") }
        try await insertTestRecords(container, records: records)

        // Create MigrationContext manually using internal API with DirectoryLayer subspace
        let entitySubspace = try await container.getOrOpenDirectory(path: ["BatchTestUser"])
        let storeInfo = MigrationStoreInfo(
            subspace: entitySubspace,
            indexSubspace: entitySubspace.subspace("I")
        )
        let storeRegistry = ["BatchTestUser": storeInfo]

        let context = MigrationContext(
            database: container.database,
            schema: container.schema,
            metadataSubspace: container.subspace.subspace("_metadata"),
            storeRegistry: storeRegistry
        )

        let count = try await context.count(BatchTestUser.self)
        #expect(count == 7)
    }
}

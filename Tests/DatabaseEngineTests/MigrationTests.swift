import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

/// Tests for Migration functionality
///
/// **Coverage**:
/// - Schema version operations
/// - MigrationContext batch operations
@Suite("Migration Tests", .serialized)
struct MigrationTests {

    // MARK: - Helper Types

    @Persistable
    struct MigrationTestUser {
        #Directory<MigrationTestUser>("test", "migration", "users")
        #Index(ScalarIndexKind<MigrationTestUser>(fields: [\.email]))

        var id: String = ULID().ulidString
        var email: String
        var name: String
    }

    @Persistable
    struct BatchTestRecord {
        #Directory<BatchTestRecord>("test", "migration", "batch")

        var id: String = ULID().ulidString
        var name: String
        var status: String

        init(id: String = ULID().ulidString, name: String, status: String = "active") {
            self.id = id
            self.name = name
            self.status = status
        }
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([MigrationTestUser.self], version: Schema.Version(1, 0, 0))

        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func setupBatchTestContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([BatchTestRecord.self], version: Schema.Version(1, 0, 0))

        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "migration"])
        try? await directoryLayer.remove(path: ["_metadata"])
    }

    private func insertTestRecords(
        container: FDBContainer,
        records: [BatchTestRecord]
    ) async throws {
        let encoder = ProtobufEncoder()
        let subspace = try await container.resolveDirectory(for: BatchTestRecord.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchTestRecord.persistableType)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        try await container.database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            for record in records {
                let data = try encoder.encode(record)
                let validatedID = try record.validateIDForStorage()
                let itemKey = itemSubspace.pack(Tuple(validatedID))
                try await storage.write(Array(data), for: itemKey)
            }
        }
    }

    // MARK: - Schema Version Tests

    @Test("getCurrentSchemaVersion returns nil for new database")
    func getCurrentSchemaVersionReturnsNilForNewDatabase() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let version = try await container.getCurrentSchemaVersion()
            #expect(version == nil)
        }
    }

    @Test("setCurrentSchemaVersion and getCurrentSchemaVersion roundtrip")
    func schemaVersionRoundtrip() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let testVersion = Schema.Version(1, 2, 3)
            try await container.setCurrentSchemaVersion(testVersion)

            let retrievedVersion = try await container.getCurrentSchemaVersion()
            #expect(retrievedVersion == testVersion)
        }
    }

    @Test("Schema version persists across container instances")
    func schemaVersionPersistsAcrossContainers() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema([MigrationTestUser.self], version: Schema.Version(2, 0, 0))

            // Clean up first
            let directoryLayer = DirectoryLayer(database: database)
            try? await directoryLayer.remove(path: ["_metadata"])

            // Create first container and set version
            let container1 = FDBContainer(database: database, schema: schema, security: .disabled)
            try await container1.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

            // Create second container and read version
            let container2 = FDBContainer(database: database, schema: schema, security: .disabled)
            let version = try await container2.getCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))

            // Cleanup
            try? await directoryLayer.remove(path: ["_metadata"])
        }
    }

    // MARK: - Batch Data Operations Tests

    @Test("MigrationContext batch update works correctly")
    func migrationContextBatchUpdate() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            // Create test records with known IDs
            let records = (1...5).map { BatchTestRecord(name: "User \($0)", status: "active") }
            try await insertTestRecords(container: container, records: records)

            // Setup MigrationContext
            let subspace = try await container.resolveDirectory(for: BatchTestRecord.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                indexSubspace: subspace.subspace(SubspaceKey.indexes),
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchTestRecord.persistableType: storeInfo]

            let directoryLayer = DirectoryLayer(database: container.database)
            let metadataSubspace = try await directoryLayer.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                database: container.database,
                schema: container.schema,
                metadataSubspace: metadataSubspace.subspace,
                storeRegistry: storeRegistry
            )

            // Batch update records
            let updatedRecords = records.map {
                BatchTestRecord(id: $0.id, name: $0.name, status: "migrated")
            }
            try await context.batchUpdate(updatedRecords, batchSize: 2)

            // Verify updates
            let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchTestRecord.persistableType)

            for record in records {
                let validatedID = try record.validateIDForStorage()
                let key = itemSubspace.pack(Tuple(validatedID))
                let data: FDB.Bytes? = try await container.database.withTransaction { tx in
                    let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace)
                    return try await storage.read(for: key, snapshot: false)
                }
                guard let data = data else {
                    Issue.record("Record with id \(record.id) not found after batchUpdate")
                    continue
                }
                let decoded: BatchTestRecord = try DataAccess.deserialize(data)
                #expect(decoded.status == "migrated", "Expected status 'migrated' but got '\(decoded.status)'")
            }
        }
    }

    @Test("MigrationContext count works correctly")
    func migrationContextCount() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            // Insert test records
            let records = (1...7).map { BatchTestRecord(name: "User \($0)") }
            try await insertTestRecords(container: container, records: records)

            // Setup MigrationContext
            let subspace = try await container.resolveDirectory(for: BatchTestRecord.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                indexSubspace: subspace.subspace(SubspaceKey.indexes),
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchTestRecord.persistableType: storeInfo]

            let directoryLayer = DirectoryLayer(database: container.database)
            let metadataSubspace = try await directoryLayer.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                database: container.database,
                schema: container.schema,
                metadataSubspace: metadataSubspace.subspace,
                storeRegistry: storeRegistry
            )

            let count = try await context.count(BatchTestRecord.self)
            #expect(count == 7)
        }
    }

    @Test("MigrationContext single update and delete work correctly")
    func migrationContextSingleOperations() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            // Create test records
            let updateRecord = BatchTestRecord(name: "ToUpdate", status: "active")
            let deleteRecord = BatchTestRecord(name: "ToDelete", status: "active")
            try await insertTestRecords(container: container, records: [updateRecord, deleteRecord])

            // Setup MigrationContext
            let subspace = try await container.resolveDirectory(for: BatchTestRecord.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                indexSubspace: subspace.subspace(SubspaceKey.indexes),
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchTestRecord.persistableType: storeInfo]

            let directoryLayer = DirectoryLayer(database: container.database)
            let metadataSubspace = try await directoryLayer.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                database: container.database,
                schema: container.schema,
                metadataSubspace: metadataSubspace.subspace,
                storeRegistry: storeRegistry
            )

            // Single update
            let updated = BatchTestRecord(id: updateRecord.id, name: "ToUpdate", status: "updated")
            try await context.update(updated)

            // Single delete
            try await context.delete(deleteRecord)

            // Verify
            let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchTestRecord.persistableType)

            // Check update
            let updateValidatedID = try updateRecord.validateIDForStorage()
            let updateKey = itemSubspace.pack(Tuple(updateValidatedID))
            let updateData: FDB.Bytes? = try await container.database.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace)
                return try await storage.read(for: updateKey, snapshot: false)
            }
            #expect(updateData != nil, "Updated item not found")
            if let updateData = updateData {
                let decoded: BatchTestRecord = try DataAccess.deserialize(updateData)
                #expect(decoded.status == "updated", "Expected status 'updated' but got '\(decoded.status)'")
            }

            // Check delete
            let deleteValidatedID = try deleteRecord.validateIDForStorage()
            let deleteKey = itemSubspace.pack(Tuple(deleteValidatedID))
            let deleteData: FDB.Bytes? = try await container.database.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace)
                return try await storage.read(for: deleteKey, snapshot: false)
            }
            #expect(deleteData == nil)
        }
    }
}

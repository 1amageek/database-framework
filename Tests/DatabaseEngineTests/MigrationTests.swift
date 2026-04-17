#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import TestSupport
@testable import DatabaseEngine

@Persistable(type: "SchemaRegistryAppendOnlyUser")
struct SchemaRegistryAppendOnlyUserV1 {
    var name: String
    var email: String
}

@Persistable(type: "SchemaRegistryAppendOnlyUser")
struct SchemaRegistryAppendOnlyUserV2 {
    var name: String
    var email: String
    var age: Int = 0
}

enum SchemaRegistryAppendOnlySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [SchemaRegistryAppendOnlyUserV1.self]
}

enum SchemaRegistryAppendOnlySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [SchemaRegistryAppendOnlyUserV2.self]
}

enum SchemaRegistryAppendOnlyMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SchemaRegistryAppendOnlySchemaV1.self, SchemaRegistryAppendOnlySchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: SchemaRegistryAppendOnlySchemaV1.self,
                toVersion: SchemaRegistryAppendOnlySchemaV2.self
            )
        ]
    }
}

@Persistable(type: "SchemaRegistryAppendOnlyUser")
struct SchemaRegistryAppendOnlyUserReordered {
    var email: String
    var name: String
}

@Persistable(type: "SchemaRegistryMigratedUser")
struct SchemaRegistryMigratedUserV1 {
    var name: String
    var email: String
}

@Persistable(type: "SchemaRegistryMigratedUser")
struct SchemaRegistryMigratedUserV2 {
    #Index(ScalarIndexKind<SchemaRegistryMigratedUserV2>(fields: [\.fullName]), name: "SchemaRegistryMigratedUser_fullName")

    var fullName: String
    var email: String
}

enum SchemaRegistryMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [SchemaRegistryMigratedUserV1.self]
}

enum SchemaRegistryMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [SchemaRegistryMigratedUserV2.self]
}

enum SchemaRegistryCustomMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [SchemaRegistryMigrationSchemaV1.self, SchemaRegistryMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: SchemaRegistryMigrationSchemaV1.self,
                toVersion: SchemaRegistryMigrationSchemaV2.self,
                willMigrate: migrateLegacyUsers,
                didMigrate: nil
            )
        ]
    }

    static func migrateLegacyUsers(context: MigrationContext) async throws {
        var migratedUsers: [SchemaRegistryMigratedUserV2] = []

        for try await legacyUser in context.enumerate(SchemaRegistryMigratedUserV1.self) {
            var migratedUser = SchemaRegistryMigratedUserV2(
                fullName: legacyUser.name,
                email: legacyUser.email
            )
            migratedUser.id = legacyUser.id
            migratedUsers.append(migratedUser)
        }

        guard !migratedUsers.isEmpty else {
            return
        }

        try await context.batchUpdate(migratedUsers, batchSize: 100)
    }
}

/// Tests for Migration functionality
///
/// **Coverage**:
/// - Schema version operations
/// - MigrationContext batch operations
@Suite("Migration Tests", .serialized, .heartbeat)
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

    private func makeSystemPriorityEngine() async throws -> any StorageEngine {
        let engine = try await FDBTestSetup.shared.makeEngine()
        let database = FDBSystemPriorityDatabase(wrapping: engine.database)
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func setupContainer() async throws -> DBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try await makeSystemPriorityEngine()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([MigrationTestUser.self], version: Schema.Version(1, 0, 0))

        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
            )
    }

    private func setupBatchTestContainer() async throws -> DBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try await makeSystemPriorityEngine()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([BatchTestRecord.self], version: Schema.Version(1, 0, 0))

        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
            )
    }

    private func cleanup(container: DBContainer) async throws {
        do {
            try await container.engine.directoryService.remove(path: ["test", "migration"])
        } catch {
        }
        do {
            try await container.engine.directoryService.remove(path: ["_metadata"])
        } catch {
        }
    }

    private func clearSchemaEntries(
        in database: any StorageEngine,
        typeNames: [String]
    ) async throws {
        try await database.withTransaction { transaction in
            for typeName in typeNames {
                transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private func clearMetadata(in database: any StorageEngine) async throws {
        do {
            try await database.directoryService.remove(path: ["_metadata"])
        } catch {
        }
    }

    private func insertTestRecords(
        container: DBContainer,
        records: [BatchTestRecord]
    ) async throws {
        let encoder = ProtobufEncoder()
        let subspace = try await container.resolveDirectory(for: BatchTestRecord.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchTestRecord.persistableType)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        try await container.engine.withTransaction { transaction in
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
            let database = try await makeSystemPriorityEngine()

            let schema = Schema([MigrationTestUser.self], version: Schema.Version(2, 0, 0))

            // Clean up first
            try await clearMetadata(in: database)

            // Create first container and set version
            let container1 = try await DBContainer(for: schema, configuration: .init(backend: .custom(database)), security: .disabled)
            try await container1.setCurrentSchemaVersion(Schema.Version(2, 0, 0))

            // Create second container and read version
            let container2 = try await DBContainer(for: schema, configuration: .init(backend: .custom(database)), security: .disabled)
            let version = try await container2.getCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))

            // Cleanup
            try await clearMetadata(in: database)
        }
    }

    @Test("SchemaRegistry accepts append-only field additions")
    func schemaRegistryAcceptsAppendOnlyFieldAdditions() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let registry = SchemaRegistry(database: database)
            let typeName = SchemaRegistryAppendOnlyUserV1.persistableType

            try await clearSchemaEntries(in: database, typeNames: [typeName])

            try await registry.persist(Schema([SchemaRegistryAppendOnlyUserV1.self]))
            try await registry.persist(Schema([SchemaRegistryAppendOnlyUserV2.self]))

            let entity = try await registry.load(typeName: typeName)
            #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        }
    }

    @Test("Lightweight migration keeps existing FDB data readable end-to-end")
    func lightweightMigrationPreservesExistingDataEndToEnd() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let typeName = SchemaRegistryAppendOnlyUserV1.persistableType
            let userID = "fdb-lightweight-\(UUID().uuidString)"

            try await clearSchemaEntries(in: database, typeNames: [typeName])
            try await clearMetadata(in: database)

            let initialContainer = try await DBContainer(
                for: SchemaRegistryAppendOnlySchemaV1.makeSchema(),
                configuration: .init(backend: .custom(database)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var user = SchemaRegistryAppendOnlyUserV1(
                name: "Alice",
                email: "alice@example.com"
            )
            user.id = userID
            initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: SchemaRegistryAppendOnlySchemaV2.self,
                migrationPlan: SchemaRegistryAppendOnlyMigrationPlan.self,
                configuration: .init(backend: .custom(database))
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await DBContainer(
                for: SchemaRegistryAppendOnlySchemaV2.makeSchema(),
                configuration: .init(backend: .custom(database)),
                security: .disabled
            )
            let migratedUsers = try await verificationContainer
                .newContext()
                .fetch(SchemaRegistryAppendOnlyUserV2.self)
                .execute()
            let migratedUser = migratedUsers.first { $0.id == userID }

            #expect(migratedUser != nil)
            #expect(migratedUser?.name == "Alice")
            #expect(migratedUser?.email == "alice@example.com")
            #expect(migratedUser?.age == 0)
        }
    }

    @Test("SchemaRegistry rejects reordered fields without migration")
    func schemaRegistryRejectsReorderedFieldsWithoutMigration() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let registry = SchemaRegistry(database: database)
            let typeName = SchemaRegistryAppendOnlyUserV1.persistableType

            try await clearSchemaEntries(in: database, typeNames: [typeName])

            try await registry.persist(Schema([SchemaRegistryAppendOnlyUserV1.self]))

            do {
                try await registry.persist(Schema([SchemaRegistryAppendOnlyUserReordered.self]))
                Issue.record("Expected incompatibleEntityEvolution error")
            } catch let error as SchemaRegistryError {
                if case .incompatibleEntityEvolution(let entityName, let issues) = error {
                    #expect(entityName == typeName)
                    #expect(
                        issues.contains(
                            .renumberedField(
                                entityName: typeName,
                                fieldName: "email",
                                expected: 3,
                                actual: 2
                            )
                        )
                    )
                } else {
                    Issue.record("Unexpected schema registry error: \(error)")
                }
            }
        }
    }

    @Test("Custom migration can persist breaking schema changes")
    func customMigrationCanPersistBreakingSchemaChanges() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let typeName = SchemaRegistryMigratedUserV1.persistableType

            try await clearSchemaEntries(in: database, typeNames: [typeName])
            try await clearMetadata(in: database)

            let initialContainer = try await DBContainer(
                for: SchemaRegistryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(database)),
                security: .disabled
            )
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: SchemaRegistryMigrationSchemaV2.self,
                migrationPlan: SchemaRegistryCustomMigrationPlan.self,
                configuration: .init(backend: .custom(database))
            )
            try await migratedContainer.migrateIfNeeded()

            let registry = SchemaRegistry(database: database)
            let entity = try await registry.load(typeName: typeName)
            let version = try await migratedContainer.getCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["name"] == nil)
        }
    }

    @Test("Custom migration transforms FDB data end-to-end")
    func customMigrationTransformsDataEndToEnd() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let typeName = SchemaRegistryMigratedUserV1.persistableType
            let idPrefix = UUID().uuidString
            let firstID = "fdb-migrated-\(idPrefix)-1"
            let secondID = "fdb-migrated-\(idPrefix)-2"

            try await clearSchemaEntries(in: database, typeNames: [typeName])
            try await clearMetadata(in: database)

            let initialContainer = try await DBContainer(
                for: SchemaRegistryMigrationSchemaV1.makeSchema(),
                configuration: .init(backend: .custom(database)),
                security: .disabled
            )
            let initialContext = initialContainer.newContext()

            var firstUser = SchemaRegistryMigratedUserV1(
                name: "Alice",
                email: "alice@example.com"
            )
            firstUser.id = firstID
            initialContext.insert(firstUser)

            var secondUser = SchemaRegistryMigratedUserV1(
                name: "Bob",
                email: "bob@example.com"
            )
            secondUser.id = secondID
            initialContext.insert(secondUser)

            try await initialContext.save()
            try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer(
                for: SchemaRegistryMigrationSchemaV2.self,
                migrationPlan: SchemaRegistryCustomMigrationPlan.self,
                configuration: .init(backend: .custom(database))
            )
            try await migratedContainer.migrateIfNeeded()

            let verificationContainer = try await DBContainer(
                for: SchemaRegistryMigrationSchemaV2.makeSchema(),
                configuration: .init(backend: .custom(database)),
                security: .disabled
            )
            let migratedUsers = try await verificationContainer
                .newContext()
                .fetch(SchemaRegistryMigratedUserV2.self)
                .execute()
            let migratedUsersByID = Dictionary(uniqueKeysWithValues: migratedUsers.map { ($0.id, $0) })

            #expect(migratedUsersByID[firstID]?.fullName == "Alice")
            #expect(migratedUsersByID[firstID]?.email == "alice@example.com")
            #expect(migratedUsersByID[secondID]?.fullName == "Bob")
            #expect(migratedUsersByID[secondID]?.email == "bob@example.com")
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

            let metadataSubspace = try await container.engine.directoryService.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
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
                let data: Bytes? = try await container.engine.withTransaction { tx in
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

            let metadataSubspace = try await container.engine.directoryService.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
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

            let metadataSubspace = try await container.engine.directoryService.createOrOpen(path: ["_metadata"])

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
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
            let updateData: Bytes? = try await container.engine.withTransaction { tx in
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
            let deleteData: Bytes? = try await container.engine.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace)
                return try await storage.read(for: deleteKey, snapshot: false)
            }
            #expect(deleteData == nil)
        }
    }
}
#endif

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

@Persistable(type: "SchemaRegistryAppendOnlyUser")
struct SchemaRegistryAppendOnlyUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SchemaRegistryAppendOnlyUser")
struct SchemaRegistryAppendOnlyUserV2 {
    var id: String = ""
    var name: String
    var email: String
    var age: Int64 = 0
}

enum SchemaRegistryAppendOnlySchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try SchemaRegistryAppendOnlyUserV1.schemaEntity] }
    }
}

enum SchemaRegistryAppendOnlySchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try SchemaRegistryAppendOnlyUserV2.schemaEntity] }
    }
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
    var id: String = ""
    var email: String
    var name: String
}

@Persistable(type: "SchemaRegistryMigratedUser")
struct SchemaRegistryMigratedUserV1 {
    var id: String = ""
    var name: String
    var email: String
}

@Persistable(type: "SchemaRegistryMigratedUser")
struct SchemaRegistryMigratedUserV2 {
    #Index(
        .ordered(
            name: "SchemaRegistryMigratedUser_fullName",
            keys: [.ascending(\SchemaRegistryMigratedUserV2.fullName)], unique: false))

    var id: String = ""
    var fullName: String
    var email: String
}

enum SchemaRegistryMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try SchemaRegistryMigratedUserV1.schemaEntity] }
    }
}

enum SchemaRegistryMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) { [try SchemaRegistryMigratedUserV2.schemaEntity] }
    }
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
@Suite("Migration Tests", .foundationDBScenario, .serialized, .heartbeat)
struct MigrationTests {

    // MARK: - Helper Types

    @Persistable
    struct MigrationUser {
        #Directory<MigrationUser>("test", "migration", "users")
        #Index(
            .ordered(
                name: "MigrationUser_email", keys: [.ascending(\MigrationUser.email)],
                unique: false))

        var id: String = UUID().uuidString
        var email: String
        var name: String
    }

    @Persistable
    struct BatchMigrationEntity {
        #Directory<BatchMigrationEntity>("test", "migration", "batch")

        var id: String = UUID().uuidString
        var name: String
        var status: String = "active"
    }

    // MARK: - Helper Methods

    private func makeSystemPriorityEngine() async throws -> any StorageEngine {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try FDBSystemPriorityDatabase()
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func setupContainer(
        databaseIdentifier: String
    ) async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await makeSystemPriorityEngine()

        // Use try Schema(entities: [try Type.schemaEntity]) to properly register types
        let schema = try Schema(entities: [try MigrationUser.schemaEntity], version: Schema.Version(1, 0, 0))

        return try await DBContainer.open(
            for: schema,
            configuration: .testing(
                databaseIdentifier: databaseIdentifier,
                storageEngine: database
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(MigrationUser.self)]
            ),
            security: .testingDisabled
            )
    }

    private func setupBatchTestContainer(
        databaseIdentifier: String
    ) async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await makeSystemPriorityEngine()

        // Use try Schema(entities: [try Type.schemaEntity]) to properly register types
        let schema = try Schema(entities: [try BatchMigrationEntity.schemaEntity], version: Schema.Version(1, 0, 0))

        return try await DBContainer.open(
            for: schema,
            configuration: .testing(
                databaseIdentifier: databaseIdentifier,
                storageEngine: database
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(BatchMigrationEntity.self)]
            ),
            security: .testingDisabled
            )
    }

    private func clearSchemaEntries(
        in database: any StorageEngine,
        typeNames: [String]
    ) async throws {
        try await database.withTransaction { transaction in
            for typeName in typeNames {
                try transaction.clear(key: Tuple(["_schema", typeName]).pack())
            }
        }
    }

    private func insertTestEntities(
        container: DBContainer,
        entities: [BatchMigrationEntity]
    ) async throws {
        let subspace = try await container.testBaseDirectory(for: BatchMigrationEntity.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchMigrationEntity.persistableType)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        try await container.engine.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            for entity in entities {
                let data = try PersistableStorageCodec.encode(entity)
                let identifier = try entity.persistableIdentifierTuple()
                let itemKey = itemSubspace.pack(identifier)
                try await storage.write(data, for: itemKey)
            }
        }
    }

    // MARK: - Schema Version Tests

    @Test("A newly provisioned Base exposes its compiled schema version")
    func newlyProvisionedBaseExposesCompiledSchemaVersion() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer(
                databaseIdentifier: "migration-version-bootstrap"
            )

            let version = try await container.testBaseCurrentSchemaVersion()
            #expect(version == Schema.Version(1, 0, 0))
        }
    }

    @Test("Installed schema snapshot exposes its compiled version")
    func schemaSnapshotVersionRoundtrip() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer(
                databaseIdentifier: "migration-version-roundtrip"
            )

            let testVersion = Schema.Version(1, 0, 0)
            try await container.installTestBaseSchemaSnapshot(for: testVersion)

            let retrievedVersion = try await container.testBaseCurrentSchemaVersion()
            #expect(retrievedVersion == testVersion)
        }
    }

    @Test("Schema version persists across container instances")
    func schemaVersionPersistsAcrossContainers() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-version-persistence"

            let schema = try Schema(entities: [try MigrationUser.schemaEntity], version: Schema.Version(2, 0, 0))

            // Create first container and set version
            let container1 = try await DBContainer.open(
                for: schema,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(MigrationUser.self)]
                ),
                security: .testingDisabled
            )
            try await container1.installTestBaseSchemaSnapshot(for: Schema.Version(2, 0, 0))

            // Create second container and read version
            let container2 = try await DBContainer.open(
                for: schema,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(MigrationUser.self)]
                ),
                security: .testingDisabled
            )
            let version = try await container2.testBaseCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))
        }
    }

    @Test("SchemaRegistry accepts append-only field additions")
    func schemaRegistryAcceptsAppendOnlyFieldAdditions() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let registry = SchemaRegistry(
                database: database,
                root: Subspace(),
                clock: TestProcessMonotonicClock()
            )
            let typeName = SchemaRegistryAppendOnlyUserV1.persistableType

            try await clearSchemaEntries(in: database, typeNames: [typeName])

            try await registry.persist(try Schema(entities: [try SchemaRegistryAppendOnlyUserV1.schemaEntity]))
            try await registry.persist(try Schema(entities: [try SchemaRegistryAppendOnlyUserV2.schemaEntity]))

            let entity = try await registry.load(typeName: typeName)
            #expect(entity?.fieldMapByName["name"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["age"]?.fieldNumber == 4)
        }
    }

    @Test("Lightweight migration keeps existing FDB data readable end-to-end")
    func lightweightMigrationPreservesExistingDataEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-lightweight-data"
            let userID = "fdb-lightweight-\(UUID().uuidString)"

            let initialContainer = try await DBContainer.open(
                for: SchemaRegistryAppendOnlySchemaV1.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryAppendOnlyUserV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var user = SchemaRegistryAppendOnlyUserV1(
                name: "Alice",
                email: "alice@example.com"
            )
            user.id = userID
            try initialContext.insert(user)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: SchemaRegistryAppendOnlySchemaV2.self,
                migrationPlan: SchemaRegistryAppendOnlyMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryAppendOnlyUserV2.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContainer = try await DBContainer.open(
                for: SchemaRegistryAppendOnlySchemaV2.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryAppendOnlyUserV2.self)]),
                security: .testingDisabled
            )
            let migratedUsers = try await verificationContainer
                .testBaseContext()
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let registry = SchemaRegistry(
                database: database,
                root: Subspace(),
                clock: TestProcessMonotonicClock()
            )
            let typeName = SchemaRegistryAppendOnlyUserV1.persistableType

            try await clearSchemaEntries(in: database, typeNames: [typeName])

            try await registry.persist(try Schema(entities: [try SchemaRegistryAppendOnlyUserV1.schemaEntity]))

            do {
                try await registry.persist(try Schema(entities: [try SchemaRegistryAppendOnlyUserReordered.schemaEntity]))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-breaking-schema"
            let typeName = SchemaRegistryMigratedUserV1.persistableType
            let idPrefix = UUID().uuidString
            let seededID = "fdb-breaking-\(idPrefix)"

            let initialContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV1.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()
            var seededUser = SchemaRegistryMigratedUserV1(
                name: "Charlie",
                email: "charlie@example.com"
            )
            seededUser.id = seededID
            try initialContext.insert(seededUser)
            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV2.self,
                migrationPlan: SchemaRegistryCustomMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV2.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let entity = try await migratedContainer
                .testPersistedControlSchemaEntities()
                .first { $0.name == typeName }
            let version = try await migratedContainer.testBaseCurrentSchemaVersion()

            #expect(version == Schema.Version(2, 0, 0))
            #expect(entity?.fieldMapByName["fullName"]?.fieldNumber == 2)
            #expect(entity?.fieldMapByName["email"]?.fieldNumber == 3)
            #expect(entity?.fieldMapByName["name"] == nil)

            let verificationContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV2.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV2.self)]),
                security: .testingDisabled
            )
            let verificationContext = verificationContainer.testBaseContext()
            let migratedUsers = try await verificationContext
                .fetch(SchemaRegistryMigratedUserV2.self)
                .execute()
            let migratedUser = try #require(migratedUsers.first { $0.id == seededID })

            #expect(migratedUser.fullName == "Charlie")
            #expect(migratedUser.email == "charlie@example.com")
        }
    }

    @Test("Custom migration transforms FDB data end-to-end")
    func customMigrationTransformsDataEndToEnd() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await makeSystemPriorityEngine()
            let databaseIdentifier = "migration-custom-data"
            let idPrefix = UUID().uuidString
            let firstID = "fdb-migrated-\(idPrefix)-1"
            let secondID = "fdb-migrated-\(idPrefix)-2"

            let initialContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV1.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV1.self)]),
                security: .testingDisabled
            )
            let initialContext = initialContainer.testBaseContext()

            var firstUser = SchemaRegistryMigratedUserV1(
                name: "Alice",
                email: "alice@example.com"
            )
            firstUser.id = firstID
            try initialContext.insert(firstUser)

            var secondUser = SchemaRegistryMigratedUserV1(
                name: "Bob",
                email: "bob@example.com"
            )
            secondUser.id = secondID
            try initialContext.insert(secondUser)

            try await initialContext.save()
            try await initialContainer.installTestBaseSchemaSnapshot(for: Schema.Version(1, 0, 0))

            let migratedContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV2.self,
                migrationPlan: SchemaRegistryCustomMigrationPlan.self,
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV2.self)]),
            )
            try await migratedContainer.testBaseAdmin().migrateIfNeeded()

            let verificationContainer = try await DBContainer.open(
                for: SchemaRegistryMigrationSchemaV2.makeSchema(),
                configuration: .testing(
                    databaseIdentifier: databaseIdentifier,
                    storageEngine: database
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [try DatabaseFrameworkRuntime.entity(SchemaRegistryMigratedUserV2.self)]),
                security: .testingDisabled
            )
            let migratedUsers = try await verificationContainer
                .testBaseContext()
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer(
                databaseIdentifier: "migration-context-batch-update"
            )

            // Create test entities with known IDs
            let entities = (1...5).map { BatchMigrationEntity(name: "User \($0)", status: "active") }
            try await insertTestEntities(container: container, entities: entities)

            // Setup MigrationContext
            let subspace = try await container.testBaseDirectory(for: BatchMigrationEntity.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchMigrationEntity.persistableType: storeInfo]

            let metadataSubspace = subspace.subspace("migration-metadata")

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
                storeRegistry: storeRegistry
            )

            // Batch update entities
            let updatedEntities = entities.map {
                BatchMigrationEntity(id: $0.id, name: $0.name, status: "migrated")
            }
            try await container.withTestBaseOperation {
                try await context.batchUpdate(updatedEntities, batchSize: 2)
            }

            // Verify updates
            let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchMigrationEntity.persistableType)

            for entity in entities {
                let identifier = try entity.persistableIdentifierTuple()
                let key = itemSubspace.pack(identifier)
                let data: ByteString? = try await container.engine.withTransaction { tx in
                    let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace, configuration: .v1)
                    return try await storage.read(for: key, snapshot: false)
                }
                guard let data = data else {
                    Issue.record("Entity with id \(entity.id) not found after batchUpdate")
                    continue
                }
                let decoded: BatchMigrationEntity = try DataAccess.deserialize(data)
                #expect(decoded.status == "migrated", "Expected status 'migrated' but got '\(decoded.status)'")
            }
        }
    }

    @Test("MigrationContext count works correctly")
    func migrationContextCount() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer(
                databaseIdentifier: "migration-context-count"
            )

            // Insert test entities
            let entities = (1...7).map { BatchMigrationEntity(name: "User \($0)") }
            try await insertTestEntities(container: container, entities: entities)

            // Setup MigrationContext
            let subspace = try await container.testBaseDirectory(for: BatchMigrationEntity.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchMigrationEntity.persistableType: storeInfo]

            let metadataSubspace = subspace.subspace("migration-metadata")

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
                storeRegistry: storeRegistry
            )

            let count = try await container.withTestBaseOperation {
                try await context.count(BatchMigrationEntity.self)
            }
            #expect(count == 7)
        }
    }

    @Test("MigrationContext single update and delete work correctly")
    func migrationContextSingleOperations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupBatchTestContainer(
                databaseIdentifier: "migration-context-single-operations"
            )

            // Create test entities
            let updateEntity = BatchMigrationEntity(name: "ToUpdate", status: "active")
            let deleteEntity = BatchMigrationEntity(name: "ToDelete", status: "active")
            try await insertTestEntities(container: container, entities: [updateEntity, deleteEntity])

            // Setup MigrationContext
            let subspace = try await container.testBaseDirectory(for: BatchMigrationEntity.self)
            let storeInfo = MigrationStoreInfo(
                subspace: subspace,
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            let storeRegistry = [BatchMigrationEntity.persistableType: storeInfo]

            let metadataSubspace = subspace.subspace("migration-metadata")

            let context = MigrationContext(
                container: container,
                schema: container.schema,
                metadataSubspace: metadataSubspace,
                storeRegistry: storeRegistry
            )

            // Single update
            let updated = BatchMigrationEntity(id: updateEntity.id, name: "ToUpdate", status: "updated")
            try await container.withTestBaseOperation {
                try await context.update(updated)
            }

            // Single delete
            try await container.withTestBaseOperation {
                try await context.delete(deleteEntity)
            }

            // Verify
            let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(BatchMigrationEntity.persistableType)

            // Check update
            let updateIdentifier = try updateEntity.persistableIdentifierTuple()
            let updateKey = itemSubspace.pack(updateIdentifier)
            let updateData: ByteString? = try await container.engine.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace, configuration: .v1)
                return try await storage.read(for: updateKey, snapshot: false)
            }
            #expect(updateData != nil, "Updated item not found")
            if let updateData = updateData {
                let decoded: BatchMigrationEntity = try DataAccess.deserialize(updateData)
                #expect(decoded.status == "updated", "Expected status 'updated' but got '\(decoded.status)'")
            }

            // Check delete
            let deleteIdentifier = try deleteEntity.persistableIdentifierTuple()
            let deleteKey = itemSubspace.pack(deleteIdentifier)
            let deleteData: ByteString? = try await container.engine.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: storeInfo.blobsSubspace, configuration: .v1)
                return try await storage.read(for: deleteKey, snapshot: false)
            }
            #expect(deleteData == nil)
        }
    }
}
#endif

#endif

#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit
import TestSupport
import Testing

@Persistable(type: "S11GenerationEntity")
private struct S11GenerationEntityV1 {
    #Directory<S11GenerationEntityV1>("s11-generation", "entities")
    var id: String = ""
    var value: String = ""
}

@Persistable(type: "S11GenerationEntity")
private struct S11GenerationEntityV2 {
    #Directory<S11GenerationEntityV2>("s11-generation", "entities")
    var id: String = ""
    var value: String = ""
    var note: String = ""
}

@Suite("S11 schema generation publication", .serialized)
struct S11SchemaGenerationPublicationTests {
    @Test("C06 old and new leases retain different SQL names over unchanged physical storage")
    func heldLeaseDoesNotBlockRename() async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let original = try #require(old.sqlCatalog.catalog.relations.first)
        let catalog = try renamed(old.sqlCatalog.catalog, to: "renamed_entities")
        let result = try await container.publishSQLCatalog(catalog,
            expectedGeneration: old.generation, idempotencyKey: "held-lease",
            authorization: TestBaseEnvironment.authorization)
        let current = container.acquirePublishedSchemaLease()
        #expect(result.generation == old.generation + 1)
        #expect(current.sqlCatalog !== old.sqlCatalog)
        #expect(current.indexPhysicalFingerprint == old.indexPhysicalFingerprint)
        #expect(current.executionRuntimeFingerprint != old.executionRuntimeFingerprint)
        #expect(current.schema == old.schema)
        #expect(try old.sqlCatalog.resolveRelation(name: original.name, namespace: "public",
            searchPath: .default, currentUser: nil).entity == current.schema.entities.first)
        #expect(try current.sqlCatalog.resolveRelation(name: "renamed_entities", namespace: "public",
            searchPath: .default, currentUser: nil).entity == old.schema.entities.first)
        #expect(throws: SQLCatalogBindingError.missingRelation(namespace: "public", name: "renamed_entities")) {
            try old.sqlCatalog.resolveRelation(name: "renamed_entities", namespace: "public",
                searchPath: .default, currentUser: nil)
        }
        #expect(container.pendingSchemaDrainWaiterCount == 0)
    }

    @Test("Failed aggregate commit leaves both durable and published catalogs unchanged")
    func failedCommitDoesNotPublish() async throws {
        let control = StorageTransactionControl()
        let container = try await makeContainer(engine: ControlledStorageEngine(base: InMemoryEngine(), control: control))
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let candidate = try renamed(container.sqlCatalog, to: "uncommitted_entities")
        let failure = StorageError(code: .backendFailure, operation: .commit, backend: .inMemory,
            message: "Injected schema publication commit failure")
        control.failNextMutatingCommit(with: failure)
        await #expect(throws: failure) {
            try await container.publishSQLCatalog(candidate, expectedGeneration: old.generation,
                idempotencyKey: "failed-commit", authorization: TestBaseEnvironment.authorization)
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        #expect(container.schemaGeneration == old.generation)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.digest == old.sqlCatalog.digest)
        #expect(try await persistedGeneration(container) == old.generation)
    }

    @Test("Commit-unknown retains the old lease until idempotent retry confirms the complete aggregate")
    func commitUnknownResolvesThroughIdempotency() async throws {
        let engine = CommitOutcomeUnknownEngine()
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let candidate = try renamed(container.sqlCatalog, to: "committed_entities")
        engine.reportNextCommitAsUnknown()
        do {
            _ = try await container.publishSQLCatalog(candidate, expectedGeneration: old.generation,
                idempotencyKey: "unknown-commit", authorization: TestBaseEnvironment.authorization)
            Issue.record("Expected the injected ambiguous commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        #expect(container.schemaGeneration == old.generation)
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        #expect(try await persistedGeneration(container) == old.generation + 1)
        let result = try await container.publishSQLCatalog(candidate, expectedGeneration: old.generation,
            idempotencyKey: "unknown-commit", authorization: TestBaseEnvironment.authorization)
        #expect(result.generation == old.generation + 1)
        #expect(container.schemaGeneration == result.generation)
        #expect(container.sqlCatalog == candidate)
        #expect(try await persistedCatalog(container)?.digest == container.acquirePublishedSchemaLease().sqlCatalog.digest)
    }

    @Test("Missing catalog metadata fails before publication")
    func missingAggregateDoesNotPublish() async throws {
        try await assertPartialAggregateDoesNotPublish(malformed: false)
    }

    @Test("Malformed catalog metadata fails before publication")
    func malformedAggregateDoesNotPublish() async throws {
        try await assertPartialAggregateDoesNotPublish(malformed: true)
    }

    private func assertPartialAggregateDoesNotPublish(malformed: Bool) async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let candidate = try renamed(container.sqlCatalog, to: "must_not_publish")
        try await StorageTransactionExecutor(engine: globalEngine(container)).withTransaction(
            configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let key = container.metadataSubspace.subspace("schema").pack(Tuple("sql_catalog"))
            if malformed {
                try transaction.setValue(ByteString([0, 0, 0, 0]), for: key)
            } else {
                try transaction.clear(key: key)
            }
        }
        if malformed {
            await #expect(throws: StorageFrameError.invalidMagic) {
                try await container.publishSQLCatalog(candidate, expectedGeneration: old.generation,
                    idempotencyKey: "malformed-state", authorization: TestBaseEnvironment.authorization)
            }
        } else {
            await #expect(throws: DatabaseSchemaRestorationError.missingSQLCatalog) {
                try await container.publishSQLCatalog(candidate, expectedGeneration: old.generation,
                    idempotencyKey: "partial-state", authorization: TestBaseEnvironment.authorization)
            }
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        #expect(container.schemaGeneration == old.generation)
        #expect(try await persistedGeneration(container) == old.generation)
    }

    @Test("Application compiled evolution preserves SQL identity and prepares historical migration stages")
    func applicationEvolutionPreservesLogicalIdentity() async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        defer { await container.shutdown() }
        let originalSchema = container.schema
        let renamedCatalog = try renamed(container.sqlCatalog, to: "application_entities")
        _ = try await container.publishSQLCatalog(renamedCatalog,
            expectedGeneration: container.schemaGeneration, idempotencyKey: "before-upgrade",
            authorization: TestBaseEnvironment.authorization)
        let oldLease = container.acquirePublishedSchemaLease()
        let target = try Schema(entities: [try S11GenerationEntityV2.schemaEntity], version: Schema.Version(2, 0, 0))
        let runtime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "s11-generation", revision: 2),
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(S11GenerationEntityV2.self)])
        let activeApplicationKey = container.metadataSubspace.subspace("schema")
            .subspace("applications").pack(Tuple("active"))
        let activeApplication = ByteString(utf8: "application-owned-upgrade")
        try await StorageTransactionExecutor(engine: globalEngine(container)).withTransaction(
            configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try transaction.setValue(activeApplication, for: activeApplicationKey)
        }
        let blockedCatalog = try renamed(container.sqlCatalog, to: "blocked_catalog_change")
        await #expect(throws: DatabaseSchemaPublicationError.transitionInProgress) {
            try await container.publishSQLCatalog(blockedCatalog,
                expectedGeneration: oldLease.generation, idempotencyKey: "active-application-catalog",
                authorization: TestBaseEnvironment.authorization)
        }
        #expect(container.sqlCatalog == renamedCatalog)
        #expect(container.schemaGeneration == oldLease.generation)
        let result = try await container.publishSchema(target,
            fingerprint: SchemaManifest(schema: target).fingerprint(), expectedFingerprint: container.schemaFingerprint,
            idempotencyKey: "compiled-upgrade", authorization: TestBaseEnvironment.authorization,
            runtimeConfiguration: runtime)
        try await StorageTransactionExecutor(engine: globalEngine(container)).withTransaction(
            configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            #expect(try await transaction.getValue(for: activeApplicationKey, snapshot: false) == activeApplication)
            try transaction.clear(key: activeApplicationKey)
        }
        #expect(container.schema == target)
        #expect(container.schemaGeneration == result.generation)
        let relation = try #require(container.sqlCatalog.relations.first)
        let oldRelation = try #require(oldLease.sqlCatalog.catalog.relations.first)
        #expect(relation.id == oldRelation.id)
        #expect(relation.name == "application_entities")
        #expect(Array(relation.columns.prefix(oldRelation.columns.count)) == oldRelation.columns)
        #expect(relation.columns.count == oldRelation.columns.count + 1)
        #expect(container.acquirePublishedSchemaLease().sqlCatalog.protectedContracts.first?.entityDigest
            != oldLease.sqlCatalog.protectedContracts.first?.entityDigest)
        let stage = try container.prepareMigrationSchemaGeneration(originalSchema)
        #expect(stage.indexPhysicalFingerprint != container.acquirePublishedSchemaLease().indexPhysicalFingerprint)
        let persisted = try #require(try await persistedCatalog(container))
        _ = try persisted.snapshot(physicalSchema: target, runtimeConfiguration: runtime)
    }

    private func makeContainer(engine: any StorageEngine) async throws -> DBContainer {
        let schema = try Schema(entities: [try S11GenerationEntityV1.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "s11-generation", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(S11GenerationEntityV1.self)]),
            security: .testingDisabled)
    }

    private func renamed(_ catalog: SQLCatalog, to name: String) throws -> SQLCatalog {
        let original = try #require(catalog.relations.first)
        return try SQLCatalog(namespaces: catalog.namespaces, relations: [SQLRelation(
            id: original.id, namespaceID: original.namespaceID, name: name, entityName: original.entityName,
            columns: original.columns, primaryKeyColumnIDs: original.primaryKeyColumnIDs,
            primaryKeyConstraint: original.primaryKeyConstraint, constraints: original.constraints)])
    }

    private func globalEngine(_ container: DBContainer) -> any StorageEngine {
        #if DATABASE_MULTI_BASE
        container.storageTopology.controlDomain.engine
        #else
        container.engine
        #endif
    }

    private func persistedCatalog(_ container: DBContainer) async throws -> SQLCatalogStoredState? {
        try await StorageTransactionExecutor(engine: globalEngine(container)).withTransaction(
            configuration: .readOnly, clock: TestProcessMonotonicClock()) { transaction in
            try await SQLCatalogStore(root: container.metadataSubspace).load(transaction: transaction)
        }
    }

    private func persistedGeneration(_ container: DBContainer) async throws -> UInt64? {
        try await StorageTransactionExecutor(engine: globalEngine(container)).withTransaction(
            configuration: .readOnly, clock: TestProcessMonotonicClock()) { transaction in
            try await DBContainer.loadPersistedSchemaGeneration(metadataSubspace: container.metadataSubspace, transaction: transaction)
        }
    }
}
#endif

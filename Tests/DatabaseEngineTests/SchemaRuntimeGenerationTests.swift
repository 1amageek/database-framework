#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit
import TestSupport
import Testing

@Persistable
private struct SchemaRuntimeGenerationEntity {
    #Directory<SchemaRuntimeGenerationEntity>(
        "schema-runtime-generation",
        "entities"
    )
    #Index(
        .custom(
            name: "schema_runtime_generation_value",
            definition: CustomIndexDefinition(
                identifier: "query-tuned",
                keys: [.ascending(\SchemaRuntimeGenerationEntity.value)]
            )
        ))

    var id: String = ""
    var value: String = ""
}

@Suite("Schema runtime generations", .serialized)
struct SchemaRuntimeGenerationTests {
    @Test("Cancelling migration drain restores data admission")
    func cancellingMigrationDrainRestoresAdmission() async throws {
        let gate = DatabaseMigrationAdmissionGate()
        try gate.enterDataOperation(schemaGeneration: 1)
        let migration = Task {
            try await gate.beginMigration()
        }

        var observedMigration = false
        for _ in 0..<100 {
            do {
                try gate.enterDataOperation(schemaGeneration: 1)
                gate.leaveDataOperation()
                await Task.yield()
            } catch DatabaseMigrationAdmissionError.migrationInProgress {
                observedMigration = true
                break
            }
        }
        #expect(observedMigration)

        migration.cancel()
        await #expect(throws: CancellationError.self) {
            try await migration.value
        }
        gate.leaveDataOperation()

        try gate.enterDataOperation(schemaGeneration: 1)
        gate.leaveDataOperation()
    }

    @Test("Older schema leases drain before retirement is admitted")
    func olderSchemaLeasesDrainBeforeRetirement() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let initialRuntime = try Self.runtime(
            searchBudget: 10,
            physicalRevision: 1
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: initialRuntime,
            security: .testingDisabled
        )
        var oldLease: DatabaseSchemaLease? =
            container
            .acquirePublishedSchemaLease()
        let oldGeneration = try #require(oldLease?.generation)
        let targetRuntime = try Self.runtime(
            searchBudget: 40,
            physicalRevision: 1
        )
        let prepared = try container.prepareSchemaGeneration(
            schema,
            runtimeConfiguration: targetRuntime
        )
        let targetGeneration = oldGeneration + 1
        try container.publishSchemaGeneration(
            schema,
            fingerprint: try SchemaManifest(schema: schema).fingerprint(),
            indexPhysicalFingerprint: prepared.indexPhysicalFingerprint,
            executionRuntimeFingerprint: prepared.executionRuntimeFingerprint,
            runtimeConfiguration: targetRuntime,
            indexPhysicalLayouts: prepared.indexPhysicalLayouts,
            generation: targetGeneration
        )

        let probe = SchemaDrainProbe()
        let barrier = Task {
            try await container.waitForSchemaLeases(
                olderThan: targetGeneration
            )
            await probe.markCompleted()
        }
        while container.pendingSchemaDrainWaiterCount == 0 {
            await Task.yield()
        }
        #expect(container.pendingSchemaDrainWaiterCount == 1)
        let completedBeforeRelease = await probe.completed
        #expect(!completedBeforeRelease)

        oldLease = nil
        try await barrier.value
        let completedAfterRelease = await probe.completed
        #expect(completedAfterRelease)

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Schema lease drain cancellation removes the pending waiter")
    func schemaLeaseDrainCancellationRemovesWaiter() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let runtime = try Self.runtime(
            searchBudget: 10,
            physicalRevision: 1
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: runtime,
            security: .testingDisabled
        )
        var oldLease: DatabaseSchemaLease? =
            container
            .acquirePublishedSchemaLease()
        let oldGeneration = try #require(oldLease?.generation)
        let targetRuntime = try Self.runtime(
            searchBudget: 40,
            physicalRevision: 1
        )
        let prepared = try container.prepareSchemaGeneration(
            schema,
            runtimeConfiguration: targetRuntime
        )
        let targetGeneration = oldGeneration + 1
        try container.publishSchemaGeneration(
            schema,
            fingerprint: try SchemaManifest(schema: schema).fingerprint(),
            indexPhysicalFingerprint: prepared.indexPhysicalFingerprint,
            executionRuntimeFingerprint: prepared.executionRuntimeFingerprint,
            runtimeConfiguration: targetRuntime,
            indexPhysicalLayouts: prepared.indexPhysicalLayouts,
            generation: targetGeneration
        )

        let waiter = Task {
            try await container.waitForSchemaLeases(
                olderThan: targetGeneration
            )
        }
        while container.pendingSchemaDrainWaiterCount == 0 {
            await Task.yield()
        }
        #expect(container.pendingSchemaDrainWaiterCount == 1)
        waiter.cancel()
        await #expect(throws: CancellationError.self) {
            try await waiter.value
        }

        oldLease = nil
        try await container.waitForSchemaLeases(
            olderThan: targetGeneration
        )

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Operation-local store releases its schema generation")
    func operationStoreReleasesSchemaGeneration() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let initialRuntime = try Self.runtime(
            searchBudget: 10,
            physicalRevision: 1
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: initialRuntime,
            security: .testingDisabled
        )
        let context = container.testBaseContext()

        _ = try await context.fetch(SchemaRuntimeGenerationEntity.self)
            .execute()

        let oldGeneration = container.schemaGeneration
        let targetRuntime = try Self.runtime(
            searchBudget: 40,
            physicalRevision: 1
        )
        let prepared = try container.prepareSchemaGeneration(
            schema,
            runtimeConfiguration: targetRuntime
        )
        let targetGeneration = oldGeneration + 1
        try container.publishSchemaGeneration(
            schema,
            fingerprint: try SchemaManifest(schema: schema).fingerprint(),
            indexPhysicalFingerprint: prepared.indexPhysicalFingerprint,
            executionRuntimeFingerprint: prepared.executionRuntimeFingerprint,
            runtimeConfiguration: targetRuntime,
            indexPhysicalLayouts: prepared.indexPhysicalLayouts,
            generation: targetGeneration
        )

        try await container.waitForSchemaLeases(
            olderThan: targetGeneration
        )

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Query-only policy changes publish without replacing physical indexes")
    func queryOnlyPolicyChangePublishesExecutionGeneration() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let first = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: try Self.runtime(
                searchBudget: 10,
                physicalRevision: 1
            ),
            security: .testingDisabled
        )
        let firstLease = first.acquirePublishedSchemaLease()
        let firstGeneration = firstLease.generation
        let firstPhysicalFingerprint = firstLease.indexPhysicalFingerprint
        let firstRuntimeFingerprint = firstLease.executionRuntimeFingerprint
        await first.shutdown()

        let reopened = try await DBContainer.openRestoringSchema(
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            security: .testingDisabled
        ) { _ in
            try Self.runtime(searchBudget: 40, physicalRevision: 1)
        }
        let reopenedLease = reopened.acquirePublishedSchemaLease()

        #expect(reopenedLease.generation == firstGeneration + 1)
        #expect(
            reopenedLease.indexPhysicalFingerprint
                == firstPhysicalFingerprint
        )
        #expect(reopenedLease.executionRuntimeFingerprint != firstRuntimeFingerprint)

        await reopened.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Execution identity and security mode changes publish generations")
    func executionIdentityAndSecurityChangesPublishGenerations() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let first = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: try Self.runtime(
                searchBudget: 10,
                physicalRevision: 1,
                executionRevision: 1
            ),
            security: .testingDisabled
        )
        let firstLease = first.acquirePublishedSchemaLease()
        let firstGeneration = firstLease.generation
        let firstPhysicalFingerprint = firstLease.indexPhysicalFingerprint
        let firstRuntimeFingerprint = firstLease.executionRuntimeFingerprint
        await first.shutdown()

        let reopened = try await DBContainer.openRestoringSchema(
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            security: .testingDisabled
        ) { _ in
            try Self.runtime(
                searchBudget: 10,
                physicalRevision: 1,
                executionRevision: 2
            )
        }
        let reopenedLease = reopened.acquirePublishedSchemaLease()

        #expect(reopenedLease.generation == firstGeneration + 1)
        #expect(
            reopenedLease.indexPhysicalFingerprint
                == firstPhysicalFingerprint
        )
        #expect(
            reopenedLease.executionRuntimeFingerprint
                != firstRuntimeFingerprint
        )

        await reopened.shutdown()
        let secured = try await DBContainer.openRestoringSchema(
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            security: .enabled()
        ) { _ in
            try Self.runtime(
                searchBudget: 10,
                physicalRevision: 1,
                executionRevision: 2
            )
        }
        let securedLease = secured.acquirePublishedSchemaLease()

        #expect(securedLease.generation == reopenedLease.generation + 1)
        #expect(
            securedLease.indexPhysicalFingerprint
                == reopenedLease.indexPhysicalFingerprint
        )
        #expect(
            securedLease.executionRuntimeFingerprint
                != reopenedLease.executionRuntimeFingerprint
        )

        await secured.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Physical policy mismatch fails before normal open mutates indexes")
    func physicalPolicyMismatchPrecedesIndexInitialization() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try SchemaRuntimeGenerationEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let first = try await DBContainer.open(
            for: schema,
            configuration: .testing(
                storageEngine: RetainedSchemaRuntimeEngine(engine)
            ),
            runtimeConfiguration: try Self.runtime(
                searchBudget: 10,
                physicalRevision: 1
            ),
            security: .testingDisabled
        )
        await first.shutdown()
        let versionBeforeMismatch = try await Self.readVersion(of: engine)

        await #expect(
            throws: DatabaseSchemaRestorationError
                .indexPhysicalFingerprintMismatch
        ) {
            _ = try await DBContainer.open(
                for: schema,
                configuration: .testing(
                    storageEngine: RetainedSchemaRuntimeEngine(engine)
                ),
                runtimeConfiguration: try Self.runtime(
                    searchBudget: 10,
                    physicalRevision: 2
                ),
                security: .testingDisabled
            )
        }

        #expect(
            try await Self.readVersion(of: engine) == versionBeforeMismatch
        )
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    private static func runtime(
        searchBudget: Int,
        physicalRevision: UInt32,
        executionRevision: UInt64 = 1
    ) throws -> DatabaseRuntimeConfiguration {
        let provider = QueryTunedIndexMaintainerProvider(
            physicalRevision: physicalRevision
        )
        var definition = try EntityRuntimeDefinition(
            SchemaRuntimeGenerationEntity.self
        )
        try definition.register(provider)
        return try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "schema-runtime-generation-tests",
                revision: executionRevision
            ),
            indexMaintainerProviderDescriptors: [
                IndexMaintainerProviderDescriptor(describing: provider)
            ],
            entityRuntimes: [definition.registration()],
            indexConfigurations: [
                QueryTunedIndexConfiguration(
                    indexName: "schema_runtime_generation_value",
                    searchBudget: searchBudget
                )
            ]
        )
    }

    private static func readVersion(
        of engine: InMemoryEngine
    ) async throws -> Int64 {
        let transaction = try engine.createTransaction()
        do {
            let version = try await transaction.getReadVersion()
            try await transaction.cancel()
            return version
        } catch {
            try await transaction.cancel()
            throw error
        }
    }
}

private actor SchemaDrainProbe {
    private(set) var completed = false

    func markCompleted() {
        completed = true
    }
}

private struct QueryTunedIndexConfiguration: IndexRuntimeConfiguration {
    static let indexType: IndexType = .custom("query-tuned")

    let indexName: String
    let searchBudget: Int

    var executionOptions: FieldObject {
        get throws {
            try FieldObject([
                ("searchBudget", .int64(Int64(searchBudget)))
            ])
        }
    }
}

private struct QueryTunedIndexMaintainerProvider: IndexMaintainerProvider {
    let indexType: IndexType = .custom("query-tuned")
    let physicalRevision: UInt32

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        _ = index
        _ = configurations
        return try IndexPhysicalLayout(
            name: "test.query-tuned",
            revision: physicalRevision
        )
    }

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = configurations
        _ = wallClock
        return QueryTunedIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}

private struct QueryTunedIndexMaintainer<Item: PersistedEntityValue>:
    IndexMaintainer
{
    let index: ResolvedIndex
    let subspace: Subspace
    let idExpression: KeyExpression

    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        _ = oldItem
        _ = newItem
        _ = transaction
    }

    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        _ = item
        _ = id
        _ = transaction
    }
}

private final class RetainedSchemaRuntimeEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}
    typealias TransactionType = InMemoryTransaction

    private let shared: InMemoryEngine

    init(_ shared: InMemoryEngine) {
        self.shared = shared
    }

    init(configuration: Configuration) async throws {
        self.shared = InMemoryEngine()
    }

    var transactionDomain: StorageTransactionDomain {
        shared.transactionDomain
    }

    var directoryAccess: any DirectoryAccess {
        shared.directoryAccess
    }

    func createTransaction() throws -> InMemoryTransaction {
        try shared.createTransaction()
    }

    func requestShutdown() {}

    func waitUntilShutdown() async {}
}
#endif

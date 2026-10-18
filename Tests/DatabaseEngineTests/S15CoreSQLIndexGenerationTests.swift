#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Suite("S15 Core SQL index generations", .timeLimit(.minutes(1)))
struct S15CoreSQLIndexGenerationTests {
    @Test("SQL index definitions change physical identity while retaining the layout")
    func definitionChangesPhysicalFingerprint() async throws {
        let container = try await open(InMemoryEngine())
        defer { await container.shutdown() }
        let lease = container.acquirePublishedSchemaLease()
        let relation = try #require(lease.sqlCatalog.catalog.relations.first)
        let first = try catalog(lease.sqlCatalog.catalog, relation: relation, unique: false)
        let second = try catalog(lease.sqlCatalog.catalog, relation: relation, unique: true)
        let firstSnapshot = try SQLCatalogSnapshot(catalog: first, physicalSchema: lease.schema,
            runtimeConfiguration: lease.runtimeConfiguration)
        let secondSnapshot = try SQLCatalogSnapshot(catalog: second, physicalSchema: lease.schema,
            runtimeConfiguration: lease.runtimeConfiguration)
        let firstPrepared = try container.prepareSchemaGeneration(lease.schema,
            runtimeConfiguration: lease.runtimeConfiguration, catalogSnapshot: firstSnapshot)
        let secondPrepared = try container.prepareSchemaGeneration(lease.schema,
            runtimeConfiguration: lease.runtimeConfiguration, catalogSnapshot: secondSnapshot)
        let name = SQLIndexRuntime.storageName(try #require(first.indexes.first))
        #expect(firstPrepared.indexPhysicalLayouts[name] != nil)
        #expect(firstPrepared.indexPhysicalLayouts == secondPrepared.indexPhysicalLayouts)
        #expect(firstPrepared.indexPhysicalFingerprint != lease.indexPhysicalFingerprint)
        #expect(firstPrepared.indexPhysicalFingerprint != secondPrepared.indexPhysicalFingerprint)
        #expect(firstPrepared.executionRuntimeFingerprint != secondPrepared.executionRuntimeFingerprint)
    }

    @Test("Both reopen paths retain SQL index identities and maintain the restored index")
    func bothReopenPathsRetainSQLIndexes() async throws {
        let engine = InMemoryEngine()
        defer { engine.requestShutdown(); await engine.waitUntilShutdown() }
        let first = try await open(S15CoreRetainedEngine(engine))
        defer { await first.shutdown() }
        try await createIndex(in: first)
        let original = first.acquirePublishedSchemaLease()
        let index = try #require(original.sqlCatalog.catalog.indexes.first)
        let name = SQLIndexRuntime.storageName(index)
        #expect(original.indexPhysicalLayouts[name] != nil)
        await first.shutdown()

        let applicationOpen = try await open(S15CoreRetainedEngine(engine))
        defer { await applicationOpen.shutdown() }
        let applicationLease = applicationOpen.acquirePublishedSchemaLease()
        #expect(applicationLease.sqlCatalog.catalog == original.sqlCatalog.catalog)
        #expect(applicationLease.indexPhysicalFingerprint == original.indexPhysicalFingerprint)
        #expect(applicationLease.indexPhysicalLayouts == original.indexPhysicalLayouts)
        #expect(try applicationOpen.executionIndexPhysicalLayout(named: name)
            == SQLIndexRuntime.physicalLayout())
        await applicationOpen.shutdown()

        let restored = try await restore(S15CoreRetainedEngine(engine))
        defer { await restored.shutdown() }
        let restoredLease = restored.acquirePublishedSchemaLease()
        #expect(restoredLease.sqlCatalog.catalog == original.sqlCatalog.catalog)
        #expect(restoredLease.indexPhysicalFingerprint == original.indexPhysicalFingerprint)
        #expect(restoredLease.indexPhysicalLayouts == original.indexPhysicalLayouts)
        await #expect(throws: SQLConstraintError.violation(relation: index.relationID, constraint: index.name)) {
            try await execute(insert(id: "duplicate", value: "retained"), in: restored)
        }
        let rows = try await restored.testBaseContext().query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S15CoreItem"))))
        #expect(rows.rows.map(\.values) == [[.primitive(.string("original"))]])
    }

    @Test("A missing persisted SQL layout rejects both reopen paths")
    func missingPersistedSQLLayoutRejectsReopen() async throws {
        let engine = InMemoryEngine()
        defer { engine.requestShutdown(); await engine.waitUntilShutdown() }
        let first = try await open(S15CoreRetainedEngine(engine))
        defer { await first.shutdown() }
        try await createIndex(in: first)
        let metadata = first.metadataSubspace
        await first.shutdown()
        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try DBContainer.setActiveIndexPhysicalLayouts([:], metadataSubspace: metadata,
                transaction: transaction)
        }
        await #expect(throws: DatabaseSchemaRestorationError.indexPhysicalFingerprintMismatch) {
            _ = try await open(S15CoreRetainedEngine(engine))
        }
        await #expect(throws: DatabaseSchemaRestorationError.indexPhysicalFingerprintMismatch) {
            _ = try await restore(S15CoreRetainedEngine(engine))
        }
    }

    private func catalog(_ original: SQLCatalog, relation: SQLRelation, unique: Bool) throws -> SQLCatalog {
        let column = try #require(relation.columns.first { $0.name == "value" })
        let index = try SQLIndex(id: "core-value-index", namespaceID: relation.namespaceID,
            name: "core_value", relationID: relation.id, keys: [SortKey(.col("value"))], unique: unique,
            bindings: [SQLViewColumnBinding(id: column.id, name: "value", type: .primitive(.string, isArray: false))])
        return try SQLCatalog(namespaces: original.namespaces, relations: original.relations,
            sequences: original.sequences, views: original.views, materializedViews: original.materializedViews,
            indexes: [index])
    }

    private func createIndex(in container: DBContainer) async throws {
        _ = try await execute(insert(id: "original", value: "retained"), in: container)
        _ = try await execute(.ddl(.createIndex(SQLCreateIndex(name: TableRef("core_value"),
            table: TableRef("S15CoreItem"), keys: [SortKey(.col("value"))], unique: true))), in: container)
    }

    private func insert(id: String, value: String) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("S15CoreItem"), columns: ["id", "value"],
            source: .values([[.string(id), .string(value)]])))
    }

    @discardableResult
    private func execute(_ statement: QueryStatement, in container: DBContainer) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(statement)]), options: SQLBlockExecutionOptions(
                checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 16,
                    maximumRetainedBytes: 4 * 1_024 * 1_024, maximumWorkUnits: 100_000,
                    maximumReadPageRows: 128)))
    }

    private func open(_ engine: any StorageEngine) async throws -> DBContainer {
        let entity = try Schema.Entity(name: "S15CoreItem", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: .string)],
            directoryComponents: [.staticPath("s15-core-items")])
        let schema = try Schema(entities: [entity])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: runtime(schema), security: .testingDisabled)
    }

    private func restore(_ engine: any StorageEngine) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine: engine),
            security: .testingDisabled) { schema in try runtime(schema) }
    }

    private func runtime(_ schema: Schema) throws -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s15-core", revision: 1),
            schema: schema)
    }
}

/// The fixture retains authoritative shutdown ownership across container reopen.
private final class S15CoreRetainedEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}
    typealias TransactionType = InMemoryTransaction
    private let shared: InMemoryEngine
    init(_ shared: InMemoryEngine) { self.shared = shared }
    init(configuration: Configuration) async throws { shared = InMemoryEngine() }
    var transactionDomain: StorageTransactionDomain { shared.transactionDomain }
    var directoryAccess: any DirectoryAccess { shared.directoryAccess }
    func createTransaction() throws -> InMemoryTransaction { try shared.createTransaction() }
    func requestShutdown() {}
    func waitUntilShutdown() async {}
}
#endif

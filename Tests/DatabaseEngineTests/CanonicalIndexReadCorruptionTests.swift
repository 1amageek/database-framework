import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Persistable
private struct CorruptionProbeItem {
    #Directory<CorruptionProbeItem>("corruption-probe", "items")

    var id: String
    var category: String
}

/// Verifies that physical index corruption surfaces as `CanonicalReadError`
/// instead of silently shrinking query results.
@Suite("Canonical Index Read Corruption")
struct CanonicalIndexReadCorruptionTests {

    @Test("A structurally invalid index key fails the read")
    func corruptedIndexKeyFailsRead() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let item = CorruptionProbeItem(id: "probe", category: "vv")
        try context.insert(item)
        try await context.save()

        let rangeQuery = Query<CorruptionProbeItem>().where(
            CorruptionProbeItem.fields.category > "a"
        )
        let baseline = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: rangeQuery
        ).execute()
        #expect(baseline.map(\.id) == [item.id])

        // Plant an index key that carries the indexed value but no primary
        // key elements: [physical index subspace]/[value] with nothing after it.
        let categoryElement = try FieldValueTupleCodec.tupleElement(
            for: .string("vv")
        )
        let corruptKey = try scenario.store.indexLifecycleStore
            .indexSubspace(for: scenario.indexName)
            .pack(Tuple(categoryElement))
        try await scenario.engine.withTransaction { transaction in
            try transaction.setValue(ByteString(), for: corruptKey)
        }

        do {
            _ = try await QueryExecutor(
                context: scenario.container.testBaseContext(),
                query: rangeQuery
            ).execute()
            Issue.record("Expected CanonicalReadError.corruptedIndexEntry")
        } catch CanonicalReadError.corruptedIndexEntry(let indexName, _) {
            #expect(indexName == scenario.indexName)
        }
    }

    @Test("An index entry whose row is missing in the same transaction fails the read")
    func danglingIndexEntryFailsSameTransactionRead() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let item = CorruptionProbeItem(id: "probe", category: "vv")
        try context.insert(item)
        try await context.save()

        let equalityQuery = Query<CorruptionProbeItem>().where(
            CorruptionProbeItem.fields.category == "vv"
        )
        let baseline = try await scenario.container.testBaseContext()
            .withFetchedModelsInTransaction(equalityQuery) { models, _ in
                models.map(\.id)
            }
        #expect(baseline == [item.id])

        // Remove the canonical row directly, bypassing index maintenance,
        // so the index entry dangles.
        let rowKey = scenario.store.itemSubspace
            .subspace(CorruptionProbeItem.persistableType)
            .pack(Tuple(item.id))
        try await scenario.engine.withTransaction { transaction in
            try transaction.clear(key: rowKey)
        }

        do {
            _ = try await scenario.container.testBaseContext()
                .withFetchedModelsInTransaction(equalityQuery) { models, _ in
                    models.map(\.id)
                }
            Issue.record("Expected CanonicalReadError.danglingIndexEntry")
        } catch CanonicalReadError.danglingIndexEntry(let indexName, _) {
            #expect(indexName == scenario.indexName)
        }
    }

    private func makeScenario() async throws -> CorruptionProbeScenario {
        let indexName = "corruption_probe_by_category"
        let descriptor = try IndexDescriptor(
            entityName: CorruptionProbeItem.persistableType,
            declaration: .ordered(
                name: indexName,
                keys: [
                    .ascending(CorruptionProbeItem.fields.category.identity)
                ]
            ),
            fieldSchemas: try CorruptionProbeItem.fieldSchemas
        )
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: CorruptionProbeItem.self,
                    including: [descriptor]
                )
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        CorruptionProbeItem.self,
                        including: [descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        let store = try await container.testBaseStore(for: CorruptionProbeItem.self)
        return CorruptionProbeScenario(
            container: container,
            engine: engine,
            store: store,
            indexName: indexName
        )
    }
}

private struct CorruptionProbeScenario: Sendable {
    let container: DBContainer
    let engine: InMemoryEngine
    let store: DatabaseDataStore
    let indexName: String
}

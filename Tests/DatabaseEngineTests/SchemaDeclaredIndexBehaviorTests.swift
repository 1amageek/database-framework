import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Schema-declared index behavior")
struct SchemaDeclaredIndexBehaviorTests {
    @Test("Application-composed index follows every mutation")
    func applicationComposedIndexFollowsMutations() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let first = CatalogItem(
            id: "first",
            category: "books",
            title: "First"
        )
        let second = CatalogItem(
            id: "second",
            category: "books",
            title: "Second"
        )
        let third = CatalogItem(
            id: "third",
            category: "music",
            title: "Third"
        )

        try context.insert(first)
        try context.insert(second)
        try context.insert(third)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 3)
        #expect(try await scenario.indexEntryCount(category: "books") == 2)
        #expect(try await scenario.indexEntryCount(category: "music") == 1)

        var updated = first
        updated.category = "music"
        try context.update(updated)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 3)
        #expect(try await scenario.indexEntryCount(category: "books") == 1)
        #expect(try await scenario.indexEntryCount(category: "music") == 2)

        try context.delete(third)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 2)
        #expect(try await scenario.indexEntryCount(category: "books") == 1)
        #expect(try await scenario.indexEntryCount(category: "music") == 1)
    }

    @Test("Every read path uses the application-composed index")
    func everyReadPathUsesApplicationComposedIndex() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let first = CatalogItem(
            id: "first",
            category: "books",
            title: "First"
        )
        let second = CatalogItem(
            id: "second",
            category: "books",
            title: "Second"
        )
        let unrelated = CatalogItem(
            id: "unrelated",
            category: "music",
            title: "Unrelated"
        )

        try context.insert(first)
        try context.insert(second)
        try context.insert(unrelated)
        try await context.save()
        try await scenario.corruptItems(ids: [unrelated.id])

        let automaticQuery = scenario.categoryQuery("books")
        let plan = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: automaticQuery
        ).executionPlan()
        guard
            case .orderedIndex(
                let name,
                let indexType,
                let indexedFields
        ) = plan.accessPath else {
            Issue.record("Expected the application-composed scalar index")
            return
        }
        #expect(name == scenario.indexName)
        #expect(indexType == .ordered)
        #expect(indexedFields == ["category"])

        let automaticResults = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: automaticQuery
        ).execute()
        #expect(Set(automaticResults.map(\.id)) == Set([first.id, second.id]))

        var forcedQuery = scenario.categoryQuery("books")
        forcedQuery.forcedIndex = IndexHint(indexName: scenario.indexName)
        let forcedResults = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: forcedQuery
        ).execute()
        #expect(Set(forcedResults.map(\.id)) == Set([first.id, second.id]))

        let canonicalResponse = try await scenario.container
            .testBaseContext()
            .query(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(CatalogItem.persistableType)),
                    accessPath: .index(
                        IndexScanSource(
                            indexName: scenario.indexName,
                            indexType: .ordered
                        )
                    ),
                    filter: .equal(
                        .col("category"),
                        .string("books")
                    )
                )
            )
        #expect(canonicalResponse.rows.count == 2)
        #expect(
            Set(canonicalResponse.rows.compactMap {
                $0.fields["id"]?.stringValue
            }) == Set([first.id, second.id])
        )

        try await scenario.corruptItems(ids: [
            first.id,
            second.id,
            unrelated.id,
        ])
        let count = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: forcedQuery
        ).count()
        #expect(count == 2)
    }

    private func makeScenario() async throws -> CatalogIndexScenario {
        let indexName = "catalog_items_by_category"
        let descriptor = try IndexDescriptor(
            entityName: CatalogItem.persistableType,
            declaration: .ordered(
                name: indexName,
                keys: [.ascending(CatalogItem.fields.category.identity)]
            ),
            fieldSchemas: CatalogItem.fieldSchemas
        )
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: CatalogItem.self,
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
                        CatalogItem.self,
                        including: [descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        let store = try await container.testBaseStore(for: CatalogItem.self)
        return CatalogIndexScenario(
            container: container,
            engine: engine,
            store: store,
            indexName: indexName
        )
    }
}

private struct CatalogIndexScenario: Sendable {
    let container: DBContainer
    let engine: InMemoryEngine
    let store: DatabaseDataStore
    let indexName: String

    func categoryQuery(_ category: String) -> Query<CatalogItem> {
        Query<CatalogItem>().where(
            CatalogItem.fields.category == category
        )
    }

    func indexEntryCount(category: String? = nil) async throws -> Int {
        let indexSubspace = try store.indexLifecycleStore.indexSubspace(
            for: indexName
        )
        let range: (begin: ByteString, end: ByteString)
        if let category {
            let categoryElement = try FieldValueTupleCodec.tupleElement(
                for: .string(category)
            )
            range = indexSubspace.subspace(categoryElement).range()
        } else {
            range = indexSubspace.range()
        }
        return try await engine.withTransaction { transaction in
            try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                snapshot: true
            ).count
        }
    }

    func corruptItems(ids: [String]) async throws {
        let itemSubspace = store.itemSubspace.subspace(
            CatalogItem.persistableType
        )
        try await engine.withTransaction { transaction in
            for id in ids {
                try transaction.setValue(
                    [0xFF],
                    for: itemSubspace.pack(Tuple(id))
                )
            }
        }
    }
}

#if !os(WASI)
#if FOUNDATION_DB
// IndexMaintenanceE2ETests.swift
// End-to-end tests verifying index maintenance via DatabaseContext.save()
//
// These tests validate that the entire CRUD path correctly maintains indexes:
//   User Code → DatabaseContext.save() → DatabaseDataStore → IndexMaintenanceService → IndexMaintainer
//
// This is distinct from existing IndexBehaviorTests which test IndexMaintainer directly.

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import FullTextIndex
@testable import GraphIndex
@testable import ScalarIndex
@testable import AggregationIndex

// MARK: - Test Models with Index Definitions

/// Article with FullTextIndex for testing CRUD path
@Persistable
struct E2EFullTextArticle {
    #Directory<E2EFullTextArticle>("index_maintenance_e2e_fulltext_articles")

    var id: String = UUID().uuidString
    var title: String = ""
    var content: String = ""

    // Full-text index on content field
    #Index(.fullText(tokenizer: .simple), fields: [\E2EFullTextArticle.content])
}

/// Edge with GraphIndex for testing CRUD path
@Persistable
struct E2EGraphEdge {
    #Directory<E2EGraphEdge>("index_maintenance_e2e_graph_edges")

    var id: String = UUID().uuidString
    var source: String = ""
    var target: String = ""
    var relation: String = ""

    // Graph index with adjacency strategy
    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \E2EGraphEdge.source,
        edge: \E2EGraphEdge.relation,
        to: \E2EGraphEdge.target
    )
}

/// Simple model with ScalarIndex for baseline comparison
@Persistable
struct E2EScalarUser {
    #Directory<E2EScalarUser>("index_maintenance_e2e_scalar_users")

    var id: String = UUID().uuidString
    var email: String = ""
    var city: String = ""

    // Scalar index on email (works correctly via default case)
    #Index(.scalar, fields: [\E2EScalarUser.email])
}

/// Model with CountIndex for testing aggregation path
@Persistable
struct E2ECountItem {
    #Directory<E2ECountItem>("index_maintenance_e2e_count_items")

    var id: String = UUID().uuidString
    var category: String = ""
    var value: Int64 = 0

    // Count index grouped by category (works correctly via explicit case)
    #Index(.count, groupBy: [\E2ECountItem.category])
}

// MARK: - Test Suite

@Suite("Index Maintenance E2E Tests", .foundationDBScenario, .serialized, .heartbeat)
struct IndexMaintenanceE2ETests {

    // MARK: - Setup

    private func setupContainer<T: Persistable>(_ types: [T.Type]) async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: try types.map { try $0.schemaEntity },
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(E2EFullTextArticle.self), try DatabaseFrameworkRuntime.entity(E2EGraphEdge.self), try DatabaseFrameworkRuntime.entity(E2EScalarUser.self), try DatabaseFrameworkRuntime.entity(E2ECountItem.self)]),
            security: .testingDisabled,
        )
    }

    private func cleanup(container: DBContainer, paths: [[String]]) async throws {
        _ = paths
        try await container.resetTestBaseData()
    }

    /// Helper to count entries in a subspace
    private func countEntriesInSubspace(
        database: any StorageEngine,
        subspace: Subspace
    ) async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = subspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    /// Helper to dump all keys in a subspace for debugging
    private func dumpSubspaceKeys(
        database: any StorageEngine,
        subspace: Subspace,
        label: String
    ) async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            print("=== \(label) ===")
            for (key, value) in try await transaction.collectRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), snapshot: true) {
                print("  Key: \(key.count) bytes, Value: \(value.count) bytes")
            }
            print("=== END ===")
        }
    }

    // MARK: - Scalar Index E2E Tests (Baseline - should work)

    @Test("ScalarIndex: Insert via DatabaseContext.save() maintains index (baseline)")
    func testScalarIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EScalarUser.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_scalar_users"]])

        let context = container.testBaseContext()

        // Create and save user
        var user = E2EScalarUser()
        user.email = "test@example.com"
        user.city = "Tokyo"

        try context.insert(user)
        try await context.save()

        // Verify the user was saved
        let fetched = try await context.model(for: user.id, as: E2EScalarUser.self)
        #expect(fetched != nil, "User should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.testBaseDirectory(for: E2EScalarUser.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let scalarIndexName = try E2EScalarUser.indexDescriptors.first { descriptor in
            descriptor.kindIdentifier == "scalar"
        }?.name

        #expect(scalarIndexName != nil, "E2EScalarUser should have a scalar index")

        if let indexName = scalarIndexName {
            let scalarIndexSubspace = indexSubspace.subspace(indexName)
            let entryCount = try await countEntriesInSubspace(
                database: container.engine,
                subspace: scalarIndexSubspace
            )

            // Scalar index should have exactly 1 entry
            #expect(entryCount == 1, "Scalar index should have 1 entry after insert, got \(entryCount)")
        }

        try await cleanup(container: container, paths: [["index_maintenance_e2e_scalar_users"]])
    }

    // MARK: - Count Index E2E Tests (Explicit case - should work)

    @Test("CountIndex: Insert via DatabaseContext.save() maintains index (explicit case)")
    func testCountIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2ECountItem.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_count_items"]])

        let context = container.testBaseContext()

        // Create and save items in same category
        var item1 = E2ECountItem()
        item1.category = "electronics"
        item1.value = 100

        var item2 = E2ECountItem()
        item2.category = "electronics"
        item2.value = 200

        var item3 = E2ECountItem()
        item3.category = "books"
        item3.value = 50

        try context.insert(item1)
        try context.insert(item2)
        try context.insert(item3)
        try await context.save()

        // Get the index subspace
        let typeSubspace = try await container.testBaseDirectory(for: E2ECountItem.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let countIndexName = try E2ECountItem.indexDescriptors.first { descriptor in
            descriptor.kindIdentifier == "count"
        }?.name

        #expect(countIndexName != nil, "E2ECountItem should have a count index")

        if let indexName = countIndexName {
            let countIndexSubspace = indexSubspace.subspace(indexName)

            // Count index should have entries (2 groups: electronics and books)
            let entryCount = try await countEntriesInSubspace(
                database: container.engine,
                subspace: countIndexSubspace
            )

            // Should have 2 entries: one for "electronics" and one for "books"
            #expect(entryCount == 2, "Count index should have 2 entries (2 groups), got \(entryCount)")
        }

        try await cleanup(container: container, paths: [["index_maintenance_e2e_count_items"]])
    }

    // MARK: - FullText Index E2E Tests (Falls to default case - EXPECTED TO FAIL)

    @Test("FullTextIndex: Insert via DatabaseContext.save() maintains index")
    func testFullTextIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EFullTextArticle.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_fulltext_articles"]])

        let context = container.testBaseContext()

        // Create and save article
        var article = E2EFullTextArticle()
        article.title = "Test Article"
        article.content = "Hello world this is a test article"

        try context.insert(article)
        try await context.save()

        // Verify the article was saved
        let fetched = try await context.model(for: article.id, as: E2EFullTextArticle.self)
        #expect(fetched != nil, "Article should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.testBaseDirectory(for: E2EFullTextArticle.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let fullTextIndexName = try E2EFullTextArticle.indexDescriptors.first { descriptor in
            descriptor.kindIdentifier == "fulltext"
        }?.name

        #expect(fullTextIndexName != nil, "E2EFullTextArticle should have a fullText index")

        if let indexName = fullTextIndexName {
            let fullTextIndexSubspace = indexSubspace.subspace(indexName)

            // Debug: dump keys
            try await dumpSubspaceKeys(
                database: container.engine,
                subspace: fullTextIndexSubspace,
                label: "FullText Index Subspace"
            )

            let entryCount = try await countEntriesInSubspace(
                database: container.engine,
                subspace: fullTextIndexSubspace
            )

            // Full-text index should have multiple entries (one per token)
            // "Hello world this is a test article" = 7 tokens minimum
            // If IndexMaintenanceService uses updateScalarIndex() for fullText,
            // it will create only 1 entry (treating content as a single value)
            #expect(
                entryCount >= 5,
                "Full-text index should have entries for tokens (>=5), got \(entryCount). If this is 1, IndexMaintenanceService is incorrectly using scalar index logic."
            )
        }

        try await cleanup(container: container, paths: [["index_maintenance_e2e_fulltext_articles"]])
    }

    // MARK: - Graph Index E2E Tests (Falls to default case - EXPECTED TO FAIL)

    @Test("GraphIndex: Insert via DatabaseContext.save() maintains index")
    func testGraphIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])

        let context = container.testBaseContext()

        // Create and save edge
        var edge = E2EGraphEdge()
        edge.source = "Alice"
        edge.target = "Bob"
        edge.relation = "follows"

        try context.insert(edge)
        try await context.save()

        // Verify the edge was saved
        let fetched = try await context.model(for: edge.id, as: E2EGraphEdge.self)
        #expect(fetched != nil, "Edge should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.testBaseDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let graphIndexName = try E2EGraphEdge.indexDescriptors.first { descriptor in
            descriptor.kindIdentifier == "graph"
        }?.name

        #expect(graphIndexName != nil, "E2EGraphEdge should have a graph index")

        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)

            // Debug: dump keys
            try await dumpSubspaceKeys(
                database: container.engine,
                subspace: graphIndexSubspace,
                label: "Graph Index Subspace"
            )

            let entryCount = try await countEntriesInSubspace(
                database: container.engine,
                subspace: graphIndexSubspace
            )

            // Adjacency strategy creates 2 entries: outgoing and incoming
            // [out]/[edge]/[from]/[to]/[id] and [in]/[edge]/[to]/[from]/[id]
            // If IndexMaintenanceService uses updateScalarIndex() for graph,
            // it will create only 1 entry
            #expect(
                entryCount == 2,
                "Graph index (adjacency) should have 2 entries (out + in), got \(entryCount). If this is 1, IndexMaintenanceService is incorrectly using scalar index logic."
            )
        }

        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])
    }

    @Test("GraphIndex: Delete via DatabaseContext.save() removes all index entries")
    func testGraphIndexDeleteViaSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])

        let context = container.testBaseContext()

        // Create and save edge
        var edge = E2EGraphEdge()
        edge.source = "Alice"
        edge.target = "Bob"
        edge.relation = "follows"

        try context.insert(edge)
        try await context.save()

        // Get index count before delete
        let typeSubspace = try await container.testBaseDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)
        let graphIndexName = try E2EGraphEdge.indexDescriptors.first { descriptor in
            descriptor.kindIdentifier == "graph"
        }?.name

        var countBeforeDelete = 0
        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)
            countBeforeDelete = try await countEntriesInSubspace(
                database: container.engine,
                subspace: graphIndexSubspace
            )
        }

        // Delete the edge
        try context.delete(edge)
        try await context.save()

        // Verify the edge was deleted
        let fetched = try await context.model(for: edge.id, as: E2EGraphEdge.self)
        #expect(fetched == nil, "Edge should be deleted")

        // Verify ALL index entries were removed
        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)
            let countAfterDelete = try await countEntriesInSubspace(
                database: container.engine,
                subspace: graphIndexSubspace
            )

            #expect(
                countAfterDelete == 0,
                "Graph index should have 0 entries after delete, got \(countAfterDelete). Had \(countBeforeDelete) before delete."
            )
        }

        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])
    }

    // MARK: - Comparison Test: Provider Registry vs DatabaseContext.save()

    @Test("Provider registry and DatabaseContext.save() maintain identical graph entries")
    func testComparisonDirectVsSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])

        // Part 1: Direct IndexMaintainer usage (should work)
        let typeSubspace = try await container.testBaseDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        guard let graphIndexDescriptor = try E2EGraphEdge.indexDescriptors.first(where: { descriptor in
            descriptor.kindIdentifier == "graph"
        }) else {
            Issue.record("Expected graph index descriptor")
            return
        }

        let graphIndexSubspace = indexSubspace.subspace(graphIndexDescriptor.name)

        let index = Index(
            name: graphIndexDescriptor.name,
            kind: graphIndexDescriptor.kind,
            rootExpression: FieldKeyExpression(fieldName: "source"),  // Placeholder
            subspaceKey: graphIndexDescriptor.name,
            itemTypes: Set([E2EGraphEdge.persistableType])
        )

        let maintainer: any IndexMaintainer<E2EGraphEdge> = try GraphIndexMaintainerProvider()
            .makeIndexMaintainer(
            index: index,
            subspace: graphIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [],
            wallClock: container.wallClock
        )

        // Create edge and use maintainer directly
        var directEdge = E2EGraphEdge()
        directEdge.source = "DirectAlice"
        directEdge.target = "DirectBob"
        directEdge.relation = "follows"
        let storedDirectEdge = directEdge

        try await container.engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: storedDirectEdge,
                transaction: transaction
            )
        }

        let directMaintainerCount = try await countEntriesInSubspace(
            database: container.engine,
            subspace: graphIndexSubspace
        )

        #expect(
            directMaintainerCount == 2,
            "Direct IndexMaintainer should create 2 entries, got \(directMaintainerCount)"
        )

        // Clean up direct test
        try await container.engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: storedDirectEdge,
                newItem: nil,
                transaction: transaction
            )
        }

        // Part 2: DatabaseContext.save()
        let context = container.testBaseContext()

        var contextEdge = E2EGraphEdge()
        contextEdge.source = "ContextAlice"
        contextEdge.target = "ContextBob"
        contextEdge.relation = "follows"

        try context.insert(contextEdge)
        try await context.save()

        let contextSaveCount = try await countEntriesInSubspace(
            database: container.engine,
            subspace: graphIndexSubspace
        )

        // This is the key assertion
        #expect(
            contextSaveCount == 2,
            "DatabaseContext.save() should create the same 2 entries as the provider registry, got \(contextSaveCount). The provider registry created \(directMaintainerCount)."
        )

        try await cleanup(container: container, paths: [["index_maintenance_e2e_graph_edges"]])
    }
}
#endif

#endif

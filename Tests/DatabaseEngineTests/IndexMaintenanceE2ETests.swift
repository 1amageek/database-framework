// IndexMaintenanceE2ETests.swift
// End-to-end tests verifying index maintenance via FDBContext.save()
//
// These tests validate that the entire CRUD path correctly maintains indexes:
//   User Code → FDBContext.save() → FDBDataStore → IndexMaintenanceService → IndexMaintainer
//
// This is distinct from existing IndexBehaviorTests which test IndexMaintainer directly.

import Testing
import Foundation
import FoundationDB
import Core
import FullText
import Graph
@testable import DatabaseEngine
@testable import FullTextIndex
@testable import GraphIndex
@testable import ScalarIndex
@testable import AggregationIndex

// MARK: - Test Models with Index Definitions

/// Article with FullTextIndex for testing CRUD path
@Persistable
struct E2EFullTextArticle {
    #Directory<E2EFullTextArticle>("test", "e2e", "fulltext_articles")

    var id: String = ULID().ulidString
    var title: String = ""
    var content: String = ""

    // Full-text index on content field
    #Index(FullTextIndexKind<E2EFullTextArticle>(
        fields: [\.content],
        tokenizer: .simple
    ))
}

/// Edge with GraphIndex for testing CRUD path
@Persistable
struct E2EGraphEdge {
    #Directory<E2EGraphEdge>("test", "e2e", "graph_edges")

    var id: String = ULID().ulidString
    var source: String = ""
    var target: String = ""
    var relation: String = ""

    // Graph index with adjacency strategy
    #Index(GraphIndexKind<E2EGraphEdge>(
        from: \.source,
        edge: \.relation,
        to: \.target,
        strategy: .adjacency
    ))
}

/// Simple model with ScalarIndex for baseline comparison
@Persistable
struct E2EScalarUser {
    #Directory<E2EScalarUser>("test", "e2e", "scalar_users")

    var id: String = ULID().ulidString
    var email: String = ""
    var city: String = ""

    // Scalar index on email (works correctly via default case)
    #Index(ScalarIndexKind<E2EScalarUser>(fields: [\.email]))
}

/// Model with CountIndex for testing aggregation path
@Persistable
struct E2ECountItem {
    #Directory<E2ECountItem>("test", "e2e", "count_items")

    var id: String = ULID().ulidString
    var category: String = ""
    var value: Int = 0

    // Count index grouped by category (works correctly via explicit case)
    #Index(CountIndexKind<E2ECountItem>(groupBy: [\.category]))
}

// MARK: - Test Suite

@Suite("Index Maintenance E2E Tests", .serialized)
struct IndexMaintenanceE2ETests {

    // MARK: - Setup

    private func setupContainer<T: Persistable>(_ types: [T.Type]) async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(types.map { $0 as any Persistable.Type }, version: Schema.Version(1, 0, 0))

        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func cleanup(container: FDBContainer, paths: [[String]]) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        for path in paths {
            try? await directoryLayer.remove(path: path)
        }
    }

    /// Helper to set index state to readable for testing
    /// By default, indexes are DISABLED and won't be maintained.
    /// For E2E tests, we need to set them to READABLE.
    private func setIndexStatesToReadable<T: Persistable>(
        for type: T.Type,
        container: FDBContainer
    ) async throws {
        let subspace = try await container.resolveDirectory(for: type)
        let indexStateManager = IndexStateManager(database: container.database, subspace: subspace)

        for descriptor in type.indexDescriptors {
            // Enable: disabled -> writeOnly
            try await indexStateManager.enable(descriptor.name)
            // Make readable: writeOnly -> readable
            try await indexStateManager.makeReadable(descriptor.name)
        }
    }

    /// Helper to count entries in a subspace
    private func countEntriesInSubspace(
        database: any DatabaseProtocol,
        subspace: Subspace
    ) async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = subspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    /// Helper to dump all keys in a subspace for debugging
    private func dumpSubspaceKeys(
        database: any DatabaseProtocol,
        subspace: Subspace,
        label: String
    ) async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            print("=== \(label) ===")
            for try await (key, value) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                print("  Key: \(key.count) bytes, Value: \(value.count) bytes")
            }
            print("=== END ===")
        }
    }

    // MARK: - Scalar Index E2E Tests (Baseline - should work)

    @Test("ScalarIndex: Insert via FDBContext.save() maintains index (baseline)")
    func testScalarIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EScalarUser.self])
        try await cleanup(container: container, paths: [["test", "e2e", "scalar_users"]])

        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2EScalarUser.self, container: container)

        let context = container.newContext()

        // Create and save user
        var user = E2EScalarUser()
        user.email = "test@example.com"
        user.city = "Tokyo"

        context.insert(user)
        try await context.save()

        // Verify the user was saved
        let fetched = try await context.model(for: user.id, as: E2EScalarUser.self)
        #expect(fetched != nil, "User should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.resolveDirectory(for: E2EScalarUser.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let scalarIndexName = E2EScalarUser.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "scalar"
        }?.name

        #expect(scalarIndexName != nil, "E2EScalarUser should have a scalar index")

        if let indexName = scalarIndexName {
            let scalarIndexSubspace = indexSubspace.subspace(indexName)
            let entryCount = try await countEntriesInSubspace(
                database: container.database,
                subspace: scalarIndexSubspace
            )

            // Scalar index should have exactly 1 entry
            #expect(entryCount == 1, "Scalar index should have 1 entry after insert, got \(entryCount)")
        }

        try await cleanup(container: container, paths: [["test", "e2e", "scalar_users"]])
    }

    // MARK: - Count Index E2E Tests (Explicit case - should work)

    @Test("CountIndex: Insert via FDBContext.save() maintains index (explicit case)")
    func testCountIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2ECountItem.self])
        try await cleanup(container: container, paths: [["test", "e2e", "count_items"]])

        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2ECountItem.self, container: container)

        let context = container.newContext()

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

        context.insert(item1)
        context.insert(item2)
        context.insert(item3)
        try await context.save()

        // Get the index subspace
        let typeSubspace = try await container.resolveDirectory(for: E2ECountItem.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let countIndexName = E2ECountItem.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "count"
        }?.name

        #expect(countIndexName != nil, "E2ECountItem should have a count index")

        if let indexName = countIndexName {
            let countIndexSubspace = indexSubspace.subspace(indexName)

            // Count index should have entries (2 groups: electronics and books)
            let entryCount = try await countEntriesInSubspace(
                database: container.database,
                subspace: countIndexSubspace
            )

            // Should have 2 entries: one for "electronics" and one for "books"
            #expect(entryCount == 2, "Count index should have 2 entries (2 groups), got \(entryCount)")
        }

        try await cleanup(container: container, paths: [["test", "e2e", "count_items"]])
    }

    // MARK: - FullText Index E2E Tests (Falls to default case - EXPECTED TO FAIL)

    @Test("FullTextIndex: Insert via FDBContext.save() maintains index")
    func testFullTextIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EFullTextArticle.self])
        try await cleanup(container: container, paths: [["test", "e2e", "fulltext_articles"]])

        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2EFullTextArticle.self, container: container)

        let context = container.newContext()

        // Create and save article
        var article = E2EFullTextArticle()
        article.title = "Test Article"
        article.content = "Hello world this is a test article"

        context.insert(article)
        try await context.save()

        // Verify the article was saved
        let fetched = try await context.model(for: article.id, as: E2EFullTextArticle.self)
        #expect(fetched != nil, "Article should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.resolveDirectory(for: E2EFullTextArticle.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let fullTextIndexName = E2EFullTextArticle.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "fulltext"
        }?.name

        #expect(fullTextIndexName != nil, "E2EFullTextArticle should have a fullText index")

        if let indexName = fullTextIndexName {
            let fullTextIndexSubspace = indexSubspace.subspace(indexName)

            // Debug: dump keys
            try await dumpSubspaceKeys(
                database: container.database,
                subspace: fullTextIndexSubspace,
                label: "FullText Index Subspace"
            )

            let entryCount = try await countEntriesInSubspace(
                database: container.database,
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

        try await cleanup(container: container, paths: [["test", "e2e", "fulltext_articles"]])
    }

    // MARK: - Graph Index E2E Tests (Falls to default case - EXPECTED TO FAIL)

    @Test("GraphIndex: Insert via FDBContext.save() maintains index")
    func testGraphIndexInsertViaSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])

        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2EGraphEdge.self, container: container)

        let context = container.newContext()

        // Create and save edge
        var edge = E2EGraphEdge()
        edge.source = "Alice"
        edge.target = "Bob"
        edge.relation = "follows"

        context.insert(edge)
        try await context.save()

        // Verify the edge was saved
        let fetched = try await context.model(for: edge.id, as: E2EGraphEdge.self)
        #expect(fetched != nil, "Edge should be saved")

        // Get the index subspace and count entries
        let typeSubspace = try await container.resolveDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let graphIndexName = E2EGraphEdge.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "graph"
        }?.name

        #expect(graphIndexName != nil, "E2EGraphEdge should have a graph index")

        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)

            // Debug: dump keys
            try await dumpSubspaceKeys(
                database: container.database,
                subspace: graphIndexSubspace,
                label: "Graph Index Subspace"
            )

            let entryCount = try await countEntriesInSubspace(
                database: container.database,
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

        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])
    }

    @Test("GraphIndex: Delete via FDBContext.save() removes all index entries")
    func testGraphIndexDeleteViaSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])

        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2EGraphEdge.self, container: container)

        let context = container.newContext()

        // Create and save edge
        var edge = E2EGraphEdge()
        edge.source = "Alice"
        edge.target = "Bob"
        edge.relation = "follows"

        context.insert(edge)
        try await context.save()

        // Get index count before delete
        let typeSubspace = try await container.resolveDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)
        let graphIndexName = E2EGraphEdge.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "graph"
        }?.name

        var countBeforeDelete = 0
        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)
            countBeforeDelete = try await countEntriesInSubspace(
                database: container.database,
                subspace: graphIndexSubspace
            )
        }

        // Delete the edge
        context.delete(edge)
        try await context.save()

        // Verify the edge was deleted
        let fetched = try await context.model(for: edge.id, as: E2EGraphEdge.self)
        #expect(fetched == nil, "Edge should be deleted")

        // Verify ALL index entries were removed
        if let indexName = graphIndexName {
            let graphIndexSubspace = indexSubspace.subspace(indexName)
            let countAfterDelete = try await countEntriesInSubspace(
                database: container.database,
                subspace: graphIndexSubspace
            )

            #expect(
                countAfterDelete == 0,
                "Graph index should have 0 entries after delete, got \(countAfterDelete). Had \(countBeforeDelete) before delete."
            )
        }

        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])
    }

    // MARK: - Comparison Test: Direct Maintainer vs FDBContext.save()

    @Test("Comparison: Direct IndexMaintainer works but FDBContext.save() may not")
    func testComparisonDirectVsSave() async throws {
        let container = try await setupContainer([E2EGraphEdge.self])
        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])

        // Part 1: Direct IndexMaintainer usage (should work)
        let typeSubspace = try await container.resolveDirectory(for: E2EGraphEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)

        let graphIndexDescriptor = E2EGraphEdge.indexDescriptors.first { descriptor in
            type(of: descriptor.kind).identifier == "graph"
        }!

        let graphIndexSubspace = indexSubspace.subspace(graphIndexDescriptor.name)

        // Create maintainer directly
        guard let maintainable = graphIndexDescriptor.kind as? any IndexKindMaintainable else {
            Issue.record("GraphIndexKind should conform to IndexKindMaintainable")
            return
        }

        // Build Index from descriptor
        let index = Index(
            name: graphIndexDescriptor.name,
            kind: graphIndexDescriptor.kind,
            rootExpression: FieldKeyExpression(fieldName: "source"),  // Placeholder
            subspaceKey: graphIndexDescriptor.name,
            itemTypes: Set([E2EGraphEdge.persistableType])
        )

        let maintainer: any IndexMaintainer<E2EGraphEdge> = maintainable.makeIndexMaintainer(
            index: index,
            subspace: graphIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: []
        )

        // Create edge and use maintainer directly
        var directEdge = E2EGraphEdge()
        directEdge.source = "DirectAlice"
        directEdge.target = "DirectBob"
        directEdge.relation = "follows"

        try await container.database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: directEdge,
                transaction: transaction
            )
        }

        let directMaintainerCount = try await countEntriesInSubspace(
            database: container.database,
            subspace: graphIndexSubspace
        )

        #expect(
            directMaintainerCount == 2,
            "Direct IndexMaintainer should create 2 entries, got \(directMaintainerCount)"
        )

        // Clean up direct test
        try await container.database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: directEdge,
                newItem: nil,
                transaction: transaction
            )
        }

        // Part 2: FDBContext.save() (may fail if IndexMaintenanceService is broken)
        // Set index states to readable (indexes are disabled by default)
        try await setIndexStatesToReadable(for: E2EGraphEdge.self, container: container)

        let context = container.newContext()

        var contextEdge = E2EGraphEdge()
        contextEdge.source = "ContextAlice"
        contextEdge.target = "ContextBob"
        contextEdge.relation = "follows"

        context.insert(contextEdge)
        try await context.save()

        let contextSaveCount = try await countEntriesInSubspace(
            database: container.database,
            subspace: graphIndexSubspace
        )

        // This is the key assertion
        #expect(
            contextSaveCount == 2,
            "FDBContext.save() should create 2 entries (same as direct maintainer), got \(contextSaveCount). Direct maintainer created \(directMaintainerCount). This discrepancy proves IndexMaintenanceService is not using IndexKindMaintainable."
        )

        try await cleanup(container: container, paths: [["test", "e2e", "graph_edges"]])
    }
}

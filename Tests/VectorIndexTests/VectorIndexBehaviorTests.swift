// VectorIndexBehaviorTests.swift
// Integration tests for VectorIndex (Flat) behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

struct TestDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, title: String, embedding: [Float]) {
        self.id = id
        self.title = title
        self.embedding = embedding
    }

    static var persistableType: String { "TestDocument" }
    static var allFields: [String] { ["id", "title", "embedding"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        case "embedding": return embedding
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestDocument, Value>) -> String {
        switch keyPath {
        case \TestDocument.id: return "id"
        case \TestDocument.title: return "title"
        case \TestDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestDocument>) -> String {
        switch keyPath {
        case \TestDocument.id: return "id"
        case \TestDocument.title: return "title"
        case \TestDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FlatVectorIndexMaintainer<TestDocument>
    let dimensions: Int

    init(dimensions: Int = 4, metric: VectorMetric = .cosine, indexName: String = "TestDocument_embedding") throws {
        self.database = try FDBClient.openDatabase()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "vector", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let kind = VectorIndexKind<TestDocument>(
            embedding: \.embedding,
            dimensions: dimensions,
            metric: metric
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: indexName,
            itemTypes: Set(["TestDocument"])
        )

        self.maintainer = FlatVectorIndexMaintainer<TestDocument>(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func search(query: [Float], k: Int) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.search(queryVector: query, k: k, transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("VectorIndex Behavior Tests", .tags(.fdb))
struct VectorIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert stores vector")
    func testInsertStoresVector() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let doc = TestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 vector entry after insert")

        try await ctx.cleanup()
    }

    @Test("Insert multiple vectors")
    func testInsertMultipleVectors() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let docs = [
            TestDocument(id: "doc1", title: "First", embedding: [1.0, 0.0, 0.0, 0.0]),
            TestDocument(id: "doc2", title: "Second", embedding: [0.0, 1.0, 0.0, 0.0]),
            TestDocument(id: "doc3", title: "Third", embedding: [0.0, 0.0, 1.0, 0.0])
        ]

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 vector entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes vector")
    func testDeleteRemovesVector() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let doc = TestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 vector entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update replaces vector")
    func testUpdateReplacesVector() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let doc = TestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        // Update with different embedding
        let updatedDoc = TestDocument(id: "doc1", title: "Test Updated", embedding: [0.0, 1.0, 0.0, 0.0])
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc,
                newItem: updatedDoc,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 vector entry after update")

        // Verify the new vector is searchable
        let results = try await ctx.search(query: [0.0, 1.0, 0.0, 0.0], k: 1)
        #expect(results.count == 1)
        #expect(results[0].distance < 0.01, "Updated vector should have near-zero distance to query")

        try await ctx.cleanup()
    }

    // MARK: - Cosine Similarity Search Tests

    @Test("Cosine similarity search returns correct order")
    func testCosineSimilaritySearch() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4, metric: .cosine)

        // Create vectors at different angles
        let docs = [
            TestDocument(id: "exact", title: "Exact", embedding: [1.0, 0.0, 0.0, 0.0]),
            TestDocument(id: "similar", title: "Similar", embedding: [0.9, 0.1, 0.0, 0.0]),
            TestDocument(id: "different", title: "Different", embedding: [0.0, 1.0, 0.0, 0.0]),
            TestDocument(id: "opposite", title: "Opposite", embedding: [-1.0, 0.0, 0.0, 0.0])
        ]

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Search for vector similar to [1, 0, 0, 0]
        let results = try await ctx.search(query: [1.0, 0.0, 0.0, 0.0], k: 4)

        #expect(results.count == 4)

        // Extract IDs from results
        let resultIds = results.compactMap { result -> String? in
            guard let id = result.primaryKey.first as? String else { return nil }
            return id
        }

        // Verify order: exact match should be first
        #expect(resultIds[0] == "exact", "Exact match should be first")
        #expect(resultIds[1] == "similar", "Similar should be second")

        // Verify distances
        #expect(results[0].distance < 0.01, "Exact match should have near-zero distance")

        try await ctx.cleanup()
    }

    // MARK: - Euclidean Distance Search Tests

    @Test("Euclidean distance search returns correct order")
    func testEuclideanDistanceSearch() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 3, metric: .euclidean)

        // Create points at known distances from origin
        let docs = [
            TestDocument(id: "close", title: "Close", embedding: [1.0, 0.0, 0.0]),
            TestDocument(id: "medium", title: "Medium", embedding: [2.0, 0.0, 0.0]),
            TestDocument(id: "far", title: "Far", embedding: [5.0, 0.0, 0.0])
        ]

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Search from origin
        let results = try await ctx.search(query: [0.0, 0.0, 0.0], k: 3)

        #expect(results.count == 3)

        let resultIds = results.compactMap { result -> String? in
            guard let id = result.primaryKey.first as? String else { return nil }
            return id
        }

        #expect(resultIds[0] == "close", "Closest point should be first")
        #expect(resultIds[1] == "medium", "Medium distance should be second")
        #expect(resultIds[2] == "far", "Farthest should be last")

        // Verify distances
        #expect(abs(results[0].distance - 1.0) < 0.01, "Distance to close should be 1.0")
        #expect(abs(results[1].distance - 2.0) < 0.01, "Distance to medium should be 2.0")
        #expect(abs(results[2].distance - 5.0) < 0.01, "Distance to far should be 5.0")

        try await ctx.cleanup()
    }

    // MARK: - Top-K Tests

    @Test("Top-K returns correct number of results")
    func testTopKReturnsCorrectCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        // Insert 10 documents
        let docs = (0..<10).map { i in
            TestDocument(id: "doc\(i)", title: "Doc \(i)", embedding: [Float(i), 0.0, 0.0, 0.0])
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Request k=3
        let results = try await ctx.search(query: [0.0, 0.0, 0.0, 0.0], k: 3)
        #expect(results.count == 3, "Should return exactly 3 results")

        // Request k larger than dataset
        let allResults = try await ctx.search(query: [0.0, 0.0, 0.0, 0.0], k: 100)
        #expect(allResults.count == 10, "Should return all 10 when k > dataset size")

        try await ctx.cleanup()
    }

    // MARK: - Error Cases

    @Test("Dimension mismatch throws error")
    func testDimensionMismatchThrowsError() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let doc = TestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        // Search with wrong dimension
        await #expect(throws: VectorIndexError.self) {
            _ = try await ctx.search(query: [1.0, 0.0], k: 1)  // 2D instead of 4D
        }

        try await ctx.cleanup()
    }

    @Test("Invalid k throws error")
    func testInvalidKThrowsError() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        await #expect(throws: VectorIndexError.self) {
            _ = try await ctx.search(query: [1.0, 0.0, 0.0, 0.0], k: 0)
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await ctx.search(query: [1.0, 0.0, 0.0, 0.0], k: -1)
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem stores vector")
    func testScanItemStoresVector() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let docs = [
            TestDocument(id: "doc1", title: "First", embedding: [1.0, 0.0, 0.0, 0.0]),
            TestDocument(id: "doc2", title: "Second", embedding: [0.0, 1.0, 0.0, 0.0])
        ]

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.scanItem(
                    doc,
                    id: Tuple(doc.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 vector entries after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Empty Index Tests

    @Test("Search on empty index returns empty results")
    func testSearchEmptyIndexReturnsEmpty() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(dimensions: 4)

        let results = try await ctx.search(query: [1.0, 0.0, 0.0, 0.0], k: 10)
        #expect(results.isEmpty, "Search on empty index should return empty results")

        try await ctx.cleanup()
    }
}

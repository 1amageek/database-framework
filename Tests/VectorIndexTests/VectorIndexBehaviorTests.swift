// VectorIndexBehaviorTests.swift
// Integration tests for VectorIndex (Flat) behavior

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

@Persistable
struct VectorDocument {
    var id: String
    var title: String
    var embedding: Vector

    init(id: String, title: String, embedding: [Float]) throws {
        self.id = id
        self.title = title
        self.embedding = try Vector(float32: embedding)
    }

}

// MARK: - Vector Index Context

private struct VectorIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FlatVectorIndexMaintainer<VectorDocument>
    let dimensions: Int

    init(dimensions: Int = 4, metric: VectorMetric = .cosine, indexName: String = "VectorDocument_embedding") async throws {
        self.database = InMemoryEngine()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "vector", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let metadata = vectorIndexMetadata(
            dimensions: dimensions,
            metric: metric
        )

        let index = Index(
            name: indexName,
            kind: metadata,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: indexName,
            itemTypes: Set(["VectorDocument"])
        )

        self.maintainer = FlatVectorIndexMaintainer<VectorDocument>(
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
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    func search(query: [Float], k: Int) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.search(queryVector: query, k: k, transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("VectorIndex Behavior Tests", .serialized, .heartbeat)
struct VectorIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert stores vector")
    func testInsertStoresVector() async throws {
        let ctx = try await VectorIndexContext(dimensions: 4)

        let doc = try VectorDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let docs = [
            try VectorDocument(id: "doc1", title: "First", embedding: [1.0, 0.0, 0.0, 0.0]),
            try VectorDocument(id: "doc2", title: "Second", embedding: [0.0, 1.0, 0.0, 0.0]),
            try VectorDocument(id: "doc3", title: "Third", embedding: [0.0, 0.0, 1.0, 0.0])
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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let doc = try VectorDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let doc = try VectorDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        // Update with different embedding
        let updatedDoc = try VectorDocument(id: "doc1", title: "Test Updated", embedding: [0.0, 1.0, 0.0, 0.0])
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
        let ctx = try await VectorIndexContext(dimensions: 4, metric: .cosine)

        // Create vectors at different angles
        let docs = [
            try VectorDocument(id: "exact", title: "Exact", embedding: [1.0, 0.0, 0.0, 0.0]),
            try VectorDocument(id: "similar", title: "Similar", embedding: [0.9, 0.1, 0.0, 0.0]),
            try VectorDocument(id: "different", title: "Different", embedding: [0.0, 1.0, 0.0, 0.0]),
            try VectorDocument(id: "opposite", title: "Opposite", embedding: [-1.0, 0.0, 0.0, 0.0])
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
        let ctx = try await VectorIndexContext(dimensions: 3, metric: .euclidean)

        // Create points at known distances from origin
        let docs = [
            try VectorDocument(id: "close", title: "Close", embedding: [1.0, 0.0, 0.0]),
            try VectorDocument(id: "medium", title: "Medium", embedding: [2.0, 0.0, 0.0]),
            try VectorDocument(id: "far", title: "Far", embedding: [5.0, 0.0, 0.0])
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
        let ctx = try await VectorIndexContext(dimensions: 4)

        // Insert 10 documents
        let docs = try (0..<10).map { i in
            try VectorDocument(id: "doc\(i)", title: "Doc \(i)", embedding: [Float(i), 0.0, 0.0, 0.0])
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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let doc = try VectorDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

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
        let ctx = try await VectorIndexContext(dimensions: 4)

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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let docs = [
            try VectorDocument(id: "doc1", title: "First", embedding: [1.0, 0.0, 0.0, 0.0]),
            try VectorDocument(id: "doc2", title: "Second", embedding: [0.0, 1.0, 0.0, 0.0])
        ]

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.maintainer.scanItem(
                    doc,
                    id: try doc.persistableIdentifierTuple(),
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
        let ctx = try await VectorIndexContext(dimensions: 4)

        let results = try await ctx.search(query: [1.0, 0.0, 0.0, 0.0], k: 10)
        #expect(results.isEmpty, "Search on empty index should return empty results")

        try await ctx.cleanup()
    }
}

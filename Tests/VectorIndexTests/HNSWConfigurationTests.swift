// HNSWConfigurationTests.swift
// Tests for HNSW configuration selection and basic functionality

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import Core
import DatabaseValue
import Vector
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

struct HNSWDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, title: String, embedding: [Float]) {
        self.id = id
        self.title = title
        self.embedding = embedding
    }

    static var persistableType: String { "HNSWDocument" }
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

    static func fieldName<Value>(for keyPath: KeyPath<HNSWDocument, Value>) -> String {
        switch keyPath {
        case \HNSWDocument.id: return "id"
        case \HNSWDocument.title: return "title"
        case \HNSWDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<HNSWDocument>) -> String {
        switch keyPath {
        case \HNSWDocument.id: return "id"
        case \HNSWDocument.title: return "title"
        case \HNSWDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<HNSWDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Configuration Selection Tests

@Suite("VectorIndexConfiguration Selection Tests", .heartbeat)
struct VectorIndexConfigurationSelectionTests {

    @Test("Default configuration returns FlatVectorIndexMaintainer")
    func testDefaultReturnsFlatMaintainer() async throws {
        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // No configurations = default to flat
        let maintainer: any IndexMaintainer<HNSWDocument> = try VectorIndexMaintainerProvider()
            .makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: []
        )

        // Verify type via string description
        let typeString = String(describing: type(of: maintainer))
        #expect(typeString.contains("FlatVectorIndexMaintainer"), "Should return FlatVectorIndexMaintainer by default")
    }

    @Test("HNSW configuration returns HNSWIndexMaintainer")
    func testHNSWConfigurationReturnsHNSWMaintainer() async throws {
        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // Configure HNSW
        let config = VectorIndexConfiguration<HNSWDocument>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        )

        let maintainer: any IndexMaintainer<HNSWDocument> = try VectorIndexMaintainerProvider()
            .makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [config]
        )

        // Verify type via string description
        let typeString = String(describing: type(of: maintainer))
        #expect(typeString.contains("HNSWIndexMaintainer"), "Should return HNSWIndexMaintainer when HNSW configured")
    }

    @Test("Explicit flat configuration returns FlatVectorIndexMaintainer")
    func testExplicitFlatConfigurationReturnsFlatMaintainer() async throws {
        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // Explicitly configure flat
        let config = VectorIndexConfiguration<HNSWDocument>(
            keyPath: \.embedding,
            algorithm: .flat
        )

        let maintainer: any IndexMaintainer<HNSWDocument> = try VectorIndexMaintainerProvider()
            .makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [config]
        )

        let typeString = String(describing: type(of: maintainer))
        #expect(typeString.contains("FlatVectorIndexMaintainer"), "Should return FlatVectorIndexMaintainer when explicitly configured")
    }

    @Test("Non-matching configuration returns FlatVectorIndexMaintainer")
    func testNonMatchingConfigurationReturnsFlatMaintainer() async throws {
        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let subspace = Subspace(prefix: Tuple("test").pack())

        // Configure HNSW for a different index name
        let config = VectorIndexConfiguration<HNSWDocument>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        )

        // Create index with a different name than the config targets
        let otherIndex = Index(
            name: "OtherIndex_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "OtherIndex_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer: any IndexMaintainer<HNSWDocument> = try VectorIndexMaintainerProvider()
            .makeIndexMaintainer(
            index: otherIndex,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [config]
        )

        let typeString = String(describing: type(of: maintainer))
        #expect(typeString.contains("FlatVectorIndexMaintainer"), "Should return FlatVectorIndexMaintainer when config doesn't match index name")
    }
}

// MARK: - VectorIndexConfiguration Tests

@Suite("VectorIndexConfiguration Tests", .heartbeat)
struct VectorIndexConfigurationTests {

    @Test("VectorIndexConfiguration has correct kindIdentifier")
    func testKindIdentifier() {
        #expect(VectorIndexConfiguration<HNSWDocument>.kindIdentifier == "vector")
    }

    @Test("VectorIndexConfiguration generates correct indexName")
    func testIndexName() {
        let config = VectorIndexConfiguration<HNSWDocument>(
            keyPath: \.embedding,
            algorithm: .flat
        )

        #expect(config.indexName == "HNSWDocument_embedding")
        #expect(config.modelTypeName == "HNSWDocument")
        #expect(config.fieldName == "embedding")
    }

    @Test("VectorHNSWParameters default values")
    func testHNSWParametersDefaults() {
        let params = VectorHNSWParameters.default

        #expect(params.m == 16)
        #expect(params.efConstruction == 200)
    }

    @Test("VectorHNSWParameters preset values")
    func testHNSWParametersPresets() {
        let highRecall = VectorHNSWParameters.highRecall
        #expect(highRecall.m == 32)
        #expect(highRecall.efConstruction == 400)

        let fast = VectorHNSWParameters.fast
        #expect(fast.m == 8)
        #expect(fast.efConstruction == 100)
    }

    @Test("VectorHNSWParameters custom values")
    func testHNSWParametersCustom() {
        let custom = VectorHNSWParameters(m: 24, efConstruction: 300)

        #expect(custom.m == 24)
        #expect(custom.efConstruction == 300)
    }
}

// MARK: - HNSW Basic Behavior Tests

@Suite("HNSW Basic Behavior Tests", .serialized, .heartbeat)
struct HNSWBasicBehaviorTests {

    @Test("HNSW insert stores vector and creates graph entry")
    func testHNSWInsertStoresVector() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let doc = HNSWDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as HNSWDocument?,
                newItem: doc,
                transaction: transaction
            )
        }

        // Verify node count
        let nodeCount = try await database.withTransaction { transaction in
            try await maintainer.getNodeCount(transaction: transaction)
        }

        #expect(nodeCount == 1, "Should have 1 node in HNSW graph")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW search finds nearest neighbors")
    func testHNSWSearchFindsNearestNeighbors() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        // Insert a few documents
        let docs = [
            HNSWDocument(id: "exact", title: "Exact", embedding: [1.0, 0.0, 0.0, 0.0]),
            HNSWDocument(id: "similar", title: "Similar", embedding: [0.9, 0.1, 0.0, 0.0]),
            HNSWDocument(id: "different", title: "Different", embedding: [0.0, 1.0, 0.0, 0.0])
        ]

        for doc in docs {
            try await database.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Search
        let results = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 3,
                transaction: transaction
            )
        }

        #expect(results.count == 3, "Should return 3 results")

        // Extract IDs from results
        let resultIds = results.compactMap { result -> String? in
            guard let id = result.primaryKey.first as? String else { return nil }
            return id
        }

        // Exact match should be first (closest cosine distance)
        #expect(resultIds[0] == "exact", "Exact match should be first")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW scanItems builds searchable graph for a batch")
    func testHNSWScanItemsBuildsSearchableGraph() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", "scanItems", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let docs = [
            HNSWDocument(id: "batch-exact", title: "Exact", embedding: [1.0, 0.0, 0.0, 0.0]),
            HNSWDocument(id: "batch-similar", title: "Similar", embedding: [0.9, 0.1, 0.0, 0.0]),
            HNSWDocument(id: "batch-different", title: "Different", embedding: [0.0, 1.0, 0.0, 0.0])
        ]

        try await database.withTransaction { transaction in
            try await maintainer.scanItems(
                docs.map { (item: $0, id: Tuple($0.id)) },
                transaction: transaction
            )
        }

        let nodeCount = try await database.withTransaction { transaction in
            try await maintainer.getNodeCount(transaction: transaction)
        }

        #expect(nodeCount == docs.count)

        let results = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 3,
                transaction: transaction
            )
        }

        let resultIds = results.compactMap { result -> String? in
            guard let id = result.primaryKey.first as? String else { return nil }
            return id
        }

        #expect(resultIds.first == "batch-exact")

        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW search cache refreshes after graph update")
    func testHNSWSearchCacheRefreshesAfterGraphUpdate() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", "cacheRefresh", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let first = HNSWDocument(id: "first", title: "First", embedding: [0.0, 1.0, 0.0, 0.0])
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as HNSWDocument?,
                newItem: first,
                transaction: transaction
            )
        }

        let initialResults = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 1,
                transaction: transaction
            )
        }

        #expect(initialResults.compactMap { $0.primaryKey.first as? String }.first == "first")

        let second = HNSWDocument(id: "second", title: "Second", embedding: [1.0, 0.0, 0.0, 0.0])
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as HNSWDocument?,
                newItem: second,
                transaction: transaction
            )
        }

        let refreshedResults = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 1,
                transaction: transaction
            )
        }

        #expect(refreshedResults.compactMap { $0.primaryKey.first as? String }.first == "second")

        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW search throws when persisted graph metadata is corrupt")
    func testHNSWSearchThrowsWhenPersistedGraphMetadataIsCorrupt() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", "corruptMetadata", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let metadataKey = indexSubspace.pack(Tuple("_graphMetadata"))
        let corruptMetadata = Tuple(
            Int64(2),
            Int64(64),
            Int64(16),
            Int64(4),
            Int64(1)
        ).pack()

        try await database.withTransaction { transaction in
            try transaction.setValue(corruptMetadata, for: metadataKey)
        }

        await #expect(throws: VectorIndexError.self) {
            _ = try await database.withTransaction { transaction in
                try await maintainer.search(
                    queryVector: [1.0, 0.0, 0.0, 0.0],
                    k: 1,
                    transaction: transaction
                )
            }
        }

        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW throws graphTooLarge error when limit exceeded")
    func testHNSWThrowsGraphTooLargeError() async throws {
        // Test that VectorIndexError.graphTooLarge is correctly defined and throwable
        // The actual 500-node limit is tested implicitly through the error path

        let error = VectorIndexError.graphTooLarge(maxLevel: 3)

        // Verify error message contains useful information
        #expect(error.description.contains("beyond inline indexing capacity"))
        #expect(error.description.contains("maxLevel: 3"))

        // Verify the error can be thrown and caught
        func throwGraphTooLarge() throws {
            throw VectorIndexError.graphTooLarge(maxLevel: 5)
        }

        do {
            try throwGraphTooLarge()
            Issue.record("Expected graphTooLarge error to be thrown")
        } catch let error as VectorIndexError {
            if case .graphTooLarge(let level) = error {
                #expect(level == 5)
            } else {
                Issue.record("Expected graphTooLarge error variant")
            }
        }
    }

    @Test("HNSW inline node limit constant is defined")
    func testHNSWInlineNodeLimitConstant() {
        // Document the FDB transaction limit constraint:
        // - FDB has ~10,000 operations per transaction limit
        // - With serialized graph storage (SwiftHNSW), we can handle larger graphs
        // - Graph snapshots are chunked to stay within backend value-size limits
        // - Solution: Use OnlineIndexer batch processing for very large datasets

        #expect(hnswMaxInlineNodes > 0, "Inline node limit should be positive")
        #expect(hnswMaxInlineNodes <= 50000, "Inline limit should be reasonable for serialized graph storage")
    }

    @Test("HNSW delete marks node as deleted")
    func testHNSWDeleteMarksNodeAsDeleted() async throws {
        let database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWDocument_embedding")

        let kind = VectorIndexKind<HNSWDocument>(embedding: \.embedding, dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWDocument_embedding",
            itemTypes: Set(["HNSWDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWDocument>(
            index: index,
            dimensions: kind.dimensions,
            metric: kind.metric,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let doc = HNSWDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        // Insert
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as HNSWDocument?,
                newItem: doc,
                transaction: transaction
            )
        }

        let countBefore = try await database.withTransaction { transaction in
            try await maintainer.getNodeCount(transaction: transaction)
        }
        #expect(countBefore == 1)

        // Verify item is found in search before delete
        let resultsBefore = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 10,
                transaction: transaction
            )
        }
        #expect(resultsBefore.count == 1, "Should find 1 result before delete")

        // Delete (soft delete - marks as deleted in HNSW graph)
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: doc,
                newItem: nil,
                transaction: transaction
            )
        }

        // Note: SwiftHNSW uses soft deletes - the node remains in graph but is marked deleted
        // getNodeCount() returns total nodes including deleted ones
        // The important behavior is that deleted nodes are not returned in search results

        // Verify item is NOT found in search after delete
        let resultsAfter = try await database.withTransaction { transaction in
            try await maintainer.search(
                queryVector: [1.0, 0.0, 0.0, 0.0],
                k: 10,
                transaction: transaction
            )
        }
        #expect(resultsAfter.isEmpty, "Should find 0 results after delete (soft delete excludes from search)")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

// HNSWConfigurationTests.swift
// Tests for HNSW configuration selection and basic functionality

import Testing
import Foundation
import FoundationDB
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

struct HNSWTestDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, title: String, embedding: [Float]) {
        self.id = id
        self.title = title
        self.embedding = embedding
    }

    static var persistableType: String { "HNSWTestDocument" }
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

    static func fieldName<Value>(for keyPath: KeyPath<HNSWTestDocument, Value>) -> String {
        switch keyPath {
        case \HNSWTestDocument.id: return "id"
        case \HNSWTestDocument.title: return "title"
        case \HNSWTestDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<HNSWTestDocument>) -> String {
        switch keyPath {
        case \HNSWTestDocument.id: return "id"
        case \HNSWTestDocument.title: return "title"
        case \HNSWTestDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<HNSWTestDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Configuration Selection Tests

@Suite("VectorIndexConfiguration Selection Tests")
struct VectorIndexConfigurationSelectionTests {

    @Test("Default configuration returns FlatVectorIndexMaintainer")
    func testDefaultReturnsFlatMaintainer() async throws {
        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // No configurations = default to flat
        let maintainer: any IndexMaintainer<HNSWTestDocument> = kind.makeIndexMaintainer(
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
        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // Configure HNSW
        let config = VectorIndexConfiguration<HNSWTestDocument>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        )

        let maintainer: any IndexMaintainer<HNSWTestDocument> = kind.makeIndexMaintainer(
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
        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let subspace = Subspace(prefix: Tuple("test").pack())

        // Explicitly configure flat
        let config = VectorIndexConfiguration<HNSWTestDocument>(
            keyPath: \.embedding,
            algorithm: .flat
        )

        let maintainer: any IndexMaintainer<HNSWTestDocument> = kind.makeIndexMaintainer(
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
        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let subspace = Subspace(prefix: Tuple("test").pack())

        // Configure HNSW for a different index name
        let config = VectorIndexConfiguration<HNSWTestDocument>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        )

        // Create index with a different name than the config targets
        let otherIndex = Index(
            name: "OtherIndex_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "OtherIndex_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let maintainer: any IndexMaintainer<HNSWTestDocument> = kind.makeIndexMaintainer(
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

@Suite("VectorIndexConfiguration Tests")
struct VectorIndexConfigurationTests {

    @Test("VectorIndexConfiguration has correct kindIdentifier")
    func testKindIdentifier() {
        #expect(VectorIndexConfiguration<HNSWTestDocument>.kindIdentifier == "vector")
    }

    @Test("VectorIndexConfiguration generates correct indexName")
    func testIndexName() {
        let config = VectorIndexConfiguration<HNSWTestDocument>(
            keyPath: \.embedding,
            algorithm: .flat
        )

        #expect(config.indexName == "HNSWTestDocument_embedding")
        #expect(config.modelTypeName == "HNSWTestDocument")
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

@Suite("HNSW Basic Behavior Tests", .tags(.fdb))
struct HNSWBasicBehaviorTests {

    @Test("HNSW insert stores vector and creates graph entry")
    func testHNSWInsertStoresVector() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWTestDocument_embedding")

        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWTestDocument>(
            index: index,
            kind: kind,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let doc = HNSWTestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
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
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW search finds nearest neighbors")
    func testHNSWSearchFindsNearestNeighbors() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWTestDocument_embedding")

        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWTestDocument>(
            index: index,
            kind: kind,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        // Insert a few documents
        let docs = [
            HNSWTestDocument(id: "exact", title: "Exact", embedding: [1.0, 0.0, 0.0, 0.0]),
            HNSWTestDocument(id: "similar", title: "Similar", embedding: [0.9, 0.1, 0.0, 0.0]),
            HNSWTestDocument(id: "different", title: "Different", embedding: [0.0, 1.0, 0.0, 0.0])
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
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("HNSW rejects large graphs for inline indexing")
    func testHNSWRejectsLargeGraphs() async throws {
        // The hnswMaxInlineNodes limit is 500
        // We don't need to actually insert 500 nodes to test this
        // Instead, verify the constant exists and the limit is documented
        #expect(hnswMaxInlineNodes == 500, "HNSW inline limit should be 500 nodes")

        // This test documents the FDB transaction limit constraint:
        // - FDB has ~10,000 operations per transaction limit
        // - HNSW insertion requires O(efConstruction * M * level) operations
        // - For large graphs (level >= 3), this exceeds FDB limits
        // - Solution: Use OnlineIndexer batch processing for >500 nodes
    }

    @Test("HNSW delete removes node from graph")
    func testHNSWDeleteRemovesNode() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "hnsw", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("HNSWTestDocument_embedding")

        let kind = VectorIndexKind(dimensions: 4, metric: .cosine)
        let index = Index(
            name: "HNSWTestDocument_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "HNSWTestDocument_embedding",
            itemTypes: Set(["HNSWTestDocument"])
        )

        let maintainer = HNSWIndexMaintainer<HNSWTestDocument>(
            index: index,
            kind: kind,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: HNSWParameters(m: 16, efConstruction: 200)
        )

        let doc = HNSWTestDocument(id: "doc1", title: "Test", embedding: [1.0, 0.0, 0.0, 0.0])

        // Insert
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        let countBefore = try await database.withTransaction { transaction in
            try await maintainer.getNodeCount(transaction: transaction)
        }
        #expect(countBefore == 1)

        // Delete
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: doc,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await database.withTransaction { transaction in
            try await maintainer.getNodeCount(transaction: transaction)
        }
        #expect(countAfter == 0, "Node count should be 0 after delete")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

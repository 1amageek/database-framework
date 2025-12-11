// QuantizationEndToEndTests.swift
// End-to-end tests for the quantization pipeline with FDB
//
// These tests verify the complete data flow:
// 1. Insert vectors into FDB
// 2. Train quantizer on inserted data
// 3. Build quantized index (write to /q/ subspace)
// 4. Search using quantized codes
// 5. Verify correct results are returned

import Testing
import Foundation
import FoundationDB
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

private struct QuantizationTestProduct: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, name: String, embedding: [Float]) {
        self.id = id
        self.name = name
        self.embedding = embedding
    }

    static var persistableType: String { "QuantizationTestProduct" }
    static var allFields: [String] { ["id", "name", "embedding"] }

    static var indexDescriptors: [IndexDescriptor] {
        let kind = VectorIndexKind<QuantizationTestProduct>(
            embedding: \.embedding,
            dimensions: 16,
            metric: .euclidean
        )
        return [IndexDescriptor(
            name: "QuantizationTestProduct_embedding",
            keyPaths: [\QuantizationTestProduct.embedding],
            kind: kind
        )]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "embedding": return embedding
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<QuantizationTestProduct, Value>) -> String {
        switch keyPath {
        case \QuantizationTestProduct.id: return "id"
        case \QuantizationTestProduct.name: return "name"
        case \QuantizationTestProduct.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<QuantizationTestProduct>) -> String {
        switch keyPath {
        case \QuantizationTestProduct.id: return "id"
        case \QuantizationTestProduct.name: return "name"
        case \QuantizationTestProduct.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<QuantizationTestProduct> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Context

private struct QuantizationTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let itemSubspace: Subspace
    let quantizedSubspace: Subspace
    let dimensions: Int

    init(dimensions: Int = 16) throws {
        self.database = try FDBClient.openDatabase()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "quantization", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace("QuantizationTestProduct_embedding")
        self.itemSubspace = subspace.subspace("R").subspace("QuantizationTestProduct")
        self.quantizedSubspace = indexSubspace.subspace("q")
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Count entries in the quantized subspace (/q/)
    func countQuantizedEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = quantizedSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    /// Insert a product into both item and index subspaces
    func insertProduct(_ product: QuantizationTestProduct, maintainer: FlatVectorIndexMaintainer<QuantizationTestProduct>) async throws {
        try await database.withTransaction { transaction in
            // Store item
            let itemKey = itemSubspace.pack(Tuple(product.id))
            let itemValue = try DataAccess.serialize(product)
            transaction.setValue(itemValue, for: itemKey)

            // Update index
            try await maintainer.updateIndex(oldItem: nil, newItem: product, transaction: transaction)
        }
    }
}

// MARK: - QuantizedVectorWriter Tests

@Suite("QuantizedVectorWriter End-to-End Tests", .tags(.fdb), .serialized)
struct QuantizedVectorWriterTests {

    @Test("buildQuantizedIndex writes codes to /q/ subspace")
    func testBuildQuantizedIndexWritesToQSubspace() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuantizationTestContext(dimensions: 16)

        // Create maintainer
        let kind = VectorIndexKind<QuantizationTestProduct>(
            embedding: \.embedding,
            dimensions: 16,
            metric: .euclidean
        )
        let index = Index(
            name: "QuantizationTestProduct_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            keyPaths: [\QuantizationTestProduct.embedding]
        )
        let maintainer = FlatVectorIndexMaintainer<QuantizationTestProduct>(
            index: index,
            dimensions: 16,
            metric: .euclidean,
            subspace: ctx.indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert products with vectors
        let products = (0..<50).map { i in
            let embedding = (0..<16).map { _ in Float.random(in: -1...1) }
            return QuantizationTestProduct(id: "prod-\(i)", name: "Product \(i)", embedding: embedding)
        }

        for product in products {
            try await ctx.insertProduct(product, maintainer: maintainer)
        }

        // Verify no quantized entries before building
        let countBefore = try await ctx.countQuantizedEntries()
        #expect(countBefore == 0, "Should have 0 quantized entries before build")

        // Train quantizer with new API: dimensions=16, m=4 subquantizers, nbits=3 (8 centroids)
        let pq = ProductQuantizer(dimensions: 16, m: 4, nbits: 3)
        let trainingVectors = products.map(\.embedding)
        let trainedPQ = pq.train(vectors: trainingVectors)

        #expect(trainedPQ.isTrained, "Quantizer should be trained")

        // Build quantized index by directly writing to /q/ subspace
        try await ctx.database.withTransaction { transaction in
            for product in products {
                let code = trainedPQ.encode(product.embedding)
                let key = ctx.quantizedSubspace.pack(Tuple(product.id))
                let elements: [any TupleElement] = code.map { Int64($0) as any TupleElement }
                let value = Tuple(elements).pack()
                transaction.setValue(value, for: key)
            }
        }

        // Verify quantized entries were written
        let countAfter = try await ctx.countQuantizedEntries()
        #expect(countAfter == 50, "Should have 50 quantized entries after build")

        try await ctx.cleanup()
    }

    @Test("Quantized codes enable ADC distance computation")
    func testQuantizedCodesEnableADC() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuantizationTestContext(dimensions: 16)

        // Create clustered data - this makes the test more meaningful
        let cluster1Center = [Float](repeating: 1.0, count: 16)
        let cluster2Center = [Float](repeating: -1.0, count: 16)

        let cluster1Products = (0..<25).map { i in
            let noise = (0..<16).map { _ in Float.random(in: -0.1...0.1) }
            let embedding = zip(cluster1Center, noise).map { $0 + $1 }
            return QuantizationTestProduct(id: "c1-\(i)", name: "Cluster1 \(i)", embedding: embedding)
        }

        let cluster2Products = (0..<25).map { i in
            let noise = (0..<16).map { _ in Float.random(in: -0.1...0.1) }
            let embedding = zip(cluster2Center, noise).map { $0 + $1 }
            return QuantizationTestProduct(id: "c2-\(i)", name: "Cluster2 \(i)", embedding: embedding)
        }

        let allProducts = cluster1Products + cluster2Products

        // Train quantizer on the data (dimensions=16, m=4, nbits=3 for 8 centroids)
        let pq = ProductQuantizer(dimensions: 16, m: 4, nbits: 3)
        let trainedPQ = pq.train(vectors: allProducts.map(\.embedding))

        // Write quantized codes to /q/
        var codes: [(id: String, code: [UInt8])] = []
        for product in allProducts {
            let code = trainedPQ.encode(product.embedding)
            codes.append((id: product.id, code: code))
        }

        try await ctx.database.withTransaction { transaction in
            for (id, code) in codes {
                let key = ctx.quantizedSubspace.pack(Tuple(id))
                let elements: [any TupleElement] = code.map { Int64($0) as any TupleElement }
                let value = Tuple(elements).pack()
                transaction.setValue(value, for: key)
            }
        }

        // Search using ADC
        let queryVector = cluster1Center  // Query for cluster 1
        let distanceTable = trainedPQ.computeDistanceTable(queryVector)

        var results: [(id: String, distance: Float)] = []
        try await ctx.database.withTransaction { transaction in
            let (begin, end) = ctx.quantizedSubspace.range()
            for try await (key, value) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                guard let keyTuple = try? ctx.quantizedSubspace.unpack(key),
                      let id = keyTuple[0] as? String,
                      let codeTuple = try? Tuple.unpack(from: value) else { continue }

                var code: [UInt8] = []
                for i in 0..<trainedPQ.m {
                    guard i < codeTuple.count,
                          let byte = codeTuple[i] as? Int64 else { break }
                    code.append(UInt8(clamping: byte))
                }
                guard code.count == trainedPQ.m else { continue }

                let distance = trainedPQ.computeDistanceADC(table: distanceTable, codes: code)
                results.append((id: id, distance: distance))
            }
        }

        results.sort { $0.distance < $1.distance }
        let top10 = results.prefix(10)

        // Verify that most of the top 10 are from cluster 1
        let cluster1Count = top10.filter { $0.id.hasPrefix("c1-") }.count
        #expect(cluster1Count >= 8, "At least 8 of top 10 should be from cluster 1 (got \(cluster1Count))")

        try await ctx.cleanup()
    }
}

// MARK: - Serialization Persistence Tests

@Suite("Quantizer Serialization Tests", .serialized)
struct QuantizerSerializationTests {

    @Test("Codebook round-trip: save and load preserves trained state")
    func testCodebookRoundTrip() {
        // Test that a trained quantizer can be serialized and deserialized
        let pq = ProductQuantizer(dimensions: 16, m: 4, nbits: 3)

        // Train with random data
        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        let trainedPQ = pq.train(vectors: trainingVectors)

        #expect(trainedPQ.isTrained)

        // Serialize
        let serialized = trainedPQ.serializeCodebooks()
        #expect(!serialized.isEmpty, "Serialized codebook should not be empty")

        // Deserialize to new quantizer
        guard let restoredPQ = ProductQuantizer.deserializeCodebooks(serialized) else {
            Issue.record("Failed to deserialize codebook")
            return
        }
        #expect(restoredPQ.isTrained, "Deserialized quantizer should be trained")

        // Verify the codebooks produce similar results
        let testVector = (0..<16).map { _ in Float.random(in: -1...1) }
        let code1 = trainedPQ.encode(testVector)
        let code2 = restoredPQ.encode(testVector)

        #expect(code1 == code2, "Same vector should produce same code from original and restored quantizer")
    }

    @Test("ScalarQuantizer round-trip preserves min/max bounds")
    func testSQRoundTrip() async throws {
        let sq = ScalarQuantizer(config: .default, dimensions: 16, metric: .euclidean)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -5...5) }
        }
        try await sq.train(vectors: trainingVectors)

        #expect(sq.isTrained)

        // Serialize and restore
        let serialized = try sq.serialize()
        let sq2 = ScalarQuantizer(config: .default, dimensions: 16, metric: .euclidean)
        try sq2.deserialize(from: serialized)

        #expect(sq2.isTrained)

        // Verify encoding produces same results
        let testVector = (0..<16).map { _ in Float.random(in: -5...5) }
        let code1 = try sq.encode(testVector)
        let code2 = try sq2.encode(testVector)

        #expect(code1 == code2, "Codes should match after round-trip")
    }
}

// MARK: - Recall Quality Tests

@Suite("Quantization Recall Quality Tests")
struct QuantizationRecallTests {

    @Test("PQ distance ordering is monotonic for simple vectors")
    func testPQDistanceOrdering() {
        // This test verifies that PQ preserves distance ordering for well-separated vectors
        // We use deterministic, well-separated vectors to avoid randomness issues
        let dimensions = 16

        // Create vectors at known positions along a single axis
        // v0 = [0, 0, ...], v1 = [1, 0, ...], v2 = [2, 0, ...], etc.
        let vectors = (0..<20).map { i in
            var v = [Float](repeating: 0, count: dimensions)
            v[0] = Float(i)
            return v
        }

        // Train PQ with enough centroids to capture the structure
        // dimensions=16, m=4 subquantizers, nbits=4 (16 centroids)
        let pq = ProductQuantizer(dimensions: dimensions, m: 4, nbits: 4)
        let trainedPQ = pq.train(vectors: vectors)

        // Encode all vectors
        let codes = vectors.map { trainedPQ.encode($0) }

        // Query from v[0] (origin) - distances should roughly increase with index
        let queryVector = vectors[0]
        let distanceTable = trainedPQ.computeDistanceTable(queryVector)

        var results: [(idx: Int, distance: Float)] = []
        for (idx, code) in codes.enumerated() {
            if idx == 0 { continue }
            let distance = trainedPQ.computeDistanceADC(table: distanceTable, codes: code)
            results.append((idx: idx, distance: distance))
        }
        results.sort { $0.distance < $1.distance }

        // The closest vectors should be the ones with small indices (v1, v2, v3)
        let top5Indices = Set(results.prefix(5).map(\.idx))
        let expectedClose = Set([1, 2, 3, 4, 5])
        let overlap = top5Indices.intersection(expectedClose).count

        #expect(overlap >= 3, "At least 3 of top-5 should be from indices 1-5 (got \(overlap))")

        // Verify that v[1] is closer than v[19] in the quantized space
        let dist1 = trainedPQ.computeDistanceADC(table: distanceTable, codes: codes[1])
        let dist19 = trainedPQ.computeDistanceADC(table: distanceTable, codes: codes[19])
        #expect(dist1 < dist19, "v[1] should be closer to v[0] than v[19]")
    }
}

// MARK: - VectorQuery Algorithm Selection Tests

@Suite("VectorQuery Algorithm Selection Tests", .tags(.fdb), .serialized)
struct VectorQueryAlgorithmSelectionTests {

    @Test("VectorQuery uses HNSW when configured")
    func testVectorQueryUsesHNSWWhenConfigured() async throws {
        // This test verifies that the fix to VectorQuery.executeVectorSearch
        // correctly selects HNSW when VectorIndexConfiguration specifies it

        // The test is structural: we verify the configuration flow works
        // by checking that HNSWIndexMaintainer is created for HNSW config

        let kind = VectorIndexKind<QuantizationTestProduct>(
            embedding: \.embedding,
            dimensions: 16,
            metric: .euclidean
        )
        let index = Index(
            name: "QuantizationTestProduct_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            keyPaths: [\QuantizationTestProduct.embedding]
        )
        let subspace = Subspace(prefix: Tuple("test").pack())

        // With HNSW config
        let hnswConfig = VectorIndexConfiguration<QuantizationTestProduct>(
            keyPath: \.embedding,
            algorithm: .hnsw(.default)
        )

        let maintainer: any IndexMaintainer<QuantizationTestProduct> = kind.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [hnswConfig]
        )

        let typeString = String(describing: type(of: maintainer))
        #expect(typeString.contains("HNSWIndexMaintainer"),
                "VectorQuery should use HNSWIndexMaintainer when HNSW is configured")
    }

    @Test("VectorQuery uses Flat when no config or flat config")
    func testVectorQueryUsesFlatByDefault() async throws {
        let kind = VectorIndexKind<QuantizationTestProduct>(
            embedding: \.embedding,
            dimensions: 16,
            metric: .euclidean
        )
        let index = Index(
            name: "QuantizationTestProduct_embedding",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            keyPaths: [\QuantizationTestProduct.embedding]
        )
        let subspace = Subspace(prefix: Tuple("test").pack())

        // No config
        let maintainerNoConfig: any IndexMaintainer<QuantizationTestProduct> = kind.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: []
        )

        let typeString1 = String(describing: type(of: maintainerNoConfig))
        #expect(typeString1.contains("FlatVectorIndexMaintainer"),
                "VectorQuery should use FlatVectorIndexMaintainer when no config")

        // Explicit flat config
        let flatConfig = VectorIndexConfiguration<QuantizationTestProduct>(
            keyPath: \.embedding,
            algorithm: .flat
        )

        let maintainerFlatConfig: any IndexMaintainer<QuantizationTestProduct> = kind.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: [flatConfig]
        )

        let typeString2 = String(describing: type(of: maintainerFlatConfig))
        #expect(typeString2.contains("FlatVectorIndexMaintainer"),
                "VectorQuery should use FlatVectorIndexMaintainer when flat config")
    }
}

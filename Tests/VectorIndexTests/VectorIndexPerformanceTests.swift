// VectorIndexPerformanceTests.swift
// Performance benchmarks for VectorIndex

import Testing
import Foundation
import Core
import FoundationDB
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

struct BenchmarkDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, title: String, embedding: [Float]) {
        self.id = id
        self.title = title
        self.embedding = embedding
    }

    static var persistableType: String { "BenchmarkDocument" }
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

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkDocument, Value>) -> String {
        switch keyPath {
        case \BenchmarkDocument.id: return "id"
        case \BenchmarkDocument.title: return "title"
        case \BenchmarkDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkDocument>) -> String {
        switch keyPath {
        case \BenchmarkDocument.id: return "id"
        case \BenchmarkDocument.title: return "title"
        case \BenchmarkDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BenchmarkDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let flatMaintainer: FlatVectorIndexMaintainer<BenchmarkDocument>
    let dimensions: Int

    init(dimensions: Int = 128, metric: VectorMetric = .cosine, indexName: String = "BenchmarkDocument_embedding") throws {
        self.database = try FDBClient.openDatabase()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("benchmark", "vector", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let kind = VectorIndexKind<BenchmarkDocument>(
            embedding: \.embedding,
            dimensions: dimensions,
            metric: metric
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: indexName,
            itemTypes: Set(["BenchmarkDocument"])
        )

        self.flatMaintainer = FlatVectorIndexMaintainer<BenchmarkDocument>(
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

    func flatSearch(query: [Float], k: Int) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            try await flatMaintainer.search(queryVector: query, k: k, transaction: transaction)
        }
    }
}

// MARK: - Vector Generation

/// Generate a random unit vector
private func randomUnitVector(dimensions: Int) -> [Float] {
    var vector = (0..<dimensions).map { _ in Float.random(in: -1...1) }
    let norm = sqrt(vector.reduce(0) { $0 + $1 * $1 })
    if norm > 0 {
        vector = vector.map { $0 / norm }
    }
    return vector
}

/// Generate a vector similar to the given vector (small perturbation)
private func similarVector(to base: [Float], perturbation: Float = 0.1) -> [Float] {
    var vector = base.map { $0 + Float.random(in: -perturbation...perturbation) }
    let norm = sqrt(vector.reduce(0) { $0 + $1 * $1 })
    if norm > 0 {
        vector = vector.map { $0 / norm }
    }
    return vector
}

// MARK: - Performance Tests

@Suite("VectorIndex Performance Tests", .serialized)
struct VectorIndexPerformanceTests {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Flat Scan Performance

    @Test("Flat scan performance - 100 vectors, 128 dimensions")
    func testFlatScan100Vectors() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        // Setup: Insert 100 vectors
        let vectorCount = 100
        let docs = (0..<vectorCount).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Search k=10
        let queryVector = randomUnitVector(dimensions: 128)
        let searchCount = 20
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.flatSearch(query: queryVector, k: 10)
            #expect(results.count == 10)
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("VectorIndex Flat Scan (100 vectors, 128d):")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(searchCount) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion: should be under 100ms average
        #expect(avgMs < 100, "Flat scan should be under 100ms average for 100 vectors")

        try await ctx.cleanup()
    }

    @Test("Flat scan performance - 500 vectors, 128 dimensions")
    func testFlatScan500Vectors() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        // Setup: Insert 500 vectors
        let vectorCount = 500
        let docs = (0..<vectorCount).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Search k=10
        let queryVector = randomUnitVector(dimensions: 128)
        let searchCount = 10
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.flatSearch(query: queryVector, k: 10)
            #expect(results.count == 10)
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("VectorIndex Flat Scan (500 vectors, 128d):")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion: should be under 500ms average
        #expect(avgMs < 500, "Flat scan should be under 500ms average for 500 vectors")

        try await ctx.cleanup()
    }

    @Test("Flat scan performance - varying k")
    func testFlatScanVaryingK() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        // Setup: Insert 200 vectors
        let vectorCount = 200
        let docs = (0..<vectorCount).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let queryVector = randomUnitVector(dimensions: 128)

        // Test different k values
        for k in [1, 10, 50, 100] {
            let searchCount = 5
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.flatSearch(query: queryVector, k: k)
                #expect(results.count == k)
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("VectorIndex Flat Scan k=\(k): \(String(format: "%.2f", avgMs))ms")
        }

        try await ctx.cleanup()
    }

    // MARK: - Dimension Scaling

    @Test("Flat scan performance - dimension scaling")
    func testFlatScanDimensionScaling() async throws {
        try await FDBTestSetup.shared.initialize()

        let vectorCount = 100

        for dimensions in [64, 128, 256, 384] {
            let ctx = try BenchmarkContext(dimensions: dimensions)

            let docs = (0..<vectorCount).map { i in
                BenchmarkDocument(
                    id: "\(uniqueID("doc"))-\(i)",
                    title: "Document \(i)",
                    embedding: randomUnitVector(dimensions: dimensions)
                )
            }

            try await ctx.database.withTransaction { transaction in
                for doc in docs {
                    try await ctx.flatMaintainer.updateIndex(
                        oldItem: nil,
                        newItem: doc,
                        transaction: transaction
                    )
                }
            }

            let queryVector = randomUnitVector(dimensions: dimensions)
            let searchCount = 10
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.flatSearch(query: queryVector, k: 10)
                #expect(results.count == 10)
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("VectorIndex Flat Scan (100 vectors, \(dimensions)d): \(String(format: "%.2f", avgMs))ms")

            try await ctx.cleanup()
        }
    }

    // MARK: - Bulk Insert Performance

    @Test("Bulk insert performance")
    func testBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        let batchSize = 100
        let docs = (0..<batchSize).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        let startTime = DispatchTime.now()

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let totalMs = Double(totalNs) / 1_000_000

        print("VectorIndex Bulk Insert (128d):")
        print("  - Vectors inserted: \(batchSize)")
        print("  - Total time: \(String(format: "%.2f", totalMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(batchSize) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion
        #expect(totalMs < 30000, "Bulk insert of \(batchSize) vectors should complete in under 30s")

        try await ctx.cleanup()
    }

    // MARK: - Distance Metric Comparison

    @Test("Distance metric comparison")
    func testDistanceMetricComparison() async throws {
        try await FDBTestSetup.shared.initialize()

        let vectorCount = 100
        let dimensions = 128
        let searchCount = 10

        for metric in [VectorMetric.cosine, VectorMetric.euclidean, VectorMetric.dotProduct] {
            let ctx = try BenchmarkContext(dimensions: dimensions, metric: metric)

            let docs = (0..<vectorCount).map { i in
                BenchmarkDocument(
                    id: "\(uniqueID("doc"))-\(i)",
                    title: "Document \(i)",
                    embedding: randomUnitVector(dimensions: dimensions)
                )
            }

            try await ctx.database.withTransaction { transaction in
                for doc in docs {
                    try await ctx.flatMaintainer.updateIndex(
                        oldItem: nil,
                        newItem: doc,
                        transaction: transaction
                    )
                }
            }

            let queryVector = randomUnitVector(dimensions: dimensions)
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.flatSearch(query: queryVector, k: 10)
                #expect(results.count == 10)
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("VectorIndex \(metric) metric: \(String(format: "%.2f", avgMs))ms avg")

            try await ctx.cleanup()
        }
    }

    // MARK: - Recall Quality

    @Test("Search recall quality - cosine similarity")
    func testSearchRecallQuality() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 64, metric: .cosine)

        // Create a base vector
        let baseVector = randomUnitVector(dimensions: 64)

        // Create documents with known similarity patterns
        var docs: [BenchmarkDocument] = []

        // 10 similar vectors (small perturbation)
        for i in 0..<10 {
            docs.append(BenchmarkDocument(
                id: "\(uniqueID("similar"))-\(i)",
                title: "Similar \(i)",
                embedding: similarVector(to: baseVector, perturbation: 0.1)
            ))
        }

        // 50 random vectors
        for i in 0..<50 {
            docs.append(BenchmarkDocument(
                id: "\(uniqueID("random"))-\(i)",
                title: "Random \(i)",
                embedding: randomUnitVector(dimensions: 64)
            ))
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Search for vectors similar to base
        let results = try await ctx.flatSearch(query: baseVector, k: 10)
        #expect(results.count == 10)

        // Count how many of the top 10 are from the "similar" group
        var similarCount = 0
        for result in results {
            if let id = result.primaryKey.first as? String, id.contains("similar") {
                similarCount += 1
            }
        }

        print("VectorIndex Recall Quality:")
        print("  - Similar vectors in top 10: \(similarCount)/10")
        print("  - Top result distance: \(String(format: "%.4f", results[0].distance))")

        // Expect most similar vectors to be in top 10 (at least 7)
        #expect(similarCount >= 7, "Expected at least 7 similar vectors in top 10")

        try await ctx.cleanup()
    }

    // MARK: - Update Performance

    @Test("Update performance - replace vector")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        // Setup: Insert initial vectors
        let vectorCount = 50
        var docs = (0..<vectorCount).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Update vectors
        let updateCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<updateCount {
            let oldDoc = docs[i]
            let newDoc = BenchmarkDocument(
                id: oldDoc.id,
                title: "Updated \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )

            try await ctx.database.withTransaction { transaction in
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: oldDoc,
                    newItem: newDoc,
                    transaction: transaction
                )
            }

            docs[i] = newDoc
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(updateCount) / 1_000_000

        print("VectorIndex Update Performance:")
        print("  - Total updates: \(updateCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 200, "Vector update should be under 200ms average")

        try await ctx.cleanup()
    }

    // MARK: - Delete Performance

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(dimensions: 128)

        // Setup: Insert vectors
        let vectorCount = 50
        let docs = (0..<vectorCount).map { i in
            BenchmarkDocument(
                id: "\(uniqueID("doc"))-\(i)",
                title: "Document \(i)",
                embedding: randomUnitVector(dimensions: 128)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for doc in docs {
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Delete vectors
        let deleteCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<deleteCount {
            try await ctx.database.withTransaction { transaction in
                try await ctx.flatMaintainer.updateIndex(
                    oldItem: docs[i],
                    newItem: nil,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(deleteCount) / 1_000_000

        print("VectorIndex Delete Performance:")
        print("  - Total deletes: \(deleteCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 100, "Vector delete should be under 100ms average")

        try await ctx.cleanup()
    }
}

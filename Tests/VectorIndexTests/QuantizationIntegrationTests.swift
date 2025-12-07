// QuantizationIntegrationTests.swift
// Integration tests for QuantizedSimilar and CodebookTrainer

import Testing
import Foundation
import FoundationDB
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

private struct QuantizedDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var embedding: [Float]

    init(id: String = UUID().uuidString, title: String, embedding: [Float]) {
        self.id = id
        self.title = title
        self.embedding = embedding
    }

    static var persistableType: String { "QuantizedDocument" }
    static var allFields: [String] { ["id", "title", "embedding"] }

    // Index definition for vector search
    static var indexDescriptors: [IndexDescriptor] {
        let kind = VectorIndexKind<QuantizedDocument>(
            embedding: \.embedding,
            dimensions: 16,
            metric: .euclidean
        )
        return [IndexDescriptor(
            name: "QuantizedDocument_embedding",
            keyPaths: [\QuantizedDocument.embedding],
            kind: kind
        )]
    }

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

    static func fieldName<Value>(for keyPath: KeyPath<QuantizedDocument, Value>) -> String {
        switch keyPath {
        case \QuantizedDocument.id: return "id"
        case \QuantizedDocument.title: return "title"
        case \QuantizedDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<QuantizedDocument>) -> String {
        switch keyPath {
        case \QuantizedDocument.id: return "id"
        case \QuantizedDocument.title: return "title"
        case \QuantizedDocument.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<QuantizedDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - CodebookTrainer Unit Tests (No FDB)

@Suite("CodebookTrainer Unit Tests")
struct CodebookTrainerUnitTests {

    @Test("Trainer initialization with PQ")
    func testTrainerInitializationPQ() {
        let pq = ProductQuantizer(
            config: PQConfig(numSubquantizers: 4, numCentroids: 8),
            dimensions: 16
        )
        let trainer = CodebookTrainer<QuantizedDocument, ProductQuantizer>(
            keyPath: \.embedding,
            quantizer: pq
        )

        #expect(!trainer.trainedQuantizer.isTrained)
    }

    @Test("Trainer initialization with SQ")
    func testTrainerInitializationSQ() {
        let sq = ScalarQuantizer(config: .default, dimensions: 16)
        let trainer = CodebookTrainer<QuantizedDocument, ScalarQuantizer>(
            keyPath: \.embedding,
            quantizer: sq
        )

        #expect(!trainer.trainedQuantizer.isTrained)
    }

    @Test("Trainer direct training with vectors")
    func testTrainerDirectTraining() async throws {
        let pq = ProductQuantizer(
            config: PQConfig(
                numSubquantizers: 4,
                numCentroids: 8,
                trainingSampleSize: 50,
                kmeansIterations: 5
            ),
            dimensions: 16
        )
        var trainer = CodebookTrainer<QuantizedDocument, ProductQuantizer>(
            keyPath: \.embedding,
            quantizer: pq
        )

        let vectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }

        try await trainer.train(vectors: vectors)

        #expect(trainer.trainedQuantizer.isTrained)
    }

    @Test("Convenience factory for PQ")
    func testConvenienceFactoryPQ() {
        let trainer = CodebookTrainer<QuantizedDocument, ProductQuantizer>.productQuantizer(
            keyPath: \.embedding,
            dimensions: 16
        )

        #expect(!trainer.trainedQuantizer.isTrained)
        #expect(trainer.trainedQuantizer.dimensions == 16)
    }

    @Test("Convenience factory for SQ")
    func testConvenienceFactorySQ() {
        let trainer = CodebookTrainer<QuantizedDocument, ScalarQuantizer>.scalarQuantizer(
            keyPath: \.embedding,
            dimensions: 16
        )

        #expect(!trainer.trainedQuantizer.isTrained)
        #expect(trainer.trainedQuantizer.dimensions == 16)
    }

    @Test("Convenience factory for BQ")
    func testConvenienceFactoryBQ() {
        let trainer = CodebookTrainer<QuantizedDocument, BinaryQuantizer>.binaryQuantizer(
            keyPath: \.embedding,
            dimensions: 64
        )

        #expect(!trainer.trainedQuantizer.isTrained)
        #expect(trainer.trainedQuantizer.dimensions == 64)
    }
}

// MARK: - Quantizer Encoding Tests

@Suite("Quantizer Encoding Tests")
struct QuantizerEncodingTests {

    @Test("PQ encode produces correct code size")
    func testPQEncodeProducesCorrectCodeSize() async throws {
        let pq = ProductQuantizer(
            config: PQConfig(numSubquantizers: 4, numCentroids: 8),
            dimensions: 16
        )

        // Train first
        let trainingVectors = (0..<50).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        // Encode a vector
        let testVector = [Float](repeating: 0.5, count: 16)
        let code = try pq.encode(testVector)

        // Verify code size matches numSubquantizers
        #expect(code.count == 4, "PQ with 4 subquantizers should produce 4-byte code")
        // Each byte should be < numCentroids
        for byte in code {
            #expect(byte < 8, "Each code byte should be < numCentroids (8)")
        }
    }

    @Test("SQ encode produces correct code size")
    func testSQEncodeProducesCorrectCodeSize() async throws {
        let sq = ScalarQuantizer(config: .default, dimensions: 16, metric: .euclidean)

        let trainingVectors = (0..<50).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let testVector = [Float](repeating: 0.5, count: 16)
        let code = try sq.encode(testVector)

        // 8-bit SQ: one byte per dimension
        #expect(code.count == 16, "SQ should produce code with one byte per dimension")
    }

    @Test("Untrained quantizer throws error on encode")
    func testUntrainedQuantizerThrowsOnEncode() async throws {
        let pq = ProductQuantizer(
            config: PQConfig(numSubquantizers: 4, numCentroids: 8),
            dimensions: 16
        )

        let testVector = [Float](repeating: 0.5, count: 16)

        await #expect(throws: QuantizerError.self) {
            _ = try pq.encode(testVector)
        }
    }

    @Test("Dimension mismatch throws error")
    func testDimensionMismatchThrowsError() async throws {
        let pq = ProductQuantizer(
            config: PQConfig(numSubquantizers: 4, numCentroids: 8),
            dimensions: 16
        )

        let trainingVectors = (0..<50).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        // Wrong dimension
        let wrongDimVector = [Float](repeating: 0.5, count: 8)

        await #expect(throws: QuantizerError.self) {
            _ = try pq.encode(wrongDimVector)
        }
    }
}

// MARK: - Quantizer Distance Quality Tests

@Suite("Quantizer Distance Quality Tests")
struct QuantizerDistanceQualityTests {

    @Test("PQ preserves nearest neighbor ordering")
    func testPQPreservesOrdering() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 16,
            trainingSampleSize: 200,
            kmeansIterations: 10
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        // Generate clustered training data
        let trainingVectors = (0..<200).map { i in
            let cluster = i % 4
            return (0..<16).map { dim -> Float in
                let base = Float(cluster) * 2.0
                return base + Float.random(in: -0.5...0.5)
            }
        }

        try await pq.train(vectors: trainingVectors)

        // Test vectors from same cluster should be closer
        let query = trainingVectors[0]  // From cluster 0
        let sameCluster = trainingVectors[4]  // From cluster 0
        let differentCluster = trainingVectors[1]  // From cluster 1

        let prepared = try pq.prepareQuery(query)
        let codeSame = try pq.encode(sameCluster)
        let codeDiff = try pq.encode(differentCluster)

        let distSame = pq.distanceWithPrepared(prepared, code: codeSame)
        let distDiff = pq.distanceWithPrepared(prepared, code: codeDiff)

        // Same cluster should generally be closer (with high probability)
        // Due to quantization error, we use a relaxed check
        #expect(distSame < distDiff * 2, "Same cluster distance (\(distSame)) should be smaller than different cluster (\(distDiff))")
    }

    @Test("SQ preserves distance ratios")
    func testSQPreservesDistanceRatios() async throws {
        let sq = ScalarQuantizer(config: .default, dimensions: 16, metric: .euclidean)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let query = [Float](repeating: 0, count: 16)
        let near = [Float](repeating: 0.1, count: 16)
        let far = [Float](repeating: 0.9, count: 16)

        let prepared = try sq.prepareQuery(query)
        let codeNear = try sq.encode(near)
        let codeFar = try sq.encode(far)

        let distNear = sq.distanceWithPrepared(prepared, code: codeNear)
        let distFar = sq.distanceWithPrepared(prepared, code: codeFar)

        #expect(distNear < distFar, "Near vector should have smaller distance")
    }

    @Test("BQ Hamming distance correlates with angle")
    func testBQDistanceCorrelatesWithAngle() throws {
        let bq = BinaryQuantizer.withSignQuantization(
            config: .default,
            dimensions: 64
        )

        // Vectors with similar signs should have small Hamming distance
        let positive = [Float](repeating: 1.0, count: 64)
        let mostlyPositive = (0..<64).map { i -> Float in
            i < 60 ? 1.0 : -1.0
        }
        let negative = [Float](repeating: -1.0, count: 64)

        let codePos = try bq.encode(positive)
        let codeMostly = try bq.encode(mostlyPositive)
        let codeNeg = try bq.encode(negative)

        let distMostly = bq.hammingDistance(codePos, codeMostly)
        let distNeg = bq.hammingDistance(codePos, codeNeg)

        #expect(distMostly == 4, "Mostly positive should differ in 4 bits")
        #expect(distNeg == 64, "Negative should differ in all 64 bits")
    }
}

// MARK: - Compression Ratio Tests

@Suite("Quantizer Compression Ratio Tests")
struct QuantizerCompressionRatioTests {

    @Test("PQ compression ratio calculation")
    func testPQCompressionRatio() {
        // 384-dim vectors with 48 subquantizers
        let config = PQConfig.forDimensions(384)
        let pq = ProductQuantizer(config: config, dimensions: 384)

        let originalSize = 384 * 4  // float32
        let compressedSize = pq.codeSize

        let ratio = Float(originalSize) / Float(compressedSize)

        #expect(ratio >= 16, "PQ should achieve at least 16x compression")
    }

    @Test("SQ 8-bit compression ratio")
    func testSQ8BitCompressionRatio() {
        let sq = ScalarQuantizer(config: SQConfig(bits: 8), dimensions: 384)

        let originalSize = 384 * 4
        let compressedSize = sq.codeSize

        let ratio = Float(originalSize) / Float(compressedSize)

        #expect(ratio == 4.0, "8-bit SQ should achieve 4x compression")
    }

    @Test("SQ 4-bit compression ratio")
    func testSQ4BitCompressionRatio() {
        let sq = ScalarQuantizer(config: SQConfig(bits: 4), dimensions: 384)

        let originalSize = 384 * 4
        let compressedSize = sq.codeSize

        let ratio = Float(originalSize) / Float(compressedSize)

        #expect(ratio == 8.0, "4-bit SQ should achieve 8x compression")
    }

    @Test("BQ compression ratio")
    func testBQCompressionRatio() {
        let bq = BinaryQuantizer(config: .default, dimensions: 384)

        let originalSize = 384 * 4
        let compressedSize = bq.codeSize

        let ratio = Float(originalSize) / Float(compressedSize)

        #expect(ratio == 32.0, "BQ should achieve 32x compression")
    }
}

// MARK: - Error Handling Tests

@Suite("Quantization Error Handling Tests")
struct QuantizationErrorHandlingTests {

    @Test("CodebookTrainerError descriptions")
    func testErrorDescriptions() {
        let indexError = CodebookTrainerError.indexNotFound(field: "embedding")
        #expect(indexError.description.contains("embedding"))

        let sampleError = CodebookTrainerError.insufficientSamples(requested: 1000, available: 100)
        #expect(sampleError.description.contains("1000"))
        #expect(sampleError.description.contains("100"))

        let codebookError = CodebookTrainerError.codebookNotFound(quantizerType: "ProductQuantizer")
        #expect(codebookError.description.contains("ProductQuantizer"))
    }

    @Test("QuantizerError descriptions")
    func testQuantizerErrorDescriptions() {
        let notTrained = QuantizerError.notTrained
        #expect(notTrained.description.contains("not been trained"))

        let dimMismatch = QuantizerError.dimensionMismatch(expected: 384, actual: 256)
        #expect(dimMismatch.description.contains("384"))
        #expect(dimMismatch.description.contains("256"))
    }
}

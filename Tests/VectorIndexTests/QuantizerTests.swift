// QuantizerTests.swift
// Unit tests for Vector Quantization implementations
//
// Tests ProductQuantizer, ScalarQuantizer, and BinaryQuantizer
// without requiring FoundationDB.

import Testing
import Foundation
@testable import VectorIndex

// MARK: - ProductQuantizer Tests

@Suite("ProductQuantizer Tests")
struct ProductQuantizerTests {

    // MARK: - Initialization

    @Test("PQ initialization with valid parameters")
    func testInitialization() {
        let config = PQConfig(numSubquantizers: 4, numCentroids: 16)
        let pq = ProductQuantizer(config: config, dimensions: 32)

        #expect(pq.dimensions == 32)
        #expect(pq.numSubquantizers == 4)
        #expect(pq.numCentroids == 16)
        #expect(pq.codeSize == 4)
        #expect(!pq.isTrained)
    }

    // MARK: - Training

    @Test("PQ training produces codebook")
    func testTraining() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 8,
            trainingSampleSize: 100,
            kmeansIterations: 5
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }

        try await pq.train(vectors: trainingVectors)

        #expect(pq.isTrained)
    }

    @Test("PQ training fails with empty vectors")
    func testTrainingEmptyVectors() async {
        let config = PQConfig(numSubquantizers: 4, numCentroids: 8)
        let pq = ProductQuantizer(config: config, dimensions: 16)

        await #expect(throws: QuantizerError.self) {
            try await pq.train(vectors: [])
        }
    }

    @Test("PQ training fails with dimension mismatch")
    func testTrainingDimensionMismatch() async {
        let config = PQConfig(numSubquantizers: 4, numCentroids: 8)
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let wrongDimVectors = [[Float](repeating: 0, count: 8)]

        await #expect(throws: QuantizerError.self) {
            try await pq.train(vectors: wrongDimVectors)
        }
    }

    // MARK: - Encoding/Decoding

    @Test("PQ encode and decode round-trip")
    func testEncodeDecode() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 16,
            trainingSampleSize: 100,
            kmeansIterations: 10
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        let original = trainingVectors[0]
        let code = try pq.encode(original)
        let reconstructed = try pq.decode(code)

        #expect(code.count == 4)
        #expect(reconstructed.count == 16)

        var mse: Float = 0
        for i in 0..<16 {
            let diff = original[i] - reconstructed[i]
            mse += diff * diff
        }
        mse /= 16

        #expect(mse < 1.0, "MSE should be bounded: \(mse)")
    }

    @Test("PQ encode fails when not trained")
    func testEncodeNotTrained() {
        let config = PQConfig(numSubquantizers: 4, numCentroids: 8)
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let vector = [Float](repeating: 0, count: 16)

        #expect(throws: QuantizerError.self) {
            _ = try pq.encode(vector)
        }
    }

    // MARK: - Distance Computation

    @Test("PQ ADC distance computation")
    func testADCDistance() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 16,
            trainingSampleSize: 100,
            kmeansIterations: 10
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        let query = trainingVectors[0]
        let code = try pq.encode(trainingVectors[1])

        let prepared = try pq.prepareQuery(query)
        let distance = pq.distanceWithPrepared(prepared, code: code)

        #expect(distance >= 0, "Distance should be non-negative")
        #expect(distance.isFinite, "Distance should be finite")
    }

    @Test("PQ reconstruction error is bounded")
    func testReconstructionError() async throws {
        // Product Quantization approximates vectors using centroids.
        // The reconstruction error (distance between original and quantized)
        // depends on: number of subquantizers, centroids, and data distribution.
        //
        // For random data in [-1, 1]^16 with 4 subquantizers and 16 centroids:
        // - Each 4-dim subvector is approximated by its nearest centroid
        // - Expected error grows with subspace dimensionality
        // - Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search"
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 16,
            trainingSampleSize: 100,
            kmeansIterations: 10
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        let vector = trainingVectors[0]
        let code = try pq.encode(vector)
        let prepared = try pq.prepareQuery(vector)
        let distance = pq.distanceWithPrepared(prepared, code: code)

        // Reconstruction error bounds for this configuration:
        // - Max possible error: sqrt(16 * 4) = 8.0 (if every subvector is maximally wrong)
        // - Typical error: 0.5-2.0 for random data with this many centroids
        // - We use a generous bound to avoid flaky tests with random data
        #expect(distance >= 0, "Distance should be non-negative")
        #expect(distance < 3.0, "Reconstruction error should be bounded: \(distance)")
    }

    // MARK: - Serialization

    @Test("PQ serialize and deserialize")
    func testSerialization() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 8,
            trainingSampleSize: 100,
            kmeansIterations: 5
        )
        let pq1 = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq1.train(vectors: trainingVectors)

        let data = try pq1.serialize()

        let pq2 = ProductQuantizer(config: config, dimensions: 16)
        try pq2.deserialize(from: data)

        #expect(pq2.isTrained)

        let vector = trainingVectors[0]
        let code1 = try pq1.encode(vector)
        let code2 = try pq2.encode(vector)

        #expect(code1 == code2)
    }

    @Test("PQ deserialize fails with wrong dimensions")
    func testDeserializeWrongDimensions() async throws {
        let config = PQConfig(numSubquantizers: 4, numCentroids: 8)
        let pq1 = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq1.train(vectors: trainingVectors)

        let data = try pq1.serialize()

        let pq2 = ProductQuantizer(config: config, dimensions: 32)

        #expect(throws: QuantizerError.self) {
            try pq2.deserialize(from: data)
        }
    }

    // MARK: - k-means++ Edge Cases

    @Test("k-means++ handles identical vectors")
    func testKMeansPlusPlusIdenticalVectors() async throws {
        let config = PQConfig(
            numSubquantizers: 2,
            numCentroids: 16,
            trainingSampleSize: 50,
            kmeansIterations: 3
        )
        let pq = ProductQuantizer(config: config, dimensions: 8)

        let identicalVectors = (0..<50).map { _ in
            [Float](repeating: 1.0, count: 8)
        }

        try await pq.train(vectors: identicalVectors)
        #expect(pq.isTrained)

        let code = try pq.encode(identicalVectors[0])
        #expect(code.count == 2)
    }
}

// MARK: - ScalarQuantizer Tests

@Suite("ScalarQuantizer Tests")
struct ScalarQuantizerTests {

    // MARK: - Initialization

    @Test("SQ initialization")
    func testInitialization() {
        let config = SQConfig(bits: 8)
        let sq = ScalarQuantizer(config: config, dimensions: 128)

        #expect(sq.dimensions == 128)
        #expect(sq.codeSize == 128)
        #expect(!sq.isTrained)
    }

    @Test("SQ 4-bit mode code size")
    func testFourBitCodeSize() {
        let config = SQConfig(bits: 4)
        let sq = ScalarQuantizer(config: config, dimensions: 128)

        #expect(sq.codeSize == 64)
    }

    // MARK: - Training

    @Test("SQ training learns min/max")
    func testTraining() async throws {
        let config = SQConfig(bits: 8, trainingSampleSize: 100)
        let sq = ScalarQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -2...2) }
        }

        try await sq.train(vectors: trainingVectors)
        #expect(sq.isTrained)
    }

    // MARK: - Encoding/Decoding

    @Test("SQ 8-bit encode and decode")
    func testEncodeDecode8Bit() async throws {
        let config = SQConfig(bits: 8)
        let sq = ScalarQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let original = trainingVectors[0]
        let code = try sq.encode(original)
        let reconstructed = try sq.decode(code)

        #expect(code.count == 16)
        #expect(reconstructed.count == 16)

        var maxError: Float = 0
        for i in 0..<16 {
            let error = abs(original[i] - reconstructed[i])
            maxError = max(maxError, error)
        }

        #expect(maxError < 0.1, "Max error too large: \(maxError)")
    }

    @Test("SQ 4-bit encode and decode")
    func testEncodeDecode4Bit() async throws {
        let config = SQConfig(bits: 4)
        let sq = ScalarQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let original = trainingVectors[0]
        let code = try sq.encode(original)
        let reconstructed = try sq.decode(code)

        #expect(code.count == 8)
        #expect(reconstructed.count == 16)
    }

    // MARK: - Distance Computation

    @Test("SQ Euclidean distance")
    func testEuclideanDistance() async throws {
        let config = SQConfig(bits: 8)
        let sq = ScalarQuantizer(config: config, dimensions: 16, metric: .euclidean)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let query = trainingVectors[0]
        let code = try sq.encode(trainingVectors[1])

        let prepared = try sq.prepareQuery(query)
        let distance = sq.distanceWithPrepared(prepared, code: code)

        #expect(distance >= 0)
        #expect(distance.isFinite)
    }

    @Test("SQ Cosine distance bounds")
    func testCosineDistanceBounds() async throws {
        let config = SQConfig(bits: 8)
        let sq = ScalarQuantizer(config: config, dimensions: 16, metric: .cosine)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let query = trainingVectors[0]
        let code = try sq.encode(trainingVectors[1])

        let prepared = try sq.prepareQuery(query)
        let distance = sq.distanceWithPrepared(prepared, code: code)

        #expect(distance >= 0)
        #expect(distance <= QuantizerConstants.maxCosineDistance)
    }

    // MARK: - Serialization

    @Test("SQ serialize and deserialize")
    func testSerialization() async throws {
        let config = SQConfig(bits: 8)
        let sq1 = ScalarQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq1.train(vectors: trainingVectors)

        let data = try sq1.serialize()

        let sq2 = ScalarQuantizer(config: config, dimensions: 16)
        try sq2.deserialize(from: data)

        #expect(sq2.isTrained)

        let vector = trainingVectors[0]
        let code1 = try sq1.encode(vector)
        let code2 = try sq2.encode(vector)

        #expect(code1 == code2)
    }

    @Test("SQ deserialize fails with bits mismatch")
    func testDeserializeBitsMismatch() async throws {
        let config8 = SQConfig(bits: 8)
        let sq1 = ScalarQuantizer(config: config8, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq1.train(vectors: trainingVectors)

        let data = try sq1.serialize()

        let config4 = SQConfig(bits: 4)
        let sq2 = ScalarQuantizer(config: config4, dimensions: 16)

        #expect(throws: QuantizerError.self) {
            try sq2.deserialize(from: data)
        }
    }
}

// MARK: - BinaryQuantizer Tests

@Suite("BinaryQuantizer Tests")
struct BinaryQuantizerTests {

    // MARK: - Initialization

    @Test("BQ initialization")
    func testInitialization() {
        let config = BQConfig()
        let bq = BinaryQuantizer(config: config, dimensions: 128)

        #expect(bq.dimensions == 128)
        #expect(bq.codeSize == 16) // 128 / 64 * 8 = 16 bytes
        #expect(!bq.isTrained)
    }

    @Test("BQ sign quantization (no training)")
    func testSignQuantization() {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        #expect(bq.isTrained)
    }

    // MARK: - Training

    @Test("BQ training learns thresholds")
    func testTraining() async throws {
        let config = BQConfig()
        let bq = BinaryQuantizer(config: config, dimensions: 64)

        let trainingVectors = (0..<100).map { _ in
            (0..<64).map { _ in Float.random(in: -1...1) }
        }

        try await bq.train(vectors: trainingVectors)
        #expect(bq.isTrained)
    }

    // MARK: - Encoding/Decoding

    @Test("BQ encode produces binary codes")
    func testEncode() throws {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let vector = (0..<64).map { _ in Float.random(in: -1...1) }
        let code = try bq.encode(vector)

        #expect(code.count == 1) // 64 bits = 1 UInt64
    }

    @Test("BQ decode preserves signs")
    func testDecodePreservesSigns() throws {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let original = (0..<64).map { _ in Float.random(in: -1...1) }
        let code = try bq.encode(original)
        let reconstructed = try bq.decode(code)

        #expect(reconstructed.count == 64)

        for i in 0..<64 {
            let originalSign = original[i] >= 0
            let reconstructedSign = reconstructed[i] >= 0
            #expect(originalSign == reconstructedSign, "Sign mismatch at \(i)")
        }
    }

    // MARK: - Hamming Distance

    @Test("BQ Hamming distance all bits differ")
    func testHammingDistanceAllDiffer() {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let codeA: [UInt64] = [0]
        let codeB: [UInt64] = [UInt64.max]

        let distance = bq.hammingDistance(codeA, codeB)
        #expect(distance == 64)
    }

    @Test("BQ Hamming distance to self is zero")
    func testHammingDistanceSelf() throws {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let vector = (0..<64).map { _ in Float.random(in: -1...1) }
        let code = try bq.encode(vector)

        let distance = bq.hammingDistance(code, code)
        #expect(distance == 0)
    }

    @Test("BQ Hamming to cosine conversion")
    func testHammingToCosine() {
        let config = BQConfig()
        let bq = BinaryQuantizer(config: config, dimensions: 128)

        #expect(bq.hammingToCosine(64) == 1.0)
        #expect(bq.hammingToCosine(0) == 0.0)
    }

    // MARK: - Prepared Query

    @Test("BQ prepared query distance")
    func testPreparedQueryDistance() throws {
        let config = BQConfig()
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let query = (0..<64).map { _ in Float.random(in: -1...1) }
        let vector = (0..<64).map { _ in Float.random(in: -1...1) }

        let prepared = try bq.prepareQuery(query)
        let code = try bq.encode(vector)
        let distance = bq.distanceWithPrepared(prepared, code: code)

        #expect(distance >= 0)
        #expect(distance <= 64)
    }

    // MARK: - Rescoring

    @Test("BQ search with rescoring")
    func testSearchWithRescoring() throws {
        let config = BQConfig(rescoringFactor: 2)
        let bq = BinaryQuantizer.withSignQuantization(config: config, dimensions: 64)

        let candidates: [(code: [UInt64], data: Int)] = (0..<10).map { i in
            let vector = (0..<64).map { _ in Float.random(in: -1...1) }
            let code = try! bq.encode(vector)
            return (code: code, data: i)
        }

        let query = (0..<64).map { _ in Float.random(in: -1...1) }
        let queryCode = try bq.encode(query)

        let results = bq.searchWithRescoring(
            queryCode: queryCode,
            candidates: candidates,
            k: 3
        ) { idx in Float(idx) }

        #expect(results.count == 3)
        #expect(results[0].distance <= results[1].distance)
        #expect(results[1].distance <= results[2].distance)
    }

    // MARK: - Serialization

    @Test("BQ serialize and deserialize")
    func testSerialization() async throws {
        let config = BQConfig()
        let bq1 = BinaryQuantizer(config: config, dimensions: 64)

        let trainingVectors = (0..<100).map { _ in
            (0..<64).map { _ in Float.random(in: -1...1) }
        }
        try await bq1.train(vectors: trainingVectors)

        let data = try bq1.serialize()

        let bq2 = BinaryQuantizer(config: config, dimensions: 64)
        try bq2.deserialize(from: data)

        #expect(bq2.isTrained)

        let vector = trainingVectors[0]
        let code1 = try bq1.encode(vector)
        let code2 = try bq2.encode(vector)

        #expect(code1 == code2)
    }

    @Test("BQ deserialize fails with dimension mismatch")
    func testDeserializeDimensionMismatch() async throws {
        let config = BQConfig()
        let bq1 = BinaryQuantizer(config: config, dimensions: 64)

        let trainingVectors = (0..<100).map { _ in
            (0..<64).map { _ in Float.random(in: -1...1) }
        }
        try await bq1.train(vectors: trainingVectors)

        let data = try bq1.serialize()

        let bq2 = BinaryQuantizer(config: config, dimensions: 128)

        #expect(throws: QuantizerError.self) {
            try bq2.deserialize(from: data)
        }
    }
}

// MARK: - QuantizerConstants Tests

@Suite("QuantizerConstants Tests")
struct QuantizerConstantsTests {

    @Test("floatTolerance value")
    func testFloatTolerance() {
        #expect(QuantizerConstants.floatTolerance > 0)
        #expect(QuantizerConstants.floatTolerance < 1e-6)
    }

    @Test("maxCosineDistance value")
    func testMaxCosineDistance() {
        #expect(QuantizerConstants.maxCosineDistance == 2.0)
    }
}

// MARK: - QuantizerEvaluator Tests

@Suite("QuantizerEvaluator Tests")
struct QuantizerEvaluatorTests {

    @Test("Evaluate PQ metrics")
    func testEvaluatePQ() async throws {
        let config = PQConfig(
            numSubquantizers: 4,
            numCentroids: 16,
            trainingSampleSize: 100,
            kmeansIterations: 10
        )
        let pq = ProductQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await pq.train(vectors: trainingVectors)

        let testVectors = (0..<50).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }

        let metrics = try QuantizerEvaluator.evaluate(pq, on: testVectors)

        #expect(metrics.sampleSize == 50)
        #expect(metrics.reconstructionError >= 0)
        #expect(metrics.compressionRatio > 1)
    }

    @Test("Evaluate SQ compression ratio")
    func testEvaluateSQCompression() async throws {
        let config = SQConfig(bits: 8)
        let sq = ScalarQuantizer(config: config, dimensions: 16)

        let trainingVectors = (0..<100).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }
        try await sq.train(vectors: trainingVectors)

        let testVectors = (0..<50).map { _ in
            (0..<16).map { _ in Float.random(in: -1...1) }
        }

        let metrics = try QuantizerEvaluator.evaluate(sq, on: testVectors)

        #expect(metrics.sampleSize == 50)
        #expect(metrics.compressionRatio == 4.0)
    }
}

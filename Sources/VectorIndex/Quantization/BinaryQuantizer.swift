// BinaryQuantizer.swift
// VectorIndex/Quantization - Binary Quantization implementation
//
// Reference: Norouzi et al., "Minimal Loss Hashing for Compact Binary Codes", ICML 2011

import Foundation
import Synchronization

// MARK: - BinaryQuantizer

/// Binary Quantization implementation
///
/// BQ converts each dimension to a single bit (sign of the value).
/// This provides 32x compression with fast Hamming distance computation.
///
/// **Algorithm**:
/// 1. For each dimension: bit = (value >= threshold) ? 1 : 0
/// 2. Pack bits into UInt64 words
/// 3. Use Hamming distance (XOR + popcount) for comparison
///
/// **Distance Computation**:
/// Hamming distance = number of differing bits
/// For normalized vectors: Hamming ≈ (1 - cosine_similarity) * D / 2
///
/// **Rescoring**:
/// Because BQ is very lossy, a rescoring step is recommended:
/// 1. Retrieve k * rescoringFactor candidates using Hamming distance
/// 2. Rescore candidates using original vectors
/// 3. Return top-k from rescored results
///
/// **Thread Safety**:
/// After initialization, all operations are thread-safe.
/// BQ doesn't require training (threshold is fixed or learned).
public final class BinaryQuantizer: VectorQuantizer, @unchecked Sendable {
    public typealias Code = [UInt64]

    // MARK: - Configuration

    private let config: BQConfig
    public let dimensions: Int
    private let numWords: Int

    // MARK: - Learned Parameters

    private struct State: Sendable {
        var thresholds: [Float] = []  // Per-dimension threshold (default: 0)
        var trained: Bool = false
    }

    private let state: Mutex<State>

    // MARK: - Computed Properties

    public var isTrained: Bool {
        state.withLock { $0.trained }
    }

    public var codeSize: Int {
        numWords * 8  // bytes
    }

    // MARK: - Initialization

    /// Create a Binary Quantizer
    ///
    /// - Parameters:
    ///   - config: BQ configuration
    ///   - dimensions: Vector dimensions
    public init(config: BQConfig, dimensions: Int) {
        precondition(dimensions > 0, "Dimensions must be positive")

        self.config = config
        self.dimensions = dimensions
        self.numWords = (dimensions + 63) / 64  // Ceiling division
        self.state = Mutex(State())
    }

    // MARK: - Training

    /// Train the quantizer to learn optimal thresholds
    ///
    /// For basic BQ, threshold is 0 (sign quantization).
    /// Advanced: learns per-dimension median as threshold.
    ///
    /// - Parameter vectors: Training vectors
    public func train(vectors: [[Float]]) async throws {
        guard !vectors.isEmpty else {
            throw QuantizerError.trainingFailed("No training vectors provided")
        }

        // Validate dimensions
        for v in vectors {
            guard v.count == dimensions else {
                throw QuantizerError.dimensionMismatch(expected: dimensions, actual: v.count)
            }
        }

        // Compute median per dimension as threshold
        var thresholds = [Float](repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            let values = vectors.map { $0[d] }.sorted()
            let mid = values.count / 2

            if values.count % 2 == 0 {
                thresholds[d] = (values[mid - 1] + values[mid]) / 2
            } else {
                thresholds[d] = values[mid]
            }
        }

        state.withLock { state in
            state.thresholds = thresholds
            state.trained = true
        }
    }

    /// Use default thresholds (0) without training
    ///
    /// Call this to enable encoding without training.
    /// Uses sign quantization (positive -> 1, negative -> 0).
    public func useDefaultThresholds() {
        state.withLock { state in
            state.thresholds = [Float](repeating: 0, count: dimensions)
            state.trained = true
        }
    }

    // MARK: - Encoding

    /// Encode vector to binary code
    ///
    /// - Parameter vector: Input vector
    /// - Returns: Packed binary code as UInt64 array
    public func encode(_ vector: [Float]) throws -> Code {
        guard vector.count == dimensions else {
            throw QuantizerError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let (thresholds, trained) = state.withLock { state in
            (state.thresholds, state.trained)
        }

        guard trained else {
            throw QuantizerError.notTrained
        }

        var code = Code(repeating: 0, count: numWords)

        for d in 0..<dimensions {
            if vector[d] >= thresholds[d] {
                let wordIdx = d / 64
                let bitIdx = d % 64
                code[wordIdx] |= (1 << bitIdx)
            }
        }

        return code
    }

    // MARK: - Decoding

    /// Decode binary code to approximate vector
    ///
    /// **Warning**: BQ decoding is extremely lossy - only sign information is preserved.
    /// The reconstructed values are unit magnitude (+1/-1 offset from threshold).
    /// For accurate rescoring, store and use original vectors separately.
    ///
    /// **Reconstruction Strategy**:
    /// Returns threshold ± 1.0 based on bit value. The choice of ±1.0 (unit magnitude)
    /// follows the convention from Norouzi et al. (2011) for binary hashing,
    /// where decoded vectors maintain unit contribution per dimension.
    ///
    /// - Parameter code: Binary code
    /// - Returns: Reconstructed vector (threshold ± 1.0 per dimension)
    public func decode(_ code: Code) throws -> [Float] {
        guard code.count == numWords else {
            throw QuantizerError.invalidCode("Code size \(code.count) != expected \(numWords)")
        }

        let (thresholds, trained) = state.withLock { state in
            (state.thresholds, state.trained)
        }

        guard trained else {
            throw QuantizerError.notTrained
        }

        var vector = [Float](repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            let wordIdx = d / 64
            let bitIdx = d % 64
            let bit = (code[wordIdx] >> bitIdx) & 1

            // Unit magnitude offset from threshold
            // Reference: Norouzi et al., "Minimal Loss Hashing for Compact Binary Codes", ICML 2011
            vector[d] = bit == 1 ? thresholds[d] + 1.0 : thresholds[d] - 1.0
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Prepare query for distance computation
    public func prepareQuery(_ query: [Float]) throws -> PreparedQuery {
        // Encode query to binary
        let binaryCode = try encode(query)
        return PreparedQuery(storage: .bq(binaryCode: binaryCode))
    }

    /// Compute Hamming distance using prepared query
    public func distanceWithPrepared(_ prepared: PreparedQuery, code: Code) -> Float {
        guard case .bq(let queryCode) = prepared.storage else {
            return Float.infinity
        }

        return Float(hammingDistance(queryCode, code))
    }

    /// Compute Hamming distance between two binary codes
    ///
    /// Uses XOR + popcount for efficiency.
    /// Modern CPUs have hardware popcount instruction.
    public func hammingDistance(_ a: Code, _ b: Code) -> Int {
        var distance = 0

        for i in 0..<min(a.count, b.count) {
            let xor = a[i] ^ b[i]
            distance += xor.nonzeroBitCount
        }

        return distance
    }

    /// Convert Hamming distance to approximate cosine distance
    ///
    /// For normalized vectors:
    /// hamming_distance ≈ D * (1 - cos_similarity) / 2
    /// Therefore:
    /// cos_distance ≈ 2 * hamming_distance / D
    public func hammingToCosine(_ hamming: Int) -> Float {
        return Float(2 * hamming) / Float(dimensions)
    }

    // MARK: - Serialization

    public func serialize() throws -> Data {
        let currentState = state.withLock { $0 }

        guard currentState.trained else {
            throw QuantizerError.notTrained
        }

        var data = Data()

        // Magic bytes
        data.append(contentsOf: "BQ01".utf8)

        // Header
        var dims = Int32(dimensions)
        withUnsafeBytes(of: &dims) { data.append(contentsOf: $0) }

        // Thresholds
        for d in 0..<dimensions {
            var t = currentState.thresholds[d]
            withUnsafeBytes(of: &t) { data.append(contentsOf: $0) }
        }

        return data
    }

    public func deserialize(from data: Data) throws {
        guard data.count >= 8 else {
            throw QuantizerError.deserializationFailed("Data too short for header")
        }

        let magic = String(data: data.prefix(4), encoding: .utf8)
        guard magic == "BQ01" else {
            throw QuantizerError.deserializationFailed("Invalid magic bytes. Expected BQ01.")
        }

        var offset = 4

        let dims = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4

        guard Int(dims) == dimensions else {
            throw QuantizerError.deserializationFailed("Dimension mismatch: expected \(dimensions), got \(dims)")
        }

        // Validate data size (1 float per dimension for thresholds)
        let expectedBytes = dimensions * 4
        guard data.count >= offset + expectedBytes else {
            throw QuantizerError.deserializationFailed("Data too short for \(dimensions) thresholds")
        }

        var thresholds = [Float](repeating: 0, count: dimensions)
        for d in 0..<dimensions {
            thresholds[d] = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Float.self) }
            offset += 4
        }

        state.withLock { state in
            state.thresholds = thresholds
            state.trained = true
        }
    }
}

// MARK: - BinaryQuantizer Extensions

extension BinaryQuantizer {
    /// Create a binary quantizer with default thresholds (no training required)
    ///
    /// Uses sign quantization (positive -> 1, negative -> 0).
    public static func withSignQuantization(
        config: BQConfig,
        dimensions: Int
    ) -> BinaryQuantizer {
        let quantizer = BinaryQuantizer(config: config, dimensions: dimensions)
        quantizer.useDefaultThresholds()
        return quantizer
    }

    /// Search using binary codes with rescoring
    ///
    /// 1. Find candidates using Hamming distance
    /// 2. Rescore with original vectors
    /// 3. Return top-k
    ///
    /// - Parameters:
    ///   - queryCode: Binary query code
    ///   - candidateCodes: Database binary codes
    ///   - k: Number of results
    ///   - rescore: Closure to compute exact distance for rescoring
    /// - Returns: Top-k indices and distances
    public func searchWithRescoring<T>(
        queryCode: Code,
        candidates: [(code: Code, data: T)],
        k: Int,
        rescore: (T) -> Float
    ) -> [(data: T, distance: Float)] {
        let expandedK = k * config.rescoringFactor

        // First pass: Hamming distance
        var hammingResults: [(data: T, hamming: Int)] = candidates.map { candidate in
            let hamming = hammingDistance(queryCode, candidate.code)
            return (data: candidate.data, hamming: hamming)
        }

        // Sort by Hamming distance
        hammingResults.sort { $0.hamming < $1.hamming }

        // Take top expandedK candidates
        let topCandidates = Array(hammingResults.prefix(expandedK))

        // Second pass: exact rescoring
        var rescored: [(data: T, distance: Float)] = topCandidates.map { candidate in
            let exactDist = rescore(candidate.data)
            return (data: candidate.data, distance: exactDist)
        }

        // Sort by exact distance
        rescored.sort { $0.distance < $1.distance }

        return Array(rescored.prefix(k))
    }
}

// ScalarQuantizer.swift
// VectorIndex/Quantization - Scalar Quantization implementation
//
// Reference: Guo et al., "Accelerating Large-Scale Inference with Anisotropic Vector Quantization", ICML 2020

import Foundation
import Synchronization
import Vector

// MARK: - ScalarQuantizer

/// Scalar Quantization implementation
///
/// SQ quantizes each dimension independently to an integer representation.
/// This provides 4x compression (float32 -> uint8) with minimal accuracy loss.
///
/// **Algorithm**:
/// 1. Learn min/max values for each dimension from training data
/// 2. Linearly map each dimension to [0, 255] (for 8-bit) or [0, 15] (for 4-bit)
/// 3. Store as uint8 array
///
/// **Advantages**:
/// - Simple and fast encoding/decoding
/// - Very high accuracy (minimal quantization error)
/// - SIMD-friendly integer arithmetic
///
/// **Thread Safety**:
/// After training, all operations are thread-safe.
public final class ScalarQuantizer: VectorQuantizer, @unchecked Sendable {
    public typealias Code = [UInt8]

    // MARK: - Configuration

    private let config: SQConfig
    public let dimensions: Int
    private let metric: VectorMetric
    private let maxValue: Float

    // MARK: - Learned Parameters

    private struct State: Sendable {
        var minValues: [Float] = []
        var maxValues: [Float] = []
        var scales: [Float] = []  // scale = (max - min) / maxQuantValue
        var trained: Bool = false
    }

    private let state: Mutex<State>

    // MARK: - Computed Properties

    public var isTrained: Bool {
        state.withLock { $0.trained }
    }

    public var codeSize: Int {
        if config.bits == 8 {
            return dimensions
        } else {
            // 4-bit: 2 values per byte
            return (dimensions + 1) / 2
        }
    }

    // MARK: - Initialization

    /// Create a Scalar Quantizer
    ///
    /// - Parameters:
    ///   - config: SQ configuration
    ///   - dimensions: Vector dimensions
    ///   - metric: Distance metric
    public init(config: SQConfig, dimensions: Int, metric: VectorMetric = .euclidean) {
        precondition(dimensions > 0, "Dimensions must be positive")

        self.config = config
        self.dimensions = dimensions
        self.metric = metric
        // Maximum quantized value = 2^bits - 1
        // 8-bit: 255 (0-255 range), 4-bit: 15 (0-15 range)
        self.maxValue = Float((1 << config.bits) - 1)
        self.state = Mutex(State())
    }

    // MARK: - Training

    /// Train the quantizer by learning min/max values per dimension
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

        // Sample if needed
        let sampleVectors: [[Float]]
        if vectors.count > config.trainingSampleSize {
            sampleVectors = Array(vectors.shuffled().prefix(config.trainingSampleSize))
        } else {
            sampleVectors = vectors
        }

        // Compute min/max per dimension
        var minValues = [Float](repeating: Float.infinity, count: dimensions)
        var maxValues = [Float](repeating: -Float.infinity, count: dimensions)

        for vector in sampleVectors {
            for d in 0..<dimensions {
                minValues[d] = min(minValues[d], vector[d])
                maxValues[d] = max(maxValues[d], vector[d])
            }
        }

        // Compute scales
        var scales = [Float](repeating: 1.0, count: dimensions)
        for d in 0..<dimensions {
            let range = maxValues[d] - minValues[d]
            if range > QuantizerConstants.floatTolerance {
                scales[d] = range / maxValue
            } else {
                // Constant dimension: use unit scale
                scales[d] = 1.0
            }
        }

        state.withLock { state in
            state.minValues = minValues
            state.maxValues = maxValues
            state.scales = scales
            state.trained = true
        }
    }

    // MARK: - Encoding

    /// Encode a vector to quantized code
    ///
    /// - Parameter vector: Input vector
    /// - Returns: Quantized bytes
    public func encode(_ vector: [Float]) throws -> Code {
        guard vector.count == dimensions else {
            throw QuantizerError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let (minValues, scales, trained) = state.withLock { state in
            (state.minValues, state.scales, state.trained)
        }

        guard trained else {
            throw QuantizerError.notTrained
        }

        if config.bits == 8 {
            return encode8Bit(vector, minValues: minValues, scales: scales)
        } else {
            return encode4Bit(vector, minValues: minValues, scales: scales)
        }
    }

    private func encode8Bit(_ vector: [Float], minValues: [Float], scales: [Float]) -> Code {
        var code = Code(repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            var normalized = (vector[d] - minValues[d]) / scales[d]

            if config.saturate {
                normalized = max(0, min(maxValue, normalized))
            }

            code[d] = UInt8(clamping: Int(normalized.rounded()))
        }

        return code
    }

    private func encode4Bit(_ vector: [Float], minValues: [Float], scales: [Float]) -> Code {
        let codeSize = (dimensions + 1) / 2
        var code = Code(repeating: 0, count: codeSize)

        for d in 0..<dimensions {
            var normalized = (vector[d] - minValues[d]) / scales[d]

            if config.saturate {
                normalized = max(0, min(maxValue, normalized))
            }

            let quantized = UInt8(clamping: Int(normalized.rounded()))
            let byteIdx = d / 2

            if d % 2 == 0 {
                code[byteIdx] = quantized // Lower 4 bits
            } else {
                code[byteIdx] |= (quantized << 4) // Upper 4 bits
            }
        }

        return code
    }

    // MARK: - Decoding

    /// Decode quantized code to approximate vector
    ///
    /// - Parameter code: Quantized bytes
    /// - Returns: Reconstructed vector
    public func decode(_ code: Code) throws -> [Float] {
        let expectedSize = config.bits == 8 ? dimensions : (dimensions + 1) / 2
        guard code.count == expectedSize else {
            throw QuantizerError.invalidCode("Code size \(code.count) != expected \(expectedSize)")
        }

        let (minValues, scales, trained) = state.withLock { state in
            (state.minValues, state.scales, state.trained)
        }

        guard trained else {
            throw QuantizerError.notTrained
        }

        if config.bits == 8 {
            return decode8Bit(code, minValues: minValues, scales: scales)
        } else {
            return decode4Bit(code, minValues: minValues, scales: scales)
        }
    }

    private func decode8Bit(_ code: Code, minValues: [Float], scales: [Float]) -> [Float] {
        var vector = [Float](repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            vector[d] = Float(code[d]) * scales[d] + minValues[d]
        }

        return vector
    }

    private func decode4Bit(_ code: Code, minValues: [Float], scales: [Float]) -> [Float] {
        var vector = [Float](repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            let byteIdx = d / 2
            let quantized: UInt8

            if d % 2 == 0 {
                quantized = code[byteIdx] & 0x0F
            } else {
                quantized = code[byteIdx] >> 4
            }

            vector[d] = Float(quantized) * scales[d] + minValues[d]
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Prepare query for distance computation
    public func prepareQuery(_ query: [Float]) throws -> PreparedQuery {
        guard query.count == dimensions else {
            throw QuantizerError.dimensionMismatch(expected: dimensions, actual: query.count)
        }

        let (minValues, scales, trained) = state.withLock { state in
            (state.minValues, state.scales, state.trained)
        }

        guard trained else {
            throw QuantizerError.notTrained
        }

        // Normalize query to quantized space
        var normalizedQuery = [Float](repeating: 0, count: dimensions)
        for d in 0..<dimensions {
            normalizedQuery[d] = (query[d] - minValues[d]) / scales[d]
        }

        return PreparedQuery(storage: .sq(
            normalizedQuery: normalizedQuery,
            scale: scales,
            offset: minValues
        ))
    }

    /// Compute distance using prepared query
    public func distanceWithPrepared(_ prepared: PreparedQuery, code: Code) -> Float {
        guard case .sq(let normalizedQuery, _, _) = prepared.storage else {
            return Float.infinity
        }

        switch metric {
        case .euclidean:
            return euclideanDistanceSQ(normalizedQuery, code)

        case .cosine:
            return cosineDistanceSQ(normalizedQuery, code)

        case .dotProduct:
            return dotProductDistanceSQ(normalizedQuery, code)
        }
    }

    private func euclideanDistanceSQ(_ query: [Float], _ code: Code) -> Float {
        var sumSq: Float = 0

        if config.bits == 8 {
            for d in 0..<dimensions {
                let diff = query[d] - Float(code[d])
                sumSq += diff * diff
            }
        } else {
            for d in 0..<dimensions {
                let byteIdx = d / 2
                let quantized: UInt8 = (d % 2 == 0) ? (code[byteIdx] & 0x0F) : (code[byteIdx] >> 4)
                let diff = query[d] - Float(quantized)
                sumSq += diff * diff
            }
        }

        return sqrt(sumSq)
    }

    private func cosineDistanceSQ(_ query: [Float], _ code: Code) -> Float {
        var dot: Float = 0
        var normQ: Float = 0
        var normC: Float = 0

        if config.bits == 8 {
            for d in 0..<dimensions {
                let c = Float(code[d])
                dot += query[d] * c
                normQ += query[d] * query[d]
                normC += c * c
            }
        } else {
            for d in 0..<dimensions {
                let byteIdx = d / 2
                let quantized: UInt8 = (d % 2 == 0) ? (code[byteIdx] & 0x0F) : (code[byteIdx] >> 4)
                let c = Float(quantized)
                dot += query[d] * c
                normQ += query[d] * query[d]
                normC += c * c
            }
        }

        let denom = sqrt(normQ * normC)
        if denom < QuantizerConstants.floatTolerance {
            return QuantizerConstants.maxCosineDistance
        }
        return 1.0 - dot / denom
    }

    private func dotProductDistanceSQ(_ query: [Float], _ code: Code) -> Float {
        var dot: Float = 0

        if config.bits == 8 {
            for d in 0..<dimensions {
                dot += query[d] * Float(code[d])
            }
        } else {
            for d in 0..<dimensions {
                let byteIdx = d / 2
                let quantized: UInt8 = (d % 2 == 0) ? (code[byteIdx] & 0x0F) : (code[byteIdx] >> 4)
                dot += query[d] * Float(quantized)
            }
        }

        return -dot
    }

    // MARK: - Serialization

    public func serialize() throws -> Data {
        let currentState = state.withLock { $0 }

        guard currentState.trained else {
            throw QuantizerError.notTrained
        }

        var data = Data()

        // Magic bytes
        data.append(contentsOf: "SQ01".utf8)

        // Header
        var dims = Int32(dimensions)
        var bits = Int32(config.bits)

        withUnsafeBytes(of: &dims) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: &bits) { data.append(contentsOf: $0) }

        // Min/max/scale values
        for d in 0..<dimensions {
            var minV = currentState.minValues[d]
            var maxV = currentState.maxValues[d]
            var scale = currentState.scales[d]

            withUnsafeBytes(of: &minV) { data.append(contentsOf: $0) }
            withUnsafeBytes(of: &maxV) { data.append(contentsOf: $0) }
            withUnsafeBytes(of: &scale) { data.append(contentsOf: $0) }
        }

        return data
    }

    public func deserialize(from data: Data) throws {
        guard data.count >= 12 else {
            throw QuantizerError.deserializationFailed("Data too short for header")
        }

        let magic = String(data: data.prefix(4), encoding: .utf8)
        guard magic == "SQ01" else {
            throw QuantizerError.deserializationFailed("Invalid magic bytes. Expected SQ01.")
        }

        var offset = 4

        let dims = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4
        let bits = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4

        // Validate all parameters
        guard Int(dims) == dimensions else {
            throw QuantizerError.deserializationFailed("Dimension mismatch: expected \(dimensions), got \(dims)")
        }
        guard Int(bits) == config.bits else {
            throw QuantizerError.deserializationFailed("Bits mismatch: expected \(config.bits), got \(bits)")
        }

        // Validate data size (3 floats per dimension: min, max, scale)
        let expectedBytes = dimensions * 3 * 4
        guard data.count >= offset + expectedBytes else {
            throw QuantizerError.deserializationFailed("Data too short for \(dimensions) dimensions")
        }

        var minValues = [Float](repeating: 0, count: dimensions)
        var maxValues = [Float](repeating: 0, count: dimensions)
        var scales = [Float](repeating: 0, count: dimensions)

        for d in 0..<dimensions {
            minValues[d] = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Float.self) }
            offset += 4
            maxValues[d] = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Float.self) }
            offset += 4
            scales[d] = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Float.self) }
            offset += 4
        }

        state.withLock { state in
            state.minValues = minValues
            state.maxValues = maxValues
            state.scales = scales
            state.trained = true
        }
    }
}

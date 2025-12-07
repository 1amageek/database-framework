// VectorQuantizer.swift
// VectorIndex/Quantization - Vector quantization protocol
//
// Defines the interface for vector quantization algorithms.
//
// References:
// - Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
// - Asymmetric Distance Computation: Jégou et al., "Searching in one billion vectors", IEEE ICME 2011

import Foundation

// MARK: - Numerical Constants

/// Numerical constants for quantization algorithms
public enum QuantizerConstants {
    /// Tolerance for floating-point near-zero comparisons
    ///
    /// Used to detect:
    /// - Zero-range dimensions (constant values)
    /// - Near-zero denominators (cosine similarity)
    /// - Degenerate k-means clusters
    ///
    /// **Value Rationale**:
    /// - Float32 machine epsilon ≈ 1.19e-7
    /// - 1e-10 is ~1000x smaller, providing margin for accumulated error
    /// - Smaller than typical embedding value ranges (usually -1 to 1)
    ///
    /// This value is conservative. For most applications, 1e-6 to 1e-8 would suffice.
    public static let floatTolerance: Float = 1e-10

    /// Maximum possible cosine distance
    ///
    /// Cosine similarity ranges from -1 (opposite) to 1 (identical).
    /// Cosine distance = 1 - similarity, ranging from 0 to 2.
    ///
    /// Returned when vectors are zero-magnitude (undefined direction).
    public static let maxCosineDistance: Float = 2.0
}

// MARK: - VectorQuantizer Protocol

/// Protocol for vector quantization algorithms
///
/// Quantizers compress high-dimensional vectors into compact codes
/// for memory-efficient storage and fast distance computation.
///
/// **Lifecycle**:
/// 1. Create quantizer with configuration
/// 2. Train on sample vectors (builds codebook/parameters)
/// 3. Encode vectors to codes
/// 4. Compute distances using codes
///
/// **Asymmetric Distance Computation (ADC)**:
/// For efficient search, quantizers use ADC:
/// 1. `prepareQuery()`: Precompute distance tables for query vector
/// 2. `distanceWithPrepared()`: Use tables for O(M) distance vs O(D)
///
/// **Thread Safety**:
/// All implementations must be thread-safe after training.
/// Training itself may not be thread-safe.
public protocol VectorQuantizer: Sendable {
    /// Type for compressed codes
    associatedtype Code: Sendable

    /// Whether the quantizer has been trained
    var isTrained: Bool { get }

    /// Vector dimensions this quantizer was configured for
    var dimensions: Int { get }

    /// Size of compressed code in bytes
    var codeSize: Int { get }

    // MARK: - Training

    /// Train the quantizer on sample vectors
    ///
    /// Must be called before encoding. May be called multiple times
    /// to update the codebook with new data.
    ///
    /// - Parameter vectors: Training vectors (should be representative sample)
    /// - Throws: If training fails
    func train(vectors: [[Float]]) async throws

    // MARK: - Encoding/Decoding

    /// Encode a vector to compressed code
    ///
    /// - Parameter vector: Input vector (must match dimensions)
    /// - Returns: Compressed code
    /// - Throws: If not trained or dimension mismatch
    func encode(_ vector: [Float]) throws -> Code

    /// Decode a code back to approximate vector
    ///
    /// - Parameter code: Compressed code
    /// - Returns: Reconstructed vector (approximate)
    /// - Throws: If not trained or invalid code
    func decode(_ code: Code) throws -> [Float]

    /// Encode multiple vectors (batch operation)
    ///
    /// Default implementation calls encode() for each vector.
    /// Override for optimized batch encoding.
    ///
    /// - Parameter vectors: Input vectors
    /// - Returns: Compressed codes
    /// - Throws: If not trained or dimension mismatch
    func encodeBatch(_ vectors: [[Float]]) throws -> [Code]

    // MARK: - Distance Computation

    /// Compute distance between query vector and encoded vector
    ///
    /// This is a convenience method that prepares query and computes distance.
    /// For batch searches, use prepareQuery + distanceWithPrepared.
    ///
    /// - Parameters:
    ///   - query: Query vector (full precision)
    ///   - code: Encoded vector
    /// - Returns: Distance (interpretation depends on metric)
    /// - Throws: If not trained
    func distance(query: [Float], code: Code) throws -> Float

    /// Prepare distance computation for a query vector
    ///
    /// Precomputes lookup tables for efficient distance computation.
    /// Call once per query, then use distanceWithPrepared() for each candidate.
    ///
    /// - Parameter query: Query vector
    /// - Returns: Prepared query state
    /// - Throws: If not trained
    func prepareQuery(_ query: [Float]) throws -> PreparedQuery

    /// Compute distance using prepared query state
    ///
    /// O(M) complexity for PQ vs O(D) for raw vectors.
    ///
    /// - Parameters:
    ///   - prepared: Result from prepareQuery()
    ///   - code: Encoded vector
    /// - Returns: Distance
    func distanceWithPrepared(_ prepared: PreparedQuery, code: Code) -> Float

    // MARK: - Serialization

    /// Serialize quantizer state for storage
    ///
    /// Includes codebook and all learned parameters.
    ///
    /// - Returns: Serialized data
    /// - Throws: If not trained or serialization fails
    func serialize() throws -> Data

    /// Deserialize quantizer state
    ///
    /// - Parameter data: Serialized data from serialize()
    /// - Throws: If data is invalid or corrupted
    mutating func deserialize(from data: Data) throws
}

// MARK: - Default Implementations

extension VectorQuantizer {
    /// Default batch encoding (sequential)
    public func encodeBatch(_ vectors: [[Float]]) throws -> [Code] {
        try vectors.map { try encode($0) }
    }

    /// Default distance computation (prepares and computes)
    public func distance(query: [Float], code: Code) throws -> Float {
        let prepared = try prepareQuery(query)
        return distanceWithPrepared(prepared, code: code)
    }
}

// MARK: - PreparedQuery

/// Prepared query state for efficient distance computation
///
/// Contains precomputed lookup tables specific to each quantization method.
/// Opaque type - internals depend on the quantizer implementation.
public struct PreparedQuery: Sendable {
    /// Internal storage for lookup tables
    internal let storage: PreparedQueryStorage

    internal init(storage: PreparedQueryStorage) {
        self.storage = storage
    }
}

/// Internal storage for prepared query
///
/// Each quantizer type uses a different storage format.
internal enum PreparedQueryStorage: Sendable {
    /// PQ distance tables: [M][K] lookup table
    case pq(distanceTables: [[Float]])

    /// SQ: normalized query vector
    case sq(normalizedQuery: [Float], scale: [Float], offset: [Float])

    /// BQ: binary query code
    case bq(binaryCode: [UInt64])

    /// No preparation needed (for testing/debugging)
    case none
}

// MARK: - QuantizerError

/// Errors from quantizer operations
public enum QuantizerError: Error, CustomStringConvertible, Sendable {
    case notTrained
    case dimensionMismatch(expected: Int, actual: Int)
    case invalidConfiguration(String)
    case trainingFailed(String)
    case invalidCode(String)
    case serializationFailed(String)
    case deserializationFailed(String)

    public var description: String {
        switch self {
        case .notTrained:
            return "Quantizer has not been trained"
        case .dimensionMismatch(let expected, let actual):
            return "Dimension mismatch: expected \(expected), got \(actual)"
        case .invalidConfiguration(let message):
            return "Invalid configuration: \(message)"
        case .trainingFailed(let message):
            return "Training failed: \(message)"
        case .invalidCode(let message):
            return "Invalid code: \(message)"
        case .serializationFailed(let message):
            return "Serialization failed: \(message)"
        case .deserializationFailed(let message):
            return "Deserialization failed: \(message)"
        }
    }
}

// MARK: - QuantizerMetrics

/// Metrics for evaluating quantizer quality
public struct QuantizerMetrics: Sendable {
    /// Mean squared error between original and reconstructed vectors
    public let reconstructionError: Float

    /// Distortion (average distance from vectors to their centroids)
    public let distortion: Float

    /// Compression ratio (original size / compressed size)
    public let compressionRatio: Float

    /// Number of vectors used for evaluation
    public let sampleSize: Int

    public init(
        reconstructionError: Float,
        distortion: Float,
        compressionRatio: Float,
        sampleSize: Int
    ) {
        self.reconstructionError = reconstructionError
        self.distortion = distortion
        self.compressionRatio = compressionRatio
        self.sampleSize = sampleSize
    }
}

// MARK: - QuantizerEvaluator

/// Utility for evaluating quantizer quality
public enum QuantizerEvaluator {
    /// Evaluate quantizer on test vectors
    ///
    /// - Parameters:
    ///   - quantizer: Trained quantizer
    ///   - testVectors: Vectors to evaluate on
    /// - Returns: Quality metrics
    public static func evaluate<Q: VectorQuantizer>(
        _ quantizer: Q,
        on testVectors: [[Float]]
    ) throws -> QuantizerMetrics where Q.Code: Sendable {
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        var totalError: Float = 0
        var validCount = 0

        for vector in testVectors {
            do {
                let code = try quantizer.encode(vector)
                let reconstructed = try quantizer.decode(code)

                // Calculate MSE
                var error: Float = 0
                for i in 0..<vector.count {
                    let diff = vector[i] - reconstructed[i]
                    error += diff * diff
                }
                totalError += error / Float(vector.count)
                validCount += 1
            } catch {
                continue // Skip invalid vectors
            }
        }

        let mse = validCount > 0 ? totalError / Float(validCount) : Float.infinity
        let originalSize = Float(quantizer.dimensions * 4) // float32
        let compressedSize = Float(quantizer.codeSize)
        let ratio = originalSize / compressedSize

        return QuantizerMetrics(
            reconstructionError: mse,
            distortion: sqrt(mse), // RMSE as distortion proxy
            compressionRatio: ratio,
            sampleSize: validCount
        )
    }
}

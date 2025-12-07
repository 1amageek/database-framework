// QuantizationConfig.swift
// VectorIndex - Vector quantization configuration
//
// Runtime configuration for vector compression algorithms.
// Used with VectorIndexConfiguration to enable quantization.
//
// References:
// - Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
// - Scalar Quantization: Guo et al., "Accelerating Large-Scale Inference with Anisotropic Vector Quantization", ICML 2020
// - Binary Quantization: Norouzi et al., "Minimal Loss Hashing for Compact Binary Codes", ICML 2011

import Foundation

// MARK: - QuantizationConfig

/// Vector quantization configuration
///
/// Quantization reduces memory usage by compressing high-dimensional vectors into compact codes.
/// This enables storing larger datasets while maintaining reasonable search accuracy.
///
/// | Method | Compression | Accuracy | Use Case |
/// |--------|-------------|----------|----------|
/// | **PQ** | 4-32x | High | General purpose |
/// | **SQ** | 4x | Very High | When accuracy is critical |
/// | **BQ** | 32x | Medium | Extremely large datasets |
///
/// **Usage**:
/// ```swift
/// // Configure quantization at runtime
/// let config = VectorIndexConfiguration<Product>(
///     keyPath: \.embedding,
///     algorithm: .flat,
///     quantization: .pq(.default)  // Enable PQ compression
/// )
///
/// let container = try await FDBContainer(
///     for: schema,
///     indexConfigurations: [config]
/// )
/// ```
public enum QuantizationConfig: Sendable, Codable, Hashable {
    /// No quantization (full precision float32)
    case none

    /// Product Quantization
    ///
    /// Divides vector into M subvectors, quantizes each to K centroids.
    /// Compression ratio: 4 * dimensions / M bytes
    ///
    /// **Best for**: General purpose, 4-32x compression with high recall
    case pq(PQConfig)

    /// Scalar Quantization
    ///
    /// Quantizes each dimension to 8-bit integer.
    /// Compression ratio: 4x (float32 -> uint8)
    ///
    /// **Best for**: When accuracy is critical, 4x compression
    case sq(SQConfig)

    /// Binary Quantization
    ///
    /// Converts each dimension to 1 bit (sign bit).
    /// Compression ratio: 32x (float32 -> 1 bit)
    ///
    /// **Best for**: Extremely large datasets, speed over accuracy
    case bq(BQConfig)

    // MARK: - Convenience Presets

    /// Default Product Quantization (48 subquantizers, 256 centroids)
    public static let defaultPQ = QuantizationConfig.pq(.default)

    /// Default Scalar Quantization (8-bit)
    public static let defaultSQ = QuantizationConfig.sq(.default)

    /// Default Binary Quantization (4x rescoring)
    public static let defaultBQ = QuantizationConfig.bq(.default)
}

// MARK: - PQConfig

/// Product Quantization configuration
///
/// PQ divides a D-dimensional vector into M subvectors of D/M dimensions each.
/// Each subvector is quantized to one of K centroids (typically K=256 for 1-byte codes).
///
/// **Compression Calculation**:
/// - Original: D * 4 bytes (float32)
/// - Compressed: M bytes (1 byte per subquantizer when K=256)
/// - Ratio: 4D / M
///
/// **Example (384-dim vector, M=48)**:
/// - Original: 384 * 4 = 1536 bytes
/// - Compressed: 48 bytes
/// - Ratio: 32x compression
///
/// **Parameters**:
/// - `numSubquantizers` (M): Number of subspaces. Must divide dimensions evenly.
///   Higher M = better accuracy, larger codes.
/// - `numCentroids` (K): Centroids per subspace. K=256 allows 1-byte codes.
///   Higher K = better accuracy, slower training.
/// - `trainingSampleSize`: Number of vectors to sample for codebook training.
///   More samples = better codebook quality, slower training.
/// - `kmeansIterations`: Number of k-means iterations for codebook training.
///
/// **Selection Guidelines by Dimension**:
/// | Dimensions | Recommended M | Subspace Dims | Compression |
/// |------------|---------------|---------------|-------------|
/// | 128 | 16 | 8 | 32x |
/// | 384 | 48 | 8 | 32x |
/// | 768 | 96 | 8 | 32x |
/// | 1536 | 96-192 | 8-16 | 32-64x |
public struct PQConfig: Sendable, Codable, Hashable {
    /// Number of subquantizers (subspaces)
    ///
    /// Must divide vector dimensions evenly.
    /// Typical values: 8-96
    ///
    /// Higher M:
    /// - Better accuracy (smaller quantization error)
    /// - Larger compressed codes (M bytes when K=256)
    /// - Slower distance computation
    public let numSubquantizers: Int

    /// Number of centroids per subquantizer
    ///
    /// K=256 allows 1-byte codes per subspace (recommended).
    /// K=16 allows 4-bit codes (more compression, lower accuracy).
    ///
    /// Higher K:
    /// - Better accuracy
    /// - Slower training (O(K * iterations) per subspace)
    /// - Same code size when K <= 256
    public let numCentroids: Int

    /// Training sample size
    ///
    /// Number of vectors to sample for k-means training.
    /// Recommended: 10x to 100x the number of centroids.
    ///
    /// More samples:
    /// - Better codebook quality
    /// - Slower training
    public let trainingSampleSize: Int

    /// Number of k-means iterations for training
    ///
    /// More iterations:
    /// - Better codebook convergence
    /// - Slower training
    public let kmeansIterations: Int

    /// Default PQ configuration
    ///
    /// **Parameter Rationale**:
    /// - M=48: For 384-dim vectors (common embedding size), yields 8-dim subspaces.
    ///   8-dim subspaces are empirically optimal per Jégou et al. (2011), balancing
    ///   quantization error and codebook size.
    /// - K=256: Allows 1-byte (8-bit) codes per subquantizer. Standard choice from
    ///   Jégou et al. (2011) - larger K gives diminishing returns.
    /// - 10000 samples: ~40x centroids ensures k-means convergence.
    ///   Rule of thumb: 10-100x centroids for stable codebooks.
    /// - 25 iterations: Lloyd's algorithm typically converges in 20-30 iterations.
    ///
    /// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
    public static let `default` = PQConfig(
        numSubquantizers: 48,
        numCentroids: 256,
        trainingSampleSize: 10000,
        kmeansIterations: 25
    )

    /// High accuracy PQ configuration
    ///
    /// - M: 96 subquantizers (smaller subspaces)
    /// - K: 256 centroids
    /// - Training: 50000 samples, 50 iterations
    public static let highAccuracy = PQConfig(
        numSubquantizers: 96,
        numCentroids: 256,
        trainingSampleSize: 50000,
        kmeansIterations: 50
    )

    /// Fast training PQ configuration
    ///
    /// - M: 32 subquantizers
    /// - K: 256 centroids
    /// - Training: 5000 samples, 15 iterations
    public static let fast = PQConfig(
        numSubquantizers: 32,
        numCentroids: 256,
        trainingSampleSize: 5000,
        kmeansIterations: 15
    )

    public init(
        numSubquantizers: Int = 48,
        numCentroids: Int = 256,
        trainingSampleSize: Int = 10000,
        kmeansIterations: Int = 25
    ) {
        precondition(numSubquantizers > 0, "numSubquantizers must be positive")
        precondition(numCentroids > 0 && numCentroids <= 256,
                     "numCentroids must be in [1, 256] for byte-sized codes")
        precondition(trainingSampleSize > 0, "trainingSampleSize must be positive")
        precondition(kmeansIterations > 0, "kmeansIterations must be positive")

        self.numSubquantizers = numSubquantizers
        self.numCentroids = numCentroids
        self.trainingSampleSize = trainingSampleSize
        self.kmeansIterations = kmeansIterations
    }

    /// Create configuration optimized for given dimensions
    ///
    /// Automatically selects M to achieve 8-dimensional subspaces
    /// (empirically optimal for most embedding models).
    ///
    /// - Parameters:
    ///   - dimensions: Vector dimensions
    ///   - compressionLevel: Target compression (default: 32x)
    /// - Returns: Optimized PQ configuration
    public static func forDimensions(_ dimensions: Int, compressionLevel: Int = 32) -> PQConfig {
        // Target 8-dim subspaces for best accuracy/compression tradeoff
        let targetSubspaceDim = 8
        var m = dimensions / targetSubspaceDim

        // Ensure M divides dimensions evenly
        while dimensions % m != 0 && m > 1 {
            m -= 1
        }

        // Fallback if no good divisor found
        if m < 1 { m = 1 }

        return PQConfig(
            numSubquantizers: m,
            numCentroids: 256,
            trainingSampleSize: 10000,
            kmeansIterations: 25
        )
    }
}

// MARK: - SQConfig

/// Scalar Quantization configuration
///
/// SQ quantizes each dimension independently to an integer representation.
/// This provides 4x compression (float32 -> uint8) with minimal accuracy loss.
///
/// **How it works**:
/// 1. Learn min/max values for each dimension from training data
/// 2. Linearly map each dimension to [0, 255] range
/// 3. Store as uint8 array
///
/// **Compression**:
/// - 8-bit: 4x compression (float32 -> uint8)
/// - 4-bit: 8x compression (two values per byte)
///
/// **Distance Computation**:
/// Uses integer arithmetic for fast distance calculation.
/// Slightly less accurate than PQ for same compression ratio,
/// but much simpler and faster.
public struct SQConfig: Sendable, Codable, Hashable {
    /// Quantization bits per dimension
    ///
    /// - 8 bits: 256 levels, 4x compression
    /// - 4 bits: 16 levels, 8x compression
    public let bits: Int

    /// Training sample size for min/max estimation
    ///
    /// More samples = more accurate min/max bounds.
    /// Recommendation: At least 1000 samples.
    public let trainingSampleSize: Int

    /// Whether to use saturating quantization
    ///
    /// When true, values outside learned [min, max] are clamped.
    /// When false, such values may overflow (faster but riskier).
    public let saturate: Bool

    /// Default SQ configuration (8-bit)
    public static let `default` = SQConfig(
        bits: 8,
        trainingSampleSize: 10000,
        saturate: true
    )

    /// 4-bit SQ configuration (higher compression)
    public static let fourBit = SQConfig(
        bits: 4,
        trainingSampleSize: 10000,
        saturate: true
    )

    public init(
        bits: Int = 8,
        trainingSampleSize: Int = 10000,
        saturate: Bool = true
    ) {
        precondition(bits == 4 || bits == 8, "Only 4-bit and 8-bit SQ supported")
        precondition(trainingSampleSize > 0, "trainingSampleSize must be positive")

        self.bits = bits
        self.trainingSampleSize = trainingSampleSize
        self.saturate = saturate
    }
}

// MARK: - BQConfig

/// Binary Quantization configuration
///
/// BQ converts each dimension to a single bit (sign of the value).
/// This provides 32x compression but with lower accuracy.
///
/// **How it works**:
/// 1. For each dimension: bit = (value >= 0) ? 1 : 0
/// 2. Pack bits into UInt64 words
/// 3. Use Hamming distance (XOR + popcount) for comparison
///
/// **Compression**: 32x (float32 -> 1 bit)
///
/// **Rescoring**:
/// Because BQ is lossy, a rescoring step is typically used:
/// 1. Retrieve k * rescoringFactor candidates using binary codes
/// 2. Rescore candidates using original full-precision vectors
/// 3. Return top-k from rescored results
///
/// **Best for**:
/// - Extremely large datasets (billions of vectors)
/// - First-stage retrieval in multi-stage pipelines
/// - When speed is more important than precision
public struct BQConfig: Sendable, Codable, Hashable {
    /// Rescoring factor
    ///
    /// Retrieve this many times more candidates than requested k,
    /// then rescore with full-precision vectors.
    ///
    /// Higher factor:
    /// - Better recall
    /// - More rescoring overhead
    ///
    /// Typical values: 2-10
    public let rescoringFactor: Int

    /// Whether to store original vectors for rescoring
    ///
    /// When true, original float32 vectors are stored alongside binary codes.
    /// Required for rescoring to work.
    public let storeOriginalVectors: Bool

    /// Default BQ configuration
    public static let `default` = BQConfig(
        rescoringFactor: 4,
        storeOriginalVectors: true
    )

    /// Fast BQ configuration (minimal rescoring)
    public static let fast = BQConfig(
        rescoringFactor: 2,
        storeOriginalVectors: true
    )

    /// High recall BQ configuration
    public static let highRecall = BQConfig(
        rescoringFactor: 10,
        storeOriginalVectors: true
    )

    public init(
        rescoringFactor: Int = 4,
        storeOriginalVectors: Bool = true
    ) {
        precondition(rescoringFactor >= 1, "rescoringFactor must be >= 1")

        self.rescoringFactor = rescoringFactor
        self.storeOriginalVectors = storeOriginalVectors
    }
}

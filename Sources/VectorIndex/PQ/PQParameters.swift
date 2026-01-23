// PQParameters.swift
// VectorIndex - Product Quantization parameters
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import Foundation

/// Parameters for Product Quantization
///
/// **Algorithm Overview**:
/// Product Quantization compresses high-dimensional vectors by:
/// 1. Splitting vectors into M equal-sized subvectors
/// 2. Training K-means (K=256) on each subspace independently
/// 3. Encoding each subvector as a single byte (centroid index)
///
/// **Compression Ratio**:
/// - Original: d × 4 bytes (Float32)
/// - Compressed: M bytes
/// - Typical: 16-32x compression with M=8
///
/// **Parameters Guide**:
/// - **m**: Number of subquantizers (subspaces)
///   - Must divide dimensions evenly
///   - Common values: 8, 16, 32
///   - Lower M = higher compression, lower accuracy
/// - **ksub**: Number of centroids per subspace (default: 256)
///   - Always 256 for byte encoding
/// - **niter**: K-means training iterations (default: 25)
///
/// **Trade-offs**:
/// - More subquantizers (M) → better accuracy, larger codes
/// - Fewer subquantizers → higher compression, lower accuracy
///
/// **Usage**:
/// ```swift
/// let pq = PQParameters(m: 8)  // 8 subquantizers
/// // For 384-dim vectors: 384*4=1536 bytes → 8 bytes (192x compression)
/// ```
public struct PQParameters: Sendable, Codable, Hashable {
    /// Number of subquantizers (subspaces to split vector into)
    ///
    /// Must divide the vector dimension evenly.
    /// - 8: High compression, lower accuracy
    /// - 16: Balanced
    /// - 32: Lower compression, higher accuracy
    public let m: Int

    /// Number of centroids per subspace (always 256 for byte encoding)
    public let ksub: Int

    /// K-means training iterations
    public let niter: Int

    /// Create PQ parameters
    ///
    /// - Parameters:
    ///   - m: Number of subquantizers (default: 8)
    ///   - ksub: Centroids per subspace (default: 256, must be 256 for byte encoding)
    ///   - niter: Training iterations (default: 25)
    public init(
        m: Int = 8,
        ksub: Int = 256,
        niter: Int = 25
    ) {
        precondition(m > 0, "m must be positive")
        precondition(ksub == 256, "ksub must be 256 for byte encoding")
        precondition(niter > 0, "niter must be positive")

        self.m = m
        self.ksub = ksub
        self.niter = niter
    }

    /// Default parameters (m=8, balanced compression)
    public static let `default` = PQParameters(m: 8)

    /// High compression parameters (m=4)
    ///
    /// - 4 bytes per vector
    /// - Lower accuracy but maximum compression
    public static let highCompression = PQParameters(m: 4)

    /// High accuracy parameters (m=16)
    ///
    /// - 16 bytes per vector
    /// - Better accuracy but larger codes
    public static let highAccuracy = PQParameters(m: 16)

    /// Very high accuracy parameters (m=32)
    ///
    /// - 32 bytes per vector
    /// - Best accuracy for PQ
    public static let veryHighAccuracy = PQParameters(m: 32)

    /// Compute the dimension of each subspace
    ///
    /// - Parameter dimensions: Total vector dimensions
    /// - Returns: Dimension of each subspace (d/m)
    /// - Throws: If dimensions is not divisible by m
    public func subspaceDimension(for dimensions: Int) -> Int {
        precondition(dimensions % m == 0,
            "Vector dimensions (\(dimensions)) must be divisible by m (\(m))")
        return dimensions / m
    }

    /// Compute compressed code size in bytes
    ///
    /// - Returns: Code size (equal to m)
    public var codeSize: Int { m }

    /// Compute compression ratio
    ///
    /// - Parameter dimensions: Vector dimensions
    /// - Returns: Compression ratio (e.g., 192 for 384-dim with m=8)
    public func compressionRatio(for dimensions: Int) -> Double {
        let originalSize = Double(dimensions * 4)  // Float32
        let compressedSize = Double(m)
        return originalSize / compressedSize
    }
}

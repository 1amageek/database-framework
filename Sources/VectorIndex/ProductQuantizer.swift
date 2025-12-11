// ProductQuantizer.swift
// VectorIndex - Product Quantization for vector compression
//
// Reference: Jégou, H., Douze, M., & Schmid, C. (2011).
// "Product Quantization for Nearest Neighbor Search"
// IEEE TPAMI, 33(1), 117-128.

import Foundation
import Synchronization

// MARK: - Product Quantizer

/// Product Quantizer for vector compression
///
/// **Algorithm Overview**:
/// 1. Split D-dimensional vector into M subvectors of dimension D/M
/// 2. For each subvector, find nearest centroid from ksub centroids
/// 3. Store M indices (each 1 byte for nbits=8) instead of D floats
///
/// **Memory Savings**:
/// - Full vector: D × 4 bytes (e.g., 384 × 4 = 1,536 bytes)
/// - PQ code: M bytes (e.g., 48 bytes)
/// - Compression ratio: D × 4 / M (e.g., 32x)
///
/// **Reference**: Jégou et al., "Product Quantization for Nearest Neighbor Search", TPAMI 2011
public struct ProductQuantizer: Sendable {

    /// Vector dimension
    public let dimensions: Int

    /// Number of subquantizers (subvector count)
    public let m: Int

    /// Bits per subquantizer code
    public let nbits: Int

    /// Dimension of each subvector (D / M)
    public let dsub: Int

    /// Number of centroids per subquantizer (2^nbits)
    public let ksub: Int

    /// Codebooks: M arrays of ksub centroids, each of dimension dsub
    /// Shape: [M][ksub][dsub]
    private let codebooks: [[Float]]

    /// Flag indicating if codebooks are trained
    public var isTrained: Bool {
        !codebooks.isEmpty && codebooks[0].count == ksub * dsub
    }

    // MARK: - Initialization

    /// Create an untrained product quantizer
    ///
    /// - Parameters:
    ///   - dimensions: Vector dimension (must be divisible by m)
    ///   - m: Number of subquantizers
    ///   - nbits: Bits per code (default: 8)
    public init(dimensions: Int, m: Int, nbits: Int = 8) {
        precondition(dimensions > 0, "dimensions must be positive")
        precondition(m > 0, "m must be positive")
        precondition(dimensions % m == 0, "dimensions must be divisible by m")
        precondition(nbits > 0 && nbits <= 16, "nbits must be 1-16")

        self.dimensions = dimensions
        self.m = m
        self.nbits = nbits
        self.dsub = dimensions / m
        self.ksub = 1 << nbits
        self.codebooks = []
    }

    /// Create a trained product quantizer with existing codebooks
    ///
    /// - Parameters:
    ///   - dimensions: Vector dimension
    ///   - m: Number of subquantizers
    ///   - nbits: Bits per code
    ///   - codebooks: Pre-trained codebooks [M][ksub × dsub]
    public init(dimensions: Int, m: Int, nbits: Int, codebooks: [[Float]]) {
        precondition(dimensions > 0, "dimensions must be positive")
        precondition(m > 0, "m must be positive")
        precondition(dimensions % m == 0, "dimensions must be divisible by m")
        precondition(codebooks.count == m, "codebooks count must equal m")

        self.dimensions = dimensions
        self.m = m
        self.nbits = nbits
        self.dsub = dimensions / m
        self.ksub = 1 << nbits
        self.codebooks = codebooks
    }

    // MARK: - Training

    /// Train codebooks using K-means on training vectors
    ///
    /// **Algorithm**:
    /// For each subvector dimension m:
    ///   1. Extract subvectors from all training samples
    ///   2. Run K-means to find ksub centroids
    ///   3. Store centroids as codebook[m]
    ///
    /// - Parameters:
    ///   - vectors: Training vectors [N][D]
    ///   - iterations: K-means iterations (default: 25)
    /// - Returns: Trained ProductQuantizer with codebooks
    public func train(vectors: [[Float]], iterations: Int = 25) -> ProductQuantizer {
        precondition(!vectors.isEmpty, "Need training vectors")
        precondition(vectors[0].count == dimensions, "Vector dimension mismatch")

        var trainedCodebooks: [[Float]] = []
        trainedCodebooks.reserveCapacity(m)

        // Train each subquantizer independently
        for subIdx in 0..<m {
            // Extract subvectors for this subspace
            var subvectors: [[Float]] = []
            subvectors.reserveCapacity(vectors.count)

            let startDim = subIdx * dsub
            let endDim = startDim + dsub

            for vector in vectors {
                let subvector = Array(vector[startDim..<endDim])
                subvectors.append(subvector)
            }

            // Run K-means on subvectors
            let centroids = kmeansCluster(
                vectors: subvectors,
                k: ksub,
                iterations: iterations
            )

            // Flatten centroids into codebook: [ksub × dsub]
            var codebook: [Float] = []
            codebook.reserveCapacity(ksub * dsub)
            for centroid in centroids {
                codebook.append(contentsOf: centroid)
            }

            trainedCodebooks.append(codebook)
        }

        return ProductQuantizer(
            dimensions: dimensions,
            m: m,
            nbits: nbits,
            codebooks: trainedCodebooks
        )
    }

    // MARK: - Encoding

    /// Encode a vector into PQ codes
    ///
    /// - Parameter vector: D-dimensional vector
    /// - Returns: M bytes (PQ codes)
    public func encode(_ vector: [Float]) -> [UInt8] {
        precondition(isTrained, "ProductQuantizer must be trained")
        precondition(vector.count == dimensions, "Vector dimension mismatch")

        var codes: [UInt8] = []
        codes.reserveCapacity(m)

        for subIdx in 0..<m {
            let startDim = subIdx * dsub
            let subvector = Array(vector[startDim..<(startDim + dsub)])

            // Find nearest centroid in codebook
            let code = findNearestCentroid(subvector: subvector, subIdx: subIdx)
            codes.append(UInt8(code))
        }

        return codes
    }

    /// Encode a residual vector (vector - centroid)
    ///
    /// - Parameters:
    ///   - vector: Original vector
    ///   - centroid: Cluster centroid
    /// - Returns: M bytes (PQ codes for residual)
    public func encodeResidual(_ vector: [Float], centroid: [Float]) -> [UInt8] {
        precondition(vector.count == centroid.count, "Dimension mismatch")

        // Compute residual: r = x - c
        var residual: [Float] = []
        residual.reserveCapacity(dimensions)
        for i in 0..<dimensions {
            residual.append(vector[i] - centroid[i])
        }

        return encode(residual)
    }

    // MARK: - Decoding

    /// Decode PQ codes back to approximate vector
    ///
    /// - Parameter codes: M bytes (PQ codes)
    /// - Returns: Reconstructed D-dimensional vector
    public func decode(_ codes: [UInt8]) -> [Float] {
        precondition(isTrained, "ProductQuantizer must be trained")
        precondition(codes.count == m, "Code length must equal m")

        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for subIdx in 0..<m {
            let code = Int(codes[subIdx])
            let centroidOffset = code * dsub
            let codebook = codebooks[subIdx]

            for d in 0..<dsub {
                vector.append(codebook[centroidOffset + d])
            }
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Compute distance table for Asymmetric Distance Computation (ADC)
    ///
    /// **ADC Formula**:
    /// For query q and encoded vector with codes c:
    ///   d(q, x̂) ≈ Σ_m ||q_m - codebook[m][c_m]||²
    ///
    /// Precompute table[m][j] = ||q_m - codebook[m][j]||² for all m, j
    /// Then distance = Σ_m table[m][codes[m]]
    ///
    /// - Parameter query: Query vector
    /// - Returns: Distance table [M][ksub]
    public func computeDistanceTable(_ query: [Float]) -> [[Float]] {
        precondition(isTrained, "ProductQuantizer must be trained")
        precondition(query.count == dimensions, "Query dimension mismatch")

        var table: [[Float]] = []
        table.reserveCapacity(m)

        for subIdx in 0..<m {
            let startDim = subIdx * dsub
            let querySubvector = Array(query[startDim..<(startDim + dsub)])
            let codebook = codebooks[subIdx]

            var distances: [Float] = []
            distances.reserveCapacity(ksub)

            for centroidIdx in 0..<ksub {
                let centroidOffset = centroidIdx * dsub
                var dist: Float = 0

                for d in 0..<dsub {
                    let diff = querySubvector[d] - codebook[centroidOffset + d]
                    dist += diff * diff
                }

                distances.append(dist)
            }

            table.append(distances)
        }

        return table
    }

    /// Compute distance using precomputed table (ADC)
    ///
    /// - Parameters:
    ///   - table: Distance table from computeDistanceTable
    ///   - codes: PQ codes
    /// - Returns: Approximate squared L2 distance
    public func computeDistanceADC(table: [[Float]], codes: [UInt8]) -> Float {
        var distance: Float = 0
        for subIdx in 0..<m {
            distance += table[subIdx][Int(codes[subIdx])]
        }
        return distance
    }

    // MARK: - Serialization

    /// Serialize codebooks to bytes for storage
    public func serializeCodebooks() -> Data {
        var data = Data()

        // Header: dimensions, m, nbits
        let header = [Int32(dimensions), Int32(m), Int32(nbits)]
        data.append(contentsOf: header.withUnsafeBytes { Data($0) })

        // Codebooks
        for codebook in codebooks {
            data.append(contentsOf: codebook.withUnsafeBytes { Data($0) })
        }

        return data
    }

    /// Deserialize codebooks from bytes
    public static func deserializeCodebooks(_ data: Data) -> ProductQuantizer? {
        guard data.count >= 12 else { return nil }

        // Read header
        let headerSize = 12  // 3 × Int32
        let header = data.prefix(headerSize).withUnsafeBytes {
            Array($0.bindMemory(to: Int32.self))
        }

        guard header.count == 3 else { return nil }

        let dimensions = Int(header[0])
        let m = Int(header[1])
        let nbits = Int(header[2])
        let dsub = dimensions / m
        let ksub = 1 << nbits

        // Read codebooks
        var codebooks: [[Float]] = []
        var offset = headerSize
        let codebookSize = ksub * dsub * 4  // float32

        for _ in 0..<m {
            guard offset + codebookSize <= data.count else { return nil }

            let codebookData = data[offset..<(offset + codebookSize)]
            let codebook = codebookData.withUnsafeBytes {
                Array($0.bindMemory(to: Float.self))
            }

            codebooks.append(codebook)
            offset += codebookSize
        }

        return ProductQuantizer(
            dimensions: dimensions,
            m: m,
            nbits: nbits,
            codebooks: codebooks
        )
    }

    // MARK: - Private Helpers

    /// Find nearest centroid index for a subvector
    private func findNearestCentroid(subvector: [Float], subIdx: Int) -> Int {
        let codebook = codebooks[subIdx]
        var minDist = Float.infinity
        var minIdx = 0

        for centroidIdx in 0..<ksub {
            let centroidOffset = centroidIdx * dsub
            var dist: Float = 0

            for d in 0..<dsub {
                let diff = subvector[d] - codebook[centroidOffset + d]
                dist += diff * diff
            }

            if dist < minDist {
                minDist = dist
                minIdx = centroidIdx
            }
        }

        return minIdx
    }

    /// K-means clustering for codebook training
    ///
    /// Uses Lloyd's algorithm with random initialization
    ///
    /// - Parameters:
    ///   - vectors: Training vectors
    ///   - k: Number of clusters
    ///   - iterations: Max iterations
    /// - Returns: k centroids
    private func kmeansCluster(
        vectors: [[Float]],
        k: Int,
        iterations: Int
    ) -> [[Float]] {
        guard !vectors.isEmpty else { return [] }

        let n = vectors.count
        let d = vectors[0].count

        // Initialize centroids randomly from data points
        var centroids: [[Float]] = []
        var usedIndices = Set<Int>()

        for _ in 0..<min(k, n) {
            var idx: Int
            repeat {
                idx = Int.random(in: 0..<n)
            } while usedIndices.contains(idx)
            usedIndices.insert(idx)
            centroids.append(vectors[idx])
        }

        // Pad with random vectors if needed
        while centroids.count < k {
            let randomVector = (0..<d).map { _ in Float.random(in: -1...1) }
            centroids.append(randomVector)
        }

        // Lloyd's algorithm
        var assignments = [Int](repeating: 0, count: n)

        for _ in 0..<iterations {
            // Assignment step: assign each vector to nearest centroid
            for i in 0..<n {
                var minDist = Float.infinity
                var minIdx = 0

                for j in 0..<k {
                    var dist: Float = 0
                    for dim in 0..<d {
                        let diff = vectors[i][dim] - centroids[j][dim]
                        dist += diff * diff
                    }
                    if dist < minDist {
                        minDist = dist
                        minIdx = j
                    }
                }

                assignments[i] = minIdx
            }

            // Update step: recompute centroids
            var newCentroids = [[Float]](repeating: [Float](repeating: 0, count: d), count: k)
            var counts = [Int](repeating: 0, count: k)

            for i in 0..<n {
                let cluster = assignments[i]
                counts[cluster] += 1
                for dim in 0..<d {
                    newCentroids[cluster][dim] += vectors[i][dim]
                }
            }

            for j in 0..<k {
                if counts[j] > 0 {
                    for dim in 0..<d {
                        newCentroids[j][dim] /= Float(counts[j])
                    }
                } else {
                    // Empty cluster: reinitialize with random vector
                    newCentroids[j] = vectors[Int.random(in: 0..<n)]
                }
            }

            centroids = newCentroids
        }

        return centroids
    }
}

// MARK: - Codebook Accessor

extension ProductQuantizer {
    /// Get codebook for a specific subquantizer
    ///
    /// - Parameter subIdx: Subquantizer index (0..<m)
    /// - Returns: Flattened codebook [ksub × dsub]
    public func getCodebook(_ subIdx: Int) -> [Float] {
        precondition(subIdx >= 0 && subIdx < m, "Invalid subquantizer index")
        return codebooks[subIdx]
    }

    /// Get all codebooks
    public func getAllCodebooks() -> [[Float]] {
        return codebooks
    }
}

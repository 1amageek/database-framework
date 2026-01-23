// ProductQuantizer.swift
// VectorIndex - Product Quantization for vector compression
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import Foundation

/// Product Quantizer for compressing high-dimensional vectors
///
/// **Algorithm**:
/// 1. **Splitting**: Divide d-dimensional vector into M subvectors of d/M dimensions
/// 2. **Training**: Learn 256 centroids for each subspace using K-means
/// 3. **Encoding**: Map each subvector to nearest centroid index (1 byte each)
/// 4. **Search**: Asymmetric Distance Computation (ADC) using lookup tables
///
/// **Memory Layout**:
/// - Codebooks: M × 256 × (d/M) floats = M × 256 × dsub floats
/// - Codes: M bytes per vector
///
/// **Complexity**:
/// - Training: O(n × M × 256 × dsub × iterations)
/// - Encoding: O(M × 256 × dsub) per vector
/// - Search: O(M × 256 × dsub) precompute + O(n × M) scan
public struct ProductQuantizer: Sendable {
    /// Number of subquantizers
    public let m: Int

    /// Number of centroids per subspace (256)
    public let ksub: Int

    /// Total vector dimensions
    public let dimensions: Int

    /// Dimension of each subspace
    public let dsub: Int

    /// K-means training iterations
    public let niter: Int

    /// Codebooks: M arrays of 256 centroids, each centroid is dsub floats
    /// Shape: [m][ksub][dsub]
    private var codebooks: [[[Float]]]

    /// Whether the quantizer has been trained
    public var isTrained: Bool { !codebooks.isEmpty && codebooks[0].count == ksub }

    /// Create a product quantizer
    ///
    /// - Parameters:
    ///   - dimensions: Total vector dimensions (must be divisible by m)
    ///   - parameters: PQ parameters
    public init(dimensions: Int, parameters: PQParameters = .default) {
        precondition(dimensions % parameters.m == 0,
            "Dimensions (\(dimensions)) must be divisible by m (\(parameters.m))")

        self.dimensions = dimensions
        self.m = parameters.m
        self.ksub = parameters.ksub
        self.dsub = dimensions / parameters.m
        self.niter = parameters.niter
        self.codebooks = []
    }

    /// Create a product quantizer with pre-trained codebooks
    ///
    /// - Parameters:
    ///   - dimensions: Total vector dimensions
    ///   - codebooks: Pre-trained codebooks [m][ksub][dsub]
    public init(dimensions: Int, codebooks: [[[Float]]]) {
        self.dimensions = dimensions
        self.m = codebooks.count
        self.ksub = codebooks.first?.count ?? 256
        self.dsub = dimensions / m
        self.niter = 0
        self.codebooks = codebooks
    }

    // MARK: - Training

    /// Train codebooks from training vectors
    ///
    /// - Parameter vectors: Training vectors [n][d]
    /// - Returns: Trained ProductQuantizer
    public func train(vectors: [[Float]]) -> ProductQuantizer {
        guard !vectors.isEmpty else {
            return self
        }

        // Train each subquantizer independently
        var trainedCodebooks: [[[Float]]] = []

        for subIndex in 0..<m {
            // Extract subvectors for this subspace
            let subvectors = vectors.map { vector in
                extractSubvector(from: vector, subIndex: subIndex)
            }

            // Train K-means on this subspace
            let clustering = SubspaceKMeans(
                k: ksub,
                dimensions: dsub,
                maxIterations: niter
            )
            let centroids = clustering.train(vectors: subvectors)
            trainedCodebooks.append(centroids)
        }

        return ProductQuantizer(dimensions: dimensions, codebooks: trainedCodebooks)
    }

    // MARK: - Encoding

    /// Encode a vector to PQ codes
    ///
    /// - Parameter vector: Vector to encode [d]
    /// - Returns: PQ codes [m] (each in 0-255)
    public func encode(vector: [Float]) -> [UInt8] {
        precondition(isTrained, "Quantizer must be trained before encoding")
        precondition(vector.count == dimensions,
            "Vector dimension mismatch: expected \(dimensions), got \(vector.count)")

        var codes: [UInt8] = []
        codes.reserveCapacity(m)

        for subIndex in 0..<m {
            let subvector = extractSubvector(from: vector, subIndex: subIndex)
            let nearestIdx = findNearestCentroid(subvector: subvector, subIndex: subIndex)
            codes.append(UInt8(nearestIdx))
        }

        return codes
    }

    /// Decode PQ codes back to approximate vector
    ///
    /// - Parameter codes: PQ codes [m]
    /// - Returns: Reconstructed vector [d]
    public func decode(codes: [UInt8]) -> [Float] {
        precondition(isTrained, "Quantizer must be trained before decoding")
        precondition(codes.count == m,
            "Code length mismatch: expected \(m), got \(codes.count)")

        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for (subIndex, code) in codes.enumerated() {
            let centroid = codebooks[subIndex][Int(code)]
            vector.append(contentsOf: centroid)
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Precompute distance table for a query vector (ADC)
    ///
    /// The distance table contains distances from each query subvector
    /// to all centroids in that subspace.
    ///
    /// - Parameter query: Query vector [d]
    /// - Returns: Distance table [m][ksub]
    public func computeDistanceTable(query: [Float]) -> [[Float]] {
        precondition(isTrained, "Quantizer must be trained")
        precondition(query.count == dimensions,
            "Query dimension mismatch: expected \(dimensions), got \(query.count)")

        var table: [[Float]] = []
        table.reserveCapacity(m)

        for subIndex in 0..<m {
            let querySubvector = extractSubvector(from: query, subIndex: subIndex)
            var distances: [Float] = []
            distances.reserveCapacity(ksub)

            for centroid in codebooks[subIndex] {
                let dist = euclideanDistanceSquared(querySubvector, centroid)
                distances.append(dist)
            }
            table.append(distances)
        }

        return table
    }

    /// Compute distance using precomputed table (ADC)
    ///
    /// - Parameters:
    ///   - codes: PQ codes for a database vector
    ///   - table: Precomputed distance table from query
    /// - Returns: Squared Euclidean distance (approximate)
    public func computeDistance(codes: [UInt8], table: [[Float]]) -> Float {
        var distance: Float = 0
        for (subIndex, code) in codes.enumerated() {
            distance += table[subIndex][Int(code)]
        }
        return distance
    }

    /// Compute distance directly (slower, for verification)
    ///
    /// - Parameters:
    ///   - query: Query vector
    ///   - codes: PQ codes for database vector
    /// - Returns: Squared Euclidean distance (approximate)
    public func computeDistanceDirect(query: [Float], codes: [UInt8]) -> Float {
        let reconstructed = decode(codes: codes)
        return euclideanDistanceSquared(query, reconstructed)
    }

    // MARK: - Codebook Access

    /// Get all codebooks for serialization
    ///
    /// - Returns: Codebooks [m][ksub][dsub]
    public func getCodebooks() -> [[[Float]]] {
        return codebooks
    }

    /// Get a specific centroid
    ///
    /// - Parameters:
    ///   - subIndex: Subspace index (0 to m-1)
    ///   - centroidIndex: Centroid index (0 to ksub-1)
    /// - Returns: Centroid vector [dsub]
    public func getCentroid(subIndex: Int, centroidIndex: Int) -> [Float] {
        precondition(isTrained, "Quantizer must be trained")
        precondition(subIndex < m, "subIndex out of range")
        precondition(centroidIndex < ksub, "centroidIndex out of range")
        return codebooks[subIndex][centroidIndex]
    }

    // MARK: - Private Methods

    /// Extract subvector for a specific subspace
    private func extractSubvector(from vector: [Float], subIndex: Int) -> [Float] {
        let start = subIndex * dsub
        let end = start + dsub
        return Array(vector[start..<end])
    }

    /// Find nearest centroid in a subspace
    private func findNearestCentroid(subvector: [Float], subIndex: Int) -> Int {
        var bestIdx = 0
        var bestDist = Float.infinity

        for (idx, centroid) in codebooks[subIndex].enumerated() {
            let dist = euclideanDistanceSquared(subvector, centroid)
            if dist < bestDist {
                bestDist = dist
                bestIdx = idx
            }
        }

        return bestIdx
    }

    /// Squared Euclidean distance
    private func euclideanDistanceSquared(_ v1: [Float], _ v2: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<min(v1.count, v2.count) {
            let diff = v1[i] - v2[i]
            sum += diff * diff
        }
        return sum
    }
}

// MARK: - Subspace K-Means

/// K-means clustering for a single subspace
///
/// Simpler than the full KMeansClustering, optimized for PQ training.
private struct SubspaceKMeans {
    let k: Int
    let dimensions: Int
    let maxIterations: Int

    init(k: Int, dimensions: Int, maxIterations: Int) {
        self.k = k
        self.dimensions = dimensions
        self.maxIterations = maxIterations
    }

    /// Train centroids
    func train(vectors: [[Float]]) -> [[Float]] {
        guard vectors.count >= k else {
            // Not enough vectors, pad with random duplicates
            var centroids = vectors
            while centroids.count < k {
                let idx = Int.random(in: 0..<vectors.count)
                centroids.append(vectors[idx])
            }
            return centroids
        }

        // K-means++ initialization
        var centroids = kMeansPlusPlusInit(vectors: vectors)

        for _ in 0..<maxIterations {
            // Assignment
            let assignments = assign(vectors: vectors, centroids: centroids)

            // Update centroids
            let newCentroids = updateCentroids(vectors: vectors, assignments: assignments)

            // Check convergence
            if hasConverged(old: centroids, new: newCentroids) {
                return newCentroids
            }
            centroids = newCentroids
        }

        return centroids
    }

    private func kMeansPlusPlusInit(vectors: [[Float]]) -> [[Float]] {
        var centroids: [[Float]] = []

        // First centroid: random
        let firstIdx = Int.random(in: 0..<vectors.count)
        centroids.append(vectors[firstIdx])

        // Subsequent centroids
        for _ in 1..<k {
            var distances: [Float] = []
            var total: Float = 0

            for vector in vectors {
                let minDist = centroids.map { euclideanDistanceSquared(vector, $0) }.min() ?? 0
                distances.append(minDist)
                total += minDist
            }

            if total > 0 {
                var target = Float.random(in: 0..<total)
                for (i, dist) in distances.enumerated() {
                    target -= dist
                    if target <= 0 {
                        centroids.append(vectors[i])
                        break
                    }
                }
            } else {
                let idx = Int.random(in: 0..<vectors.count)
                centroids.append(vectors[idx])
            }
        }

        return centroids
    }

    private func assign(vectors: [[Float]], centroids: [[Float]]) -> [Int] {
        vectors.map { vector in
            var bestIdx = 0
            var bestDist = Float.infinity
            for (i, c) in centroids.enumerated() {
                let d = euclideanDistanceSquared(vector, c)
                if d < bestDist {
                    bestDist = d
                    bestIdx = i
                }
            }
            return bestIdx
        }
    }

    private func updateCentroids(vectors: [[Float]], assignments: [Int]) -> [[Float]] {
        var sums: [[Float]] = Array(repeating: Array(repeating: 0, count: dimensions), count: k)
        var counts: [Int] = Array(repeating: 0, count: k)

        for (i, assignment) in assignments.enumerated() {
            for d in 0..<dimensions {
                sums[assignment][d] += vectors[i][d]
            }
            counts[assignment] += 1
        }

        var centroids: [[Float]] = []
        for i in 0..<k {
            if counts[i] > 0 {
                let centroid = sums[i].map { $0 / Float(counts[i]) }
                centroids.append(centroid)
            } else {
                let idx = Int.random(in: 0..<vectors.count)
                centroids.append(vectors[idx])
            }
        }

        return centroids
    }

    private func hasConverged(old: [[Float]], new: [[Float]]) -> Bool {
        let threshold: Float = 1e-4
        for (o, n) in zip(old, new) {
            if sqrt(euclideanDistanceSquared(o, n)) > threshold {
                return false
            }
        }
        return true
    }

    private func euclideanDistanceSquared(_ v1: [Float], _ v2: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<min(v1.count, v2.count) {
            let diff = v1[i] - v2[i]
            sum += diff * diff
        }
        return sum
    }
}

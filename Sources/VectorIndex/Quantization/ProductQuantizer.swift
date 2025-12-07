// ProductQuantizer.swift
// VectorIndex/Quantization - Product Quantization implementation
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
// https://lear.inrialpes.fr/pubs/2011/JDS11/jegou_searching_with_quantization.pdf

import Foundation
import Synchronization
import Vector

// MARK: - ProductQuantizer

/// Product Quantization implementation
///
/// PQ divides a D-dimensional vector into M subvectors of D/M dimensions each.
/// Each subvector is independently quantized to one of K centroids.
///
/// **Algorithm**:
/// 1. Split vector into M subvectors
/// 2. For each subspace, find nearest centroid (from K options)
/// 3. Store centroid indices as code (M bytes when K=256)
///
/// **Asymmetric Distance Computation (ADC)**:
/// For query q and database vector x encoded as code c:
/// ```
/// distance(q, x) ≈ sum_{m=1}^{M} d(q_m, centroid[m][c_m])
/// ```
/// Precompute d(q_m, centroid[m][k]) for all m,k in O(M*K*D/M) = O(K*D)
/// Then each distance is O(M) table lookups.
///
/// **Thread Safety**:
/// After training, all operations are thread-safe.
/// Training itself is not thread-safe.
public final class ProductQuantizer: VectorQuantizer, @unchecked Sendable {
    public typealias Code = [UInt8]

    // MARK: - Configuration

    private let config: PQConfig
    public let dimensions: Int
    private let subspaceDim: Int

    // MARK: - Learned Parameters

    /// Codebook: [M][K][subspaceDim]
    /// Protected by mutex for thread-safe access
    private struct State: Sendable {
        var codebook: [[[Float]]] = []
        var trained: Bool = false
    }

    private let state: Mutex<State>

    // MARK: - Computed Properties

    public var isTrained: Bool {
        state.withLock { $0.trained }
    }

    public var codeSize: Int {
        config.numSubquantizers
    }

    /// Number of subquantizers (M)
    public var numSubquantizers: Int {
        config.numSubquantizers
    }

    /// Number of centroids per subquantizer (K)
    public var numCentroids: Int {
        config.numCentroids
    }

    // MARK: - Initialization

    /// Create a Product Quantizer
    ///
    /// - Parameters:
    ///   - config: PQ configuration
    ///   - dimensions: Vector dimensions (must be divisible by numSubquantizers)
    ///
    /// - Note: PQ uses Euclidean distance for ADC (Asymmetric Distance Computation).
    ///   For Cosine similarity, L2-normalize vectors before quantization.
    ///   Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
    public init(config: PQConfig, dimensions: Int) {
        precondition(dimensions > 0, "Dimensions must be positive")
        precondition(dimensions % config.numSubquantizers == 0,
                     "Dimensions (\(dimensions)) must be divisible by numSubquantizers (\(config.numSubquantizers))")

        self.config = config
        self.dimensions = dimensions
        self.subspaceDim = dimensions / config.numSubquantizers
        self.state = Mutex(State())
    }

    // MARK: - Training

    /// Train the quantizer using k-means clustering
    ///
    /// For each subspace, runs k-means to learn K centroids.
    ///
    /// - Parameter vectors: Training vectors (should be representative sample)
    public func train(vectors: [[Float]]) async throws {
        guard !vectors.isEmpty else {
            throw QuantizerError.trainingFailed("No training vectors provided")
        }

        // Validate dimensions
        for (i, v) in vectors.enumerated() {
            guard v.count == dimensions else {
                throw QuantizerError.dimensionMismatch(expected: dimensions, actual: v.count)
            }
            if i >= config.trainingSampleSize { break }
        }

        // Sample training data if needed
        let sampleVectors: [[Float]]
        if vectors.count > config.trainingSampleSize {
            sampleVectors = Array(vectors.shuffled().prefix(config.trainingSampleSize))
        } else {
            sampleVectors = vectors
        }

        // Train each subspace independently
        var newCodebook: [[[Float]]] = []

        for m in 0..<config.numSubquantizers {
            // Extract subvectors for this subspace
            let subvectors = sampleVectors.map { vector -> [Float] in
                let start = m * subspaceDim
                let end = start + subspaceDim
                return Array(vector[start..<end])
            }

            // Run k-means clustering
            let centroids = try kMeans(
                vectors: subvectors,
                k: config.numCentroids,
                maxIterations: config.kmeansIterations
            )

            newCodebook.append(centroids)
        }

        // Update state atomically
        state.withLock { state in
            state.codebook = newCodebook
            state.trained = true
        }
    }

    // MARK: - Encoding

    /// Encode a vector to PQ code
    ///
    /// For each subspace, finds the nearest centroid.
    ///
    /// - Parameter vector: Input vector
    /// - Returns: Array of centroid indices (M bytes)
    public func encode(_ vector: [Float]) throws -> Code {
        guard vector.count == dimensions else {
            throw QuantizerError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let codebook = state.withLock { state -> [[[Float]]] in
            guard state.trained else { return [] }
            return state.codebook
        }

        guard !codebook.isEmpty else {
            throw QuantizerError.notTrained
        }

        var code = Code(repeating: 0, count: config.numSubquantizers)

        for m in 0..<config.numSubquantizers {
            let start = m * subspaceDim
            let end = start + subspaceDim
            let subvector = Array(vector[start..<end])

            // Find nearest centroid
            var minDist = Float.infinity
            var minIdx = 0

            for (k, centroid) in codebook[m].enumerated() {
                let dist = squaredEuclideanDistance(subvector, centroid)
                if dist < minDist {
                    minDist = dist
                    minIdx = k
                }
            }

            code[m] = UInt8(minIdx)
        }

        return code
    }

    // MARK: - Decoding

    /// Decode PQ code to approximate vector
    ///
    /// Concatenates centroids from each subspace.
    ///
    /// - Parameter code: PQ code (M bytes)
    /// - Returns: Reconstructed vector
    public func decode(_ code: Code) throws -> [Float] {
        guard code.count == config.numSubquantizers else {
            throw QuantizerError.invalidCode("Code length \(code.count) != numSubquantizers \(config.numSubquantizers)")
        }

        let codebook = state.withLock { state -> [[[Float]]] in
            guard state.trained else { return [] }
            return state.codebook
        }

        guard !codebook.isEmpty else {
            throw QuantizerError.notTrained
        }

        var vector = [Float](repeating: 0, count: dimensions)

        for m in 0..<config.numSubquantizers {
            let centroidIdx = Int(code[m])
            guard centroidIdx < codebook[m].count else {
                throw QuantizerError.invalidCode("Centroid index \(centroidIdx) out of range for subspace \(m)")
            }

            let centroid = codebook[m][centroidIdx]
            let start = m * subspaceDim

            for i in 0..<subspaceDim {
                vector[start + i] = centroid[i]
            }
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Prepare query for ADC (Asymmetric Distance Computation)
    ///
    /// Precomputes distance table: distTable[m][k] = d(q_m, centroid[m][k])
    ///
    /// - Parameter query: Query vector
    /// - Returns: Prepared query with distance tables
    public func prepareQuery(_ query: [Float]) throws -> PreparedQuery {
        guard query.count == dimensions else {
            throw QuantizerError.dimensionMismatch(expected: dimensions, actual: query.count)
        }

        let codebook = state.withLock { state -> [[[Float]]] in
            guard state.trained else { return [] }
            return state.codebook
        }

        guard !codebook.isEmpty else {
            throw QuantizerError.notTrained
        }

        // Compute distance tables (squared Euclidean distances)
        var distanceTables: [[Float]] = []
        distanceTables.reserveCapacity(config.numSubquantizers)

        for m in 0..<config.numSubquantizers {
            let start = m * subspaceDim
            let querySubvector = Array(query[start..<(start + subspaceDim)])

            var table = [Float](repeating: 0, count: config.numCentroids)
            for k in 0..<config.numCentroids {
                table[k] = squaredEuclideanDistance(querySubvector, codebook[m][k])
            }

            distanceTables.append(table)
        }

        return PreparedQuery(storage: .pq(distanceTables: distanceTables))
    }

    /// Compute distance using prepared query tables
    ///
    /// O(M) table lookups instead of O(D) vector operations.
    ///
    /// - Parameters:
    ///   - prepared: Prepared query from prepareQuery()
    ///   - code: PQ code
    /// - Returns: Euclidean distance
    public func distanceWithPrepared(_ prepared: PreparedQuery, code: Code) -> Float {
        guard case .pq(let distanceTables) = prepared.storage else {
            return Float.infinity
        }

        var squaredDistance: Float = 0
        for m in 0..<config.numSubquantizers {
            squaredDistance += distanceTables[m][Int(code[m])]
        }

        return sqrt(squaredDistance)
    }

    // MARK: - Serialization

    /// Serialize quantizer state
    ///
    /// Format:
    /// - Magic bytes: "PQ02"
    /// - dimensions: Int32
    /// - numSubquantizers: Int32
    /// - numCentroids: Int32
    /// - subspaceDim: Int32
    /// - codebook: M * K * subspaceDim floats
    public func serialize() throws -> Data {
        let currentState = state.withLock { $0 }

        guard currentState.trained else {
            throw QuantizerError.notTrained
        }

        var data = Data()

        // Magic bytes (version 2 - removed metric field)
        data.append(contentsOf: "PQ02".utf8)

        // Header
        var dims = Int32(dimensions)
        var m = Int32(config.numSubquantizers)
        var k = Int32(config.numCentroids)
        var sd = Int32(subspaceDim)

        withUnsafeBytes(of: &dims) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: &m) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: &k) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: &sd) { data.append(contentsOf: $0) }

        // Codebook
        for subspace in currentState.codebook {
            for centroid in subspace {
                for value in centroid {
                    var v = value
                    withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
                }
            }
        }

        return data
    }

    /// Deserialize quantizer state
    public func deserialize(from data: Data) throws {
        guard data.count >= 20 else {
            throw QuantizerError.deserializationFailed("Data too short")
        }

        // Check magic
        let magic = String(data: data.prefix(4), encoding: .utf8)
        guard magic == "PQ02" else {
            throw QuantizerError.deserializationFailed("Invalid magic bytes. Expected PQ02.")
        }

        var offset = 4

        // Read header
        let dims = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4
        let m = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4
        let k = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4
        let sd = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Int32.self) }
        offset += 4

        // Validate all parameters
        guard Int(dims) == dimensions else {
            throw QuantizerError.deserializationFailed("Dimension mismatch: expected \(dimensions), got \(dims)")
        }
        guard Int(m) == config.numSubquantizers else {
            throw QuantizerError.deserializationFailed("numSubquantizers mismatch: expected \(config.numSubquantizers), got \(m)")
        }
        guard Int(k) == config.numCentroids else {
            throw QuantizerError.deserializationFailed("numCentroids mismatch: expected \(config.numCentroids), got \(k)")
        }
        guard Int(sd) == subspaceDim else {
            throw QuantizerError.deserializationFailed("subspaceDim mismatch: expected \(subspaceDim), got \(sd)")
        }

        // Read codebook
        let expectedBytes = Int(m) * Int(k) * Int(sd) * 4
        guard data.count >= offset + expectedBytes else {
            throw QuantizerError.deserializationFailed("Codebook data too short")
        }

        var codebook: [[[Float]]] = []
        codebook.reserveCapacity(Int(m))

        for _ in 0..<m {
            var subspace: [[Float]] = []
            subspace.reserveCapacity(Int(k))
            for _ in 0..<k {
                var centroid: [Float] = []
                centroid.reserveCapacity(Int(sd))
                for _ in 0..<sd {
                    let value = data.withUnsafeBytes { $0.load(fromByteOffset: offset, as: Float.self) }
                    centroid.append(value)
                    offset += 4
                }
                subspace.append(centroid)
            }
            codebook.append(subspace)
        }

        state.withLock { state in
            state.codebook = codebook
            state.trained = true
        }
    }

    // MARK: - Private Methods

    /// K-means clustering for one subspace
    private func kMeans(vectors: [[Float]], k: Int, maxIterations: Int) throws -> [[Float]] {
        guard !vectors.isEmpty else {
            throw QuantizerError.trainingFailed("No vectors for k-means")
        }

        let dim = vectors[0].count

        // Initialize centroids (k-means++ initialization)
        var centroids = kMeansPlusPlusInit(vectors: vectors, k: k)

        // Iterate
        for _ in 0..<maxIterations {
            // Assignment step: assign each vector to nearest centroid
            var assignments = [Int](repeating: 0, count: vectors.count)
            for (i, v) in vectors.enumerated() {
                var minDist = Float.infinity
                var minIdx = 0
                for (j, c) in centroids.enumerated() {
                    let dist = squaredEuclideanDistance(v, c)
                    if dist < minDist {
                        minDist = dist
                        minIdx = j
                    }
                }
                assignments[i] = minIdx
            }

            // Update step: compute new centroids
            var newCentroids = [[Float]](repeating: [Float](repeating: 0, count: dim), count: k)
            var counts = [Int](repeating: 0, count: k)

            for (i, v) in vectors.enumerated() {
                let cluster = assignments[i]
                counts[cluster] += 1
                for d in 0..<dim {
                    newCentroids[cluster][d] += v[d]
                }
            }

            // Normalize and handle empty clusters
            for j in 0..<k {
                if counts[j] > 0 {
                    for d in 0..<dim {
                        newCentroids[j][d] /= Float(counts[j])
                    }
                } else {
                    // Empty cluster: reinitialize from random vector
                    newCentroids[j] = vectors.randomElement() ?? centroids[j]
                }
            }

            centroids = newCentroids
        }

        return centroids
    }

    /// K-means++ initialization
    ///
    /// Reference: Arthur & Vassilvitskii, "k-means++: The Advantages of Careful Seeding", SODA 2007
    private func kMeansPlusPlusInit(vectors: [[Float]], k: Int) -> [[Float]] {
        guard !vectors.isEmpty else { return [] }
        guard k > 0 else { return [] }

        var centroids: [[Float]] = []
        centroids.reserveCapacity(k)

        // First centroid: random
        centroids.append(vectors.randomElement()!)

        // Subsequent centroids: probability proportional to squared distance
        while centroids.count < k {
            var distances = [Float](repeating: 0, count: vectors.count)
            var totalDist: Float = 0

            for (i, v) in vectors.enumerated() {
                var minDist = Float.infinity
                for c in centroids {
                    let dist = squaredEuclideanDistance(v, c)
                    minDist = min(minDist, dist)
                }
                distances[i] = minDist
                totalDist += minDist
            }

            // Handle edge case where all distances are zero
            if totalDist < QuantizerConstants.floatTolerance {
                // All remaining vectors are duplicates of existing centroids
                // Add random vectors to fill remaining slots
                while centroids.count < k {
                    centroids.append(vectors.randomElement()!)
                }
                break
            }

            // Sample proportional to distance
            let threshold = Float.random(in: 0..<totalDist)
            var cumulative: Float = 0
            var added = false

            for (i, d) in distances.enumerated() {
                cumulative += d
                if cumulative >= threshold {
                    centroids.append(vectors[i])
                    added = true
                    break
                }
            }

            // Fallback: should not happen, but ensure we always add a centroid
            if !added {
                centroids.append(vectors.randomElement()!)
            }
        }

        return centroids
    }

    /// Squared Euclidean distance (no sqrt for efficiency)
    private func squaredEuclideanDistance(_ a: [Float], _ b: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<min(a.count, b.count) {
            let diff = a[i] - b[i]
            sum += diff * diff
        }
        return sum
    }
}

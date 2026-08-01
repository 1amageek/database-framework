// ProductQuantizer.swift
// VectorIndex - Product Quantization for vector compression
//
// Reference: Jégou et al., "Product Quantization for Nearest Neighbor Search",
// IEEE Transactions on Pattern Analysis and Machine Intelligence, 2011

import DatabaseMath
import DatabaseKit

/// Query-specific lookup data for product-quantized distance evaluation.
///
/// The table owns one scalar contribution per centroid. Search evaluates a
/// stored code by indexing these contributions without reconstructing a vector.
public struct ProductQuantizedDistanceTable: Sendable {
    let metric: VectorMetric
    let contributions: [Double]
    let queryNormSquared: Double
    let centroidNormsSquared: [Double]
    let subquantizerCount: Int
    let centroidCount: Int
}

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
    public init(
        dimensions: Int,
        parameters: PQParameters = .default
    ) throws(ProductQuantizationError) {
        guard dimensions > 0 else {
            throw .invalidDimensions(dimensions)
        }
        guard dimensions % parameters.m == 0 else {
            throw .incompatibleSubspaceCount(
                dimensions: dimensions,
                subquantizers: parameters.m
            )
        }

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
    public init(
        dimensions: Int,
        codebooks: [[[Float]]]
    ) throws(ProductQuantizationError) {
        guard dimensions > 0 else {
            throw .invalidDimensions(dimensions)
        }
        guard !codebooks.isEmpty else {
            throw .emptyCodebooks
        }
        guard dimensions % codebooks.count == 0 else {
            throw .incompatibleSubspaceCount(
                dimensions: dimensions,
                subquantizers: codebooks.count
            )
        }
        let centroidCount = codebooks[0].count
        guard (1...256).contains(centroidCount) else {
            throw .invalidCentroidCount(centroidCount)
        }
        let subspaceDimensions = dimensions / codebooks.count
        for (subspaceIndex, codebook) in codebooks.enumerated() {
            guard codebook.count == centroidCount else {
                throw .inconsistentCentroidCount(
                    subspace: subspaceIndex,
                    expected: centroidCount,
                    actual: codebook.count
                )
            }
            for (centroidIndex, centroid) in codebook.enumerated() {
                guard centroid.count == subspaceDimensions else {
                    throw .centroidDimensionMismatch(
                        subspace: subspaceIndex,
                        centroid: centroidIndex,
                        expected: subspaceDimensions,
                        actual: centroid.count
                    )
                }
                if let elementIndex = centroid.firstIndex(where: {
                    !$0.isFinite
                }) {
                    throw .nonFiniteCentroidElement(
                        subspace: subspaceIndex,
                        centroid: centroidIndex,
                        element: elementIndex
                    )
                }
            }
        }

        self.dimensions = dimensions
        self.m = codebooks.count
        self.ksub = centroidCount
        self.dsub = subspaceDimensions
        self.niter = 0
        self.codebooks = codebooks
    }

    // MARK: - Training

    /// Train codebooks from training vectors
    ///
    /// - Parameter vectors: Training vectors [n][d]
    /// - Returns: Trained ProductQuantizer
    public func train(
        vectors: [[Float]]
    ) throws(ProductQuantizationError) -> ProductQuantizer {
        guard !vectors.isEmpty else {
            throw .emptyTrainingSet
        }
        for (vectorIndex, vector) in vectors.enumerated() {
            guard vector.count == dimensions else {
                throw .vectorDimensionMismatch(
                    expected: dimensions,
                    actual: vector.count
                )
            }
            if let elementIndex = vector.firstIndex(where: {
                !$0.isFinite
            }) {
                throw .nonFiniteTrainingElement(
                    vector: vectorIndex,
                    element: elementIndex
                )
            }
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

        return try ProductQuantizer(
            dimensions: dimensions,
            codebooks: trainedCodebooks
        )
    }

    // MARK: - Encoding

    /// Encode a vector to PQ codes
    ///
    /// - Parameter vector: Vector to encode [d]
    /// - Returns: PQ codes [m] (each in 0-255)
    public func encode(
        vector: [Float]
    ) throws(ProductQuantizationError) -> [UInt8] {
        guard isTrained else {
            throw .untrained
        }
        guard vector.count == dimensions else {
            throw .vectorDimensionMismatch(
                expected: dimensions,
                actual: vector.count
            )
        }
        try validateFiniteInput(vector)

        var codes: [UInt8] = []
        codes.reserveCapacity(m)

        for subIndex in 0..<m {
            let nearestIdx = findNearestCentroid(in: vector, subIndex: subIndex)
            codes.append(UInt8(nearestIdx))
        }

        return codes
    }

    /// Decode PQ codes back to approximate vector
    ///
    /// - Parameter codes: PQ codes [m]
    /// - Returns: Reconstructed vector [d]
    public func decode(
        codes: [UInt8]
    ) throws(ProductQuantizationError) -> [Float] {
        try validate(codes)

        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for (subIndex, code) in codes.enumerated() {
            let centroid = codebooks[subIndex][Int(code)]
            vector.append(contentsOf: centroid)
        }

        return vector
    }

    // MARK: - Distance Computation

    /// Build a metric-specific distance table for a query vector.
    ///
    /// Euclidean search stores squared-distance contributions. Dot-product and
    /// cosine search store dot-product contributions; cosine also stores each
    /// centroid norm so the reconstructed vector norm can be evaluated without
    /// materializing the reconstructed vector.
    ///
    /// - Parameters:
    ///   - query: Query vector [d]
    ///   - metric: Distance metric used by the index.
    /// - Returns: Lookup data for evaluating compressed codes.
    public func distanceTable(
        for query: [Float],
        metric: VectorMetric
    ) throws(ProductQuantizationError) -> ProductQuantizedDistanceTable {
        guard isTrained else {
            throw .untrained
        }
        guard query.count == dimensions else {
            throw .vectorDimensionMismatch(
                expected: dimensions,
                actual: query.count
            )
        }
        try validateFiniteInput(query)

        let (tableEntryCount, tableSizeOverflow) = m
            .multipliedReportingOverflow(by: ksub)
        guard !tableSizeOverflow else {
            throw .incompatibleDistanceTable
        }
        var contributions: [Double] = []
        contributions.reserveCapacity(tableEntryCount)
        var centroidNormsSquared: [Double] = []
        if metric == .cosine {
            centroidNormsSquared.reserveCapacity(tableEntryCount)
        }

        var queryNormSquared = 0.0
        if metric == .cosine {
            for component in query {
                let widened = Double(component)
                queryNormSquared += widened * widened
            }
        }

        for subIndex in 0..<m {
            let queryOffset = subIndex * dsub
            for centroid in codebooks[subIndex] {
                var contribution = 0.0
                var centroidNormSquared = 0.0
                for componentIndex in 0..<dsub {
                    let queryComponent = Double(
                        query[queryOffset + componentIndex]
                    )
                    let centroidComponent = Double(centroid[componentIndex])
                    switch metric {
                    case .euclidean:
                        let difference = queryComponent - centroidComponent
                        contribution += difference * difference
                    case .cosine, .dotProduct:
                        contribution += queryComponent * centroidComponent
                    }
                    if metric == .cosine {
                        centroidNormSquared += centroidComponent * centroidComponent
                    }
                }
                contributions.append(contribution)
                if metric == .cosine {
                    centroidNormsSquared.append(centroidNormSquared)
                }
            }
        }

        return ProductQuantizedDistanceTable(
            metric: metric,
            contributions: contributions,
            queryNormSquared: queryNormSquared,
            centroidNormsSquared: centroidNormsSquared,
            subquantizerCount: m,
            centroidCount: ksub
        )
    }

    /// Evaluate compressed codes using a query-specific distance table.
    ///
    /// - Parameters:
    ///   - codes: PQ codes for a stored vector.
    ///   - table: Lookup data returned by `distanceTable(for:metric:)`.
    /// - Returns: Approximate distance with the same meaning as the selected metric.
    public func distance<Codes: RandomAccessCollection>(
        for codes: Codes,
        using table: ProductQuantizedDistanceTable
    ) throws(ProductQuantizationError) -> Double where Codes.Element == UInt8, Codes.Index == Int {
        try validate(codes)
        let (expectedEntryCount, entryCountOverflow) = m
            .multipliedReportingOverflow(by: ksub)
        guard table.subquantizerCount == m,
              table.centroidCount == ksub,
              !entryCountOverflow,
              table.contributions.count == expectedEntryCount,
              table.metric != .cosine || (
                table.centroidNormsSquared.count == expectedEntryCount
              ) else {
            throw .incompatibleDistanceTable
        }

        var contribution = 0.0
        var reconstructedNormSquared = 0.0
        for (subIndex, code) in codes.enumerated() {
            let tableIndex = subIndex * ksub + Int(code)
            contribution += table.contributions[tableIndex]
            if table.metric == .cosine {
                reconstructedNormSquared += table.centroidNormsSquared[
                    tableIndex
                ]
            }
        }

        switch table.metric {
        case .euclidean:
            return DatabaseMath.squareRoot(contribution)
        case .dotProduct:
            return -contribution
        case .cosine:
            return productQuantizedCosineDistance(
                dotProduct: contribution,
                queryNormSquared: table.queryNormSquared,
                reconstructedNormSquared: reconstructedNormSquared
            )
        }
    }

    /// Reconstruct codes and evaluate squared Euclidean distance.
    ///
    /// - Parameters:
    ///   - query: Query vector
    ///   - codes: PQ codes for database vector
    /// - Returns: Squared Euclidean distance (approximate)
    public func squaredEuclideanDistance(
        from query: [Float],
        to codes: [UInt8]
    ) throws(ProductQuantizationError) -> Double {
        guard query.count == dimensions else {
            throw .vectorDimensionMismatch(
                expected: dimensions,
                actual: query.count
            )
        }
        try validateFiniteInput(query)
        let reconstructed = try decode(codes: codes)
        return VectorConversion.euclideanDistanceSquared(query, reconstructed)
    }

    // MARK: - Codebook Access

    /// Return all trained codebooks for persistence.
    ///
    /// - Returns: Codebooks [m][ksub][dsub]
    public var trainedCodebooks: [[[Float]]] { codebooks }

    /// Return a centroid from a trained subspace.
    ///
    /// - Parameters:
    ///   - subspace: Subspace index.
    ///   - index: Centroid index within the subspace.
    /// - Returns: Centroid vector [dsub]
    public func centroid(
        in subspace: Int,
        at index: Int
    ) throws(ProductQuantizationError) -> [Float] {
        guard isTrained else {
            throw .untrained
        }
        guard codebooks.indices.contains(subspace) else {
            throw .subspaceOutOfRange(subspace)
        }
        guard codebooks[subspace].indices.contains(index) else {
            throw .centroidOutOfRange(subspace: subspace, centroid: index)
        }
        return codebooks[subspace][index]
    }

    // MARK: - Private Methods

    /// Extract subvector for a specific subspace
    private func extractSubvector(
        from vector: [Float],
        subIndex: Int
    ) -> ArraySlice<Float> {
        let start = subIndex * dsub
        let end = start + dsub
        return vector[start..<end]
    }

    /// Find the nearest centroid without materializing the source subvector.
    private func findNearestCentroid(in vector: [Float], subIndex: Int) -> Int {
        let vectorOffset = subIndex * dsub
        var bestIdx = 0
        var bestDist = Double.infinity

        for (idx, centroid) in codebooks[subIndex].enumerated() {
            var dist = 0.0
            for componentIndex in 0..<dsub {
                let difference = Double(
                    vector[vectorOffset + componentIndex]
                ) - Double(centroid[componentIndex])
                dist += difference * difference
            }
            if dist < bestDist {
                bestDist = dist
                bestIdx = idx
            }
        }

        return bestIdx
    }

    private func validate<Codes: RandomAccessCollection>(
        _ codes: Codes
    ) throws(ProductQuantizationError) where Codes.Element == UInt8, Codes.Index == Int {
        guard isTrained else {
            throw .untrained
        }
        guard codes.count == m else {
            throw .codeCountMismatch(expected: m, actual: codes.count)
        }
        for (subspaceIndex, code) in codes.enumerated() {
            guard Int(code) < codebooks[subspaceIndex].count else {
                throw .centroidCodeOutOfRange(
                    subspace: subspaceIndex,
                    code: Int(code),
                    centroidCount: codebooks[subspaceIndex].count
                )
            }
        }
    }

    private func validateFiniteInput(
        _ vector: [Float]
    ) throws(ProductQuantizationError) {
        if let elementIndex = vector.firstIndex(where: {
            !$0.isFinite
        }) {
            throw .nonFiniteInputElement(elementIndex)
        }
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
    func train(vectors: [ArraySlice<Float>]) -> [[Float]] {
        guard vectors.count >= k else {
            // Not enough vectors, pad with random duplicates
            var centroids = vectors.map(Array.init)
            while centroids.count < k {
                let idx = Int.random(in: 0..<vectors.count)
                // Centroids are mutable training outputs and therefore own
                // their storage independently from the input training set.
                centroids.append(Array(vectors[idx]))
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

    private func kMeansPlusPlusInit(
        vectors: [ArraySlice<Float>]
    ) -> [[Float]] {
        var centroids: [[Float]] = []

        // First centroid: random
        let firstIdx = Int.random(in: 0..<vectors.count)
        centroids.append(Array(vectors[firstIdx]))

        // Subsequent centroids
        for _ in 1..<k {
            var distances: [Double] = []
            var total = 0.0

            for vector in vectors {
                let minDist = centroids.map {
                    squaredDistance(vector, $0)
                }.min() ?? 0
                distances.append(minDist)
                total += minDist
            }

            if total > 0 {
                var target = Double.random(in: 0..<total)
                for (i, dist) in distances.enumerated() {
                    target -= dist
                    if target <= 0 {
                        centroids.append(Array(vectors[i]))
                        break
                    }
                }
            } else {
                let idx = Int.random(in: 0..<vectors.count)
                centroids.append(Array(vectors[idx]))
            }
        }

        return centroids
    }

    private func assign(
        vectors: [ArraySlice<Float>],
        centroids: [[Float]]
    ) -> [Int] {
        vectors.map { vector in
            var bestIdx = 0
            var bestDist = Double.infinity
            for (i, c) in centroids.enumerated() {
                let d = squaredDistance(vector, c)
                if d < bestDist {
                    bestDist = d
                    bestIdx = i
                }
            }
            return bestIdx
        }
    }

    private func updateCentroids(
        vectors: [ArraySlice<Float>],
        assignments: [Int]
    ) -> [[Float]] {
        var sums: [[Double]] = Array(
            repeating: Array(repeating: 0, count: dimensions),
            count: k
        )
        var counts: [Int] = Array(repeating: 0, count: k)

        for (i, assignment) in assignments.enumerated() {
            for d in 0..<dimensions {
                let vector = vectors[i]
                sums[assignment][d] += Double(
                    vector[vector.startIndex + d]
                )
            }
            counts[assignment] += 1
        }

        var centroids: [[Float]] = []
        for i in 0..<k {
            if counts[i] > 0 {
                let centroid = sums[i].map { value in
                    Float(value / Double(counts[i]))
                }
                centroids.append(centroid)
            } else {
                let idx = Int.random(in: 0..<vectors.count)
                centroids.append(Array(vectors[idx]))
            }
        }

        return centroids
    }

    private func hasConverged(old: [[Float]], new: [[Float]]) -> Bool {
        let threshold = 1e-4
        for (o, n) in zip(old, new) {
            if DatabaseMath.squareRoot(
                VectorConversion.euclideanDistanceSquared(o, n)
            ) > threshold {
                return false
            }
        }
        return true
    }

    private func squaredDistance(
        _ vector: ArraySlice<Float>,
        _ centroid: [Float]
    ) -> Double {
        precondition(vector.count == dimensions)
        precondition(centroid.count == dimensions)
        var result = 0.0
        for offset in 0..<dimensions {
            let difference = Double(vector[vector.startIndex + offset])
                - Double(centroid[offset])
            result += difference * difference
        }
        return result
    }
}

@inline(__always)
func productQuantizedCosineDistance(
    dotProduct: Double,
    queryNormSquared: Double,
    reconstructedNormSquared: Double
) -> Double {
    let queryNorm = DatabaseMath.squareRoot(queryNormSquared)
    let reconstructedNorm = DatabaseMath.squareRoot(
        reconstructedNormSquared
    )
    // Match the canonical cosine contract and SwiftHNSW: a zero vector is
    // treated as orthogonal, yielding `1 - 0`.
    guard queryNorm > 0, reconstructedNorm > 0 else { return 1.0 }
    let similarity = min(
        1.0,
        max(-1.0, dotProduct / (queryNorm * reconstructedNorm))
    )
    return 1.0 - similarity
}

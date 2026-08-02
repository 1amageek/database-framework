// KMeansClustering.swift
// VectorIndex - K-means clustering for IVF centroid training
//
// Reference: Lloyd's algorithm with K-means++ initialization
// Arthur & Vassilvitskii, "k-means++: The Advantages of Careful Seeding", 2007

import DatabaseMath

/// K-means clustering for IVF centroid training
///
/// **Algorithm**:
/// 1. K-means++ initialization for better starting centroids
/// 2. Lloyd's algorithm for iterative refinement
/// 3. Convergence detection based on centroid movement
///
/// **Complexity**:
/// - Time: O(n × k × d × iterations)
/// - Space: O(k × d) for centroids
///
/// where n = vectors, k = clusters, d = dimensions
struct KMeansClustering: Sendable {
    /// Number of clusters
    let k: Int

    /// Vector dimensions
    let dimensions: Int

    /// Maximum iterations
    let maxIterations: Int

    /// Convergence threshold (stop if centroid movement < threshold)
    let convergenceThreshold: Double

    /// Create K-means clustering configuration
    ///
    /// - Parameters:
    ///   - k: Number of clusters
    ///   - dimensions: Vector dimensions
    ///   - maxIterations: Maximum iterations (default: 20)
    ///   - convergenceThreshold: Stop threshold (default: 1e-4)
    init(
        k: Int,
        dimensions: Int,
        maxIterations: Int = 20,
        convergenceThreshold: Double = 1e-4
    ) {
        precondition(k > 0, "k must be positive")
        precondition(dimensions > 0, "dimensions must be positive")
        precondition(maxIterations > 0, "maxIterations must be positive")

        self.k = k
        self.dimensions = dimensions
        self.maxIterations = maxIterations
        self.convergenceThreshold = convergenceThreshold
    }

    /// Train centroids from a set of vectors
    ///
    /// - Parameter vectors: Training vectors (n × d)
    /// - Returns: Trained centroids (k × d)
    func train(vectors: [[Float]]) -> [[Float]] {
        guard !vectors.isEmpty else { return [] }
        guard vectors.count >= k else {
            // Not enough vectors for k clusters, use all vectors as centroids
            return vectors
        }

        // K-means++ initialization
        var randomNumberGenerator = VectorTrainingRandomNumberGenerator(
            seed: 0x4956_465F_4B4D_5050
                ^ UInt64(truncatingIfNeeded: k)
                ^ UInt64(truncatingIfNeeded: dimensions) << 32
        )
        var centroids = kMeansPlusPlusInit(
            vectors: vectors,
            randomNumberGenerator: &randomNumberGenerator
        )

        for _ in 0..<maxIterations {
            // Assignment step: assign each vector to nearest centroid
            let assignments = assignToCentroids(vectors: vectors, centroids: centroids)

            // Update step: compute new centroids
            let newCentroids = computeCentroids(
                vectors: vectors,
                assignments: assignments,
                randomNumberGenerator: &randomNumberGenerator
            )

            // Check convergence
            if hasConverged(oldCentroids: centroids, newCentroids: newCentroids) {
                centroids = newCentroids
                break
            }

            centroids = newCentroids
        }

        return centroids
    }

    /// Assign a single vector to its nearest centroid
    ///
    /// - Parameters:
    ///   - vector: Vector to assign
    ///   - centroids: Current centroids
    /// - Returns: Index of nearest centroid
    private func assignToNearestCentroid(
        vector: [Float],
        centroids: [[Float]]
    ) -> Int {
        var bestIndex = 0
        var bestDistance = Double.infinity

        for (i, centroid) in centroids.enumerated() {
            let distance = VectorConversion.euclideanDistanceSquared(vector, centroid)
            if distance < bestDistance {
                bestDistance = distance
                bestIndex = i
            }
        }

        return bestIndex
    }

    // MARK: - Private Methods

    /// K-means++ initialization
    ///
    /// Selects initial centroids with probability proportional to
    /// squared distance from nearest existing centroid.
    private func kMeansPlusPlusInit(
        vectors: [[Float]],
        randomNumberGenerator: inout VectorTrainingRandomNumberGenerator
    ) -> [[Float]] {
        var centroids: [[Float]] = []

        // First centroid: random selection
        let firstIndex = Int.random(
            in: 0..<vectors.count,
            using: &randomNumberGenerator
        )
        centroids.append(vectors[firstIndex])

        // Subsequent centroids: probability proportional to D^2
        for _ in 1..<k {
            var distances: [Double] = []
            var totalDistance: Double = 0

            for vector in vectors {
                let minDist = centroids.map { VectorConversion.euclideanDistanceSquared(vector, $0) }.min() ?? 0
                distances.append(minDist)
                totalDistance += minDist
            }

            // Sample with probability proportional to distance
            if totalDistance > 0 {
                var target = Double.random(
                    in: 0..<totalDistance,
                    using: &randomNumberGenerator
                )
                for (i, dist) in distances.enumerated() {
                    target -= dist
                    if target <= 0 {
                        centroids.append(vectors[i])
                        break
                    }
                }
            } else {
                // All distances are 0, pick random
                let idx = Int.random(
                    in: 0..<vectors.count,
                    using: &randomNumberGenerator
                )
                centroids.append(vectors[idx])
            }
        }

        return centroids
    }

    /// Assign each vector to its nearest centroid
    private func assignToCentroids(vectors: [[Float]], centroids: [[Float]]) -> [Int] {
        vectors.map { vector in
            assignToNearestCentroid(vector: vector, centroids: centroids)
        }
    }

    /// Compute new centroids as mean of assigned vectors
    private func computeCentroids(
        vectors: [[Float]],
        assignments: [Int],
        randomNumberGenerator: inout VectorTrainingRandomNumberGenerator
    ) -> [[Float]] {
        var sums: [[Double]] = Array(repeating: Array(repeating: 0.0, count: dimensions), count: k)
        var counts: [Int] = Array(repeating: 0, count: k)

        for (i, assignment) in assignments.enumerated() {
            let vector = vectors[i]
            for d in 0..<dimensions {
                sums[assignment][d] += Double(vector[d])
            }
            counts[assignment] += 1
        }

        var centroids: [[Float]] = []
        for i in 0..<k {
            if counts[i] > 0 {
                let centroid = sums[i].map { Float($0 / Double(counts[i])) }
                centroids.append(centroid)
            } else {
                // Empty cluster: reinitialize with random vector
                let randomIdx = Int.random(
                    in: 0..<vectors.count,
                    using: &randomNumberGenerator
                )
                centroids.append(vectors[randomIdx])
            }
        }

        return centroids
    }

    /// Check if centroids have converged
    private func hasConverged(oldCentroids: [[Float]], newCentroids: [[Float]]) -> Bool {
        var maxMovement: Double = 0

        for (old, new) in zip(oldCentroids, newCentroids) {
            let movement = DatabaseMath.squareRoot(
                VectorConversion.euclideanDistanceSquared(old, new)
            )
            maxMovement = max(maxMovement, movement)
        }

        return maxMovement < convergenceThreshold
    }
}

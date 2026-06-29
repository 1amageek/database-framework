// TDigest.swift
// DatabaseEngine - t-digest algorithm for streaming quantile estimation
//
// Reference: Dunning, T. & Ertl, O. "Computing Extremely Accurate Quantiles Using t-Digests" (2019)
// Used by: Elasticsearch, InfluxDB, Prometheus, Apache Spark
//
// Key properties:
// - High accuracy at extreme quantiles (p99, p99.9)
// - O(log n) per add operation
// - Supports merging for distributed computation
// - Memory: ~10KB per instance (compression=100)

import Foundation

// MARK: - TDigest

/// Streaming quantile estimation using the t-digest algorithm
///
/// t-digest maintains a compressed representation of a distribution using "centroids",
/// where each centroid represents a group of nearby values. The key insight is that
/// centroids near the tails (q→0 or q→1) are kept small for high precision, while
/// centroids in the middle can be larger.
///
/// **Usage**:
/// ```swift
/// var digest = TDigest(compression: 100)
///
/// // Add values
/// for value in measurements {
///     digest.add(value)
/// }
///
/// // Query quantiles
/// let median = digest.quantile(0.5)
/// let p99 = digest.quantile(0.99)
/// let p999 = digest.quantile(0.999)
/// ```
///
/// **Accuracy**:
/// - Extreme quantiles (p1, p99, p999): Very high accuracy
/// - Median (p50): Good accuracy
/// - Compression parameter controls accuracy vs memory trade-off
///
/// **Limitations**:
/// - Add-only: Cannot remove values once added
/// - Approximate: Results are estimates, not exact values
public struct TDigest: Sendable, Codable, Equatable {

    // MARK: - Centroid

    /// A centroid represents a cluster of values
    ///
    /// Each centroid has:
    /// - mean: The weighted average of values in this cluster
    /// - weight: The total count of values represented
    public struct Centroid: Sendable, Codable, Equatable, Comparable {
        public var mean: Double
        public var weight: Int64

        public init(mean: Double, weight: Int64 = 1) {
            self.mean = mean
            self.weight = weight
        }

        /// Add a value to this centroid, updating the weighted mean
        public mutating func add(_ value: Double, weight: Int64 = 1) {
            let totalWeight = self.weight + weight
            self.mean = (self.mean * Double(self.weight) + value * Double(weight)) / Double(totalWeight)
            self.weight = totalWeight
        }

        public static func < (lhs: Centroid, rhs: Centroid) -> Bool {
            lhs.mean < rhs.mean
        }
    }

    // MARK: - Properties

    /// Centroids sorted by mean value
    private var centroids: [Centroid]

    /// Total weight across all centroids
    private var totalWeight: Int64

    /// Compression parameter (δ)
    ///
    /// Higher values = more centroids = better accuracy but more memory.
    /// Typical values: 100 (default), 200 (high accuracy), 50 (low memory)
    ///
    /// Memory usage: approximately 8 * compression bytes
    public let compression: Double

    /// Minimum value seen
    public private(set) var min: Double

    /// Maximum value seen
    public private(set) var max: Double

    /// Buffer for unprocessed values (batch processing optimization)
    private var buffer: [Centroid]

    /// Buffer size threshold before compression
    private let bufferSize: Int

    // MARK: - Initialization

    /// Create a new t-digest
    ///
    /// - Parameter compression: Compression parameter (default: 100)
    ///   - 50: Lower memory, less accuracy
    ///   - 100: Balanced (recommended)
    ///   - 200: Higher accuracy, more memory
    public init(compression: Double = 100) {
        self.compression = compression
        self.centroids = []
        self.totalWeight = 0
        self.min = .infinity
        self.max = -.infinity
        self.buffer = []
        self.bufferSize = Int(compression * 5)  // Buffer up to 5x compression values
    }

    // MARK: - Scale Function

    /// Scale function k(q) for determining centroid size limits
    ///
    /// Uses the arcsin-based scale function which provides better accuracy
    /// at the tails compared to the original k1 function.
    ///
    /// k(q) = (δ/2) * (arcsin(2q - 1) / π + 0.5)
    ///
    /// This function maps quantile q ∈ [0, 1] to k ∈ [0, δ].
    /// The derivative k'(q) is large near q=0 and q=1, meaning small
    /// changes in q correspond to large changes in k, forcing small centroids.
    private func k(_ q: Double) -> Double {
        compression / 2.0 * (asin(2.0 * q - 1.0) / .pi + 0.5)
    }

    /// Inverse scale function: given k, return q
    private func kInverse(_ k: Double) -> Double {
        let normalized = 2.0 * k / compression - 0.5
        return (sin(normalized * .pi) + 1.0) / 2.0
    }

    // MARK: - Add Values

    /// Add a single value to the digest
    ///
    /// - Parameters:
    ///   - value: The value to add
    ///   - weight: The weight of the value (default: 1)
    public mutating func add(_ value: Double, weight: Int64 = 1) {
        guard weight > 0 else { return }
        guard value.isFinite else { return }

        // Update min/max
        min = Swift.min(min, value)
        max = Swift.max(max, value)

        // Add to buffer
        buffer.append(Centroid(mean: value, weight: weight))

        // Compress if buffer is full
        if buffer.count >= bufferSize {
            compress()
        }
    }

    /// Add multiple values at once
    ///
    /// - Parameter values: Array of values to add
    public mutating func addAll(_ values: [Double]) {
        for value in values {
            add(value)
        }
    }

    // MARK: - Compression

    /// Compress the digest by merging centroids
    ///
    /// This is the core t-digest algorithm. It sorts all centroids (including buffer)
    /// and merges them while respecting the size constraint from the scale function.
    private mutating func compress() {
        // Merge buffer into centroids
        var allCentroids = centroids + buffer
        buffer.removeAll()

        guard !allCentroids.isEmpty else { return }

        // Sort by mean
        allCentroids.sort()

        // Compute total weight
        let total = allCentroids.reduce(Int64(0)) { $0 + $1.weight }
        totalWeight = total

        // Merge centroids using the scale function constraint
        var result: [Centroid] = []
        var weightSoFar: Int64 = 0
        var currentCentroid = allCentroids[0]

        for i in 1..<allCentroids.count {
            let proposedWeight = currentCentroid.weight + allCentroids[i].weight

            // Compute the quantile range this merged centroid would span
            let q0 = Double(weightSoFar) / Double(total)
            let q1 = Double(weightSoFar + proposedWeight) / Double(total)

            // Check if merging would violate the size constraint
            // The constraint is: k(q1) - k(q0) <= 1
            let kDiff = k(q1) - k(q0)

            if kDiff <= 1.0 {
                // Merge is allowed
                currentCentroid.add(allCentroids[i].mean, weight: allCentroids[i].weight)
            } else {
                // Start a new centroid
                result.append(currentCentroid)
                weightSoFar += currentCentroid.weight
                currentCentroid = allCentroids[i]
            }
        }

        // Don't forget the last centroid
        result.append(currentCentroid)

        centroids = result
    }

    // MARK: - Query Quantiles

    /// Get the estimated value at a given quantile
    ///
    /// - Parameter q: Quantile in range [0, 1]
    ///   - 0.5 = median
    ///   - 0.99 = 99th percentile
    ///   - 0.999 = 99.9th percentile
    /// - Returns: Estimated value at the quantile
    ///
    /// **Accuracy**:
    /// The t-digest provides higher accuracy at extreme quantiles (near 0 or 1)
    /// compared to the middle. This is ideal for monitoring use cases where
    /// p99 and p999 latencies are most important.
    public mutating func quantile(_ q: Double) -> Double {
        // Ensure buffer is processed
        if !buffer.isEmpty {
            compress()
        }

        guard !centroids.isEmpty else {
            return .nan
        }

        // Clamp q to valid range
        let q = Swift.max(0, Swift.min(1, q))

        // Edge cases
        if q == 0 { return min }
        if q == 1 { return max }
        if centroids.count == 1 { return centroids[0].mean }

        // Target weight position
        let targetWeight = q * Double(totalWeight)

        // Find the centroid containing the target weight using interpolation
        var weightSoFar: Double = 0

        for i in 0..<centroids.count {
            let centroid = centroids[i]
            let centroidWeight = Double(centroid.weight)

            // Weight range for this centroid: [weightSoFar, weightSoFar + centroidWeight]
            // But we consider the centroid's mean to be at the center of its weight
            let leftWeight = weightSoFar
            let rightWeight = weightSoFar + centroidWeight

            // Check if target is in this centroid's range
            if targetWeight <= rightWeight {
                // Interpolate within this centroid
                if i == 0 {
                    // First centroid: interpolate between min and centroid mean
                    let ratio = (targetWeight - leftWeight) / centroidWeight
                    return min + ratio * (centroid.mean - min) * 2.0
                } else if i == centroids.count - 1 {
                    // Last centroid: interpolate between centroid mean and max
                    let ratio = (targetWeight - leftWeight) / centroidWeight
                    return centroid.mean + ratio * (max - centroid.mean) * 2.0
                } else {
                    // Middle centroid: interpolate between adjacent centroids
                    let prevCentroid = centroids[i - 1]
                    let prevMidWeight = weightSoFar - Double(prevCentroid.weight) / 2.0
                    let curMidWeight = weightSoFar + centroidWeight / 2.0

                    if targetWeight < weightSoFar + centroidWeight / 2.0 {
                        // Left half: interpolate from previous centroid
                        let ratio = (targetWeight - prevMidWeight) / (curMidWeight - prevMidWeight)
                        return prevCentroid.mean + ratio * (centroid.mean - prevCentroid.mean)
                    } else {
                        // Right half: interpolate to next centroid
                        if i + 1 < centroids.count {
                            let nextCentroid = centroids[i + 1]
                            let nextMidWeight = curMidWeight + centroidWeight / 2.0 + Double(nextCentroid.weight) / 2.0
                            let ratio = (targetWeight - curMidWeight) / (nextMidWeight - curMidWeight)
                            return centroid.mean + ratio * (nextCentroid.mean - centroid.mean)
                        }
                    }
                }

                return centroid.mean
            }

            weightSoFar = rightWeight
        }

        return max
    }

    /// Get multiple quantiles efficiently
    ///
    /// - Parameter quantiles: Array of quantiles to compute
    /// - Returns: Dictionary mapping each quantile to its estimated value
    public mutating func quantiles(_ quantiles: [Double]) -> [Double: Double] {
        var result: [Double: Double] = [:]
        for q in quantiles {
            result[q] = quantile(q)
        }
        return result
    }

    /// Get the estimated quantile (CDF) for a given value
    ///
    /// - Parameter value: The value to find the quantile of
    /// - Returns: Estimated quantile (0 to 1)
    public mutating func cdf(_ value: Double) -> Double {
        // Ensure buffer is processed
        if !buffer.isEmpty {
            compress()
        }

        guard !centroids.isEmpty else {
            return .nan
        }

        if value < min { return 0 }
        if value > max { return 1 }
        if centroids.count == 1 {
            return value <= centroids[0].mean ? 0.5 : 0.5
        }

        var weightBelow: Double = 0

        for i in 0..<centroids.count {
            let centroid = centroids[i]

            if value < centroid.mean {
                // Value is before this centroid
                if i == 0 {
                    // Interpolate between min and first centroid
                    let ratio = (value - min) / (centroid.mean - min)
                    return ratio * Double(centroid.weight) / 2.0 / Double(totalWeight)
                } else {
                    // Interpolate between previous and current centroid
                    let prev = centroids[i - 1]
                    let ratio = (value - prev.mean) / (centroid.mean - prev.mean)
                    let partialWeight = Double(prev.weight) / 2.0 + ratio * (Double(centroid.weight) / 2.0 + Double(prev.weight) / 2.0)
                    return (weightBelow - Double(prev.weight) / 2.0 + partialWeight) / Double(totalWeight)
                }
            }

            weightBelow += Double(centroid.weight)
        }

        return 1.0
    }

    // MARK: - Merge

    /// Merge another t-digest into this one
    ///
    /// This allows combining digests from distributed computation.
    ///
    /// - Parameter other: The digest to merge
    public mutating func merge(with other: TDigest) {
        // Update min/max
        min = Swift.min(min, other.min)
        max = Swift.max(max, other.max)

        // Add other's centroids to buffer
        buffer.append(contentsOf: other.centroids)
        buffer.append(contentsOf: other.buffer)

        // Compress
        compress()
    }

    /// Merge multiple t-digests
    ///
    /// - Parameter digests: Array of digests to merge
    /// - Returns: Combined digest
    public static func merge(_ digests: [TDigest]) -> TDigest {
        guard !digests.isEmpty else {
            return TDigest()
        }

        var result = digests[0]
        for i in 1..<digests.count {
            result.merge(with: digests[i])
        }
        return result
    }

    // MARK: - Statistics

    /// Total number of values added
    public var count: Int64 {
        totalWeight + buffer.reduce(Int64(0)) { $0 + $1.weight }
    }

    /// Number of centroids (after compression)
    public var centroidCount: Int {
        centroids.count
    }

    /// Check if the digest is empty
    public var isEmpty: Bool {
        centroids.isEmpty && buffer.isEmpty
    }

    /// Estimated memory usage in bytes
    public var estimatedMemoryBytes: Int {
        // Each centroid: 8 bytes (mean) + 8 bytes (weight) = 16 bytes
        // Plus buffer and overhead
        return (centroids.count + buffer.count) * 16 + 64
    }

    // MARK: - Serialization

    /// Encode the digest to binary data
    ///
    /// Format: [compression: Float64][totalWeight: Int64][min: Float64][max: Float64]
    ///         [centroidCount: UInt32][[mean: Float64][weight: Int64]]...
    public func encode() -> Data {
        // Ensure we have a compressed state for encoding
        var copy = self
        if !copy.buffer.isEmpty {
            copy.compress()
        }

        var data = Data()
        data.reserveCapacity(36 + copy.centroids.count * 16)

        // Helper to append values in little-endian format
        func appendDouble(_ value: Double) {
            var bits = value.bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { data.append(contentsOf: $0) }
        }

        func appendInt64(_ value: Int64) {
            var le = value.littleEndian
            withUnsafeBytes(of: &le) { data.append(contentsOf: $0) }
        }

        func appendUInt32(_ value: UInt32) {
            var le = value.littleEndian
            withUnsafeBytes(of: &le) { data.append(contentsOf: $0) }
        }

        // Header
        appendDouble(compression)
        appendInt64(copy.totalWeight)
        appendDouble(copy.min)
        appendDouble(copy.max)

        // Centroid count
        appendUInt32(UInt32(copy.centroids.count))

        // Centroids
        for centroid in copy.centroids {
            appendDouble(centroid.mean)
            appendInt64(centroid.weight)
        }

        return data
    }

    /// Decode a digest from binary data
    ///
    /// - Parameter data: Binary data from `encode()`
    /// - Returns: Decoded TDigest
    public static func decode(from data: Data) -> TDigest? {
        guard data.count >= 36 else { return nil }  // Minimum header size

        var offset = 0

        // Helper to read values safely (handles alignment)
        func readDouble() -> Double? {
            guard offset + 8 <= data.count else { return nil }
            let bytes = data.subdata(in: offset..<offset+8)
            offset += 8
            return bytes.withUnsafeBytes { ptr -> Double in
                var bits: UInt64 = 0
                withUnsafeMutableBytes(of: &bits) { dest in
                    _ = ptr.copyBytes(to: dest)
                }
                return Double(bitPattern: UInt64(littleEndian: bits))
            }
        }

        func readInt64() -> Int64? {
            guard offset + 8 <= data.count else { return nil }
            let bytes = data.subdata(in: offset..<offset+8)
            offset += 8
            return bytes.withUnsafeBytes { ptr -> Int64 in
                var value: Int64 = 0
                withUnsafeMutableBytes(of: &value) { dest in
                    _ = ptr.copyBytes(to: dest)
                }
                return Int64(littleEndian: value)
            }
        }

        func readUInt32() -> UInt32? {
            guard offset + 4 <= data.count else { return nil }
            let bytes = data.subdata(in: offset..<offset+4)
            offset += 4
            return bytes.withUnsafeBytes { ptr -> UInt32 in
                var value: UInt32 = 0
                withUnsafeMutableBytes(of: &value) { dest in
                    _ = ptr.copyBytes(to: dest)
                }
                return UInt32(littleEndian: value)
            }
        }

        // Read header
        guard let compression = readDouble(),
              let totalWeight = readInt64(),
              let minVal = readDouble(),
              let maxVal = readDouble(),
              let centroidCount = readUInt32() else {
            return nil
        }

        // Verify data size
        let expectedSize = 36 + Int(centroidCount) * 16
        guard data.count >= expectedSize else { return nil }

        // Read centroids
        var centroids: [Centroid] = []
        centroids.reserveCapacity(Int(centroidCount))

        for _ in 0..<centroidCount {
            guard let mean = readDouble(),
                  let weight = readInt64() else {
                return nil
            }
            centroids.append(Centroid(mean: mean, weight: weight))
        }

        var digest = TDigest(compression: compression)
        digest.centroids = centroids
        digest.totalWeight = totalWeight
        digest.min = minVal
        digest.max = maxVal

        return digest
    }

    // MARK: - Equatable

    public static func == (lhs: TDigest, rhs: TDigest) -> Bool {
        // Compare compressed states
        var lhsCopy = lhs
        var rhsCopy = rhs

        if !lhsCopy.buffer.isEmpty { lhsCopy.compress() }
        if !rhsCopy.buffer.isEmpty { rhsCopy.compress() }

        return lhsCopy.compression == rhsCopy.compression &&
               lhsCopy.totalWeight == rhsCopy.totalWeight &&
               lhsCopy.min == rhsCopy.min &&
               lhsCopy.max == rhsCopy.max &&
               lhsCopy.centroids == rhsCopy.centroids
    }
}

// MARK: - Debug Description

extension TDigest: CustomDebugStringConvertible {
    public var debugDescription: String {
        var copy = self
        if !copy.buffer.isEmpty { copy.compress() }

        return """
        TDigest(compression: \(compression), count: \(count), centroids: \(centroidCount), \
        min: \(min), max: \(max), memory: ~\(estimatedMemoryBytes) bytes)
        """
    }
}

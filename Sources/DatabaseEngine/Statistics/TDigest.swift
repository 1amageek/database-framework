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

import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseMath
import StorageKit

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
/// var digest = try TDigest(compression: 100)
///
/// // Add values
/// for value in measurements {
///     try digest.add(value)
/// }
///
/// // Query quantiles
/// let median = try digest.quantile(0.5)
/// let p99 = try digest.quantile(0.99)
/// let p999 = try digest.quantile(0.999)
/// ```
///
/// **Accuracy**:
/// - Extreme quantiles (p1, p99, p999): Very high accuracy
/// - Median (p50): Good accuracy
/// - Compression parameter controls accuracy vs memory trade-off
///
/// **Limitations**:
/// - A digest instance is add-only; delete-capable indexes retain exact
///   membership and rebuild an affected digest transactionally.
/// - Approximate: Results are estimates, not exact values
public struct TDigest: Sendable, Equatable {
    public static let supportedCompression = 1.0...1_000.0
    public static let maximumEncodedBytes = 100_000

    // MARK: - Centroid

    /// A centroid represents a cluster of values
    ///
    /// Each centroid has:
    /// - mean: The weighted average of values in this cluster
    /// - weight: The total count of values represented
    private struct Centroid: Sendable, Equatable, Comparable {
        var mean: Double
        var weight: Int64

        init(mean: Double, weight: Int64 = 1) {
            self.mean = mean
            self.weight = weight
        }

        /// Add a value to this centroid, updating the weighted mean
        mutating func add(_ value: Double, weight: Int64 = 1) {
            let totalWeight = self.weight + weight
            self.mean = (self.mean * Double(self.weight) + value * Double(weight)) / Double(totalWeight)
            self.weight = totalWeight
        }

        static func < (lhs: Centroid, rhs: Centroid) -> Bool {
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
    public init() {
        self.init(validatedCompression: 100)
    }

    public init(
        compression: Double
    ) throws(TDigestError) {
        guard compression.isFinite,
              Self.supportedCompression.contains(compression) else {
            throw .invalidCompression(compression)
        }
        self.init(validatedCompression: compression)
    }

    private init(validatedCompression compression: Double) {
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
        compression / 2.0 * (DatabaseMath.arcSine(2.0 * q - 1.0) / .pi + 0.5)
    }

    // MARK: - Add Values

    /// Add a single value to the digest
    ///
    /// - Parameters:
    ///   - value: The value to add
    ///   - weight: The weight of the value (default: 1)
    public mutating func add(
        _ value: Double,
        weight: Int64 = 1
    ) throws(TDigestError) {
        guard weight > 0 else {
            throw .invalidWeight(weight)
        }
        guard value.isFinite else {
            throw .invalidValue(value)
        }
        let (_, overflow) = count.addingReportingOverflow(weight)
        guard !overflow else {
            throw .weightOverflow
        }

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
    public mutating func addAll<Values: Sequence>(
        _ values: Values
    ) throws(TDigestError) where Values.Element == Double {
        for value in values {
            try add(value)
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
    public mutating func quantile(
        _ q: Double
    ) throws(TDigestError) -> Double {
        guard q.isFinite, (0.0...1.0).contains(q) else {
            throw .invalidQuantile(q)
        }
        // Ensure buffer is processed
        if !buffer.isEmpty {
            compress()
        }

        guard !centroids.isEmpty else {
            throw .emptyDigest
        }

        // Edge cases
        if q == 0 { return min }
        if q == 1 { return max }
        if centroids.count == 1 { return centroids[0].mean }

        // Interpolate between centroid centers. The endpoints are anchored at
        // the exact minimum and maximum, keeping the result monotonic and
        // bounded even when a tail centroid represents multiple observations.
        let targetWeight = q * Double(totalWeight)
        var previous = centroids[0]
        var previousCenter = Double(previous.weight) / 2.0
        if targetWeight <= previousCenter {
            let ratio = targetWeight / previousCenter
            return min + ratio * (previous.mean - min)
        }

        var cumulativeWeight = Double(previous.weight)
        for centroid in centroids.dropFirst() {
            let center = cumulativeWeight + Double(centroid.weight) / 2.0
            if targetWeight <= center {
                let ratio = (targetWeight - previousCenter)
                    / (center - previousCenter)
                return previous.mean
                    + ratio * (centroid.mean - previous.mean)
            }
            cumulativeWeight += Double(centroid.weight)
            previous = centroid
            previousCenter = center
        }

        let finalWeight = Double(totalWeight)
        let ratio = (targetWeight - previousCenter)
            / (finalWeight - previousCenter)
        return previous.mean + ratio * (max - previous.mean)
    }

    /// Get multiple quantiles efficiently
    ///
    /// - Parameter quantiles: Array of quantiles to compute
    /// - Returns: Dictionary mapping each quantile to its estimated value
    public mutating func quantiles(
        _ quantiles: [Double]
    ) throws(TDigestError) -> [Double: Double] {
        var result: [Double: Double] = [:]
        for q in quantiles {
            result[q] = try quantile(q)
        }
        return result
    }

    /// Get the estimated quantile (CDF) for a given value
    ///
    /// - Parameter value: The value to find the quantile of
    /// - Returns: Estimated quantile (0 to 1)
    public mutating func cdf(
        _ value: Double
    ) throws(TDigestError) -> Double {
        guard value.isFinite else {
            throw .invalidValue(value)
        }
        // Ensure buffer is processed
        if !buffer.isEmpty {
            compress()
        }

        guard !centroids.isEmpty else {
            throw .emptyDigest
        }

        if value < min { return 0 }
        if value > max { return 1 }
        let total = Double(totalWeight)
        var previous = centroids[0]
        var previousCenter = Double(previous.weight) / 2.0
        if value <= previous.mean {
            guard previous.mean > min else {
                return previousCenter / total
            }
            let ratio = (value - min) / (previous.mean - min)
            return ratio * previousCenter / total
        }

        var cumulativeWeight = Double(previous.weight)
        for centroid in centroids.dropFirst() {
            let center = cumulativeWeight + Double(centroid.weight) / 2.0
            if value <= centroid.mean {
                guard centroid.mean > previous.mean else {
                    return center / total
                }
                let ratio = (value - previous.mean)
                    / (centroid.mean - previous.mean)
                let interpolatedCenter = previousCenter
                    + ratio * (center - previousCenter)
                return interpolatedCenter / total
            }
            cumulativeWeight += Double(centroid.weight)
            previous = centroid
            previousCenter = center
        }

        guard max > previous.mean else { return 1 }
        let ratio = (value - previous.mean) / (max - previous.mean)
        return (previousCenter + ratio * (total - previousCenter)) / total
    }

    // MARK: - Merge

    /// Merge another t-digest into this one
    ///
    /// This allows combining digests from distributed computation.
    ///
    /// - Parameter other: The digest to merge
    public mutating func merge(
        with other: TDigest
    ) throws(TDigestError) {
        guard compression == other.compression else {
            throw .compressionMismatch(
                expected: compression,
                actual: other.compression
            )
        }
        let (_, overflow) = count.addingReportingOverflow(other.count)
        guard !overflow else {
            throw .weightOverflow
        }
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
    public static func merge(
        _ digests: [TDigest]
    ) throws(TDigestError) -> TDigest {
        guard !digests.isEmpty else {
            return TDigest()
        }

        var result = digests[0]
        for i in 1..<digests.count {
            try result.merge(with: digests[i])
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

    /// Encodes one strict v1 binary frame directly into final `ByteString` storage.
    ///
    /// Format: `TDG1`, compression, total weight, min, max, centroid count,
    /// followed by `(mean, weight)` centroid pairs. All numbers are little-endian.
    public func encodeBytes() throws(TDigestError) -> ByteString {
        var copy = self
        if !copy.buffer.isEmpty {
            copy.compress()
        }
        try copy.validatePersistentState()

        let headerByteCount = 40
        let maximumCentroids =
            (Self.maximumEncodedBytes - headerByteCount) / 16
        guard copy.centroids.count <= maximumCentroids else {
            throw .centroidLimitExceeded(maximumCentroids)
        }
        let byteCount = headerByteCount + copy.centroids.count * 16

        return ByteString.copying(count: byteCount) { destination in
            destination[0] = 0x54
            destination[1] = 0x44
            destination[2] = 0x47
            destination[3] = 0x01
            writeTDigestUInt64(
                copy.compression.bitPattern,
                to: destination,
                at: 4
            )
            writeTDigestUInt64(
                UInt64(bitPattern: copy.totalWeight),
                to: destination,
                at: 12
            )
            writeTDigestUInt64(copy.min.bitPattern, to: destination, at: 20)
            writeTDigestUInt64(copy.max.bitPattern, to: destination, at: 28)
            writeTDigestUInt32(
                UInt32(copy.centroids.count),
                to: destination,
                at: 36
            )

            var offset = headerByteCount
            for centroid in copy.centroids {
                writeTDigestUInt64(
                    centroid.mean.bitPattern,
                    to: destination,
                    at: offset
                )
                writeTDigestUInt64(
                    UInt64(bitPattern: centroid.weight),
                    to: destination,
                    at: offset + 8
                )
                offset += 16
            }
        }
    }

    /// Decodes a strict v1 binary frame from borrowed `ByteString` storage.
    /// Scalar reads are unaligned loads; no `Data` or scalar sub-buffers are made.
    public static func decode(
        from bytes: ByteString
    ) throws(TDigestError) -> TDigest {
        let headerByteCount = 40
        guard bytes.count >= headerByteCount else {
            throw .invalidByteCount(
                expected: headerByteCount,
                actual: bytes.count
            )
        }
        let startIndex = bytes.startIndex
        guard bytes[startIndex] == 0x54,
              bytes[startIndex + 1] == 0x44,
              bytes[startIndex + 2] == 0x47,
              bytes[startIndex + 3] == 0x01 else {
            throw .invalidHeader
        }

        let compression = Double(
            bitPattern: readTDigestUInt64(bytes, at: 4)
        )
        guard compression.isFinite,
              Self.supportedCompression.contains(compression) else {
            throw .invalidCompression(compression)
        }
        let totalWeight = Int64(
            bitPattern: readTDigestUInt64(bytes, at: 12)
        )
        let minimum = Double(
            bitPattern: readTDigestUInt64(bytes, at: 20)
        )
        let maximum = Double(
            bitPattern: readTDigestUInt64(bytes, at: 28)
        )
        let encodedCentroidCount = readTDigestUInt32(bytes, at: 36)
        let maximumCentroids =
            (Self.maximumEncodedBytes - headerByteCount) / 16
        guard encodedCentroidCount <= UInt32(maximumCentroids),
              let centroidCount = Int(exactly: encodedCentroidCount) else {
            throw .centroidLimitExceeded(maximumCentroids)
        }
        let expectedByteCount = headerByteCount + centroidCount * 16
        guard bytes.count == expectedByteCount else {
            throw .invalidByteCount(
                expected: expectedByteCount,
                actual: bytes.count
            )
        }

        var centroids: [Centroid] = []
        centroids.reserveCapacity(centroidCount)
        var offset = headerByteCount
        for _ in 0..<centroidCount {
            centroids.append(
                Centroid(
                    mean: Double(
                        bitPattern: readTDigestUInt64(bytes, at: offset)
                    ),
                    weight: Int64(
                        bitPattern: readTDigestUInt64(bytes, at: offset + 8)
                    )
                )
            )
            offset += 16
        }

        var digest = try TDigest(compression: compression)
        digest.centroids = centroids
        digest.totalWeight = totalWeight
        digest.min = minimum
        digest.max = maximum
        try digest.validatePersistentState()
        return digest
    }

    private func validatePersistentState() throws(TDigestError) {
        guard compression.isFinite,
              Self.supportedCompression.contains(compression) else {
            throw .invalidCompression(compression)
        }
        if centroids.isEmpty {
            guard totalWeight == 0 else {
                throw .invalidTotalWeight(totalWeight)
            }
            guard min == .infinity, max == -.infinity else {
                throw .invalidBounds
            }
            return
        }
        guard totalWeight > 0 else {
            throw .invalidTotalWeight(totalWeight)
        }
        guard min.isFinite, max.isFinite, min <= max else {
            throw .invalidBounds
        }

        var calculatedWeight: Int64 = 0
        var previousMean = -Double.infinity
        for (index, centroid) in centroids.enumerated() {
            guard centroid.mean.isFinite,
                  centroid.mean >= min,
                  centroid.mean <= max,
                  centroid.weight > 0 else {
                throw .invalidCentroid(index: index)
            }
            guard centroid.mean >= previousMean else {
                throw .unsortedCentroids
            }
            let (updatedWeight, overflow) = calculatedWeight.addingReportingOverflow(
                centroid.weight
            )
            guard !overflow else {
                throw .weightOverflow
            }
            calculatedWeight = updatedWeight
            previousMean = centroid.mean
        }
        guard calculatedWeight == totalWeight else {
            throw .weightMismatch(
                expected: totalWeight,
                actual: calculatedWeight
            )
        }
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

private func writeTDigestUInt64(
    _ value: UInt64,
    to destination: UnsafeMutableRawBufferPointer,
    at offset: Int
) {
    var littleEndian = value.littleEndian
    withUnsafeBytes(of: &littleEndian) { source in
        let target = UnsafeMutableRawBufferPointer(
            rebasing: destination[offset..<(offset + 8)]
        )
        target.copyMemory(from: source)
    }
}

private func writeTDigestUInt32(
    _ value: UInt32,
    to destination: UnsafeMutableRawBufferPointer,
    at offset: Int
) {
    var littleEndian = value.littleEndian
    withUnsafeBytes(of: &littleEndian) { source in
        let target = UnsafeMutableRawBufferPointer(
            rebasing: destination[offset..<(offset + 4)]
        )
        target.copyMemory(from: source)
    }
}

private func readTDigestUInt64(_ bytes: ByteString, at offset: Int) -> UInt64 {
    bytes.withUnsafeBytes { source in
        UInt64(
            littleEndian: source.loadUnaligned(
                fromByteOffset: offset,
                as: UInt64.self
            )
        )
    }
}

private func readTDigestUInt32(_ bytes: ByteString, at offset: Int) -> UInt32 {
    bytes.withUnsafeBytes { source in
        UInt32(
            littleEndian: source.loadUnaligned(
                fromByteOffset: offset,
                as: UInt32.self
            )
        )
    }
}

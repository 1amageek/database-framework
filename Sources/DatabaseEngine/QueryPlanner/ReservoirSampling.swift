// ReservoirSampling.swift
// QueryPlanner - Efficient sampling for histogram construction

import Foundation
import Core
import FoundationDB

/// Reservoir Sampling implementation using Algorithm L
///
/// Maintains a fixed-size random sample from a stream of unknown length.
/// Used for building histograms without loading entire datasets into memory.
///
/// **Algorithm L** (Kim-Hung Li, 1994):
/// Instead of checking each element (Algorithm R), Algorithm L computes
/// skip distances using geometric distribution, requiring far fewer
/// random number generations.
///
/// **Complexity**:
/// - Time: O(k(1 + log(N/k))) expected, where k = reservoir size, N = stream size
/// - Space: O(k)
/// - Random numbers: O(k(1 + log(N/k))) vs O(N) for Algorithm R
///
/// **Usage**:
/// ```swift
/// var sampler = ReservoirSampling<Int>(reservoirSize: 1000)
///
/// for await value in stream {
///     sampler.add(value)
/// }
///
/// let sample = sampler.sample
/// ```
///
/// **References**:
/// - Kim-Hung Li, "Reservoir-Sampling Algorithms of Time Complexity O(n(1+log(N/n)))",
///   ACM Transactions on Mathematical Software, 20(4):481-493, 1994
/// - J. S. Vitter, "Random Sampling with a Reservoir", ACM TOMS 1985 (Algorithm R)
public struct ReservoirSampling<T: Sendable>: Sendable {

    /// Maximum size of the reservoir
    private let reservoirSize: Int

    /// Current reservoir contents
    private var reservoir: [T]

    /// Total elements seen so far
    private(set) var elementsSeen: Int

    /// Algorithm L state: W parameter for skip calculation
    /// W is the probability threshold for inclusion
    private var w: Double

    /// Algorithm L state: next index to sample
    private var nextSampleIndex: Int

    /// Create a new reservoir sampler
    ///
    /// - Parameter reservoirSize: Maximum number of samples to keep (default: 10,000)
    public init(reservoirSize: Int = 10_000) {
        precondition(reservoirSize > 0, "Reservoir size must be positive")
        self.reservoirSize = reservoirSize
        self.reservoir = []
        self.reservoir.reserveCapacity(reservoirSize)
        self.elementsSeen = 0
        self.w = 1.0
        self.nextSampleIndex = 0
    }

    /// Add an element to the sampling process
    ///
    /// Uses Algorithm L for efficient sampling:
    /// - Phase 1 (filling): First k elements go directly into reservoir
    /// - Phase 2 (sampling): Use skip distances to determine which elements to sample
    ///
    /// - Parameter element: The element to potentially sample
    public mutating func add(_ element: T) {
        elementsSeen += 1

        if reservoir.count < reservoirSize {
            // Phase 1: Fill the reservoir
            reservoir.append(element)

            // Initialize Algorithm L when reservoir is full
            if reservoir.count == reservoirSize {
                initializeAlgorithmL()
            }
        } else {
            // Phase 2: Algorithm L sampling
            if elementsSeen == nextSampleIndex {
                // Replace random element in reservoir
                let replaceIndex = Int.random(in: 0..<reservoirSize)
                reservoir[replaceIndex] = element

                // Calculate next sample index using Algorithm L
                calculateNextSampleIndex()
            }
        }
    }

    /// Initialize Algorithm L parameters after reservoir is filled
    ///
    /// Formula: W = exp(log(random()) / k)
    /// This gives W an initial value following the correct distribution
    private mutating func initializeAlgorithmL() {
        // W = random()^(1/k) = exp(log(random()) / k)
        w = exp(log(Double.random(in: 0..<1)) / Double(reservoirSize))
        calculateNextSampleIndex()
    }

    /// Calculate the next index to sample using Algorithm L
    ///
    /// Skip distance follows geometric distribution:
    /// skip = floor(log(random()) / log(1 - W)) + 1
    ///
    /// Then update W for next iteration:
    /// W = W * exp(log(random()) / k)
    private mutating func calculateNextSampleIndex() {
        // Calculate skip distance
        // skip = floor(log(U) / log(1 - W)) where U ~ Uniform(0,1)
        let u = Double.random(in: 0..<1)

        // Use log1p(-w) = log(1-w) for numerical stability when w is small
        // Note: log1p(x) = log(1 + x), so log1p(-w) = log(1 - w)
        let skip = floor(log(u) / log1p(-w))

        // Next sample index (1-based in paper, we use 0-based so add elementsSeen)
        nextSampleIndex = elementsSeen + Int(skip) + 1

        // Update W for next iteration: W = W * random()^(1/k)
        w = w * exp(log(Double.random(in: 0..<1)) / Double(reservoirSize))
    }

    /// Add multiple elements
    ///
    /// - Parameter elements: Elements to potentially sample
    public mutating func addAll<S: Sequence>(_ elements: S) where S.Element == T {
        for element in elements {
            add(element)
        }
    }

    /// Get the current sample
    public var sample: [T] {
        reservoir
    }

    /// Check if reservoir is full
    public var isFull: Bool {
        reservoir.count >= reservoirSize
    }

    /// Get the sample rate (sample size / total elements)
    public var sampleRate: Double {
        guard elementsSeen > 0 else { return 0 }
        return Double(reservoir.count) / Double(elementsSeen)
    }

    /// Reset the sampler
    public mutating func reset() {
        reservoir.removeAll(keepingCapacity: true)
        elementsSeen = 0
        w = 1.0
        nextSampleIndex = 0
    }
}

// MARK: - Histogram Building

extension ReservoirSampling where T: Comparable & Hashable & TupleElement {

    /// Build a histogram from the sampled values
    ///
    /// Uses equi-height bucketing for better query selectivity estimation.
    ///
    /// - Parameter bucketCount: Number of histogram buckets (default: 100)
    /// - Returns: Array of histogram buckets
    public func buildHistogram(bucketCount: Int = 100) -> [HistogramBucket] {
        guard !reservoir.isEmpty else { return [] }

        let sorted = reservoir.sorted()
        let totalSampled = sorted.count

        // Scale factor to estimate population from sample
        let scaleFactor = elementsSeen > 0 ? Double(elementsSeen) / Double(totalSampled) : 1.0

        // Handle case where we have fewer unique values than buckets
        let uniqueValues = Set(sorted)
        if uniqueValues.count <= bucketCount {
            return buildValueBasedBuckets(sorted: sorted, scaleFactor: scaleFactor)
        }

        // Equi-height bucketing
        return buildEquiHeightBuckets(sorted: sorted, bucketCount: bucketCount, scaleFactor: scaleFactor)
    }

    /// Build buckets where each unique value gets its own bucket
    private func buildValueBasedBuckets(sorted: [T], scaleFactor: Double) -> [HistogramBucket] {
        var buckets: [HistogramBucket] = []
        var cumulativeCount = 0

        var i = 0
        while i < sorted.count {
            let value = sorted[i]
            var count = 0

            // Count occurrences of this value
            while i < sorted.count && sorted[i] == value {
                count += 1
                i += 1
            }

            let scaledCount = Int(Double(count) * scaleFactor)
            cumulativeCount += scaledCount

            guard let comparableValue = FieldValue(tupleElement: value) else { continue }
            buckets.append(HistogramBucket(
                lowerBound: comparableValue,
                upperBound: comparableValue,
                count: scaledCount,
                cumulativeCount: cumulativeCount
            ))
        }

        return buckets
    }

    /// Build equi-height buckets (approximately equal number of values per bucket)
    private func buildEquiHeightBuckets(sorted: [T], bucketCount: Int, scaleFactor: Double) -> [HistogramBucket] {
        let valuesPerBucket = max(1, sorted.count / bucketCount)
        var buckets: [HistogramBucket] = []
        var cumulativeCount = 0

        var i = 0
        while i < sorted.count {
            let startIndex = i
            let endIndex = min(i + valuesPerBucket, sorted.count)

            guard let lowerBound = FieldValue(tupleElement: sorted[startIndex]),
                  let upperBound = FieldValue(tupleElement: sorted[endIndex - 1]) else {
                i = endIndex
                continue
            }
            let count = endIndex - startIndex

            let scaledCount = Int(Double(count) * scaleFactor)
            cumulativeCount += scaledCount

            buckets.append(HistogramBucket(
                lowerBound: lowerBound,
                upperBound: upperBound,
                count: scaledCount,
                cumulativeCount: cumulativeCount
            ))

            i = endIndex
        }

        return buckets
    }
}

// MARK: - FieldValue Sampling

extension ReservoirSampling where T == FieldValue {

    /// Build a histogram from FieldValue samples
    ///
    /// - Parameter bucketCount: Number of histogram buckets
    /// - Returns: Array of histogram buckets
    public func buildFieldValueHistogram(bucketCount: Int = 100) -> [HistogramBucket] {
        guard !reservoir.isEmpty else { return [] }

        // Sort the FieldValue reservoir
        let sorted = reservoir.sorted()
        let totalSampled = sorted.count

        let scaleFactor = elementsSeen > 0 ? Double(elementsSeen) / Double(totalSampled) : 1.0

        // Group by unique values
        var valueGroups: [(value: FieldValue, count: Int)] = []
        var currentValue: FieldValue? = nil
        var currentCount = 0

        for value in sorted {
            if let current = currentValue, current == value {
                currentCount += 1
            } else {
                if let current = currentValue {
                    valueGroups.append((current, currentCount))
                }
                currentValue = value
                currentCount = 1
            }
        }
        if let current = currentValue {
            valueGroups.append((current, currentCount))
        }

        // Build buckets
        if valueGroups.count <= bucketCount {
            return buildValueGroupBuckets(groups: valueGroups, scaleFactor: scaleFactor)
        }

        return buildEquiHeightFieldValueBuckets(groups: valueGroups, bucketCount: bucketCount, scaleFactor: scaleFactor)
    }

    private func buildValueGroupBuckets(groups: [(value: FieldValue, count: Int)], scaleFactor: Double) -> [HistogramBucket] {
        var buckets: [HistogramBucket] = []
        var cumulativeCount = 0

        for (value, count) in groups {
            let scaledCount = Int(Double(count) * scaleFactor)
            cumulativeCount += scaledCount

            buckets.append(HistogramBucket(
                lowerBound: value,
                upperBound: value,
                count: scaledCount,
                cumulativeCount: cumulativeCount
            ))
        }

        return buckets
    }

    private func buildEquiHeightFieldValueBuckets(
        groups: [(value: FieldValue, count: Int)],
        bucketCount: Int,
        scaleFactor: Double
    ) -> [HistogramBucket] {
        let totalValues = groups.reduce(0) { $0 + $1.count }
        let targetPerBucket = max(1, totalValues / bucketCount)

        var buckets: [HistogramBucket] = []
        var cumulativeCount = 0
        var currentBucketStart: FieldValue? = nil
        var currentBucketEnd: FieldValue? = nil
        var currentBucketCount = 0

        for (value, count) in groups {
            if currentBucketStart == nil {
                currentBucketStart = value
            }
            currentBucketEnd = value
            currentBucketCount += count

            if currentBucketCount >= targetPerBucket || value == groups.last?.value {
                let scaledCount = Int(Double(currentBucketCount) * scaleFactor)
                cumulativeCount += scaledCount

                buckets.append(HistogramBucket(
                    lowerBound: currentBucketStart!,
                    upperBound: currentBucketEnd!,
                    count: scaledCount,
                    cumulativeCount: cumulativeCount
                ))

                currentBucketStart = nil
                currentBucketEnd = nil
                currentBucketCount = 0
            }
        }

        return buckets
    }
}

// MARK: - Numeric Statistics

extension ReservoirSampling where T: BinaryFloatingPoint {

    /// Compute basic statistics from the sample
    public func computeStatistics() -> (mean: Double, stdDev: Double, min: T, max: T)? {
        guard !reservoir.isEmpty else { return nil }

        let count = Double(reservoir.count)
        var sum: Double = 0
        var sumSquared: Double = 0
        var minVal = reservoir[0]
        var maxVal = reservoir[0]

        for value in reservoir {
            let d = Double(value)
            sum += d
            sumSquared += d * d
            if value < minVal { minVal = value }
            if value > maxVal { maxVal = value }
        }

        let mean = sum / count
        let variance = count > 1 ? (sumSquared - sum * sum / count) / (count - 1) : 0
        let stdDev = sqrt(max(0, variance))

        return (mean, stdDev, minVal, maxVal)
    }
}

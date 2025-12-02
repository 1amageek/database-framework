// MostCommonValues.swift
// QueryPlanner - Most Common Values (MCV) for selectivity estimation

import Foundation
import Core

/// Most Common Values (MCV) list for selectivity estimation
///
/// Tracks the most frequently occurring values in a column along with their
/// frequencies. Used in conjunction with histograms for accurate query
/// selectivity estimation, especially for skewed distributions.
///
/// **PostgreSQL Integration Pattern**:
/// ```
/// Combined selectivity = mcv_selectivity + histogram_selectivity × histogram_fraction
/// where histogram_fraction = 1 - sum(mcv_frequencies)
/// ```
///
/// **Usage**:
/// ```swift
/// // Build MCV during sampling
/// var mcvBuilder = MCVBuilder(maxSize: 100)
/// for value in samples {
///     mcvBuilder.add(value)
/// }
/// let mcv = mcvBuilder.build(totalCount: 10000, minFrequency: 0.01)
///
/// // Use for selectivity estimation
/// let selectivity = mcv.selectivity(for: someValue)
/// ```
///
/// **Reference**:
/// PostgreSQL src/backend/utils/adt/selfuncs.c
/// - MCV lists stored in pg_statistic
/// - Values excluded from histogram to avoid double-counting
public struct MostCommonValues: Sendable, Codable {

    /// MCV entry: value and its frequency
    public struct Entry: Sendable, Codable {
        /// The value
        public let value: ComparableValue

        /// Frequency (fraction of total rows)
        public let frequency: Double

        /// Estimated count
        public let count: Int64

        private enum CodingKeys: String, CodingKey {
            case value, frequency, count

            var intValue: Int? {
                switch self {
                case .value: return 1
                case .frequency: return 2
                case .count: return 3
                }
            }

            init?(intValue: Int) {
                switch intValue {
                case 1: self = .value
                case 2: self = .frequency
                case 3: self = .count
                default: return nil
                }
            }

            init?(stringValue: String) { self.init(rawValue: stringValue) }
            var stringValue: String { rawValue }
        }

        public init(value: ComparableValue, frequency: Double, count: Int64) {
            self.value = value
            self.frequency = frequency
            self.count = count
        }
    }

    /// Ordered list of most common values (most frequent first)
    public let entries: [Entry]

    /// Sum of all MCV frequencies
    /// This is the fraction of the table covered by MCVs
    public let totalFrequency: Double

    /// Number of distinct values in MCV
    public var count: Int { entries.count }

    /// Timestamp when statistics were collected
    public let timestamp: Date

    private enum CodingKeys: String, CodingKey {
        case entries, totalFrequency, timestamp

        var intValue: Int? {
            switch self {
            case .entries: return 1
            case .totalFrequency: return 2
            case .timestamp: return 3
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .entries
            case 2: self = .totalFrequency
            case 3: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) { self.init(rawValue: stringValue) }
        var stringValue: String { rawValue }
    }

    /// Create an MCV list
    public init(entries: [Entry], timestamp: Date = Date()) {
        self.entries = entries
        self.totalFrequency = entries.reduce(0) { $0 + $1.frequency }
        self.timestamp = timestamp
    }

    /// Empty MCV
    public static let empty = MostCommonValues(entries: [])

    // MARK: - Selectivity Estimation

    /// Get selectivity for exact equality: P(column = value)
    ///
    /// - Parameter value: The value to match
    /// - Returns: Selectivity if value is in MCV, nil otherwise
    public func selectivity(for value: ComparableValue) -> Double? {
        entries.first { $0.value == value }?.frequency
    }

    /// Get combined selectivity for multiple values (IN clause)
    ///
    /// - Parameter values: Values to match
    /// - Returns: Combined selectivity for values in MCV (partial result)
    public func selectivity(forIn values: [ComparableValue]) -> Double {
        let valueSet = Set(values)
        return entries
            .filter { valueSet.contains($0.value) }
            .reduce(0) { $0 + $1.frequency }
    }

    /// Check if value is in MCV list
    public func contains(_ value: ComparableValue) -> Bool {
        entries.contains { $0.value == value }
    }

    /// Get histogram fraction (portion of data NOT in MCV)
    ///
    /// Used in combined selectivity calculation:
    /// `histogram_selectivity × histogram_fraction`
    public var histogramFraction: Double {
        1.0 - totalFrequency
    }

    // MARK: - Range Selectivity

    /// Calculate MCV selectivity for range query
    ///
    /// Sums frequencies of all MCV values that fall within the range.
    ///
    /// - Parameters:
    ///   - min: Minimum value (nil for unbounded)
    ///   - max: Maximum value (nil for unbounded)
    ///   - minInclusive: Whether min is inclusive
    ///   - maxInclusive: Whether max is inclusive
    /// - Returns: Selectivity from MCV values in range
    public func rangeSelectivity(
        min: ComparableValue?,
        max: ComparableValue?,
        minInclusive: Bool = true,
        maxInclusive: Bool = true
    ) -> Double {
        var selectivity = 0.0

        for entry in entries {
            let value = entry.value

            // Check lower bound
            if let min = min {
                if minInclusive {
                    if value < min { continue }
                } else {
                    if value <= min { continue }
                }
            }

            // Check upper bound
            if let max = max {
                if maxInclusive {
                    if value > max { continue }
                } else {
                    if value >= max { continue }
                }
            }

            selectivity += entry.frequency
        }

        return selectivity
    }
}

// MARK: - MCV Builder

/// Builder for constructing MCV lists from sampled data
///
/// Uses a hash map to count frequencies during sampling, then extracts
/// the top-k most frequent values that meet the minimum frequency threshold.
///
/// **Algorithm**:
/// 1. Count occurrences of each value during sampling
/// 2. Scale counts to estimated population frequencies
/// 3. Filter by minimum frequency threshold
/// 4. Sort by frequency and keep top maxSize entries
///
/// **Reference**:
/// PostgreSQL's ANALYZE uses similar approach with default_statistics_target
/// controlling the maximum MCV list size (default: 100)
public struct MCVBuilder: Sendable {

    /// Maximum number of MCV entries
    private let maxSize: Int

    /// Minimum frequency to be included (default: 0.01 = 1%)
    private let minFrequency: Double

    /// Value counts during sampling
    private var counts: [ComparableValue: Int]

    /// Total samples seen
    public private(set) var totalSamples: Int

    /// Create an MCV builder
    ///
    /// - Parameters:
    ///   - maxSize: Maximum MCV entries (default: 100, PostgreSQL default)
    ///   - minFrequency: Minimum frequency threshold (default: 0.01 = 1%)
    public init(maxSize: Int = 100, minFrequency: Double = 0.01) {
        self.maxSize = maxSize
        self.minFrequency = minFrequency
        self.counts = [:]
        self.totalSamples = 0
    }

    /// Add a value to the frequency count
    public mutating func add(_ value: ComparableValue) {
        totalSamples += 1
        counts[value, default: 0] += 1
    }

    /// Add a FieldValue
    public mutating func add(_ value: FieldValue) {
        add(ComparableValue(fieldValue: value))
    }

    /// Build the MCV list
    ///
    /// - Parameters:
    ///   - totalCount: Total population count (for frequency scaling)
    ///   - sampleCount: Number of samples taken (if different from totalSamples)
    /// - Returns: MostCommonValues instance
    public func build(totalCount: Int64, sampleCount: Int? = nil) -> MostCommonValues {
        guard totalSamples > 0 else {
            return .empty
        }

        let effectiveSampleCount = sampleCount ?? totalSamples

        // Calculate frequencies and filter
        var entries: [(value: ComparableValue, frequency: Double, count: Int64)] = []

        for (value, count) in counts {
            // Estimate population frequency from sample
            let frequency = Double(count) / Double(effectiveSampleCount)

            // Only include if above minimum frequency
            if frequency >= minFrequency {
                let estimatedCount = Int64(Double(totalCount) * frequency)
                entries.append((value, frequency, estimatedCount))
            }
        }

        // Sort by frequency (descending) and limit to maxSize
        entries.sort { $0.frequency > $1.frequency }
        let limited = entries.prefix(maxSize)

        let mcvEntries = limited.map { entry in
            MostCommonValues.Entry(
                value: entry.value,
                frequency: entry.frequency,
                count: entry.count
            )
        }

        return MostCommonValues(entries: mcvEntries)
    }

    /// Get values that should be excluded from histogram
    ///
    /// Returns the set of values in MCV (these should not be included
    /// in histogram buckets to avoid double-counting)
    public func mcvValues() -> Set<ComparableValue> {
        let built = build(totalCount: Int64(totalSamples))
        return Set(built.entries.map { $0.value })
    }

    /// Reset the builder
    public mutating func reset() {
        counts.removeAll()
        totalSamples = 0
    }
}

// MARK: - Combined Selectivity Estimator

/// Combined MCV + Histogram selectivity estimation
///
/// Implements PostgreSQL's approach to selectivity estimation:
/// ```
/// selectivity = mcv_selectivity + histogram_selectivity × histogram_fraction
/// ```
///
/// **Reference**:
/// PostgreSQL src/backend/utils/adt/selfuncs.c, scalarineqsel()
public struct CombinedSelectivityEstimator: Sendable {

    /// MCV list
    public let mcv: MostCommonValues

    /// Histogram (excludes MCV values)
    public let histogram: Histogram

    /// Create a combined estimator
    public init(mcv: MostCommonValues, histogram: Histogram) {
        self.mcv = mcv
        self.histogram = histogram
    }

    /// Estimate equality selectivity: P(column = value)
    ///
    /// - Parameter value: The value to match
    /// - Returns: Estimated selectivity
    ///
    /// **Algorithm**:
    /// - If value is in MCV: return MCV frequency directly
    /// - If value is not in MCV: return histogram estimate
    ///
    /// **Note**: Histogram selectivity is already relative to total population
    /// (bucket counts are scaled by totalCount/sampleCount, then divided by totalCount).
    /// We do NOT multiply by histogramFraction since histogram only contains
    /// non-MCV values and its selectivity already represents the fraction of
    /// total data that is both in the bucket range AND not in MCV.
    public func equalitySelectivity(value: ComparableValue) -> Double {
        // Check MCV first
        if let mcvSel = mcv.selectivity(for: value) {
            return mcvSel
        }

        // Not in MCV: use histogram estimate directly
        // Histogram selectivity is relative to total population, not histogram-only portion
        return histogram.estimateEqualsSelectivity(value: value)
    }

    /// Estimate range selectivity
    ///
    /// - Parameters:
    ///   - min: Minimum value (nil for unbounded)
    ///   - max: Maximum value (nil for unbounded)
    ///   - minInclusive: Whether min is inclusive
    ///   - maxInclusive: Whether max is inclusive
    /// - Returns: Estimated selectivity
    ///
    /// **Algorithm**:
    /// P(value in range) = P(value in range AND in MCV) + P(value in range AND not in MCV)
    ///                   = mcvSel + histSel
    ///
    /// **Note**: Unlike the PostgreSQL formula `mcv_sel + hist_sel × hist_fraction`,
    /// our histogram selectivity is already relative to total population
    /// (not relative to the histogram-only portion), so we add directly.
    public func rangeSelectivity(
        min: ComparableValue?,
        max: ComparableValue?,
        minInclusive: Bool = true,
        maxInclusive: Bool = true
    ) -> Double {
        // MCV contribution: sum frequencies of MCVs in range
        let mcvSel = mcv.rangeSelectivity(
            min: min,
            max: max,
            minInclusive: minInclusive,
            maxInclusive: maxInclusive
        )

        // Histogram contribution (already relative to total population)
        let histSel = histogram.estimateRangeSelectivity(
            min: min,
            max: max,
            minInclusive: minInclusive,
            maxInclusive: maxInclusive
        )

        // Combined: direct sum since both are relative to total population
        return mcvSel + histSel
    }

    /// Estimate IN clause selectivity
    ///
    /// - Parameter values: List of values
    /// - Returns: Estimated selectivity
    public func inSelectivity(values: [ComparableValue]) -> Double {
        var total = 0.0

        for value in values {
            total += equalitySelectivity(value: value)
        }

        // Cap at 1.0
        return min(1.0, total)
    }
}

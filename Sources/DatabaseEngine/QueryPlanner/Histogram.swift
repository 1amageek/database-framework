// Histogram.swift
// QueryPlanner - Histogram for selectivity estimation

import Foundation
import Core
import FoundationDB

/// Histogram for value distribution and selectivity estimation
///
/// Provides accurate selectivity estimates for:
/// - Equality queries: P(field = value)
/// - Range queries: P(field > value), P(field BETWEEN a AND b)
/// - NULL checks: P(field IS NULL)
///
/// **Storage Format**:
/// Histograms are stored as JSON in FoundationDB for persistence.
///
/// **Usage**:
/// ```swift
/// let histogram = Histogram(buckets: buckets, totalCount: 10000, nullCount: 500)
///
/// // Equality selectivity
/// let eqSel = histogram.estimateEqualsSelectivity(value: .int64(42))
///
/// // Range selectivity
/// let rangeSel = histogram.estimateRangeSelectivity(
///     min: .int64(10),
///     max: .int64(100),
///     minInclusive: true,
///     maxInclusive: false
/// )
/// ```
public struct Histogram: Sendable, Codable {

    /// Histogram buckets (sorted by lowerBound)
    public let buckets: [Bucket]

    /// Total value count (excluding nulls)
    public let totalCount: Int64

    /// Number of null values
    public let nullCount: Int64

    /// Number of distinct values (estimated)
    public let distinctCount: Int64

    /// Collection timestamp
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case buckets, totalCount, nullCount, distinctCount, timestamp

        var intValue: Int? {
            switch self {
            case .buckets: return 1
            case .totalCount: return 2
            case .nullCount: return 3
            case .distinctCount: return 4
            case .timestamp: return 5
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .buckets
            case 2: self = .totalCount
            case 3: self = .nullCount
            case 4: self = .distinctCount
            case 5: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) { self.init(rawValue: stringValue) }
        var stringValue: String { rawValue }
    }

    /// Create a histogram
    public init(
        buckets: [Bucket],
        totalCount: Int64,
        nullCount: Int64 = 0,
        distinctCount: Int64? = nil,
        timestamp: Date = Date()
    ) {
        self.buckets = buckets
        self.totalCount = totalCount
        self.nullCount = nullCount
        self.distinctCount = distinctCount ?? Int64(buckets.count)
        self.timestamp = timestamp
    }

    // MARK: - Bucket

    /// A histogram bucket representing a range of values
    public struct Bucket: Sendable, Codable {
        /// Lower bound (inclusive)
        public let lowerBound: FieldValue

        /// Upper bound (inclusive for last bucket, exclusive otherwise)
        public let upperBound: FieldValue

        /// Number of values in this bucket
        public let count: Int64

        /// Number of distinct values in this bucket
        public let distinctCount: Int64

        /// Protobuf field numbers
        private enum CodingKeys: String, CodingKey {
            case lowerBound, upperBound, count, distinctCount

            var intValue: Int? {
                switch self {
                case .lowerBound: return 1
                case .upperBound: return 2
                case .count: return 3
                case .distinctCount: return 4
                }
            }

            init?(intValue: Int) {
                switch intValue {
                case 1: self = .lowerBound
                case 2: self = .upperBound
                case 3: self = .count
                case 4: self = .distinctCount
                default: return nil
                }
            }

            init?(stringValue: String) { self.init(rawValue: stringValue) }
            var stringValue: String { rawValue }
        }

        public init(
            lowerBound: FieldValue,
            upperBound: FieldValue,
            count: Int64,
            distinctCount: Int64
        ) {
            self.lowerBound = lowerBound
            self.upperBound = upperBound
            self.count = count
            self.distinctCount = distinctCount
        }
    }

    // MARK: - Selectivity Estimation

    /// Estimate selectivity for equality query: P(field = value)
    ///
    /// - Parameter value: The value to match
    /// - Returns: Estimated selectivity (0.0 - 1.0)
    public func estimateEqualsSelectivity(value: FieldValue) -> Double {
        guard totalCount > 0 else { return 0.0 }

        // Find the bucket containing this value
        guard let bucket = findBucket(containing: value) else {
            return 0.0  // Value outside histogram range
        }

        // Estimate: assume uniform distribution within bucket
        // selectivity = (1 / distinctCount) * (bucket.count / totalCount)
        let bucketSelectivity = Double(bucket.count) / Double(totalCount)
        let distinctInBucket = max(1, bucket.distinctCount)

        return bucketSelectivity / Double(distinctInBucket)
    }

    /// Estimate selectivity for range query
    ///
    /// - Parameters:
    ///   - min: Minimum value (nil for unbounded)
    ///   - max: Maximum value (nil for unbounded)
    ///   - minInclusive: Whether min is inclusive
    ///   - maxInclusive: Whether max is inclusive
    /// - Returns: Estimated selectivity (0.0 - 1.0)
    public func estimateRangeSelectivity(
        min: FieldValue?,
        max: FieldValue?,
        minInclusive: Bool = true,
        maxInclusive: Bool = true
    ) -> Double {
        guard totalCount > 0, !buckets.isEmpty else { return 0.0 }

        var matchingCount: Double = 0

        for bucket in buckets {
            let overlap = calculateBucketOverlap(
                bucket: bucket,
                rangeMin: min,
                rangeMax: max,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive
            )

            matchingCount += Double(bucket.count) * overlap
        }

        return matchingCount / Double(totalCount)
    }

    /// Estimate selectivity for less-than query: P(field < value)
    public func estimateLessThanSelectivity(value: FieldValue, inclusive: Bool = false) -> Double {
        return estimateRangeSelectivity(min: nil, max: value, minInclusive: true, maxInclusive: inclusive)
    }

    /// Estimate selectivity for greater-than query: P(field > value)
    public func estimateGreaterThanSelectivity(value: FieldValue, inclusive: Bool = false) -> Double {
        return estimateRangeSelectivity(min: value, max: nil, minInclusive: inclusive, maxInclusive: true)
    }

    /// Estimate selectivity for NULL check
    ///
    /// - Parameter isNull: true for IS NULL, false for IS NOT NULL
    /// - Returns: Estimated selectivity (0.0 - 1.0)
    public func estimateNullSelectivity(isNull: Bool) -> Double {
        let total = totalCount + nullCount
        guard total > 0 else { return 0.0 }

        if isNull {
            return Double(nullCount) / Double(total)
        } else {
            return Double(totalCount) / Double(total)
        }
    }

    // MARK: - Private Methods

    /// Find the bucket containing a value
    private func findBucket(containing value: FieldValue) -> Bucket? {
        for bucket in buckets {
            if value >= bucket.lowerBound && value <= bucket.upperBound {
                return bucket
            }
        }
        return nil
    }

    /// Calculate the overlap fraction between a bucket and a query range
    ///
    /// Returns a value between 0.0 (no overlap) and 1.0 (full overlap)
    private func calculateBucketOverlap(
        bucket: Bucket,
        rangeMin: FieldValue?,
        rangeMax: FieldValue?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) -> Double {
        let bucketLower = bucket.lowerBound
        let bucketUpper = bucket.upperBound

        // Check for no overlap
        if let rangeMax = rangeMax {
            if maxInclusive {
                if bucketLower > rangeMax { return 0.0 }
            } else {
                if bucketLower >= rangeMax { return 0.0 }
            }
        }

        if let rangeMin = rangeMin {
            if minInclusive {
                if bucketUpper < rangeMin { return 0.0 }
            } else {
                if bucketUpper <= rangeMin { return 0.0 }
            }
        }

        // Full overlap check
        let minCovered = rangeMin == nil || rangeMin! <= bucketLower
        let maxCovered = rangeMax == nil || rangeMax! >= bucketUpper

        if minCovered && maxCovered {
            return 1.0  // Full bucket overlap
        }

        // Partial overlap - use interpolation for numeric types
        return interpolateOverlap(
            bucketLower: bucketLower,
            bucketUpper: bucketUpper,
            rangeMin: rangeMin,
            rangeMax: rangeMax
        )
    }

    /// Interpolate overlap fraction for partial bucket overlap
    ///
    /// For numeric types (int64, double, date): Uses linear interpolation based on
    /// the actual numeric distance within the bucket range.
    ///
    /// For string types: Uses PostgreSQL-style scalar conversion (base-256 fractional).
    /// Reference: PostgreSQL src/backend/utils/adt/selfuncs.c `convert_one_string_to_scalar`
    ///
    /// For other types (bool, data, null): Falls back to 0.5 (conservative estimate).
    private func interpolateOverlap(
        bucketLower: FieldValue,
        bucketUpper: FieldValue,
        rangeMin: FieldValue?,
        rangeMax: FieldValue?
    ) -> Double {
        // Try numeric interpolation (int64, double, date)
        if let bucketWidth = bucketUpper.numericDifference(from: bucketLower), bucketWidth > 0 {
            var effectiveLower = 0.0
            var effectiveUpper = bucketWidth

            if let rangeMin = rangeMin,
               let minOffset = rangeMin.numericDifference(from: bucketLower) {
                effectiveLower = max(0, minOffset)
            }

            if let rangeMax = rangeMax,
               let maxOffset = rangeMax.numericDifference(from: bucketLower) {
                effectiveUpper = min(bucketWidth, maxOffset)
            }

            let overlapWidth = max(0, effectiveUpper - effectiveLower)
            return overlapWidth / bucketWidth
        }

        // Try string interpolation using PostgreSQL algorithm
        if case .string(let lowerStr) = bucketLower,
           case .string(let upperStr) = bucketUpper {
            return interpolateStringOverlap(
                bucketLower: lowerStr,
                bucketUpper: upperStr,
                rangeMin: rangeMin,
                rangeMax: rangeMax
            )
        }

        // Non-interpolatable types: conservative estimate (50% overlap)
        return 0.5
    }

    /// Interpolate overlap for string-typed buckets
    ///
    /// Uses PostgreSQL-style scalar conversion to map strings to numeric values.
    /// Reference: PostgreSQL src/backend/utils/adt/selfuncs.c
    /// - `convert_string_to_scalar`: Normalizes strings using common prefix stripping
    /// - `convert_one_string_to_scalar`: Base-256 fractional conversion
    ///
    /// Algorithm:
    /// 1. Strip common prefix from all strings
    /// 2. Convert remaining suffixes to scalar values in [0, 1]
    /// 3. Interpolate within the normalized range
    private func interpolateStringOverlap(
        bucketLower: String,
        bucketUpper: String,
        rangeMin: FieldValue?,
        rangeMax: FieldValue?
    ) -> Double {
        // Find common prefix length to strip (PostgreSQL optimization)
        let commonPrefixLen = Self.commonPrefixLength(bucketLower, bucketUpper)

        // Extract suffixes after common prefix
        let lowerSuffix = String(bucketLower.dropFirst(commonPrefixLen))
        let upperSuffix = String(bucketUpper.dropFirst(commonPrefixLen))

        // Determine character range from bucket bounds
        let (rangeLo, rangeHi) = Self.determineCharacterRange(lowerSuffix, upperSuffix)

        // Convert bucket bounds to scalar
        let lowerScalar = Self.convertStringToScalar(lowerSuffix, rangeLo: rangeLo, rangeHi: rangeHi)
        let upperScalar = Self.convertStringToScalar(upperSuffix, rangeLo: rangeLo, rangeHi: rangeHi)

        let bucketWidth = upperScalar - lowerScalar
        guard bucketWidth > 0 else { return 1.0 }  // Single value bucket

        var effectiveLower = lowerScalar
        var effectiveUpper = upperScalar

        if let rangeMin = rangeMin, case .string(let minStr) = rangeMin {
            let minSuffix = String(minStr.dropFirst(min(commonPrefixLen, minStr.count)))
            let minScalar = Self.convertStringToScalar(minSuffix, rangeLo: rangeLo, rangeHi: rangeHi)
            effectiveLower = max(lowerScalar, minScalar)
        }

        if let rangeMax = rangeMax, case .string(let maxStr) = rangeMax {
            let maxSuffix = String(maxStr.dropFirst(min(commonPrefixLen, maxStr.count)))
            let maxScalar = Self.convertStringToScalar(maxSuffix, rangeLo: rangeLo, rangeHi: rangeHi)
            effectiveUpper = min(upperScalar, maxScalar)
        }

        let overlapWidth = max(0, effectiveUpper - effectiveLower)
        return overlapWidth / bucketWidth
    }

    /// Convert a string to a scalar value using PostgreSQL algorithm
    ///
    /// Treats the string as a base-N fractional number where N = rangeHi - rangeLo.
    /// Each character contributes: (char - rangeLo) / N^position
    ///
    /// Reference: PostgreSQL `convert_one_string_to_scalar`
    /// - Processes up to 12 bytes (prevents denom overflow: N^13 ≤ 2.03e31)
    /// - Returns value in range [0.0, 1.0]
    ///
    /// - Parameters:
    ///   - string: The string to convert
    ///   - rangeLo: Minimum character value (typically 0 or 'a')
    ///   - rangeHi: Maximum character value (typically 255 or 'z')
    /// - Returns: Scalar value in [0.0, 1.0]
    private static func convertStringToScalar(_ string: String, rangeLo: UInt32, rangeHi: UInt32) -> Double {
        guard rangeHi > rangeLo else { return 0.0 }

        let base = Double(rangeHi - rangeLo + 1)
        var result = 0.0
        var denom = base

        // Process up to 12 bytes (PostgreSQL limit to prevent overflow)
        // denom can grow to base^13, and 256^13 ≈ 2.03e31 is safe for Double
        let maxBytes = 12
        let chars = Array(string.unicodeScalars.prefix(maxBytes))

        for char in chars {
            let charVal = char.value
            // Clamp to range
            let normalized: Double
            if charVal < rangeLo {
                normalized = 0.0
            } else if charVal > rangeHi {
                normalized = Double(rangeHi - rangeLo)
            } else {
                normalized = Double(charVal - rangeLo)
            }

            result += normalized / denom
            denom *= base
        }

        return result
    }

    /// Find the length of common prefix between two strings
    private static func commonPrefixLength(_ a: String, _ b: String) -> Int {
        let aChars = Array(a)
        let bChars = Array(b)
        var length = 0

        for i in 0..<min(aChars.count, bChars.count) {
            if aChars[i] == bChars[i] {
                length += 1
            } else {
                break
            }
        }

        return length
    }

    /// Determine character range from string samples
    ///
    /// Analyzes the strings to find the minimum and maximum character values,
    /// which defines the base for scalar conversion.
    ///
    /// Returns (0, 255) for byte-based comparison if strings contain varied characters,
    /// or a narrower range for ASCII letters/digits.
    private static func determineCharacterRange(_ a: String, _ b: String) -> (UInt32, UInt32) {
        var minChar: UInt32 = 255
        var maxChar: UInt32 = 0

        for char in a.unicodeScalars {
            minChar = min(minChar, char.value)
            maxChar = max(maxChar, char.value)
        }
        for char in b.unicodeScalars {
            minChar = min(minChar, char.value)
            maxChar = max(maxChar, char.value)
        }

        // If no characters found, use full byte range
        if maxChar < minChar {
            return (0, 255)
        }

        // Expand range slightly to handle values outside sample
        // PostgreSQL uses broader ranges; we use byte range for simplicity
        return (0, 255)
    }
}

// MARK: - Histogram Builder

/// Builder for constructing histograms from data
///
/// Supports PostgreSQL-style histogram construction that excludes MCV (Most Common Values)
/// to avoid double-counting in selectivity estimation.
///
/// **PostgreSQL Integration Pattern**:
/// ```
/// Combined selectivity = mcv_selectivity + histogram_selectivity × histogram_fraction
/// where histogram_fraction = 1 - sum(mcv_frequencies)
/// ```
///
/// **Reference**:
/// PostgreSQL src/backend/commands/analyze.c
/// - MCV values are identified first
/// - Histogram is built from remaining (non-MCV) values
/// - This prevents double-counting in selectivity calculations
public struct HistogramBuilder: Sendable {

    /// Build a histogram from FieldValue samples with HyperLogLog cardinality
    ///
    /// - Parameters:
    ///   - samples: Array of sampled values
    ///   - totalCount: Total population count (for scaling)
    ///   - nullCount: Number of null values
    ///   - bucketCount: Target number of buckets
    ///   - hll: HyperLogLog for distinct count estimation (optional)
    ///   - excludeValues: MCV values to exclude from histogram (prevents double-counting)
    /// - Returns: Histogram instance
    public static func build(
        samples: [FieldValue],
        totalCount: Int64,
        nullCount: Int64 = 0,
        bucketCount: Int = 100,
        hll: HyperLogLog? = nil,
        excludeValues: Set<FieldValue> = []
    ) -> Histogram {
        // Filter out nulls and exclude MCV values
        let values = samples.compactMap { fieldValue -> FieldValue? in
            if case .null = fieldValue { return nil }
            // Exclude MCV values from histogram to avoid double-counting
            if excludeValues.contains(fieldValue) { return nil }
            return fieldValue
        }

        guard !values.isEmpty else {
            return Histogram(buckets: [], totalCount: totalCount, nullCount: nullCount)
        }

        let sorted = values.sorted()

        // Calculate distinct count (excluding MCV values)
        let histogramDistinctCount: Int64
        if let hll = hll {
            // Adjust HLL cardinality by subtracting MCV count
            histogramDistinctCount = max(1, hll.cardinality() - Int64(excludeValues.count))
        } else {
            histogramDistinctCount = Int64(Set(values).count)
        }

        // Build equi-height buckets
        let buckets = buildEquiHeightBuckets(
            sorted: sorted,
            totalCount: totalCount,
            sampleCount: Int64(samples.count),
            bucketCount: bucketCount,
            excludeCount: Int64(excludeValues.count)
        )

        return Histogram(
            buckets: buckets,
            totalCount: totalCount,
            nullCount: nullCount,
            distinctCount: histogramDistinctCount
        )
    }

    /// Build equi-height buckets from sorted values
    ///
    /// Creates approximately equal-height buckets (equi-depth histogram).
    /// This provides better selectivity estimates than equi-width histograms
    /// for skewed data distributions.
    ///
    /// **Reference**: PostgreSQL src/backend/commands/analyze.c, compute_scalar_stats()
    private static func buildEquiHeightBuckets(
        sorted: [FieldValue],
        totalCount: Int64,
        sampleCount: Int64,
        bucketCount: Int,
        excludeCount: Int64 = 0
    ) -> [Histogram.Bucket] {
        // Scale factor accounts for sampling
        // For histogram values (excluding MCV), we scale by the fraction not in MCV
        let scaleFactor = sampleCount > 0 ? Double(totalCount) / Double(sampleCount) : 1.0
        let valuesPerBucket = max(1, sorted.count / bucketCount)

        var buckets: [Histogram.Bucket] = []
        var i = 0

        while i < sorted.count {
            let startIndex = i
            var endIndex = min(i + valuesPerBucket, sorted.count)

            // Extend to include all equal values at boundary
            // This ensures the same value doesn't appear in multiple buckets
            while endIndex < sorted.count && sorted[endIndex] == sorted[endIndex - 1] {
                endIndex += 1
            }

            let bucketValues = Array(sorted[startIndex..<endIndex])
            guard let first = bucketValues.first, let last = bucketValues.last else {
                i = endIndex
                continue
            }
            let count = Int64(Double(bucketValues.count) * scaleFactor)
            let distinctCount = Int64(Set(bucketValues).count)

            buckets.append(Histogram.Bucket(
                lowerBound: first,
                upperBound: last,
                count: count,
                distinctCount: distinctCount
            ))

            i = endIndex
        }

        return buckets
    }
}

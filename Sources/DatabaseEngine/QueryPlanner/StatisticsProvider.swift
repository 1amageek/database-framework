// StatisticsProvider.swift
// QueryPlanner - Statistics for cost estimation

import Foundation
import Core

/// Provides statistics about tables and indexes for cost estimation
public protocol StatisticsProvider: Sendable {
    /// Estimated total row count for a type
    func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int

    /// Estimated distinct values for a field
    func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int?

    /// Selectivity for equality condition
    func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double?

    /// Selectivity for range condition
    func rangeSelectivity<T: Persistable>(
        field: String,
        range: RangeBound,
        type: T.Type
    ) -> Double?

    /// Selectivity for null check
    func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double?

    /// Index entry count estimate
    func estimatedIndexEntries(
        index: IndexDescriptor
    ) -> Int?
}

// MARK: - Default Statistics Provider

/// Default statistics provider with heuristic estimates
///
/// **⚠️ PLACEHOLDER IMPLEMENTATION**
///
/// This provider uses simple heuristics and should be replaced with
/// `CollectedStatisticsProvider` in production for accurate cost estimation.
///
/// **Limitations**:
/// - Returns same row count for all types
/// - Uses fixed 10% distinct value ratio regardless of field type
/// - No per-field statistics (cardinality, null ratio, value distribution)
/// - No histogram data for range selectivity estimation
///
/// **Production Requirements** (for accurate query planning):
/// - Per-table row counts (via periodic COUNT or sampling)
/// - Per-field distinct value counts (via HyperLogLog or exact count)
/// - Histogram data for range selectivity (value distribution buckets)
/// - Null ratio per field (percentage of NULL values)
/// - Index-specific entry counts
public struct DefaultStatisticsProvider: StatisticsProvider {

    /// Default row count assumption
    private let defaultRowCount: Int

    /// Default distinct value ratio
    private let distinctValueRatio: Double

    /// Default null ratio
    private let nullRatio: Double

    public init(
        defaultRowCount: Int = 10000,
        distinctValueRatio: Double = 0.1,
        nullRatio: Double = 0.05
    ) {
        self.defaultRowCount = defaultRowCount
        self.distinctValueRatio = distinctValueRatio
        self.nullRatio = nullRatio
    }

    public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        defaultRowCount
    }

    public func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int? {
        // HEURISTIC: Assume 10% distinct values by default
        max(1, Int(Double(defaultRowCount) * distinctValueRatio))
    }

    public func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: 1 / estimated distinct values
        guard let distinct = estimatedDistinctValues(field: field, type: type) else {
            return nil
        }
        return 1.0 / Double(distinct)
    }

    public func rangeSelectivity<T: Persistable>(
        field: String,
        range: RangeBound,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: Default to 30% for ranges
        // In production, use histogram-based estimation
        0.3
    }

    public func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: Default 5% null
        nullRatio
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        defaultRowCount
    }
}

// MARK: - Table Statistics

/// Statistics for a single table/type
public struct TableStatistics: Sendable {
    /// Total row count
    public let rowCount: Int

    /// Sample size used for estimation
    public let sampleSize: Int

    /// Last update timestamp
    public let lastUpdated: Date

    public init(rowCount: Int, sampleSize: Int, lastUpdated: Date = Date()) {
        self.rowCount = rowCount
        self.sampleSize = sampleSize
        self.lastUpdated = lastUpdated
    }
}

/// Statistics for a single field
public struct FieldStatistics: Sendable {
    /// Field name
    public let fieldName: String

    /// Number of distinct values
    public let distinctValues: Int

    /// Ratio of null values (0.0 - 1.0)
    public let nullRatio: Double

    /// Minimum value (if orderable)
    public let minValue: AnySendable?

    /// Maximum value (if orderable)
    public let maxValue: AnySendable?

    /// Histogram buckets for range estimation
    public let histogram: [HistogramBucket]?

    public init(
        fieldName: String,
        distinctValues: Int,
        nullRatio: Double,
        minValue: AnySendable? = nil,
        maxValue: AnySendable? = nil,
        histogram: [HistogramBucket]? = nil
    ) {
        self.fieldName = fieldName
        self.distinctValues = distinctValues
        self.nullRatio = nullRatio
        self.minValue = minValue
        self.maxValue = maxValue
        self.histogram = histogram
    }
}

/// A histogram bucket for value distribution
public struct HistogramBucket: Sendable {
    /// Lower bound of the bucket
    public let lowerBound: AnySendable

    /// Upper bound of the bucket
    public let upperBound: AnySendable

    /// Number of values in this bucket
    public let count: Int

    /// Cumulative count up to this bucket
    public let cumulativeCount: Int

    public init(
        lowerBound: AnySendable,
        upperBound: AnySendable,
        count: Int,
        cumulativeCount: Int
    ) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.count = count
        self.cumulativeCount = cumulativeCount
    }
}

/// Statistics for an index
public struct IndexStatistics: Sendable {
    /// Index name
    public let indexName: String

    /// Total entry count
    public let entryCount: Int

    /// Average entries per key (for non-unique indexes)
    public let avgEntriesPerKey: Double

    /// Index size in bytes (approximate)
    public let sizeBytes: Int?

    public init(
        indexName: String,
        entryCount: Int,
        avgEntriesPerKey: Double = 1.0,
        sizeBytes: Int? = nil
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.avgEntriesPerKey = avgEntriesPerKey
        self.sizeBytes = sizeBytes
    }
}

// MARK: - Collected Statistics Provider

/// Statistics provider that uses collected statistics from the database
///
/// This provider maintains actual statistics collected from the database,
/// providing accurate cost estimation for query planning.
public final class CollectedStatisticsProvider: StatisticsProvider, @unchecked Sendable {

    /// Table statistics by type name
    private var tableStats: [String: TableStatistics] = [:]

    /// Field statistics by "TypeName.fieldName"
    private var fieldStats: [String: FieldStatistics] = [:]

    /// Index statistics by index name
    private var indexStats: [String: IndexStatistics] = [:]

    /// Lock for thread-safe access
    private let lock = NSLock()

    /// Default fallback provider
    private let fallback: DefaultStatisticsProvider

    public init(fallbackRowCount: Int = 10000) {
        self.fallback = DefaultStatisticsProvider(defaultRowCount: fallbackRowCount)
    }

    // MARK: - StatisticsProvider

    public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        let typeName = String(describing: type)
        lock.lock()
        defer { lock.unlock() }

        return tableStats[typeName]?.rowCount ?? fallback.estimatedRowCount(for: type)
    }

    public func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int? {
        let key = "\(type).\(field)"
        lock.lock()
        defer { lock.unlock() }

        return fieldStats[key]?.distinctValues ?? fallback.estimatedDistinctValues(field: field, type: type)
    }

    public func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        guard let distinct = estimatedDistinctValues(field: field, type: type) else {
            return fallback.equalitySelectivity(field: field, type: type)
        }
        return 1.0 / Double(max(1, distinct))
    }

    public func rangeSelectivity<T: Persistable>(
        field: String,
        range: RangeBound,
        type: T.Type
    ) -> Double? {
        let key = "\(type).\(field)"
        lock.lock()
        let stats = fieldStats[key]
        lock.unlock()

        guard let stats = stats, let histogram = stats.histogram else {
            return fallback.rangeSelectivity(field: field, range: range, type: type)
        }

        // Use histogram for range estimation
        return estimateRangeSelectivityFromHistogram(histogram: histogram, range: range)
    }

    public func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        let key = "\(type).\(field)"
        lock.lock()
        let stats = fieldStats[key]
        lock.unlock()

        return stats?.nullRatio ?? fallback.nullSelectivity(field: field, type: type)
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        lock.lock()
        let stats = indexStats[index.name]
        lock.unlock()

        return stats?.entryCount ?? fallback.estimatedIndexEntries(index: index)
    }

    // MARK: - Statistics Collection

    /// Update table statistics
    public func updateTableStats<T: Persistable>(
        for type: T.Type,
        rowCount: Int,
        sampleSize: Int
    ) {
        let typeName = String(describing: type)
        let stats = TableStatistics(rowCount: rowCount, sampleSize: sampleSize)

        lock.lock()
        tableStats[typeName] = stats
        lock.unlock()
    }

    /// Update field statistics
    public func updateFieldStats<T: Persistable>(
        for type: T.Type,
        field: String,
        stats: FieldStatistics
    ) {
        let key = "\(type).\(field)"

        lock.lock()
        fieldStats[key] = stats
        lock.unlock()
    }

    /// Update index statistics
    public func updateIndexStats(_ stats: IndexStatistics) {
        lock.lock()
        indexStats[stats.indexName] = stats
        lock.unlock()
    }

    // MARK: - Histogram Range Estimation

    private func estimateRangeSelectivityFromHistogram(
        histogram: [HistogramBucket],
        range: RangeBound
    ) -> Double {
        guard !histogram.isEmpty else { return 0.3 }

        let totalCount = histogram.last?.cumulativeCount ?? 0
        guard totalCount > 0 else { return 0.3 }

        // Simplified estimation: count buckets that overlap with range
        // Full implementation would interpolate within buckets
        var matchingCount = 0
        for bucket in histogram {
            // Assume all buckets partially match (simplified)
            matchingCount += bucket.count
        }

        return Double(matchingCount) / Double(totalCount)
    }
}

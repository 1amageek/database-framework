// StatisticsManager.swift
// QueryPlanner - Unified statistics management

import Foundation
import FoundationDB
import Core
import Synchronization

/// Unified manager for collecting, storing, and querying statistics
///
/// Combines persistence (StatisticsStorage), collection (sampling + HyperLogLog),
/// and query access (StatisticsProvider) into a single interface.
///
/// **Usage**:
/// ```swift
/// let manager = StatisticsManager(container: container, subspace: subspace)
///
/// // Collect statistics for a type
/// try await manager.collectStatistics(
///     for: User.self,
///     using: dataStore,
///     sampleRate: 0.1  // 10% sample
/// )
///
/// // Use as StatisticsProvider for query planning
/// let planner = QueryPlanner(statistics: manager)
/// ```
///
/// **Thread Safety**:
/// Uses `final class` + `Mutex` pattern per CLAUDE.md guidelines.
public final class StatisticsManager: StatisticsProvider, Sendable {

    // MARK: - Properties

    /// FDB Container for database access
    private let container: FDBContainer

    /// Persistent storage
    private let storage: StatisticsStorage

    /// In-memory cache
    private struct Cache: Sendable {
        var tableStats: [String: TableStatisticsData] = [:]
        var fieldStats: [String: FieldStatisticsData] = [:]  // Key: "TypeName.fieldName"
        var indexStats: [String: IndexStatisticsData] = [:]
        var vectorStats: [String: VectorStatisticsData] = [:]
        var fullTextStats: [String: FullTextStatisticsData] = [:]
        var spatialStats: [String: SpatialStatisticsData] = [:]
        var lastLoaded: Date?
    }

    private let cache: Mutex<Cache>

    /// Default statistics for fallback
    private let defaults: DefaultStatisticsProvider

    /// Configuration
    public let configuration: Configuration

    // MARK: - Configuration

    /// Statistics manager configuration
    ///
    /// Configuration follows PostgreSQL ANALYZE patterns:
    /// - statistics_target controls both MCV list size and histogram buckets
    /// - Sample rate determines accuracy vs performance tradeoff
    ///
    /// **Reference**: PostgreSQL default_statistics_target (default: 100)
    public struct Configuration: Sendable {
        /// Default sample rate for statistics collection (0.0 - 1.0)
        public let defaultSampleRate: Double

        /// Reservoir size for histogram building
        /// Reference: PostgreSQL uses 300 * statistics_target rows
        public let reservoirSize: Int

        /// Number of histogram buckets
        /// Reference: PostgreSQL default_statistics_target (default: 100)
        public let histogramBucketCount: Int

        /// Maximum MCV (Most Common Values) list size
        /// Reference: PostgreSQL default_statistics_target (default: 100)
        public let mcvMaxSize: Int

        /// Minimum frequency for MCV inclusion (fraction of total)
        /// Values below this threshold are not considered "common"
        /// Reference: PostgreSQL uses ~1/statistics_target as threshold
        public let mcvMinFrequency: Double

        /// Cache TTL in seconds (0 = no expiry)
        public let cacheTTL: TimeInterval

        /// Staleness threshold in seconds (when to recommend refresh)
        public let stalenessThreshold: TimeInterval

        /// Maximum value size to include in statistics (bytes)
        /// Reference: PostgreSQL excludes values > 1KB
        public let maxValueSize: Int

        public init(
            defaultSampleRate: Double = 0.1,
            reservoirSize: Int = 30_000,  // 300 * 100 (PostgreSQL pattern)
            histogramBucketCount: Int = 100,
            mcvMaxSize: Int = 100,
            mcvMinFrequency: Double = 0.01,  // 1%
            cacheTTL: TimeInterval = 3600,
            stalenessThreshold: TimeInterval = 86400,
            maxValueSize: Int = 1024  // 1KB (PostgreSQL limit)
        ) {
            self.defaultSampleRate = defaultSampleRate
            self.reservoirSize = reservoirSize
            self.histogramBucketCount = histogramBucketCount
            self.mcvMaxSize = mcvMaxSize
            self.mcvMinFrequency = mcvMinFrequency
            self.cacheTTL = cacheTTL
            self.stalenessThreshold = stalenessThreshold
            self.maxValueSize = maxValueSize
        }

        public static let `default` = Configuration()
    }

    // MARK: - Initialization

    /// Create a statistics manager
    ///
    /// - Parameters:
    ///   - container: FDBContainer for database access
    ///   - subspace: Root subspace for storage
    ///   - configuration: Optional configuration
    public init(
        container: FDBContainer,
        subspace: Subspace,
        configuration: Configuration = .default
    ) {
        self.container = container
        self.storage = StatisticsStorage(container: container, subspace: subspace)
        self.cache = Mutex(Cache())
        self.defaults = DefaultStatisticsProvider()
        self.configuration = configuration
    }

    // MARK: - StatisticsProvider Protocol

    public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        let typeName = T.persistableType
        let stats = cache.withLock { $0.tableStats[typeName] }
        return stats.map { Int($0.rowCount) } ?? defaults.estimatedRowCount(for: type)
    }

    public func estimatedDistinctValues<T: Persistable>(field: String, type: T.Type) -> Int? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }
        return stats.map { Int($0.distinctCount) } ?? defaults.estimatedDistinctValues(field: field, type: type)
    }

    public func equalitySelectivity<T: Persistable>(field: String, type: T.Type) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        if let stats = stats {
            // Use combined MCV + Histogram estimation (PostgreSQL pattern)
            // For general equality selectivity (unknown value), use 1/distinctCount
            // This is for query planning when the actual value is not known
            if stats.combinedEstimator != nil || stats.histogram != nil {
                return 1.0 / Double(max(1, stats.distinctCount))
            }

            return stats.equalitySelectivity
        }

        return defaults.equalitySelectivity(field: field, type: type)
    }

    /// Estimate equality selectivity for a specific value
    ///
    /// Uses combined MCV + Histogram estimation:
    /// - If value is in MCV: return MCV frequency
    /// - Otherwise: use histogram estimate scaled by histogram fraction
    ///
    /// **PostgreSQL Reference**: src/backend/utils/adt/selfuncs.c, var_eq_const()
    public func equalitySelectivity<T: Persistable>(
        field: String,
        value: Any,
        type: T.Type
    ) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            return defaults.equalitySelectivity(field: field, type: type)
        }

        let comparableValue = FieldValue(value) ?? .null

        // Use combined estimator for accurate selectivity
        if let estimator = stats.combinedEstimator {
            return estimator.equalitySelectivity(value: comparableValue)
        }

        // Fallback to histogram-only
        if let histogram = stats.histogram {
            return histogram.estimateEqualsSelectivity(value: comparableValue)
        }

        return stats.equalitySelectivity
    }

    public func rangeSelectivity<T: Persistable>(field: String, range: RangeBound, type: T.Type) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            return defaults.rangeSelectivity(field: field, range: range, type: type)
        }

        // Convert RangeBound to FieldValue bounds
        let (minValue, maxValue, minInclusive, maxInclusive) = convertRangeBound(range)

        // Use combined MCV + Histogram estimation (PostgreSQL pattern)
        // Combined selectivity = mcv_selectivity + histogram_selectivity × histogram_fraction
        if let estimator = stats.combinedEstimator {
            return estimator.rangeSelectivity(
                min: minValue,
                max: maxValue,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive
            )
        }

        // Fallback to histogram-only
        if let histogram = stats.histogram {
            return histogram.estimateRangeSelectivity(
                min: minValue,
                max: maxValue,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive
            )
        }

        return defaults.rangeSelectivity(field: field, range: range, type: type)
    }

    /// Estimate selectivity for IN clause
    ///
    /// Uses combined MCV + Histogram estimation for each value.
    ///
    /// **PostgreSQL Reference**: src/backend/utils/adt/selfuncs.c, scalararraysel()
    public func inSelectivity<T: Persistable>(
        field: String,
        values: [Any],
        type: T.Type
    ) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            // Fallback: assume uniform distribution
            let distinctCount = defaults.estimatedDistinctValues(field: field, type: type) ?? 100
            return min(1.0, Double(values.count) / Double(distinctCount))
        }

        let comparableValues = values.map { FieldValue($0) ?? .null }

        // Use combined estimator for accurate selectivity
        if let estimator = stats.combinedEstimator {
            return estimator.inSelectivity(values: comparableValues)
        }

        // Fallback: sum individual equality selectivities
        var total = 0.0
        if let histogram = stats.histogram {
            for value in comparableValues {
                total += histogram.estimateEqualsSelectivity(value: value)
            }
        } else {
            total = Double(values.count) / Double(max(1, stats.distinctCount))
        }

        return min(1.0, total)
    }

    public func nullSelectivity<T: Persistable>(field: String, type: T.Type) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }
        return stats?.nullSelectivity ?? defaults.nullSelectivity(field: field, type: type)
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        let stats = cache.withLock { $0.indexStats[index.name] }
        return stats.map { Int($0.entryCount) } ?? defaults.estimatedIndexEntries(index: index)
    }

    // MARK: - Statistics Collection

    /// Collect statistics for a Persistable type
    ///
    /// Implements PostgreSQL ANALYZE-style statistics collection:
    /// 1. Scan records and collect samples (reservoir sampling)
    /// 2. Build MCV (Most Common Values) list
    /// 3. Build histogram excluding MCV values (prevents double-counting)
    /// 4. Estimate cardinality using HyperLogLog++
    ///
    /// **PostgreSQL Reference**:
    /// - src/backend/commands/analyze.c, compute_scalar_stats()
    /// - MCV list contains values with frequency >= minFrequency
    /// - Histogram excludes MCV values for accurate combined selectivity
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - store: DataStore for accessing records
    ///   - sampleRate: Sample rate (0.0-1.0), nil uses default
    ///   - fields: Specific fields to collect (nil for all)
    public func collectStatistics<T: Persistable>(
        for type: T.Type,
        using store: any DataStore,
        sampleRate: Double? = nil,
        fields: [String]? = nil
    ) async throws {
        let typeName = T.persistableType
        let effectiveSampleRate = sampleRate ?? configuration.defaultSampleRate
        let fieldsToCollect = fields ?? T.allFields

        // Initialize collectors for each field
        // - ReservoirSampling: For histogram building
        // - MCVBuilder: For most common values
        // - HyperLogLog: For cardinality estimation
        var fieldSamplers: [String: ReservoirSampling<FieldValue>] = [:]
        var fieldMCVBuilders: [String: MCVBuilder] = [:]
        var fieldHLLs: [String: HyperLogLog] = [:]
        var fieldNullCounts: [String: Int64] = [:]

        for field in fieldsToCollect {
            fieldSamplers[field] = ReservoirSampling(reservoirSize: configuration.reservoirSize)
            fieldMCVBuilders[field] = MCVBuilder(
                maxSize: configuration.mcvMaxSize,
                minFrequency: configuration.mcvMinFrequency
            )
            fieldHLLs[field] = HyperLogLog()
            fieldNullCounts[field] = 0
        }

        // Scan records and collect statistics
        var totalCount: Int64 = 0
        var totalSize: Int64 = 0
        let encoder = ProtobufEncoder()

        let items = try await store.fetchAll(type)
        for item in items {
            totalCount += 1

            // Calculate encoded size for average row size
            if let encodedData = try? encoder.encode(item) {
                totalSize += Int64(encodedData.count)
            }

            // Sample this record based on sample rate
            let shouldSample = Double.random(in: 0..<1) < effectiveSampleRate

            // Collect field-level statistics
            for field in fieldsToCollect {
                if let value = item[dynamicMember: field] {
                    let fieldValue = FieldValue(value) ?? .null

                    // Always update HyperLogLog for cardinality estimation
                    fieldHLLs[field]?.add(fieldValue)

                    // Track null values
                    if case .null = fieldValue {
                        fieldNullCounts[field, default: 0] += 1
                    } else {
                        // Update MCV builder for all non-null values
                        // (MCV needs complete frequency information)
                        fieldMCVBuilders[field]?.add(fieldValue)
                    }

                    // Sample for histogram building (using reservoir sampling)
                    if shouldSample {
                        fieldSamplers[field]?.add(fieldValue)
                    }
                } else {
                    // Field not present = null
                    fieldNullCounts[field, default: 0] += 1
                }
            }
        }

        // Build and save table statistics
        let tableStats = TableStatisticsData(
            rowCount: totalCount,
            avgRowSize: totalCount > 0 ? Int(totalSize / totalCount) : 0,
            sampleSize: Int(Double(totalCount) * effectiveSampleRate),
            sampleRate: effectiveSampleRate
        )

        try await storage.saveTableStatistics(typeName: typeName, stats: tableStats)
        cache.withLock { $0.tableStats[typeName] = tableStats }

        // Build and save field statistics
        for field in fieldsToCollect {
            guard let sampler = fieldSamplers[field],
                  let mcvBuilder = fieldMCVBuilders[field],
                  let hll = fieldHLLs[field] else { continue }

            let nullCount = fieldNullCounts[field] ?? 0
            let nonNullCount = totalCount - nullCount

            // Step 1: Build MCV from complete frequency data
            let mcv = mcvBuilder.build(
                totalCount: nonNullCount,
                sampleCount: mcvBuilder.totalSamples
            )

            // Step 2: Get MCV values to exclude from histogram
            // This prevents double-counting in selectivity estimation
            let mcvValues = Set(mcv.entries.map { $0.value })

            // Step 3: Build histogram from samples, excluding MCV values
            let histogram = HistogramBuilder.build(
                samples: sampler.sample,
                totalCount: nonNullCount,
                nullCount: nullCount,
                bucketCount: configuration.histogramBucketCount,
                hll: hll,
                excludeValues: mcvValues
            )

            // Step 4: Create field statistics with both MCV and histogram
            let fieldStats = FieldStatisticsData(
                fieldName: field,
                distinctCount: hll.cardinality(),
                nullCount: nullCount,
                totalCount: totalCount,
                minValue: computeMinValue(histogram: histogram, mcv: mcv),
                maxValue: computeMaxValue(histogram: histogram, mcv: mcv),
                mcv: mcv,
                histogram: histogram
            )

            try await storage.saveFieldStatistics(typeName: typeName, fieldName: field, stats: fieldStats)
            let key = "\(typeName).\(field)"
            cache.withLock { $0.fieldStats[key] = fieldStats }
        }
    }

    /// Compute minimum value from histogram and MCV
    private func computeMinValue(histogram: Histogram, mcv: MostCommonValues) -> FieldValue? {
        let histMin = histogram.buckets.first?.lowerBound
        let mcvMin = mcv.entries.min(by: { $0.value < $1.value })?.value

        switch (histMin, mcvMin) {
        case (.some(let h), .some(let m)):
            return h < m ? h : m
        case (.some(let h), .none):
            return h
        case (.none, .some(let m)):
            return m
        case (.none, .none):
            return nil
        }
    }

    /// Compute maximum value from histogram and MCV
    private func computeMaxValue(histogram: Histogram, mcv: MostCommonValues) -> FieldValue? {
        let histMax = histogram.buckets.last?.upperBound
        let mcvMax = mcv.entries.max(by: { $0.value < $1.value })?.value

        switch (histMax, mcvMax) {
        case (.some(let h), .some(let m)):
            return h > m ? h : m
        case (.some(let h), .none):
            return h
        case (.none, .some(let m)):
            return m
        case (.none, .none):
            return nil
        }
    }

    /// Collect index statistics by scanning index entries
    ///
    /// - Parameters:
    ///   - index: Index descriptor
    ///   - indexSubspace: Subspace containing index entries
    public func collectIndexStatistics(
        index: IndexDescriptor,
        indexSubspace: Subspace
    ) async throws {

        let (entryCount, distinctKeyCount) = try await container.database.withTransaction(configuration: .batch) { transaction in
            var entryCount: Int64 = 0
            var hll = HyperLogLog()

            let (beginKey, endKey) = indexSubspace.range()

            for try await (key, _) in transaction.getRange(begin: beginKey, end: endKey, snapshot: true) {
                entryCount += 1

                // Extract key values for distinct count estimation
                if let keyTuple = try? indexSubspace.unpack(key) {
                    for i in 0..<keyTuple.count {
                        if let element = keyTuple[i] {
                            hll.add(FieldValue(element) ?? .null)
                        }
                    }
                }
            }

            return (entryCount, hll.cardinality())
        }

        let avgEntriesPerKey = distinctKeyCount > 0 ? Double(entryCount) / Double(distinctKeyCount) : 1.0

        let stats = IndexStatisticsData(
            indexName: index.name,
            entryCount: entryCount,
            distinctKeyCount: distinctKeyCount,
            avgEntriesPerKey: avgEntriesPerKey
        )

        try await storage.saveIndexStatistics(indexName: index.name, stats: stats)
        cache.withLock { $0.indexStats[index.name] = stats }
    }

    // MARK: - Cache Management

    /// Load all statistics from storage into cache
    public func loadStatistics() async throws {
        let tableStats = try await storage.loadAllTableStatistics()

        var fieldStats: [String: FieldStatisticsData] = [:]
        for typeName in tableStats.keys {
            let fields = try await storage.loadAllFieldStatistics(typeName: typeName)
            for (fieldName, stats) in fields {
                fieldStats["\(typeName).\(fieldName)"] = stats
            }
        }

        cache.withLock { cache in
            cache.tableStats = tableStats
            cache.fieldStats = fieldStats
            cache.lastLoaded = Date()
        }
    }

    /// Clear the in-memory cache
    public func clearCache() {
        cache.withLock { cache in
            cache.tableStats.removeAll()
            cache.fieldStats.removeAll()
            cache.indexStats.removeAll()
            cache.vectorStats.removeAll()
            cache.fullTextStats.removeAll()
            cache.spatialStats.removeAll()
            cache.lastLoaded = nil
        }
    }

    /// Check if statistics are stale
    public func isStale(typeName: String) -> Bool {
        guard let stats = cache.withLock({ $0.tableStats[typeName] }) else {
            return true
        }

        let age = Date().timeIntervalSince(stats.timestamp)
        return age > configuration.stalenessThreshold
    }

    // MARK: - Helper Methods

    /// Convert RangeBound to FieldValue bounds
    private func convertRangeBound(_ range: RangeBound) -> (
        min: FieldValue?,
        max: FieldValue?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) {
        var minValue: FieldValue?
        var maxValue: FieldValue?
        var minInclusive = true
        var maxInclusive = true

        if let lower = range.lower {
            minValue = FieldValue(lower.value) ?? .null
            minInclusive = lower.inclusive
        }

        if let upper = range.upper {
            maxValue = FieldValue(upper.value) ?? .null
            maxInclusive = upper.inclusive
        }

        return (minValue, maxValue, minInclusive, maxInclusive)
    }
}

// MARK: - Convenience Extensions

extension StatisticsManager {

    /// Get a summary of statistics status
    public func getStatisticsSummary() -> StatisticsSummary {
        cache.withLock { cache in
            StatisticsSummary(
                tableCount: cache.tableStats.count,
                fieldCount: cache.fieldStats.count,
                indexCount: cache.indexStats.count,
                lastLoaded: cache.lastLoaded,
                staleTypes: cache.tableStats.compactMap { (typeName, stats) in
                    let age = Date().timeIntervalSince(stats.timestamp)
                    return age > configuration.stalenessThreshold ? typeName : nil
                }
            )
        }
    }

    /// Statistics summary
    public struct StatisticsSummary: Sendable {
        public let tableCount: Int
        public let fieldCount: Int
        public let indexCount: Int
        public let lastLoaded: Date?
        public let staleTypes: [String]
    }
}

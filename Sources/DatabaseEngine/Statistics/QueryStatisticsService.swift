// QueryStatisticsService.swift
// Unified database statistics management

import StorageKit
import DatabaseTypes
import DatabaseKit
import Synchronization

/// Collects, stores, and serves database statistics.
///
/// Combines persistence (StatisticsStorage), collection (sampling + HyperLogLog),
/// and statistics access (`StatisticsProvider`) into a single interface.
///
/// **Usage**:
/// ```swift
/// let statistics = QueryStatisticsService(container: container, subspace: subspace)
///
/// // Collect statistics for a type
/// try await statistics.collectStatistics(
///     for: User.self,
///     using: dataStore,
///     sampleRate: 0.1  // 10% sample
/// )
///
/// let estimatedRows = statistics.estimatedRowCount(for: User.self)
/// ```
///
/// **Thread Safety**:
/// Uses `final class` + `Mutex` pattern per CLAUDE.md guidelines.
public final class QueryStatisticsService: StatisticsProvider, Sendable {

    private struct IndexCardinality: Sendable {
        let entryCount: Int64
        let distinctKeyCount: Int64
    }

    // MARK: - Properties

    /// FDB Container for database access
    private let container: DBContainer

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
        var lastLoaded: StorageInstant?
    }

    private let cache: Mutex<Cache>

    /// Default statistics for fallback
    private let heuristics: HeuristicStatisticsProvider

    /// Configuration
    public let configuration: Configuration

    // MARK: - Configuration

    /// Query statistics configuration.
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
        public let cacheTTL: Duration

        /// Staleness threshold in seconds (when to recommend refresh)
        public let stalenessThreshold: Duration

        /// Maximum value size to include in statistics (bytes)
        /// Reference: PostgreSQL excludes values > 1KB
        public let maxValueSize: Int

        public init(
            defaultSampleRate: Double = 0.1,
            reservoirSize: Int = 30_000,  // 300 * 100 (PostgreSQL pattern)
            histogramBucketCount: Int = 100,
            mcvMaxSize: Int = 100,
            mcvMinFrequency: Double = 0.01,  // 1%
            cacheTTL: Duration = .seconds(3_600),
            stalenessThreshold: Duration = .seconds(86_400),
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

    /// Creates a query statistics service.
    ///
    /// - Parameters:
    ///   - container: DBContainer for database access
    ///   - subspace: Root subspace for storage
    ///   - configuration: Optional configuration
    public init(
        container: DBContainer,
        subspace: Subspace,
        configuration: Configuration = .default
    ) {
        self.container = container
        self.storage = StatisticsStorage(container: container, subspace: subspace)
        self.cache = Mutex(Cache())
        self.heuristics = HeuristicStatisticsProvider()
        self.configuration = configuration
    }

    // MARK: - StatisticsProvider Protocol

    public func estimatedRowCount(entity: String) -> Int {
        let stats = cache.withLock { $0.tableStats[entity] }
        return stats.map { Int($0.rowCount) }
            ?? heuristics.estimatedRowCount(entity: entity)
    }

    public func estimatedDistinctValues(field: String, entity: String) -> Int? {
        let key = "\(entity).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }
        return stats.map { Int($0.distinctCount) }
            ?? heuristics.estimatedDistinctValues(field: field, entity: entity)
    }

    public func equalitySelectivity(field: String, entity: String) -> Double? {
        let key = "\(entity).\(field)"
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

        return heuristics.equalitySelectivity(field: field, entity: entity)
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
        value: FieldValue,
        type: T.Type
    ) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            return heuristics.equalitySelectivity(
                field: field,
                entity: T.persistableType
            )
        }

        // Use combined estimator for accurate selectivity
        if let estimator = stats.combinedEstimator {
            return estimator.equalitySelectivity(value: value)
        }

        // Fallback to histogram-only
        if let histogram = stats.histogram {
            return histogram.estimateEqualsSelectivity(value: value)
        }

        return stats.equalitySelectivity
    }

    public func rangeSelectivity(
        field: String,
        range: RangeBound,
        entity: String
    ) -> Double? {
        let key = "\(entity).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            return heuristics.rangeSelectivity(
                field: field,
                range: range,
                entity: entity
            )
        }

        let minValue = range.lower?.value
        let maxValue = range.upper?.value
        let minInclusive = range.lower?.inclusive ?? true
        let maxInclusive = range.upper?.inclusive ?? true

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

        return heuristics.rangeSelectivity(
            field: field,
            range: range,
            entity: entity
        )
    }

    /// Estimate selectivity for IN clause
    ///
    /// Uses combined MCV + Histogram estimation for each value.
    ///
    /// **PostgreSQL Reference**: src/backend/utils/adt/selfuncs.c, scalararraysel()
    public func inSelectivity<T: Persistable>(
        field: String,
        values: [FieldValue],
        type: T.Type
    ) -> Double? {
        let key = "\(T.persistableType).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }

        guard let stats = stats else {
            // Fallback: assume uniform distribution
            let distinctCount = heuristics.estimatedDistinctValues(
                field: field,
                entity: T.persistableType
            ) ?? 100
            return min(1.0, Double(values.count) / Double(distinctCount))
        }

        // Use combined estimator for accurate selectivity
        if let estimator = stats.combinedEstimator {
            return estimator.inSelectivity(values: values)
        }

        // Fallback: sum individual equality selectivities
        var total = 0.0
        if let histogram = stats.histogram {
            for value in values {
                total += histogram.estimateEqualsSelectivity(value: value)
            }
        } else {
            total = Double(values.count) / Double(max(1, stats.distinctCount))
        }

        return min(1.0, total)
    }

    public func nullSelectivity(field: String, entity: String) -> Double? {
        let key = "\(entity).\(field)"
        let stats = cache.withLock { $0.fieldStats[key] }
        return stats?.nullSelectivity
            ?? heuristics.nullSelectivity(field: field, entity: entity)
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        let stats = cache.withLock { $0.indexStats[index.name] }
        return stats.map { Int($0.entryCount) } ?? heuristics.estimatedIndexEntries(index: index)
    }

    // MARK: - Statistics Collection

    /// Collect statistics for a Persistable type
    ///
    /// Implements PostgreSQL ANALYZE-style statistics collection:
    /// 1. Scan entities and collect samples (reservoir sampling)
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
    ///   - store: DataStore for accessing entities
    ///   - sampleRate: Sample rate (0.0-1.0), nil uses default
    ///   - fields: Specific fields to collect (nil for all)
    public func collectStatistics<T: Persistable, Store: DataStore>(
        for type: T.Type,
        using store: Store,
        sampleRate: Double? = nil,
        fields: [String]? = nil
    ) async throws {
        let typeName = T.persistableType
        let effectiveSampleRate = sampleRate ?? configuration.defaultSampleRate
        let fieldsToCollect = fields ?? T.allFields
        var fieldIdentities: [String: FieldIdentity] = [:]
        fieldIdentities.reserveCapacity(fieldsToCollect.count)
        for field in fieldsToCollect {
            guard let number = T.fieldNumber(for: field) else {
                throw StatisticsCollectionError.unknownField(field)
            }
            fieldIdentities[field] = FieldIdentity(
                name: field,
                number: number
            )
        }

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

        // Scan entities and collect statistics
        var totalCount: Int64 = 0
        var totalSize: Int64 = 0

        let items = try await store.fetchAll(type)
        for item in items {
            totalCount += 1

            // Measure the exact canonical persisted representation.
            let encodedData = try DataAccess.serialize(item)
            totalSize += Int64(encodedData.count)

            // Sample this entity based on sample rate
            let shouldSample = Double.random(in: 0..<1) < effectiveSampleRate

            // Collect field-level statistics
            for field in fieldsToCollect {
                guard let identity = fieldIdentities[field] else {
                    throw StatisticsCollectionError.unknownField(field)
                }
                let fieldValue: FieldValue?
                do {
                    fieldValue = try item.persistedFieldValue(for: identity)
                } catch let error {
                    throw StatisticsCollectionError.fieldEncodingFailed(
                            field: field,
                            reason: error
                    )
                }
                if let fieldValue {
                    // Always update HyperLogLog for cardinality estimation
                    try fieldHLLs[field]?.add(fieldValue)

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

        let collectionTimestamp = container.wallClock.now

        // Build and save table statistics
        let tableStats = TableStatisticsData(
            rowCount: totalCount,
            avgRowSize: totalCount > 0 ? Int(totalSize / totalCount) : 0,
            sampleSize: Int(Double(totalCount) * effectiveSampleRate),
            sampleRate: effectiveSampleRate,
            timestamp: collectionTimestamp
        )

        try await storage.saveTableStatistics(typeName: typeName, stats: tableStats)
        cache.withLock { $0.tableStats[typeName] = tableStats }

        // Build and save field statistics
        for field in fieldsToCollect {
            guard let sampler = fieldSamplers[field],
                  let mcvBuilder = fieldMCVBuilders[field],
                  let hll = fieldHLLs[field] else {
                throw StatisticsCollectionError.missingFieldCollector(field)
            }

            let nullCount = fieldNullCounts[field] ?? 0
            let nonNullCount = totalCount - nullCount

            // Step 1: Build MCV from complete frequency data
            let mcv = mcvBuilder.build(
                totalCount: nonNullCount,
                sampleCount: mcvBuilder.totalSamples,
                timestamp: collectionTimestamp
            )

            // Step 2: Get MCV values to exclude from histogram
            // This prevents double-counting in selectivity estimation
            let mcvValues = Set(mcv.entries.map { $0.value })

            // Step 3: Build histogram from samples, excluding MCV values
            let histogram = try HistogramBuilder.build(
                samples: sampler.sample,
                totalCount: nonNullCount,
                nullCount: nullCount,
                bucketCount: configuration.histogramBucketCount,
                hll: hll,
                excludeValues: mcvValues,
                timestamp: collectionTimestamp
            )

            // Step 4: Create field statistics with both MCV and histogram
            let fieldStats = FieldStatisticsData(
                fieldName: field,
                distinctCount: try hll.cardinality(),
                nullCount: nullCount,
                totalCount: totalCount,
                minValue: computeMinValue(histogram: histogram, mcv: mcv),
                maxValue: computeMaxValue(histogram: histogram, mcv: mcv),
                mcv: mcv,
                histogram: histogram,
                timestamp: collectionTimestamp
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
        guard let capabilities = container.runtimeConfiguration
            .indexMaintainerProviders
            .physicalEntryCapabilities(for: index.kindIdentifier) else {
            throw StatisticsCollectionError.unsupportedPhysicalLayout(
                indexName: index.name,
                kindIdentifier: index.kindIdentifier
            )
        }
        let runtimeIndex = Index(
            name: index.name,
            kind: index.kind,
            rootExpression: KeyExpressionFactory.from(
                keyPaths: index.fieldNames
            ),
            subspaceKey: index.name,
            isUnique: index.isUnique,
            storedFieldNames: index.storedFieldNames
        )

        let cardinality = try await container.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: container.monotonicClock
        ) { transaction in
            var entryCount: Int64 = 0
            var hll = HyperLogLog()

            let (beginKey, endKey) = indexSubspace.range()

            for (key, _) in try await TransactionRangeCollection.collect(using: transaction, from: .firstGreaterOrEqual(beginKey), to: .firstGreaterOrEqual(endKey), limit: 0, reverse: false, snapshot: true, streamingMode: .wantAll) {
                let (nextCount, overflow) = entryCount.addingReportingOverflow(1)
                guard !overflow else {
                    throw StatisticsCollectionError.entryCountOverflow(
                        indexName: index.name
                    )
                }
                entryCount = nextCount

                let entry: IndexPhysicalEntry
                do {
                    entry = try capabilities.decoder.decode(
                        key: key,
                        in: indexSubspace,
                        index: runtimeIndex
                    )
                } catch {
                    throw StatisticsCollectionError.invalidPhysicalEntry(
                        indexName: index.name,
                        reason: "physical index entry decoding failed"
                    )
                }
                try hll.add(.array(entry.indexedValues))
            }

            return IndexCardinality(
                entryCount: entryCount,
                distinctKeyCount: try hll.cardinality()
            )
        }

        let entryCount = cardinality.entryCount
        let distinctKeyCount = cardinality.distinctKeyCount

        let avgEntriesPerKey = distinctKeyCount > 0
            ? Double(entryCount) / Double(distinctKeyCount)
            : 0.0

        let stats = IndexStatisticsData(
            indexName: index.name,
            entryCount: entryCount,
            distinctKeyCount: distinctKeyCount,
            avgEntriesPerKey: avgEntriesPerKey,
            timestamp: container.wallClock.now
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
            cache.lastLoaded = container.monotonicClock.now
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
    public func isStale(typeName: String) throws -> Bool {
        guard let stats = cache.withLock({ $0.tableStats[typeName] }) else {
            return true
        }

        let age = try DatabaseTimestampMeasurement.elapsed(
            from: stats.timestamp,
            to: container.wallClock.now
        )
        return Duration(age) > configuration.stalenessThreshold
    }

}

// MARK: - Convenience Extensions

extension QueryStatisticsService {

    /// Resolves the compiled model name before entering the runtime statistics
    /// provider boundary.
    public func estimatedRowCount<Model: Persistable>(
        for type: Model.Type
    ) -> Int {
        estimatedRowCount(entity: Model.persistableType)
    }

    public func estimatedDistinctValues<Model: Persistable>(
        field: String,
        type: Model.Type
    ) -> Int? {
        estimatedDistinctValues(
            field: field,
            entity: Model.persistableType
        )
    }

    public func equalitySelectivity<Model: Persistable>(
        field: String,
        type: Model.Type
    ) -> Double? {
        equalitySelectivity(field: field, entity: Model.persistableType)
    }

    public func rangeSelectivity<Model: Persistable>(
        field: String,
        range: RangeBound,
        type: Model.Type
    ) -> Double? {
        rangeSelectivity(
            field: field,
            range: range,
            entity: Model.persistableType
        )
    }

    public func nullSelectivity<Model: Persistable>(
        field: String,
        type: Model.Type
    ) -> Double? {
        nullSelectivity(field: field, entity: Model.persistableType)
    }

    /// Get a summary of statistics status
    public func getStatisticsSummary() throws -> StatisticsSummary {
        let currentTimestamp = container.wallClock.now
        return try cache.withLock { cache in
            StatisticsSummary(
                tableCount: cache.tableStats.count,
                fieldCount: cache.fieldStats.count,
                indexCount: cache.indexStats.count,
                lastLoaded: cache.lastLoaded,
                staleTypes: try cache.tableStats.compactMap { (typeName, stats) in
                    let age = try DatabaseTimestampMeasurement.elapsed(
                        from: stats.timestamp,
                        to: currentTimestamp
                    )
                    return Duration(age) > configuration.stalenessThreshold
                        ? typeName
                        : nil
                }
            )
        }
    }

    /// Statistics summary
    public struct StatisticsSummary: Sendable {
        public let tableCount: Int
        public let fieldCount: Int
        public let indexCount: Int
        public let lastLoaded: StorageInstant?
        public let staleTypes: [String]
    }
}

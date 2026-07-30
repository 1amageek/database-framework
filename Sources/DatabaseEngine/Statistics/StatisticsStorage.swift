// StatisticsStorage.swift
// Storage-backed persistence for database statistics

import StorageKit
import DatabaseTypes
import DatabaseKit
import Synchronization

/// Persistent storage for statistics in FoundationDB
///
/// **Storage Layout**:
/// ```
/// [subspace]/_statistics/table/[typeName]               → TableStatisticsData (STAT v1)
/// [subspace]/_statistics/field/[typeName]/[fieldName]   → FieldStatisticsData (STAT v1)
/// [subspace]/_statistics/index/[indexName]              → IndexStatisticsData (STAT v1)
/// [subspace]/_statistics/search/vector/[indexName]      → VectorStatisticsData (STAT v1)
/// [subspace]/_statistics/search/fulltext/[indexName]    → FullTextStatisticsData (STAT v1)
/// [subspace]/_statistics/search/spatial/[indexName]     → SpatialStatisticsData (STAT v1)
/// ```
///
/// **Usage**:
/// ```swift
/// let storage = StatisticsStorage(database: database, subspace: subspace)
///
/// // Save table statistics
/// try await storage.saveTableStatistics(typeName: "User", stats: tableStats)
///
/// // Load table statistics
/// let stats = try await storage.loadTableStatistics(typeName: "User")
/// ```
public final class StatisticsStorage: Sendable {

    /// FDB Container for transaction execution
    private let container: DBContainer

    /// Root subspace for statistics storage
    private let subspace: Subspace

    /// Statistics subspace prefix
    private var statsSubspace: Subspace {
        subspace.subspace("_statistics")
    }

    /// Create a statistics storage
    ///
    /// - Parameters:
    ///   - container: DBContainer for transaction execution
    ///   - subspace: Root subspace (typically container's subspace)
    public init(container: DBContainer, subspace: Subspace) {
        self.container = container
        self.subspace = subspace
    }

    // MARK: - Table Statistics

    /// Save table statistics
    public func saveTableStatistics(typeName: String, stats: TableStatisticsData) async throws {
        let key = statsSubspace.subspace("table").pack(Tuple([typeName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load table statistics
    public func loadTableStatistics(typeName: String) async throws -> TableStatisticsData? {
        let key = statsSubspace.subspace("table").pack(Tuple([typeName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeTable(data)
        }
    }

    /// Load all table statistics
    public func loadAllTableStatistics() async throws -> [String: TableStatisticsData] {
        let tableSubspace = statsSubspace.subspace("table")

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            var results: [String: TableStatisticsData] = [:]

            let (begin, end) = tableSubspace.range()
            for (key, value) in try await TransactionRangeCollection.collect(using: transaction, from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), limit: 0, reverse: false, snapshot: true, streamingMode: .wantAll) {
                let keyTuple = try tableSubspace.unpack(key)
                guard keyTuple.count == 1 else {
                    throw StatisticsStorageError.malformedKey(
                        expectedElementCount: 1,
                        actual: keyTuple.count
                    )
                }
                guard case .string(let typeName) = try keyTuple.value(at: 0) else {
                    throw StatisticsStorageError.malformedKeyElement
                }
                results[typeName] = try StatisticsEntryCodec.decodeTable(value)
            }

            return results
        }
    }

    // MARK: - Field Statistics

    /// Save field statistics
    public func saveFieldStatistics(typeName: String, fieldName: String, stats: FieldStatisticsData) async throws {
        let key = statsSubspace.subspace("field").subspace(typeName).pack(Tuple([fieldName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load field statistics
    public func loadFieldStatistics(typeName: String, fieldName: String) async throws -> FieldStatisticsData? {
        let key = statsSubspace.subspace("field").subspace(typeName).pack(Tuple([fieldName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeField(data)
        }
    }

    /// Load all field statistics for a type
    public func loadAllFieldStatistics(typeName: String) async throws -> [String: FieldStatisticsData] {
        let fieldSubspace = statsSubspace.subspace("field").subspace(typeName)

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            var results: [String: FieldStatisticsData] = [:]

            let (begin, end) = fieldSubspace.range()
            for (key, value) in try await TransactionRangeCollection.collect(using: transaction, from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), limit: 0, reverse: false, snapshot: true, streamingMode: .wantAll) {
                let keyTuple = try fieldSubspace.unpack(key)
                guard keyTuple.count == 1 else {
                    throw StatisticsStorageError.malformedKey(
                        expectedElementCount: 1,
                        actual: keyTuple.count
                    )
                }
                guard case .string(let fieldName) = try keyTuple.value(at: 0) else {
                    throw StatisticsStorageError.malformedKeyElement
                }
                results[fieldName] = try StatisticsEntryCodec.decodeField(value)
            }

            return results
        }
    }

    // MARK: - Index Statistics

    /// Save index statistics
    public func saveIndexStatistics(indexName: String, stats: IndexStatisticsData) async throws {
        let key = statsSubspace.subspace("index").pack(Tuple([indexName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load index statistics
    public func loadIndexStatistics(indexName: String) async throws -> IndexStatisticsData? {
        let key = statsSubspace.subspace("index").pack(Tuple([indexName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeIndex(data)
        }
    }

    // MARK: - Search Statistics

    /// Save vector index statistics
    public func saveVectorStatistics(indexName: String, stats: VectorStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("vector").pack(Tuple([indexName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load vector index statistics
    public func loadVectorStatistics(indexName: String) async throws -> VectorStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("vector").pack(Tuple([indexName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeVector(data)
        }
    }

    /// Save full-text index statistics
    public func saveFullTextStatistics(indexName: String, stats: FullTextStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("fulltext").pack(Tuple([indexName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load full-text index statistics
    public func loadFullTextStatistics(indexName: String) async throws -> FullTextStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("fulltext").pack(Tuple([indexName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeFullText(data)
        }
    }

    /// Save spatial index statistics
    public func saveSpatialStatistics(indexName: String, stats: SpatialStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("spatial").pack(Tuple([indexName]))
        let data = try StatisticsEntryCodec.encode(stats)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(data, for: key)
        }
    }

    /// Load spatial index statistics
    public func loadSpatialStatistics(indexName: String) async throws -> SpatialStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("spatial").pack(Tuple([indexName]))

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try StatisticsEntryCodec.decodeSpatial(data)
        }
    }

    // MARK: - Bulk Operations

    /// Delete all statistics for a type
    public func deleteAllStatistics(typeName: String) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            // Delete table stats (single key range)
            let tableKey = self.statsSubspace.subspace("table").pack(Tuple([typeName]))
            let tableKeyEnd = tableKey.appending(0x00)
            try transaction.clearRange(beginKey: tableKey, endKey: tableKeyEnd)

            // Delete all field stats
            let fieldSubspace = self.statsSubspace.subspace("field").subspace(typeName)
            let (begin, end) = fieldSubspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Delete index statistics
    public func deleteIndexStatistics(indexName: String) async throws {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            let key = self.statsSubspace.subspace("index").pack(Tuple([indexName]))
            let keyEnd = key.appending(0x00)
            try transaction.clearRange(beginKey: key, endKey: keyEnd)
        }
    }

    /// Check if statistics exist for a type
    public func hasStatistics(typeName: String) async throws -> Bool {
        let stats = try await loadTableStatistics(typeName: typeName)
        return stats != nil
    }

    /// Get statistics age for a type
    public func statisticsAge(typeName: String) async throws -> TimeSpan? {
        guard let stats = try await loadTableStatistics(typeName: typeName) else {
            return nil
        }
        return try DatabaseTimestampMeasurement.elapsed(
            from: stats.timestamp,
            to: container.wallClock.now
        )
    }
}

// MARK: - Persisted Statistics Values

/// Table statistics persisted through the bounded statistics frame.
public struct TableStatisticsData: Sendable {
    public let rowCount: Int64
    public let avgRowSize: Int
    public let sampleSize: Int
    public let sampleRate: Double
    public let timestamp: Timestamp

    public init(
        rowCount: Int64,
        avgRowSize: Int = 0,
        sampleSize: Int = 0,
        sampleRate: Double = 1.0,
        timestamp: Timestamp
    ) {
        self.rowCount = rowCount
        self.avgRowSize = avgRowSize
        self.sampleSize = sampleSize
        self.sampleRate = sampleRate
        self.timestamp = timestamp
    }
}

/// Field statistics persisted through the bounded statistics frame.
///
/// Stores both MCV (Most Common Values) and Histogram for accurate selectivity estimation.
/// Following PostgreSQL pattern where histogram excludes MCV values.
///
/// **Reference**: PostgreSQL pg_statistic, selfuncs.c
public struct FieldStatisticsData: Sendable {
    public let fieldName: String
    public let distinctCount: Int64
    public let nullCount: Int64
    public let totalCount: Int64
    public let minValue: FieldValue?
    public let maxValue: FieldValue?

    /// Most Common Values list (for skewed distributions)
    public let mcv: MostCommonValues?

    /// Histogram (excludes MCV values to avoid double-counting)
    public let histogram: Histogram?

    public let timestamp: Timestamp

    public init(
        fieldName: String,
        distinctCount: Int64,
        nullCount: Int64 = 0,
        totalCount: Int64,
        minValue: FieldValue? = nil,
        maxValue: FieldValue? = nil,
        mcv: MostCommonValues? = nil,
        histogram: Histogram? = nil,
        timestamp: Timestamp
    ) {
        self.fieldName = fieldName
        self.distinctCount = distinctCount
        self.nullCount = nullCount
        self.totalCount = totalCount
        self.minValue = minValue
        self.maxValue = maxValue
        self.mcv = mcv
        self.histogram = histogram
        self.timestamp = timestamp
    }

    /// Compute equality selectivity using MCV if available
    public var equalitySelectivity: Double {
        guard distinctCount > 0 else { return 1.0 }
        return 1.0 / Double(distinctCount)
    }

    /// Compute null selectivity
    public var nullSelectivity: Double {
        guard totalCount > 0 else { return 0.0 }
        return Double(nullCount) / Double(totalCount)
    }

    /// Create combined selectivity estimator if both MCV and histogram available
    public var combinedEstimator: CombinedSelectivityEstimator? {
        guard let mcv = mcv, let histogram = histogram else {
            return nil
        }
        return CombinedSelectivityEstimator(mcv: mcv, histogram: histogram)
    }
}

/// Index statistics persisted through the bounded statistics frame.
public struct IndexStatisticsData: Sendable {
    public let indexName: String
    public let entryCount: Int64
    public let distinctKeyCount: Int64
    public let avgEntriesPerKey: Double
    public let sizeBytes: Int64?
    public let timestamp: Timestamp

    public init(
        indexName: String,
        entryCount: Int64,
        distinctKeyCount: Int64 = 0,
        avgEntriesPerKey: Double = 1.0,
        sizeBytes: Int64? = nil,
        timestamp: Timestamp
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.distinctKeyCount = distinctKeyCount
        self.avgEntriesPerKey = avgEntriesPerKey
        self.sizeBytes = sizeBytes
        self.timestamp = timestamp
    }
}

/// Vector statistics persisted through the bounded statistics frame.
public struct VectorStatisticsData: Sendable {
    public let indexName: String
    public let vectorCount: Int64
    public let dimensions: Int
    public let avgL2Norm: Double
    public let stdDevL2Norm: Double
    public let normBuckets: [NormBucketData]?
    public let timestamp: Timestamp

    public init(
        indexName: String,
        vectorCount: Int64,
        dimensions: Int,
        avgL2Norm: Double = 1.0,
        stdDevL2Norm: Double = 0.1,
        normBuckets: [NormBucketData]? = nil,
        timestamp: Timestamp
    ) {
        self.indexName = indexName
        self.vectorCount = vectorCount
        self.dimensions = dimensions
        self.avgL2Norm = avgL2Norm
        self.stdDevL2Norm = stdDevL2Norm
        self.normBuckets = normBuckets
        self.timestamp = timestamp
    }

    public struct NormBucketData: Sendable {
        public let minNorm: Double
        public let maxNorm: Double
        public let count: Int64

        public init(minNorm: Double, maxNorm: Double, count: Int64) {
            self.minNorm = minNorm
            self.maxNorm = maxNorm
            self.count = count
        }
    }
}

/// Full-text statistics persisted through the bounded statistics frame.
public struct FullTextStatisticsData: Sendable {
    public let indexName: String
    public let totalDocs: Int64
    public let avgDocLength: Double
    public let uniqueTerms: Int64
    public let topTerms: [TermFrequency]?
    public let timestamp: Timestamp

    public init(
        indexName: String,
        totalDocs: Int64,
        avgDocLength: Double,
        uniqueTerms: Int64,
        topTerms: [TermFrequency]? = nil,
        timestamp: Timestamp
    ) {
        self.indexName = indexName
        self.totalDocs = totalDocs
        self.avgDocLength = avgDocLength
        self.uniqueTerms = uniqueTerms
        self.topTerms = topTerms
        self.timestamp = timestamp
    }

    public struct TermFrequency: Sendable {
        public let term: String
        public let docFreq: Int64

        public init(term: String, docFreq: Int64) {
            self.term = term
            self.docFreq = docFreq
        }
    }
}

/// Spatial statistics persisted through the bounded statistics frame.
public struct SpatialStatisticsData: Sendable {
    public let indexName: String
    public let entryCount: Int64
    public let boundingBox: BoundingBox?
    public let cellCount: Int64
    public let avgCellDensity: Double
    public let hotCells: [UInt64]?
    public let timestamp: Timestamp

    public init(
        indexName: String,
        entryCount: Int64,
        boundingBox: BoundingBox? = nil,
        cellCount: Int64 = 0,
        avgCellDensity: Double = 1.0,
        hotCells: [UInt64]? = nil,
        timestamp: Timestamp
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.boundingBox = boundingBox
        self.cellCount = cellCount
        self.avgCellDensity = avgCellDensity
        self.hotCells = hotCells
        self.timestamp = timestamp
    }

    public struct BoundingBox: Sendable {
        public let minLat: Double
        public let minLon: Double
        public let maxLat: Double
        public let maxLon: Double

        public init(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
            self.minLat = minLat
            self.minLon = minLon
            self.maxLat = maxLat
            self.maxLon = maxLon
        }
    }
}

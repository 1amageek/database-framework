// StatisticsProvider.swift
// QueryPlanner - Statistics for cost estimation

import Foundation
import Core
import FoundationDB
import Synchronization

// MARK: - Range Bound

/// Represents a range bound for statistics queries
///
/// Used by `rangeSelectivity` to estimate selectivity of range conditions.
public struct RangeBound: @unchecked Sendable {
    /// Lower bound (value, inclusive)
    public let lower: RangeBoundComponent?

    /// Upper bound (value, inclusive)
    public let upper: RangeBoundComponent?

    public init(
        lower: RangeBoundComponent? = nil,
        upper: RangeBoundComponent? = nil
    ) {
        self.lower = lower
        self.upper = upper
    }

    /// Create a range bound from ScalarConstraintBounds
    public init(from bounds: ScalarConstraintBounds) {
        self.lower = bounds.lower.map { RangeBoundComponent(value: $0, inclusive: bounds.lowerInclusive) }
        self.upper = bounds.upper.map { RangeBoundComponent(value: $0, inclusive: bounds.upperInclusive) }
    }
}

/// A single component of a range bound (value + inclusivity)
public struct RangeBoundComponent: @unchecked Sendable {
    /// The bound value
    public let value: any TupleElement

    /// Whether the bound is inclusive
    public let inclusive: Bool

    public init(value: any TupleElement, inclusive: Bool) {
        self.value = value
        self.inclusive = inclusive
    }
}

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

// MARK: - Live Statistics Provider Protocol

/// Extended statistics provider that supports async server-side statistics
///
/// This protocol extends `StatisticsProvider` with async methods that can
/// fetch real-time statistics from the FoundationDB server, using features like
/// `getEstimatedRangeSizeBytes` and `getRangeSplitPoints`.
///
/// **Server-Side Features Used**:
/// - `getEstimatedRangeSizeBytes`: Accurate range size estimation without scanning
/// - `getRangeSplitPoints`: Split points for parallel query execution
///
/// Reference: FoundationDB 7.x C API
public protocol LiveStatisticsProvider: StatisticsProvider {
    /// Estimate the size of a key range in bytes (server-side)
    ///
    /// Uses FoundationDB's `getEstimatedRangeSizeBytes` for accurate estimation
    /// without needing to scan the entire range.
    ///
    /// - Parameters:
    ///   - beginKey: Start of the range (inclusive)
    ///   - endKey: End of the range (exclusive)
    /// - Returns: Estimated size in bytes
    func estimatedRangeSizeBytes(
        beginKey: [UInt8],
        endKey: [UInt8]
    ) async throws -> Int

    /// Get split points to divide a range for parallel processing
    ///
    /// Uses FoundationDB's `getRangeSplitPoints` to find optimal split points
    /// that divide the range into roughly equal-sized chunks.
    ///
    /// - Parameters:
    ///   - beginKey: Start of the range
    ///   - endKey: End of the range
    ///   - chunkSize: Target size per chunk in bytes
    /// - Returns: Array of split point keys
    func rangeSplitPoints(
        beginKey: [UInt8],
        endKey: [UInt8],
        chunkSize: Int
    ) async throws -> [[UInt8]]

    /// Estimate row count for an index range based on server-side byte estimation
    ///
    /// - Parameters:
    ///   - index: The index descriptor
    ///   - beginKey: Start of the range
    ///   - endKey: End of the range
    ///   - avgRowSizeBytes: Average size per row in bytes (for conversion)
    /// - Returns: Estimated row count
    func estimatedRowsInRange(
        index: IndexDescriptor,
        beginKey: [UInt8],
        endKey: [UInt8],
        avgRowSizeBytes: Int
    ) async throws -> Int
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
public struct FieldStatistics: @unchecked Sendable {
    /// Field name
    public let fieldName: String

    /// Number of distinct values
    public let distinctValues: Int

    /// Ratio of null values (0.0 - 1.0)
    public let nullRatio: Double

    /// Minimum value (if orderable)
    public let minValue: (any TupleElement)?

    /// Maximum value (if orderable)
    public let maxValue: (any TupleElement)?

    /// Histogram buckets for range estimation
    public let histogram: [HistogramBucket]?

    public init(
        fieldName: String,
        distinctValues: Int,
        nullRatio: Double,
        minValue: (any TupleElement)? = nil,
        maxValue: (any TupleElement)? = nil,
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
///
/// Note: Uses FieldValue instead of TupleElement because:
/// 1. Histogram buckets need to be comparable for range selectivity estimation
/// 2. Histograms are stored as JSON, not in FDB tuples
public struct HistogramBucket: Sendable {
    /// Lower bound of the bucket
    public let lowerBound: FieldValue

    /// Upper bound of the bucket
    public let upperBound: FieldValue

    /// Number of values in this bucket
    public let count: Int

    /// Cumulative count up to this bucket
    public let cumulativeCount: Int

    public init(
        lowerBound: FieldValue,
        upperBound: FieldValue,
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

// MARK: - Search Index Statistics

/// Statistics for vector indexes
public struct VectorIndexStatistics: Sendable {
    /// Index name
    public let indexName: String

    /// Total vector count
    public let vectorCount: Int

    /// Vector dimensions
    public let dimensions: Int

    /// Average L2 norm of vectors
    public let avgL2Norm: Double

    /// Standard deviation of L2 norms
    public let stdDevL2Norm: Double

    /// Norm distribution buckets for filtering optimization
    public let normBuckets: [NormBucket]?

    public init(
        indexName: String,
        vectorCount: Int,
        dimensions: Int,
        avgL2Norm: Double = 1.0,
        stdDevL2Norm: Double = 0.1,
        normBuckets: [NormBucket]? = nil
    ) {
        self.indexName = indexName
        self.vectorCount = vectorCount
        self.dimensions = dimensions
        self.avgL2Norm = avgL2Norm
        self.stdDevL2Norm = stdDevL2Norm
        self.normBuckets = normBuckets
    }
}

/// Bucket for L2 norm distribution
public struct NormBucket: Sendable {
    public let minNorm: Double
    public let maxNorm: Double
    public let count: Int

    public init(minNorm: Double, maxNorm: Double, count: Int) {
        self.minNorm = minNorm
        self.maxNorm = maxNorm
        self.count = count
    }
}

/// Statistics for full-text indexes
public struct FullTextIndexStatistics: Sendable {
    /// Index name
    public let indexName: String

    /// Total document count
    public let totalDocs: Int

    /// Average document length (in terms)
    public let avgDocLength: Double

    /// Total unique terms
    public let uniqueTerms: Int

    /// Term frequency distribution
    public let termFrequencies: [String: Int]?

    /// Most frequent terms (for optimization hints)
    public let topTerms: [(term: String, docFreq: Int)]?

    public init(
        indexName: String,
        totalDocs: Int,
        avgDocLength: Double,
        uniqueTerms: Int,
        termFrequencies: [String: Int]? = nil,
        topTerms: [(term: String, docFreq: Int)]? = nil
    ) {
        self.indexName = indexName
        self.totalDocs = totalDocs
        self.avgDocLength = avgDocLength
        self.uniqueTerms = uniqueTerms
        self.termFrequencies = termFrequencies
        self.topTerms = topTerms
    }
}

/// Statistics for spatial indexes
public struct SpatialIndexStatistics: Sendable {
    /// Index name
    public let indexName: String

    /// Total entry count
    public let entryCount: Int

    /// Bounding box of all entries
    public let boundingBox: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double)?

    /// Cell density distribution (cellCode -> count)
    public let cellDensity: [UInt64: Int]?

    /// Hot cells (cells with high density)
    public let hotCells: [UInt64]?

    public init(
        indexName: String,
        entryCount: Int,
        boundingBox: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double)? = nil,
        cellDensity: [UInt64: Int]? = nil,
        hotCells: [UInt64]? = nil
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.boundingBox = boundingBox
        self.cellDensity = cellDensity
        self.hotCells = hotCells
    }
}

// MARK: - Search Statistics Storage

/// Thread-safe storage for search index statistics
///
/// Using a separate final class with Mutex for Sendable conformance.
public final class SearchStatisticsStorage: Sendable {

    /// Internal state protected by Mutex
    private struct State: Sendable {
        var vectorStats: [String: VectorIndexStatistics] = [:]
        var fullTextStats: [String: FullTextIndexStatistics] = [:]
        var spatialStats: [String: SpatialIndexStatistics] = [:]
    }

    private let state: Mutex<State>

    /// Shared instance for global access
    public static let shared = SearchStatisticsStorage()

    public init() {
        self.state = Mutex(State())
    }

    // MARK: - Vector Statistics

    /// Get vector index statistics
    public func vectorIndexStats(indexName: String) -> VectorIndexStatistics? {
        state.withLock { $0.vectorStats[indexName] }
    }

    /// Update vector index statistics
    public func updateVectorStats(_ stats: VectorIndexStatistics) {
        state.withLock { $0.vectorStats[stats.indexName] = stats }
    }

    /// Estimate vectors within distance threshold based on norm distribution
    public func estimateVectorsWithinDistance(
        indexName: String,
        queryNorm: Double,
        maxDistance: Double
    ) -> Int {
        guard let stats = vectorIndexStats(indexName: indexName) else {
            return 100 // Default estimate
        }

        guard let buckets = stats.normBuckets else {
            return stats.vectorCount / 10
        }

        var potentialCount = 0
        for bucket in buckets {
            if abs(queryNorm - bucket.minNorm) <= maxDistance * 2 ||
               abs(queryNorm - bucket.maxNorm) <= maxDistance * 2 {
                potentialCount += bucket.count
            }
        }

        return max(1, potentialCount)
    }

    // MARK: - Full-Text Statistics

    /// Get full-text index statistics
    public func fullTextIndexStats(indexName: String) -> FullTextIndexStatistics? {
        state.withLock { $0.fullTextStats[indexName] }
    }

    /// Update full-text index statistics
    public func updateFullTextStats(_ stats: FullTextIndexStatistics) {
        state.withLock { $0.fullTextStats[stats.indexName] = stats }
    }

    /// Estimate documents for a term
    public func estimateDocFrequency(indexName: String, term: String) -> Int {
        guard let stats = fullTextIndexStats(indexName: indexName) else {
            return 100
        }

        if let freq = stats.termFrequencies?[term.lowercased()] {
            return freq
        }

        return max(1, stats.totalDocs / 100)
    }

    /// Calculate IDF for a term
    public func calculateIDF(indexName: String, term: String) -> Double {
        guard let stats = fullTextIndexStats(indexName: indexName) else {
            return 1.0
        }

        let docFreq = estimateDocFrequency(indexName: indexName, term: term)
        let n = Double(stats.totalDocs)
        let df = Double(max(docFreq, 1))

        return log((n - df + 0.5) / (df + 0.5) + 1)
    }

    // MARK: - Spatial Statistics

    /// Get spatial index statistics
    public func spatialIndexStats(indexName: String) -> SpatialIndexStatistics? {
        state.withLock { $0.spatialStats[indexName] }
    }

    /// Update spatial index statistics
    public func updateSpatialStats(_ stats: SpatialIndexStatistics) {
        state.withLock { $0.spatialStats[stats.indexName] = stats }
    }

    /// Estimate entries in a cell
    public func estimateCellDensity(indexName: String, cellCode: UInt64) -> Int {
        guard let stats = spatialIndexStats(indexName: indexName) else {
            return 10
        }

        if let density = stats.cellDensity?[cellCode] {
            return density
        }

        let avgDensity = stats.entryCount / max(1, stats.cellDensity?.count ?? 100)
        return max(1, avgDensity)
    }

    /// Check if a cell is a hot cell (high density)
    public func isHotCell(indexName: String, cellCode: UInt64) -> Bool {
        guard let stats = spatialIndexStats(indexName: indexName) else {
            return false
        }
        return stats.hotCells?.contains(cellCode) ?? false
    }
}

// MARK: - Statistics Collector

/// Collector for gathering search index statistics from the database
///
/// **Usage**:
/// ```swift
/// let collector = SearchStatisticsCollector(database: database, subspace: subspace)
///
/// // Collect vector statistics
/// let vectorStats = try await collector.collectVectorStats(
///     indexName: "idx_embedding",
///     dimensions: 128
/// )
/// statisticsProvider.updateVectorStats(vectorStats)
///
/// // Collect full-text statistics
/// let ftStats = try await collector.collectFullTextStats(indexName: "idx_content")
/// statisticsProvider.updateFullTextStats(ftStats)
/// ```
public struct SearchStatisticsCollector: Sendable {

    private let reader: StorageReader
    private let indexSubspace: Subspace

    public init(reader: StorageReader, indexSubspace: Subspace) {
        self.reader = reader
        self.indexSubspace = indexSubspace
    }

    /// Collect vector index statistics
    public func collectVectorStats(
        indexName: String,
        dimensions: Int,
        sampleSize: Int = 1000
    ) async throws -> VectorIndexStatistics {
        let subspace = indexSubspace.subspace(indexName)

        var vectorCount = 0
        var sumNorm: Double = 0
        var sumNormSquared: Double = 0
        var normValues: [Double] = []

        for try await (_, value) in reader.scanSubspace(subspace) {
            vectorCount += 1

            // Parse vector and compute norm (sample only)
            if normValues.count < sampleSize {
                if let vector = try? parseVectorForStats(from: value, dimensions: dimensions) {
                    let norm = computeL2Norm(vector)
                    normValues.append(norm)
                    sumNorm += norm
                    let normSquared = norm * norm
                    sumNormSquared += normSquared
                }
            }
        }

        // Compute statistics
        let sampleCount = Double(normValues.count)
        let avgNorm = sampleCount > 0 ? sumNorm / sampleCount : 1.0
        let variance = sampleCount > 1
            ? (sumNormSquared - sumNorm * sumNorm / sampleCount) / (sampleCount - 1)
            : 0.0
        let stdDev = sqrt(max(0, variance))

        // Build norm buckets
        let buckets = buildNormBuckets(norms: normValues, bucketCount: 10)

        return VectorIndexStatistics(
            indexName: indexName,
            vectorCount: vectorCount,
            dimensions: dimensions,
            avgL2Norm: avgNorm,
            stdDevL2Norm: stdDev,
            normBuckets: buckets
        )
    }

    /// Collect full-text index statistics
    public func collectFullTextStats(
        indexName: String,
        topTermCount: Int = 100
    ) async throws -> FullTextIndexStatistics {
        let subspace = indexSubspace.subspace(indexName)
        let termsSubspace = subspace.subspace("terms")

        var totalDocs: Set<[UInt8]> = []
        var termFrequencies: [String: Int] = [:]
        var totalTermOccurrences = 0

        // Scan terms subspace
        for try await (key, _) in reader.scanSubspace(termsSubspace) {
            // Key structure: [termsSubspace][term][docID]
            guard let keyTuple = try? termsSubspace.unpack(key) else { continue }
            guard keyTuple.count >= 2 else { continue }

            if let term = keyTuple[0] as? String {
                termFrequencies[term, default: 0] += 1
                totalTermOccurrences += 1
            }

            // Extract docID (remaining elements)
            var idElements: [any TupleElement] = []
            for i in 1..<keyTuple.count {
                if let element = keyTuple[i] {
                    idElements.append(element)
                }
            }
            let docID = Tuple(idElements).pack()
            totalDocs.insert(docID)
        }

        // Calculate average document length
        let avgDocLength = totalDocs.count > 0
            ? Double(totalTermOccurrences) / Double(totalDocs.count)
            : 0.0

        // Get top terms
        let sortedTerms = termFrequencies.sorted { $0.value > $1.value }
        let topTerms = Array(sortedTerms.prefix(topTermCount).map { ($0.key, $0.value) })

        return FullTextIndexStatistics(
            indexName: indexName,
            totalDocs: totalDocs.count,
            avgDocLength: avgDocLength,
            uniqueTerms: termFrequencies.count,
            termFrequencies: termFrequencies,
            topTerms: topTerms
        )
    }

    /// Collect spatial index statistics
    public func collectSpatialStats(indexName: String) async throws -> SpatialIndexStatistics {
        let subspace = indexSubspace.subspace(indexName)

        var entryCount = 0
        var cellDensity: [UInt64: Int] = [:]
        var minLat = Double.infinity
        var minLon = Double.infinity
        var maxLat = -Double.infinity
        var maxLon = -Double.infinity

        for try await (key, _) in reader.scanSubspace(subspace) {
            guard let keyTuple = try? subspace.unpack(key) else { continue }
            guard keyTuple.count >= 1 else { continue }

            entryCount += 1

            // Extract cell code
            if let cellCode = keyTuple[0] as? Int64 {
                let code = UInt64(bitPattern: cellCode)
                cellDensity[code, default: 0] += 1

                // Decode cell to update bounding box
                let (lat, lon) = decodeMortonForStats(code, level: 15)
                minLat = min(minLat, lat)
                maxLat = max(maxLat, lat)
                minLon = min(minLon, lon)
                maxLon = max(maxLon, lon)
            }
        }

        // Find hot cells (top 10% by density)
        let sortedCells = cellDensity.sorted { $0.value > $1.value }
        let hotCellCount = max(1, sortedCells.count / 10)
        let hotCells = Array(sortedCells.prefix(hotCellCount).map { $0.key })

        let boundingBox = entryCount > 0
            ? (minLat, minLon, maxLat, maxLon)
            : nil

        return SpatialIndexStatistics(
            indexName: indexName,
            entryCount: entryCount,
            boundingBox: boundingBox,
            cellDensity: cellDensity,
            hotCells: hotCells
        )
    }

    // MARK: - Helper Methods

    private func parseVectorForStats(from bytes: [UInt8], dimensions: Int) throws -> [Float] {
        let elements = try Tuple.unpack(from: bytes)
        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for i in 0..<min(dimensions, elements.count) {
            let element = elements[i]
            if let f = element as? Float {
                vector.append(f)
            } else if let d = element as? Double {
                vector.append(Float(d))
            } else if let i64 = element as? Int64 {
                vector.append(Float(i64))
            }
        }
        return vector
    }

    private func computeL2Norm(_ vector: [Float]) -> Double {
        var sum: Float = 0
        for v in vector {
            sum += v * v
        }
        return Double(sqrtf(sum))
    }

    private func buildNormBuckets(norms: [Double], bucketCount: Int) -> [NormBucket] {
        guard !norms.isEmpty else { return [] }

        let sorted = norms.sorted()
        let minNorm = sorted.first!
        let maxNorm = sorted.last!
        let range = maxNorm - minNorm

        guard range > 0 else {
            return [NormBucket(minNorm: minNorm, maxNorm: maxNorm, count: norms.count)]
        }

        let bucketSize = range / Double(bucketCount)
        var buckets: [NormBucket] = []

        for i in 0..<bucketCount {
            let lower = minNorm + Double(i) * bucketSize
            let upper = i == bucketCount - 1 ? maxNorm : minNorm + Double(i + 1) * bucketSize
            let count = sorted.filter { $0 >= lower && $0 < upper }.count
            buckets.append(NormBucket(minNorm: lower, maxNorm: upper, count: count))
        }

        return buckets
    }

    private func decodeMortonForStats(_ code: UInt64, level: Int) -> (lat: Double, lon: Double) {
        var x: UInt32 = 0
        var y: UInt32 = 0

        for i in 0..<level {
            x |= UInt32((code >> (2 * i)) & 1) << i
            y |= UInt32((code >> (2 * i + 1)) & 1) << i
        }

        let maxVal = Double(1 << level)
        let lon = (Double(x) / maxVal) * 360.0 - 180.0
        let lat = (Double(y) / maxVal) * 180.0 - 90.0

        return (lat, lon)
    }
}

// MARK: - FDB Live Statistics Provider

/// Live statistics provider using FoundationDB server-side APIs
///
/// This provider uses FDB's `getEstimatedRangeSizeBytes` and `getRangeSplitPoints`
/// to provide accurate, real-time statistics without scanning data.
///
/// **Features**:
/// - Server-side range size estimation (O(1) instead of O(N))
/// - Automatic split point calculation for parallel queries
/// - Byte-to-row count conversion using configurable average row size
///
/// **Usage**:
/// ```swift
/// let provider = FDBLiveStatisticsProvider(
///     database: database,
///     subspace: subspace,
///     baseProvider: CollectedStatisticsProvider()
/// )
///
/// // Get accurate range size
/// let bytes = try await provider.estimatedRangeSizeBytes(
///     beginKey: startKey,
///     endKey: endKey
/// )
///
/// // Get split points for parallel execution
/// let splits = try await provider.rangeSplitPoints(
///     beginKey: startKey,
///     endKey: endKey,
///     chunkSize: 10_000_000  // 10MB chunks
/// )
/// ```
public final class FDBLiveStatisticsProvider: LiveStatisticsProvider, @unchecked Sendable {

    /// Underlying database
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Root subspace for the data
    private let subspace: Subspace

    /// Base provider for non-live statistics
    private let baseProvider: StatisticsProvider

    /// Default average row size for byte-to-row conversion
    private let defaultAvgRowSize: Int

    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        baseProvider: StatisticsProvider = DefaultStatisticsProvider(),
        defaultAvgRowSize: Int = 200
    ) {
        self.database = database
        self.subspace = subspace
        self.baseProvider = baseProvider
        self.defaultAvgRowSize = defaultAvgRowSize
    }

    // MARK: - StatisticsProvider (delegated to base)

    public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        baseProvider.estimatedRowCount(for: type)
    }

    public func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int? {
        baseProvider.estimatedDistinctValues(field: field, type: type)
    }

    public func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        baseProvider.equalitySelectivity(field: field, type: type)
    }

    public func rangeSelectivity<T: Persistable>(
        field: String,
        range: RangeBound,
        type: T.Type
    ) -> Double? {
        baseProvider.rangeSelectivity(field: field, range: range, type: type)
    }

    public func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        baseProvider.nullSelectivity(field: field, type: type)
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        baseProvider.estimatedIndexEntries(index: index)
    }

    // MARK: - LiveStatisticsProvider (server-side)

    public func estimatedRangeSizeBytes(
        beginKey: [UInt8],
        endKey: [UInt8]
    ) async throws -> Int {
        // Use readOnly config for statistics queries (GRV cache enabled)
        try await database.withTransaction(configuration: .readOnly) { transaction in
            try await transaction.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }
    }

    public func rangeSplitPoints(
        beginKey: [UInt8],
        endKey: [UInt8],
        chunkSize: Int
    ) async throws -> [[UInt8]] {
        // Use readOnly config for statistics queries (GRV cache enabled)
        try await database.withTransaction(configuration: .readOnly) { transaction in
            try await transaction.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }
    }

    public func estimatedRowsInRange(
        index: IndexDescriptor,
        beginKey: [UInt8],
        endKey: [UInt8],
        avgRowSizeBytes: Int
    ) async throws -> Int {
        let sizeBytes = try await estimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )

        let rowSize = avgRowSizeBytes > 0 ? avgRowSizeBytes : defaultAvgRowSize
        return max(1, sizeBytes / rowSize)
    }

    // MARK: - Convenience Methods

    /// Get split points for a subspace
    public func subspaceSplitPoints(
        subspace: Subspace,
        chunkSize: Int = 10_000_000  // 10MB default
    ) async throws -> [[UInt8]] {
        let (begin, end) = subspace.range()
        return try await rangeSplitPoints(
            beginKey: begin,
            endKey: end,
            chunkSize: chunkSize
        )
    }

    /// Estimate total size of a subspace in bytes
    public func subspaceSizeBytes(subspace: Subspace) async throws -> Int {
        let (begin, end) = subspace.range()
        return try await estimatedRangeSizeBytes(
            beginKey: begin,
            endKey: end
        )
    }

    /// Estimate item count for a type using server-side size estimation
    public func estimatedItemCountLive<T: Persistable>(
        for type: T.Type,
        itemSubspace: Subspace,
        avgRowSizeBytes: Int? = nil
    ) async throws -> Int {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let sizeBytes = try await subspaceSizeBytes(subspace: typeSubspace)

        let rowSize = avgRowSizeBytes ?? defaultAvgRowSize
        return max(0, sizeBytes / rowSize)
    }

    /// Estimate index entry count using server-side size estimation
    public func estimatedIndexEntriesLive(
        index: IndexDescriptor,
        indexSubspace: Subspace,
        avgEntrySizeBytes: Int = 50  // Index entries are typically small
    ) async throws -> Int {
        let specificIndexSubspace = indexSubspace.subspace(index.name)
        let sizeBytes = try await subspaceSizeBytes(subspace: specificIndexSubspace)

        return max(0, sizeBytes / avgEntrySizeBytes)
    }
}

// MARK: - Parallel Scan Support

/// Configuration for parallel range scanning
public struct ParallelScanConfiguration: Sendable {
    /// Target chunk size in bytes for splitting
    public let chunkSizeBytes: Int

    /// Maximum number of concurrent tasks
    public let maxConcurrency: Int

    /// Minimum entries per chunk (to avoid too many small chunks)
    public let minEntriesPerChunk: Int

    /// Default configuration
    public static var `default`: ParallelScanConfiguration {
        ParallelScanConfiguration(
            chunkSizeBytes: 10_000_000,  // 10MB
            maxConcurrency: 8,
            minEntriesPerChunk: 100
        )
    }

    /// Configuration for smaller datasets
    public static var small: ParallelScanConfiguration {
        ParallelScanConfiguration(
            chunkSizeBytes: 1_000_000,   // 1MB
            maxConcurrency: 4,
            minEntriesPerChunk: 50
        )
    }

    /// Configuration for large datasets
    public static var large: ParallelScanConfiguration {
        ParallelScanConfiguration(
            chunkSizeBytes: 50_000_000,  // 50MB
            maxConcurrency: 16,
            minEntriesPerChunk: 500
        )
    }

    public init(
        chunkSizeBytes: Int,
        maxConcurrency: Int,
        minEntriesPerChunk: Int
    ) {
        self.chunkSizeBytes = chunkSizeBytes
        self.maxConcurrency = maxConcurrency
        self.minEntriesPerChunk = minEntriesPerChunk
    }
}

/// A range chunk for parallel processing
public struct RangeChunk: Sendable {
    /// Start key (inclusive)
    public let beginKey: [UInt8]

    /// End key (exclusive)
    public let endKey: [UInt8]

    /// Chunk index (0-based)
    public let index: Int

    /// Estimated size in bytes (if available)
    public let estimatedSizeBytes: Int?

    public init(
        beginKey: [UInt8],
        endKey: [UInt8],
        index: Int,
        estimatedSizeBytes: Int? = nil
    ) {
        self.beginKey = beginKey
        self.endKey = endKey
        self.index = index
        self.estimatedSizeBytes = estimatedSizeBytes
    }
}

extension FDBLiveStatisticsProvider {
    /// Divide a range into chunks for parallel processing
    ///
    /// Uses `getRangeSplitPoints` to find optimal split points based on
    /// data distribution, ensuring roughly equal-sized chunks.
    ///
    /// - Parameters:
    ///   - beginKey: Start of the range
    ///   - endKey: End of the range
    ///   - configuration: Parallel scan configuration
    /// - Returns: Array of range chunks for parallel processing
    public func divideRangeForParallelScan(
        beginKey: [UInt8],
        endKey: [UInt8],
        configuration: ParallelScanConfiguration = .default
    ) async throws -> [RangeChunk] {
        // Get split points from FDB
        let splitPoints = try await rangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: configuration.chunkSizeBytes
        )

        // Build chunks from split points
        var chunks: [RangeChunk] = []
        var currentBegin = beginKey

        for (index, splitPoint) in splitPoints.enumerated() {
            chunks.append(RangeChunk(
                beginKey: currentBegin,
                endKey: splitPoint,
                index: index,
                estimatedSizeBytes: configuration.chunkSizeBytes
            ))
            currentBegin = splitPoint
        }

        // Add final chunk
        if currentBegin.lexicographicallyPrecedes(endKey) || currentBegin == beginKey {
            chunks.append(RangeChunk(
                beginKey: currentBegin,
                endKey: endKey,
                index: chunks.count,
                estimatedSizeBytes: nil
            ))
        }

        // Limit to maxConcurrency
        if chunks.count > configuration.maxConcurrency {
            // Merge chunks to fit within concurrency limit
            return mergeChunks(chunks, targetCount: configuration.maxConcurrency)
        }

        return chunks
    }

    /// Merge chunks to reduce count
    private func mergeChunks(_ chunks: [RangeChunk], targetCount: Int) -> [RangeChunk] {
        guard chunks.count > targetCount else { return chunks }

        let mergeRatio = (chunks.count + targetCount - 1) / targetCount
        var merged: [RangeChunk] = []

        for i in stride(from: 0, to: chunks.count, by: mergeRatio) {
            let endIndex = min(i + mergeRatio, chunks.count)
            let firstChunk = chunks[i]
            let lastChunk = chunks[endIndex - 1]

            merged.append(RangeChunk(
                beginKey: firstChunk.beginKey,
                endKey: lastChunk.endKey,
                index: merged.count,
                estimatedSizeBytes: nil
            ))
        }

        return merged
    }
}

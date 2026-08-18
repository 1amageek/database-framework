// StatisticsProvider.swift
// Database statistics for query analysis

import DatabaseTypes
import DatabaseKit
import DatabaseMath
import StorageKit
import Synchronization

// MARK: - Range Bound

/// Represents a range bound for statistics queries
///
/// Used by `rangeSelectivity` to estimate selectivity of range conditions.
public struct RangeBound: Sendable {
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

}

/// A single component of a range bound (value + inclusivity)
public struct RangeBoundComponent: Sendable {
    /// The bound value
    public let value: FieldValue

    /// Whether the bound is inclusive
    public let inclusive: Bool

    public init(value: FieldValue, inclusive: Bool) {
        self.value = value
        self.inclusive = inclusive
    }
}

/// Provides statistics about tables and indexes for cost estimation
public protocol StatisticsProvider: Sendable {
    /// Estimated total row count for a type
    func estimatedRowCount(entity: String) -> Int

    /// Estimated distinct values for a field
    func estimatedDistinctValues(
        field: String,
        entity: String
    ) -> Int?

    /// Selectivity for equality condition
    func equalitySelectivity(
        field: String,
        entity: String
    ) -> Double?

    /// Selectivity for range condition
    func rangeSelectivity(
        field: String,
        range: RangeBound,
        entity: String
    ) -> Double?

    /// Selectivity for null check
    func nullSelectivity(
        field: String,
        entity: String
    ) -> Double?

    /// Index entry count estimate
    func estimatedIndexEntries(
        index: IndexDescriptor
    ) -> Int?
}

// MARK: - Heuristic Statistics Provider

/// Statistics provider that derives estimates from configured heuristic ratios.
///
/// This provider is a deterministic cold-start policy for planning before
/// collected statistics are available. Applications that retain runtime
/// statistics should use `CollectedStatisticsProvider`, which delegates only
/// missing observations to this policy.
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
public struct HeuristicStatisticsProvider: StatisticsProvider {

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

    public func estimatedRowCount(entity: String) -> Int {
        defaultRowCount
    }

    public func estimatedDistinctValues(
        field: String,
        entity: String
    ) -> Int? {
        // HEURISTIC: Assume 10% distinct values by default
        max(1, Int(Double(defaultRowCount) * distinctValueRatio))
    }

    public func equalitySelectivity(
        field: String,
        entity: String
    ) -> Double? {
        // HEURISTIC: 1 / estimated distinct values
        guard let distinct = estimatedDistinctValues(field: field, entity: entity) else {
            return nil
        }
        return 1.0 / Double(distinct)
    }

    public func rangeSelectivity(
        field: String,
        range: RangeBound,
        entity: String
    ) -> Double? {
        // HEURISTIC: Default to 30% for ranges
        // In production, use histogram-based estimation
        0.3
    }

    public func nullSelectivity(
        field: String,
        entity: String
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
    public let lastUpdated: Timestamp

    public init(
        rowCount: Int,
        sampleSize: Int,
        lastUpdated: Timestamp
    ) {
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
/// 2. Histograms are stored in bounded engine-owned binary frames
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
public final class CollectedStatisticsProvider: StatisticsProvider, Sendable {
    private struct State: Sendable {
        var tableStatistics: [String: TableStatistics] = [:]
        var fieldStatistics: [String: FieldStatistics] = [:]
        var indexStatistics: [String: IndexStatistics] = [:]
    }

    private let state = Mutex(State())

    /// Default fallback provider
    private let fallback: HeuristicStatisticsProvider

    public init(fallbackRowCount: Int = 10000) {
        self.fallback = HeuristicStatisticsProvider(defaultRowCount: fallbackRowCount)
    }

    // MARK: - StatisticsProvider

    public func estimatedRowCount(entity: String) -> Int {
        return state.withLock { state in
            state.tableStatistics[entity]?.rowCount
                ?? fallback.estimatedRowCount(entity: entity)
        }
    }

    public func estimatedDistinctValues(
        field: String,
        entity: String
    ) -> Int? {
        let key = "\(entity).\(field)"
        return state.withLock { state in
            state.fieldStatistics[key]?.distinctValues
                ?? fallback.estimatedDistinctValues(field: field, entity: entity)
        }
    }

    public func equalitySelectivity(
        field: String,
        entity: String
    ) -> Double? {
        guard let distinct = estimatedDistinctValues(field: field, entity: entity) else {
            return fallback.equalitySelectivity(field: field, entity: entity)
        }
        return 1.0 / Double(max(1, distinct))
    }

    public func rangeSelectivity(
        field: String,
        range: RangeBound,
        entity: String
    ) -> Double? {
        let key = "\(entity).\(field)"
        let stats = state.withLock { $0.fieldStatistics[key] }

        guard let stats = stats, let histogram = stats.histogram else {
            return fallback.rangeSelectivity(field: field, range: range, entity: entity)
        }

        // Use histogram for range estimation
        return estimateRangeSelectivityFromHistogram(histogram: histogram, range: range)
    }

    public func nullSelectivity(
        field: String,
        entity: String
    ) -> Double? {
        let key = "\(entity).\(field)"
        let stats = state.withLock { $0.fieldStatistics[key] }

        return stats?.nullRatio ?? fallback.nullSelectivity(field: field, entity: entity)
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        let stats = state.withLock { $0.indexStatistics[index.name] }

        return stats?.entryCount ?? fallback.estimatedIndexEntries(index: index)
    }

    // MARK: - Statistics Collection

    /// Update table statistics
    public func updateTableStats<T: Persistable>(
        for type: T.Type,
        rowCount: Int,
        sampleSize: Int,
        lastUpdated: Timestamp
    ) {
        updateTableStats(
            typeName: T.persistableType,
            rowCount: rowCount,
            sampleSize: sampleSize,
            lastUpdated: lastUpdated
        )
    }

    func updateTableStats(
        typeName: String,
        rowCount: Int,
        sampleSize: Int,
        lastUpdated: Timestamp
    ) {
        let stats = TableStatistics(
            rowCount: rowCount,
            sampleSize: sampleSize,
            lastUpdated: lastUpdated
        )
        state.withLock { $0.tableStatistics[typeName] = stats }
    }

    /// Update field statistics
    public func updateFieldStats<T: Persistable>(
        for type: T.Type,
        field: String,
        stats: FieldStatistics
    ) {
        let key = "\(T.persistableType).\(field)"

        state.withLock { $0.fieldStatistics[key] = stats }
    }

    /// Update index statistics
    public func updateIndexStats(_ stats: IndexStatistics) {
        state.withLock { $0.indexStatistics[stats.indexName] = stats }
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

        return DatabaseMath.naturalLogarithm((n - df + 0.5) / (df + 0.5) + 1)
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

        var vectorCursor = try reader.subspaceCursor(
            subspace,
            reverse: false
        )
        try await vectorCursor.consume { _, value in
            vectorCount += 1

            // Parse vector and compute norm (sample only)
            if normValues.count < sampleSize {
                let vector = try parseVectorForStats(
                    from: value,
                    dimensions: dimensions
                )
                let norm = computeL2Norm(vector)
                normValues.append(norm)
                sumNorm += norm
                let normSquared = norm * norm
                sumNormSquared += normSquared
            }
        }

        // Compute statistics
        let sampleCount = Double(normValues.count)
        let avgNorm = sampleCount > 0 ? sumNorm / sampleCount : 1.0
        let variance = sampleCount > 1
            ? (sumNormSquared - sumNorm * sumNorm / sampleCount) / (sampleCount - 1)
            : 0.0
        let stdDev = DatabaseMath.squareRoot(max(0, variance))

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

        var totalDocs: Set<ByteString> = []
        var termFrequencies: [String: Int] = [:]
        var totalTermOccurrences = 0

        // Scan terms subspace
        var termCursor = try reader.subspaceCursor(
            termsSubspace,
            reverse: false
        )
        try await termCursor.consume { key, _ in
            // Key structure: [termsSubspace][term][docID]
            // Quick check before attempting unpack
            guard termsSubspace.contains(key) else {
                throw StatisticsCollectionError.keyOutsideSubspace
            }
            let keyTuple = try termsSubspace.unpack(key)
            guard keyTuple.count >= 2 else {
                throw StatisticsCollectionError.invalidElementCount(
                    expectedAtLeast: 2,
                    actual: keyTuple.count
                )
            }

            guard case .string(let term) = try keyTuple.value(at: 0) else {
                throw StatisticsCollectionError.invalidTerm
            }
            termFrequencies[term, default: 0] += 1
            totalTermOccurrences += 1

            // Extract docID (remaining elements)
            var idElements: [any TupleElement] = []
            for index in 1..<keyTuple.count {
                idElements.append(try keyTuple.element(at: index))
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
        let topTerms: [(term: String, docFreq: Int)] = sortedTerms
            .prefix(topTermCount)
            .map { (term: $0.key, docFreq: $0.value) }

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

        var spatialCursor = try reader.subspaceCursor(
            subspace,
            reverse: false
        )
        try await spatialCursor.consume { key, _ in
            // Quick check before attempting unpack
            guard subspace.contains(key) else {
                throw StatisticsCollectionError.keyOutsideSubspace
            }
            let keyTuple = try subspace.unpack(key)
            guard keyTuple.count >= 1 else {
                throw StatisticsCollectionError.invalidElementCount(
                    expectedAtLeast: 1,
                    actual: keyTuple.count
                )
            }

            entryCount += 1

            // Extract cell code
            guard case .signedInteger(let cellCode) = try keyTuple.value(at: 0) else {
                throw StatisticsCollectionError.invalidSpatialCell
            }
            let code = UInt64(bitPattern: cellCode)
            cellDensity[code, default: 0] += 1

            // Decode cell to update bounding box
            let (lat, lon) = decodeMortonForStats(code, level: 15)
            minLat = min(minLat, lat)
            maxLat = max(maxLat, lat)
            minLon = min(minLon, lon)
            maxLon = max(maxLon, lon)
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

    private func parseVectorForStats(
        from bytes: ByteString,
        dimensions: Int
    ) throws -> [Float] {
        let elements = try Tuple.unpack(from: bytes)
        var vector: [Float] = []
        vector.reserveCapacity(dimensions)

        for i in 0..<min(dimensions, elements.count) {
            let value = try FieldValue(tupleElement: elements[i])
            guard let component = exactFloat(from: value) else {
                throw StatisticsCollectionError.invalidVectorElement(index: i)
            }
            vector.append(component)
        }
        guard vector.count == dimensions else {
            throw StatisticsCollectionError.invalidVectorDimensions(
                expected: dimensions,
                actual: vector.count
            )
        }
        return vector
    }

    private func exactFloat(from value: FieldValue) -> Float? {
        switch value {
        case .int8(let value):
            return Float(exactly: value)
        case .int16(let value):
            return Float(exactly: value)
        case .int32(let value):
            return Float(exactly: value)
        case .int64(let value):
            return Float(exactly: value)
        case .uint8(let value):
            return Float(exactly: value)
        case .uint16(let value):
            return Float(exactly: value)
        case .uint32(let value):
            return Float(exactly: value)
        case .uint64(let value):
            return Float(exactly: value)
        case .float32(let value):
            return value.isFinite ? value : nil
        case .float64(let value):
            guard value.isFinite else { return nil }
            return Float(exactly: value)
        case .null, .bool, .decimal, .string, .bytes, .date, .time,
             .dateTime, .timestamp, .timeSpan, .calendarPeriod,
             .geographicPoint, .geographicPosition, .vector, .uuid,
             .array, .object, .reference, .rdfTerm:
            return nil
        }
    }

    private func computeL2Norm(_ vector: [Float]) -> Double {
        var sum: Float = 0
        for v in vector {
            sum += v * v
        }
        return Double(DatabaseMath.squareRoot(sum))
    }

    private func buildNormBuckets(norms: [Double], bucketCount: Int) -> [NormBucket] {
        guard !norms.isEmpty else { return [] }

        let sorted = norms.sorted()
        guard let minNorm = sorted.first, let maxNorm = sorted.last else { return [] }
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

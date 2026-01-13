// StatisticsStorage.swift
// QueryPlanner - FoundationDB persistence for statistics

import Foundation
import FoundationDB
import Core
import Synchronization

/// Persistent storage for statistics in FoundationDB
///
/// **Storage Layout**:
/// ```
/// [subspace]/_statistics/table/[typeName]               → TableStatisticsData (JSON)
/// [subspace]/_statistics/field/[typeName]/[fieldName]   → FieldStatisticsData (JSON)
/// [subspace]/_statistics/index/[indexName]              → IndexStatisticsData (JSON)
/// [subspace]/_statistics/search/vector/[indexName]      → VectorIndexStatistics (JSON)
/// [subspace]/_statistics/search/fulltext/[indexName]    → FullTextIndexStatistics (JSON)
/// [subspace]/_statistics/search/spatial/[indexName]     → SpatialIndexStatistics (JSON)
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
    private let container: FDBContainer

    /// Root subspace for statistics storage
    private let subspace: Subspace

    /// Statistics subspace prefix
    private var statsSubspace: Subspace {
        subspace.subspace("_statistics")
    }

    /// Create a statistics storage
    ///
    /// - Parameters:
    ///   - container: FDBContainer for transaction execution
    ///   - subspace: Root subspace (typically container's subspace)
    public init(container: FDBContainer, subspace: Subspace) {
        self.container = container
        self.subspace = subspace
    }

    // MARK: - Table Statistics

    /// Save table statistics
    public func saveTableStatistics(typeName: String, stats: TableStatisticsData) async throws {
        let key = statsSubspace.subspace("table").pack(Tuple([typeName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load table statistics
    public func loadTableStatistics(typeName: String) async throws -> TableStatisticsData? {
        let key = statsSubspace.subspace("table").pack(Tuple([typeName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(TableStatisticsData.self, from: Data(data))
        }
    }

    /// Load all table statistics
    public func loadAllTableStatistics() async throws -> [String: TableStatisticsData] {
        let tableSubspace = statsSubspace.subspace("table")

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            let decoder = ProtobufDecoder()
            var results: [String: TableStatisticsData] = [:]

            let (begin, end) = tableSubspace.range()
            for try await (key, value) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                guard let keyTuple = try? tableSubspace.unpack(key),
                      let typeName = keyTuple[0] as? String else {
                    continue
                }

                if let stats = try? decoder.decode(TableStatisticsData.self, from: Data(value)) {
                    results[typeName] = stats
                }
            }

            return results
        }
    }

    // MARK: - Field Statistics

    /// Save field statistics
    public func saveFieldStatistics(typeName: String, fieldName: String, stats: FieldStatisticsData) async throws {
        let key = statsSubspace.subspace("field").subspace(typeName).pack(Tuple([fieldName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load field statistics
    public func loadFieldStatistics(typeName: String, fieldName: String) async throws -> FieldStatisticsData? {
        let key = statsSubspace.subspace("field").subspace(typeName).pack(Tuple([fieldName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(FieldStatisticsData.self, from: Data(data))
        }
    }

    /// Load all field statistics for a type
    public func loadAllFieldStatistics(typeName: String) async throws -> [String: FieldStatisticsData] {
        let fieldSubspace = statsSubspace.subspace("field").subspace(typeName)

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            let decoder = ProtobufDecoder()
            var results: [String: FieldStatisticsData] = [:]

            let (begin, end) = fieldSubspace.range()
            for try await (key, value) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                guard let keyTuple = try? fieldSubspace.unpack(key),
                      let fieldName = keyTuple[0] as? String else {
                    continue
                }

                if let stats = try? decoder.decode(FieldStatisticsData.self, from: Data(value)) {
                    results[fieldName] = stats
                }
            }

            return results
        }
    }

    // MARK: - Index Statistics

    /// Save index statistics
    public func saveIndexStatistics(indexName: String, stats: IndexStatisticsData) async throws {
        let key = statsSubspace.subspace("index").pack(Tuple([indexName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load index statistics
    public func loadIndexStatistics(indexName: String) async throws -> IndexStatisticsData? {
        let key = statsSubspace.subspace("index").pack(Tuple([indexName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(IndexStatisticsData.self, from: Data(data))
        }
    }

    // MARK: - Search Statistics

    /// Save vector index statistics
    public func saveVectorStatistics(indexName: String, stats: VectorStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("vector").pack(Tuple([indexName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load vector index statistics
    public func loadVectorStatistics(indexName: String) async throws -> VectorStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("vector").pack(Tuple([indexName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(VectorStatisticsData.self, from: Data(data))
        }
    }

    /// Save full-text index statistics
    public func saveFullTextStatistics(indexName: String, stats: FullTextStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("fulltext").pack(Tuple([indexName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load full-text index statistics
    public func loadFullTextStatistics(indexName: String) async throws -> FullTextStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("fulltext").pack(Tuple([indexName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(FullTextStatisticsData.self, from: Data(data))
        }
    }

    /// Save spatial index statistics
    public func saveSpatialStatistics(indexName: String, stats: SpatialStatisticsData) async throws {
        let key = statsSubspace.subspace("search").subspace("spatial").pack(Tuple([indexName]))
        let data = try ProtobufEncoder().encode(stats)

        try await container.database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Load spatial index statistics
    public func loadSpatialStatistics(indexName: String) async throws -> SpatialStatisticsData? {
        let key = statsSubspace.subspace("search").subspace("spatial").pack(Tuple([indexName]))

        return try await container.database.withTransaction(configuration: .batch) { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try ProtobufDecoder().decode(SpatialStatisticsData.self, from: Data(data))
        }
    }

    // MARK: - Bulk Operations

    /// Delete all statistics for a type
    public func deleteAllStatistics(typeName: String) async throws {
        try await container.database.withTransaction(configuration: .batch) { transaction in
            try transaction.setOption(forOption: .accessSystemKeys)
            // Delete table stats (single key range)
            let tableKey = self.statsSubspace.subspace("table").pack(Tuple([typeName]))
            let tableKeyEnd = tableKey + [0x00]
            transaction.clearRange(beginKey: tableKey, endKey: tableKeyEnd)

            // Delete all field stats
            let fieldSubspace = self.statsSubspace.subspace("field").subspace(typeName)
            let (begin, end) = fieldSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Delete index statistics
    public func deleteIndexStatistics(indexName: String) async throws {
        try await container.database.withTransaction(configuration: .batch) { transaction in
            try transaction.setOption(forOption: .accessSystemKeys)
            let key = self.statsSubspace.subspace("index").pack(Tuple([indexName]))
            let keyEnd = key + [0x00]
            transaction.clearRange(beginKey: key, endKey: keyEnd)
        }
    }

    /// Check if statistics exist for a type
    public func hasStatistics(typeName: String) async throws -> Bool {
        let stats = try await loadTableStatistics(typeName: typeName)
        return stats != nil
    }

    /// Get statistics age for a type
    public func statisticsAge(typeName: String) async throws -> TimeInterval? {
        guard let stats = try await loadTableStatistics(typeName: typeName) else {
            return nil
        }
        return Date().timeIntervalSince(stats.timestamp)
    }
}

// MARK: - Codable Data Structures

/// Codable table statistics for persistence
public struct TableStatisticsData: Codable, Sendable {
    public let rowCount: Int64
    public let avgRowSize: Int
    public let sampleSize: Int
    public let sampleRate: Double
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case rowCount = "rowCount"
        case avgRowSize = "avgRowSize"
        case sampleSize = "sampleSize"
        case sampleRate = "sampleRate"
        case timestamp = "timestamp"

        var intValue: Int? {
            switch self {
            case .rowCount: return 1
            case .avgRowSize: return 2
            case .sampleSize: return 3
            case .sampleRate: return 4
            case .timestamp: return 5
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .rowCount
            case 2: self = .avgRowSize
            case 3: self = .sampleSize
            case 4: self = .sampleRate
            case 5: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) {
            self.init(rawValue: stringValue)
        }

        var stringValue: String { rawValue }
    }

    public init(
        rowCount: Int64,
        avgRowSize: Int = 0,
        sampleSize: Int = 0,
        sampleRate: Double = 1.0,
        timestamp: Date = Date()
    ) {
        self.rowCount = rowCount
        self.avgRowSize = avgRowSize
        self.sampleSize = sampleSize
        self.sampleRate = sampleRate
        self.timestamp = timestamp
    }
}

/// Codable field statistics for persistence
///
/// Stores both MCV (Most Common Values) and Histogram for accurate selectivity estimation.
/// Following PostgreSQL pattern where histogram excludes MCV values.
///
/// **Reference**: PostgreSQL pg_statistic, selfuncs.c
public struct FieldStatisticsData: Codable, Sendable {
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

    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case fieldName = "fieldName"
        case distinctCount = "distinctCount"
        case nullCount = "nullCount"
        case totalCount = "totalCount"
        case minValue = "minValue"
        case maxValue = "maxValue"
        case mcv = "mcv"
        case histogram = "histogram"
        case timestamp = "timestamp"

        var intValue: Int? {
            switch self {
            case .fieldName: return 1
            case .distinctCount: return 2
            case .nullCount: return 3
            case .totalCount: return 4
            case .minValue: return 5
            case .maxValue: return 6
            case .mcv: return 7
            case .histogram: return 8
            case .timestamp: return 9
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .fieldName
            case 2: self = .distinctCount
            case 3: self = .nullCount
            case 4: self = .totalCount
            case 5: self = .minValue
            case 6: self = .maxValue
            case 7: self = .mcv
            case 8: self = .histogram
            case 9: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) {
            self.init(rawValue: stringValue)
        }

        var stringValue: String { rawValue }
    }

    public init(
        fieldName: String,
        distinctCount: Int64,
        nullCount: Int64 = 0,
        totalCount: Int64,
        minValue: FieldValue? = nil,
        maxValue: FieldValue? = nil,
        mcv: MostCommonValues? = nil,
        histogram: Histogram? = nil,
        timestamp: Date = Date()
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

/// Codable index statistics for persistence
public struct IndexStatisticsData: Codable, Sendable {
    public let indexName: String
    public let entryCount: Int64
    public let distinctKeyCount: Int64
    public let avgEntriesPerKey: Double
    public let sizeBytes: Int64?
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case indexName = "indexName"
        case entryCount = "entryCount"
        case distinctKeyCount = "distinctKeyCount"
        case avgEntriesPerKey = "avgEntriesPerKey"
        case sizeBytes = "sizeBytes"
        case timestamp = "timestamp"

        var intValue: Int? {
            switch self {
            case .indexName: return 1
            case .entryCount: return 2
            case .distinctKeyCount: return 3
            case .avgEntriesPerKey: return 4
            case .sizeBytes: return 5
            case .timestamp: return 6
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .indexName
            case 2: self = .entryCount
            case 3: self = .distinctKeyCount
            case 4: self = .avgEntriesPerKey
            case 5: self = .sizeBytes
            case 6: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) {
            self.init(rawValue: stringValue)
        }

        var stringValue: String { rawValue }
    }

    public init(
        indexName: String,
        entryCount: Int64,
        distinctKeyCount: Int64 = 0,
        avgEntriesPerKey: Double = 1.0,
        sizeBytes: Int64? = nil,
        timestamp: Date = Date()
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.distinctKeyCount = distinctKeyCount
        self.avgEntriesPerKey = avgEntriesPerKey
        self.sizeBytes = sizeBytes
        self.timestamp = timestamp
    }
}

/// Codable vector statistics for persistence
public struct VectorStatisticsData: Codable, Sendable {
    public let indexName: String
    public let vectorCount: Int64
    public let dimensions: Int
    public let avgL2Norm: Double
    public let stdDevL2Norm: Double
    public let normBuckets: [NormBucketData]?
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case indexName, vectorCount, dimensions, avgL2Norm, stdDevL2Norm, normBuckets, timestamp

        var intValue: Int? {
            switch self {
            case .indexName: return 1
            case .vectorCount: return 2
            case .dimensions: return 3
            case .avgL2Norm: return 4
            case .stdDevL2Norm: return 5
            case .normBuckets: return 6
            case .timestamp: return 7
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .indexName
            case 2: self = .vectorCount
            case 3: self = .dimensions
            case 4: self = .avgL2Norm
            case 5: self = .stdDevL2Norm
            case 6: self = .normBuckets
            case 7: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) { self.init(rawValue: stringValue) }
        var stringValue: String { rawValue }
    }

    public init(
        indexName: String,
        vectorCount: Int64,
        dimensions: Int,
        avgL2Norm: Double = 1.0,
        stdDevL2Norm: Double = 0.1,
        normBuckets: [NormBucketData]? = nil,
        timestamp: Date = Date()
    ) {
        self.indexName = indexName
        self.vectorCount = vectorCount
        self.dimensions = dimensions
        self.avgL2Norm = avgL2Norm
        self.stdDevL2Norm = stdDevL2Norm
        self.normBuckets = normBuckets
        self.timestamp = timestamp
    }

    public struct NormBucketData: Codable, Sendable {
        public let minNorm: Double
        public let maxNorm: Double
        public let count: Int64

        private enum CodingKeys: String, CodingKey {
            case minNorm, maxNorm, count

            var intValue: Int? {
                switch self {
                case .minNorm: return 1
                case .maxNorm: return 2
                case .count: return 3
                }
            }

            init?(intValue: Int) {
                switch intValue {
                case 1: self = .minNorm
                case 2: self = .maxNorm
                case 3: self = .count
                default: return nil
                }
            }

            init?(stringValue: String) { self.init(rawValue: stringValue) }
            var stringValue: String { rawValue }
        }

        public init(minNorm: Double, maxNorm: Double, count: Int64) {
            self.minNorm = minNorm
            self.maxNorm = maxNorm
            self.count = count
        }
    }
}

/// Codable full-text statistics for persistence
public struct FullTextStatisticsData: Codable, Sendable {
    public let indexName: String
    public let totalDocs: Int64
    public let avgDocLength: Double
    public let uniqueTerms: Int64
    public let topTerms: [TermFrequency]?
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case indexName, totalDocs, avgDocLength, uniqueTerms, topTerms, timestamp

        var intValue: Int? {
            switch self {
            case .indexName: return 1
            case .totalDocs: return 2
            case .avgDocLength: return 3
            case .uniqueTerms: return 4
            case .topTerms: return 5
            case .timestamp: return 6
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .indexName
            case 2: self = .totalDocs
            case 3: self = .avgDocLength
            case 4: self = .uniqueTerms
            case 5: self = .topTerms
            case 6: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) { self.init(rawValue: stringValue) }
        var stringValue: String { rawValue }
    }

    public init(
        indexName: String,
        totalDocs: Int64,
        avgDocLength: Double,
        uniqueTerms: Int64,
        topTerms: [TermFrequency]? = nil,
        timestamp: Date = Date()
    ) {
        self.indexName = indexName
        self.totalDocs = totalDocs
        self.avgDocLength = avgDocLength
        self.uniqueTerms = uniqueTerms
        self.topTerms = topTerms
        self.timestamp = timestamp
    }

    public struct TermFrequency: Codable, Sendable {
        public let term: String
        public let docFreq: Int64

        private enum CodingKeys: String, CodingKey {
            case term, docFreq

            var intValue: Int? {
                switch self {
                case .term: return 1
                case .docFreq: return 2
                }
            }

            init?(intValue: Int) {
                switch intValue {
                case 1: self = .term
                case 2: self = .docFreq
                default: return nil
                }
            }

            init?(stringValue: String) { self.init(rawValue: stringValue) }
            var stringValue: String { rawValue }
        }

        public init(term: String, docFreq: Int64) {
            self.term = term
            self.docFreq = docFreq
        }
    }
}

/// Codable spatial statistics for persistence
public struct SpatialStatisticsData: Codable, Sendable {
    public let indexName: String
    public let entryCount: Int64
    public let boundingBox: BoundingBox?
    public let cellCount: Int64
    public let avgCellDensity: Double
    public let hotCells: [UInt64]?
    public let timestamp: Date

    /// Protobuf field numbers
    private enum CodingKeys: String, CodingKey {
        case indexName, entryCount, boundingBox, cellCount, avgCellDensity, hotCells, timestamp

        var intValue: Int? {
            switch self {
            case .indexName: return 1
            case .entryCount: return 2
            case .boundingBox: return 3
            case .cellCount: return 4
            case .avgCellDensity: return 5
            case .hotCells: return 6
            case .timestamp: return 7
            }
        }

        init?(intValue: Int) {
            switch intValue {
            case 1: self = .indexName
            case 2: self = .entryCount
            case 3: self = .boundingBox
            case 4: self = .cellCount
            case 5: self = .avgCellDensity
            case 6: self = .hotCells
            case 7: self = .timestamp
            default: return nil
            }
        }

        init?(stringValue: String) { self.init(rawValue: stringValue) }
        var stringValue: String { rawValue }
    }

    public init(
        indexName: String,
        entryCount: Int64,
        boundingBox: BoundingBox? = nil,
        cellCount: Int64 = 0,
        avgCellDensity: Double = 1.0,
        hotCells: [UInt64]? = nil,
        timestamp: Date = Date()
    ) {
        self.indexName = indexName
        self.entryCount = entryCount
        self.boundingBox = boundingBox
        self.cellCount = cellCount
        self.avgCellDensity = avgCellDensity
        self.hotCells = hotCells
        self.timestamp = timestamp
    }

    public struct BoundingBox: Codable, Sendable {
        public let minLat: Double
        public let minLon: Double
        public let maxLat: Double
        public let maxLon: Double

        private enum CodingKeys: String, CodingKey {
            case minLat, minLon, maxLat, maxLon

            var intValue: Int? {
                switch self {
                case .minLat: return 1
                case .minLon: return 2
                case .maxLat: return 3
                case .maxLon: return 4
                }
            }

            init?(intValue: Int) {
                switch intValue {
                case 1: self = .minLat
                case 2: self = .minLon
                case 3: self = .maxLat
                case 4: self = .maxLon
                default: return nil
                }
            }

            init?(stringValue: String) { self.init(rawValue: stringValue) }
            var stringValue: String { rawValue }
        }

        public init(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
            self.minLat = minLat
            self.minLon = minLon
            self.maxLat = maxLat
            self.maxLon = maxLon
        }
    }
}

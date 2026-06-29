// CostModel.swift
// QueryPlanner - Cost model configuration

import Foundation

/// Configuration for cost estimation
///
/// **Cost Weight Guidelines**:
/// - Weights represent relative costs, not absolute values
/// - Higher weights = more expensive operations to avoid
/// - Tune based on actual workload characteristics
public struct CostModel: Sendable {
    // === Basic I/O Costs ===

    /// Cost weight for reading an index entry
    public var indexReadWeight: Double

    /// Cost weight for fetching a record by primary key
    public var recordFetchWeight: Double

    /// Cost weight for post-filtering a record in memory
    public var postFilterWeight: Double

    /// Cost weight for in-memory sorting (per record)
    public var sortWeight: Double

    // === Range/Scan Costs ===

    /// Cost for initiating a new range scan (FDB range read setup)
    /// Applied once per IndexScan/TableScan operator
    public var rangeInitiationWeight: Double

    // === Union/Intersection Costs ===

    /// Cost for deduplicating results in Union (per result item)
    /// Accounts for hash set operations and memory overhead
    public var deduplicationWeight: Double

    /// Cost for intersection ID set operations (per ID)
    public var intersectionWeight: Double

    /// Additional cost for fetching records after intersection
    /// (records fetched from first child then filtered by intersection)
    public var intersectionFetchWeight: Double

    // === Default Selectivity Estimates ===

    /// Default selectivity for equality conditions (1% of rows)
    public var defaultEqualitySelectivity: Double

    /// Default selectivity for range conditions (30% of rows)
    public var defaultRangeSelectivity: Double

    /// Default selectivity for LIKE/CONTAINS patterns (10% of rows)
    public var defaultPatternSelectivity: Double

    /// Default selectivity for null checks
    public var defaultNullSelectivity: Double

    /// Default selectivity for text search
    public var defaultTextSearchSelectivity: Double

    /// Default selectivity for spatial queries
    public var defaultSpatialSelectivity: Double

    /// Default selectivity for vector search (always returns k results)
    public var defaultVectorSelectivity: Double

    // MARK: - Initialization

    public init(
        indexReadWeight: Double = 1.0,
        recordFetchWeight: Double = 10.0,
        postFilterWeight: Double = 0.1,
        sortWeight: Double = 0.01,
        rangeInitiationWeight: Double = 50.0,
        deduplicationWeight: Double = 0.5,
        intersectionWeight: Double = 0.3,
        intersectionFetchWeight: Double = 2.0,
        defaultEqualitySelectivity: Double = 0.01,
        defaultRangeSelectivity: Double = 0.3,
        defaultPatternSelectivity: Double = 0.1,
        defaultNullSelectivity: Double = 0.05,
        defaultTextSearchSelectivity: Double = 0.05,
        defaultSpatialSelectivity: Double = 0.1,
        defaultVectorSelectivity: Double = 1.0
    ) {
        self.indexReadWeight = indexReadWeight
        self.recordFetchWeight = recordFetchWeight
        self.postFilterWeight = postFilterWeight
        self.sortWeight = sortWeight
        self.rangeInitiationWeight = rangeInitiationWeight
        self.deduplicationWeight = deduplicationWeight
        self.intersectionWeight = intersectionWeight
        self.intersectionFetchWeight = intersectionFetchWeight
        self.defaultEqualitySelectivity = defaultEqualitySelectivity
        self.defaultRangeSelectivity = defaultRangeSelectivity
        self.defaultPatternSelectivity = defaultPatternSelectivity
        self.defaultNullSelectivity = defaultNullSelectivity
        self.defaultTextSearchSelectivity = defaultTextSearchSelectivity
        self.defaultSpatialSelectivity = defaultSpatialSelectivity
        self.defaultVectorSelectivity = defaultVectorSelectivity
    }

    // MARK: - Presets

    /// Default balanced cost model
    public static let `default` = CostModel()

    /// Cost model that favors index usage over table scans
    public static let favorIndexes = CostModel(
        recordFetchWeight: 20.0,
        postFilterWeight: 5.0
    )

    /// Cost model optimized for high-latency distributed environments
    public static let distributed = CostModel(
        rangeInitiationWeight: 100.0,
        deduplicationWeight: 1.0
    )

    /// Cost model for write-heavy workloads (minimize index overhead)
    public static let writeOptimized = CostModel(
        indexReadWeight: 0.5,
        recordFetchWeight: 5.0
    )

    /// Cost model for read-heavy workloads (maximize index usage)
    public static let readOptimized = CostModel(
        indexReadWeight: 0.5,
        recordFetchWeight: 20.0,
        postFilterWeight: 10.0
    )
}

// MARK: - Cost Calculation Helpers

extension CostModel {
    /// Calculate the cost of reading from an index
    public func indexCost(entries: Double, initiation: Bool = true) -> Double {
        let readCost = entries * indexReadWeight
        let initCost = initiation ? rangeInitiationWeight : 0
        return readCost + initCost
    }

    /// Calculate the cost of fetching records
    public func fetchCost(records: Double) -> Double {
        records * recordFetchWeight
    }

    /// Calculate the cost of post-filtering
    public func filterCost(records: Double, selectivity: Double) -> Double {
        records * (1 - selectivity) * postFilterWeight
    }

    /// Calculate the cost of sorting
    public func sortCost(records: Double) -> Double {
        // O(n log n) sorting, simplified as linear for cost estimation
        records * sortWeight * log2(max(2, records))
    }

    /// Calculate the cost of deduplication
    public func dedupCost(records: Double) -> Double {
        records * deduplicationWeight
    }

    /// Calculate the cost of intersection
    ///
    /// **Note**: This is a simplified convenience method. CostEstimator uses a more
    /// detailed calculation that accounts for index reads vs record fetches separately.
    /// Use this method for quick estimates; CostEstimator's implementation is preferred
    /// for accurate query planning.
    public func intersectCost(childRecords: [Double]) -> Double {
        let totalIds = childRecords.reduce(0, +)
        let idSetCost = totalIds * intersectionWeight

        // Estimate intersection result (product of selectivities, simplified)
        let minRecords = childRecords.min() ?? 0
        let intersectionRatio = 0.1 // Heuristic
        let fetchCost = minRecords * intersectionRatio * intersectionFetchWeight

        return idSetCost + fetchCost
    }
}

// MARK: - Description

extension CostModel: CustomStringConvertible {
    public var description: String {
        """
        CostModel(
          indexRead: \(indexReadWeight),
          recordFetch: \(recordFetchWeight),
          postFilter: \(postFilterWeight),
          sort: \(sortWeight),
          rangeInit: \(rangeInitiationWeight),
          dedup: \(deduplicationWeight),
          intersection: \(intersectionWeight)
        )
        """
    }
}

/// QueryCost.swift
/// Query cost estimation types
///
/// Reference:
/// - PostgreSQL Cost Model (src/backend/optimizer/path/costsize.c)
/// - FoundationDB Record Layer Cost Estimation

import Foundation

/// Query execution cost estimate
public struct QueryCost: Sendable, Equatable, Comparable {
    /// Startup cost (one-time cost before first row)
    public let startup: Double

    /// Total cost (including all rows)
    public let total: Double

    /// Estimated number of rows returned
    public let rows: Double

    /// Estimated width of each row in bytes
    public let width: Int

    public init(startup: Double = 0, total: Double, rows: Double = 1, width: Int = 100) {
        self.startup = startup
        self.total = total
        self.rows = rows
        self.width = width
    }

    public static func < (lhs: QueryCost, rhs: QueryCost) -> Bool {
        lhs.total < rhs.total
    }

    /// Zero cost (e.g., for empty results)
    public static let zero = QueryCost(total: 0, rows: 0)

    /// Add two costs
    public static func + (lhs: QueryCost, rhs: QueryCost) -> QueryCost {
        QueryCost(
            startup: lhs.startup + rhs.startup,
            total: lhs.total + rhs.total,
            rows: lhs.rows + rhs.rows,
            width: max(lhs.width, rhs.width)
        )
    }

    /// Multiply cost by a factor
    public static func * (cost: QueryCost, factor: Double) -> QueryCost {
        QueryCost(
            startup: cost.startup,
            total: cost.total * factor,
            rows: cost.rows * factor,
            width: cost.width
        )
    }
}

// MARK: - Cost Model Constants

/// Cost model parameters
public struct CostModel: Sendable {
    /// Cost of sequential page read
    public let seqPageCost: Double

    /// Cost of random page read
    public let randomPageCost: Double

    /// Cost of processing one tuple
    public let cpuTupleCost: Double

    /// Cost of processing one index entry
    public let cpuIndexTupleCost: Double

    /// Cost of evaluating one operator
    public let cpuOperatorCost: Double

    /// Cost of function call
    public let cpuFunctionCost: Double

    /// Network round-trip cost
    public let networkCost: Double

    /// Default cost model
    public static let `default` = CostModel(
        seqPageCost: 1.0,
        randomPageCost: 4.0,
        cpuTupleCost: 0.01,
        cpuIndexTupleCost: 0.005,
        cpuOperatorCost: 0.0025,
        cpuFunctionCost: 0.01,
        networkCost: 10.0
    )

    public init(
        seqPageCost: Double = 1.0,
        randomPageCost: Double = 4.0,
        cpuTupleCost: Double = 0.01,
        cpuIndexTupleCost: Double = 0.005,
        cpuOperatorCost: Double = 0.0025,
        cpuFunctionCost: Double = 0.01,
        networkCost: Double = 10.0
    ) {
        self.seqPageCost = seqPageCost
        self.randomPageCost = randomPageCost
        self.cpuTupleCost = cpuTupleCost
        self.cpuIndexTupleCost = cpuIndexTupleCost
        self.cpuOperatorCost = cpuOperatorCost
        self.cpuFunctionCost = cpuFunctionCost
        self.networkCost = networkCost
    }
}

// MARK: - Cost Estimation

/// Cost estimator
public struct CostEstimator: Sendable {
    public let model: CostModel
    public let statistics: TableStatistics?

    public init(model: CostModel = .default, statistics: TableStatistics? = nil) {
        self.model = model
        self.statistics = statistics
    }

    /// Estimate cost of table scan
    public func tableScanCost(rows: Double, width: Int) -> QueryCost {
        let pages = ceil(rows * Double(width) / 8192.0)  // Assume 8KB pages
        let diskCost = pages * model.seqPageCost
        let cpuCost = rows * model.cpuTupleCost
        return QueryCost(total: diskCost + cpuCost, rows: rows, width: width)
    }

    /// Estimate cost of index scan
    public func indexScanCost(rows: Double, indexRows: Double, width: Int) -> QueryCost {
        let indexPages = ceil(indexRows / 100.0)  // Assume 100 entries per page
        let dataPages = ceil(rows / 10.0)  // Assume clustered factor 10
        let diskCost = indexPages * model.seqPageCost + dataPages * model.randomPageCost
        let cpuCost = indexRows * model.cpuIndexTupleCost + rows * model.cpuTupleCost
        return QueryCost(startup: indexPages * model.seqPageCost, total: diskCost + cpuCost, rows: rows, width: width)
    }

    /// Estimate cost of nested loop join
    public func nestedLoopJoinCost(outer: QueryCost, inner: QueryCost, selectivity: Double) -> QueryCost {
        let startup = outer.startup + inner.startup
        let total = outer.total + outer.rows * inner.total
        let rows = outer.rows * inner.rows * selectivity
        return QueryCost(startup: startup, total: total, rows: rows, width: outer.width + inner.width)
    }

    /// Estimate cost of hash join
    public func hashJoinCost(build: QueryCost, probe: QueryCost, selectivity: Double) -> QueryCost {
        let buildCost = build.total + build.rows * model.cpuOperatorCost  // Hash insertion
        let probeCost = probe.total + probe.rows * model.cpuOperatorCost  // Hash lookup
        let startup = build.total
        let total = buildCost + probeCost
        let rows = build.rows * probe.rows * selectivity
        return QueryCost(startup: startup, total: total, rows: rows, width: build.width + probe.width)
    }

    /// Estimate cost of merge join
    public func mergeJoinCost(left: QueryCost, right: QueryCost, selectivity: Double) -> QueryCost {
        let sortCostLeft = sortCost(left)
        let sortCostRight = sortCost(right)
        let mergeCost = (left.rows + right.rows) * model.cpuOperatorCost
        let startup = sortCostLeft.startup + sortCostRight.startup
        let total = sortCostLeft.total + sortCostRight.total + mergeCost
        let rows = left.rows * right.rows * selectivity
        return QueryCost(startup: startup, total: total, rows: rows, width: left.width + right.width)
    }

    /// Estimate cost of sort
    public func sortCost(_ input: QueryCost) -> QueryCost {
        let comparisons = input.rows * log2(max(input.rows, 2))
        let cpuCost = comparisons * model.cpuOperatorCost
        return QueryCost(startup: input.total + cpuCost, total: input.total + cpuCost, rows: input.rows, width: input.width)
    }

    /// Estimate cost of filter
    public func filterCost(_ input: QueryCost, selectivity: Double) -> QueryCost {
        let cpuCost = input.rows * model.cpuOperatorCost
        let rows = input.rows * selectivity
        return QueryCost(startup: input.startup, total: input.total + cpuCost, rows: rows, width: input.width)
    }

    /// Estimate cost of aggregation
    public func aggregateCost(_ input: QueryCost, groups: Double) -> QueryCost {
        let cpuCost = input.rows * model.cpuOperatorCost
        return QueryCost(startup: input.total + cpuCost, total: input.total + cpuCost, rows: groups, width: input.width)
    }

    /// Estimate cost of distinct
    public func distinctCost(_ input: QueryCost, distinctRows: Double) -> QueryCost {
        let sortedCost = sortCost(input)
        let cpuCost = input.rows * model.cpuOperatorCost
        return QueryCost(startup: sortedCost.startup, total: sortedCost.total + cpuCost, rows: distinctRows, width: input.width)
    }

    /// Estimate cost of limit
    public func limitCost(_ input: QueryCost, limit: Int, offset: Int) -> QueryCost {
        let rowsNeeded = Double(limit + offset)
        let fraction = min(rowsNeeded / max(input.rows, 1), 1.0)
        return QueryCost(
            startup: input.startup,
            total: input.startup + (input.total - input.startup) * fraction,
            rows: Double(limit),
            width: input.width
        )
    }

    /// Estimate cost of union
    public func unionCost(_ inputs: [QueryCost]) -> QueryCost {
        var total: Double = 0
        var rows: Double = 0
        var maxWidth = 0
        for input in inputs {
            total += input.total
            rows += input.rows
            maxWidth = max(maxWidth, input.width)
        }
        return QueryCost(total: total, rows: rows, width: maxWidth)
    }

    /// Estimate cost of vector search (HNSW)
    public func vectorSearchCost(k: Int, dimensions: Int, datasetSize: Double) -> QueryCost {
        // HNSW search is O(log N * M * efSearch) where M is the max connections
        let comparisons = log2(max(datasetSize, 2)) * Double(k) * 10.0
        let cpuCost = comparisons * Double(dimensions) * model.cpuOperatorCost
        return QueryCost(total: cpuCost, rows: Double(k), width: dimensions * 8)
    }

    /// Estimate cost of full-text search
    public func fullTextSearchCost(estimatedMatches: Double, documentCount: Double) -> QueryCost {
        let indexLookupCost = log2(max(documentCount, 2)) * model.randomPageCost
        let matchCost = estimatedMatches * model.cpuTupleCost
        return QueryCost(startup: indexLookupCost, total: indexLookupCost + matchCost, rows: estimatedMatches)
    }
}

// MARK: - Table Statistics

/// Statistics for cost estimation
public struct TableStatistics: Sendable {
    /// Total number of rows
    public let rowCount: Int64

    /// Total size in bytes
    public let totalBytes: Int64

    /// Average row width
    public let avgWidth: Int

    /// Number of distinct values per column
    public let distinctCounts: [String: Int64]

    /// Histogram buckets per column (for selectivity estimation)
    public let histograms: [String: [HistogramBucket]]

    /// Most common values per column
    public let mostCommonValues: [String: [(Literal, Double)]]

    /// Correlation (clustering) per column
    public let correlations: [String: Double]

    public init(
        rowCount: Int64,
        totalBytes: Int64,
        avgWidth: Int,
        distinctCounts: [String: Int64] = [:],
        histograms: [String: [HistogramBucket]] = [:],
        mostCommonValues: [String: [(Literal, Double)]] = [:],
        correlations: [String: Double] = [:]
    ) {
        self.rowCount = rowCount
        self.totalBytes = totalBytes
        self.avgWidth = avgWidth
        self.distinctCounts = distinctCounts
        self.histograms = histograms
        self.mostCommonValues = mostCommonValues
        self.correlations = correlations
    }
}

/// Histogram bucket for selectivity estimation
public struct HistogramBucket: Sendable {
    public let lowerBound: Literal
    public let upperBound: Literal
    public let frequency: Double
    public let distinctCount: Int

    public init(lowerBound: Literal, upperBound: Literal, frequency: Double, distinctCount: Int) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.frequency = frequency
        self.distinctCount = distinctCount
    }
}

// MARK: - Plan Statistics

/// Statistics collected during planning
public struct PlanStatistics: Sendable {
    /// Planning time in milliseconds
    public let planningTimeMs: Double

    /// Number of plan alternatives considered
    public let alternativesConsidered: Int

    /// Estimated execution time in milliseconds
    public let estimatedExecutionTimeMs: Double?

    /// Memory estimate in bytes
    public let estimatedMemoryBytes: Int64?

    public init(
        planningTimeMs: Double,
        alternativesConsidered: Int,
        estimatedExecutionTimeMs: Double? = nil,
        estimatedMemoryBytes: Int64? = nil
    ) {
        self.planningTimeMs = planningTimeMs
        self.alternativesConsidered = alternativesConsidered
        self.estimatedExecutionTimeMs = estimatedExecutionTimeMs
        self.estimatedMemoryBytes = estimatedMemoryBytes
    }
}

// MARK: - Index Usage

/// Index usage information
public struct IndexUsage: Sendable, Equatable {
    /// Index name
    public let indexName: String

    /// Index kind
    public let kind: IndexKind

    /// Access pattern
    public let accessPattern: IndexAccessPattern

    /// Estimated selectivity (0.0 to 1.0)
    public let selectivity: Double

    public init(
        indexName: String,
        kind: IndexKind,
        accessPattern: IndexAccessPattern,
        selectivity: Double = 1.0
    ) {
        self.indexName = indexName
        self.kind = kind
        self.accessPattern = accessPattern
        self.selectivity = selectivity
    }
}

/// Index kind
public enum IndexKind: String, Sendable, Equatable {
    case scalar
    case vector
    case fullText
    case spatial
    case graph
    case bitmap
    case composite
    case unique
}

/// Index access pattern
public enum IndexAccessPattern: Sendable, Equatable {
    /// Exact key lookup
    case exactMatch

    /// Range scan
    case rangeScan(direction: ScanDirection)

    /// Prefix scan
    case prefixScan

    /// Full scan
    case fullScan

    /// Nearest neighbor search
    case nearestNeighbor(k: Int)

    /// Contains/intersects check
    case containsCheck
}

/// Scan direction
public enum ScanDirection: Sendable, Equatable {
    case forward
    case backward
}

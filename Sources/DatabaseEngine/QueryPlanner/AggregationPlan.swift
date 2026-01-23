// AggregationPlan.swift
// QueryPlanner - Aggregation query planning

import Foundation
import Core
import FoundationDB

/// Specification for an aggregation operation
public struct AggregationSpec: Sendable, Hashable {
    /// The aggregation type (COUNT, SUM, etc.)
    public let type: AggregationType

    /// Optional alias for the result column
    public let alias: String?

    public init(type: AggregationType, alias: String? = nil) {
        self.type = type
        self.alias = alias
    }
}

/// Cost estimate for an aggregation plan
public struct AggregationPlanCost: Sendable {
    /// Whether the aggregation is backed by a pre-computed index (O(1))
    public let isIndexBacked: Bool

    /// Estimated number of rows to scan (0 if index-backed)
    public let estimatedRowsToScan: Int

    /// Estimated number of groups (for GROUP BY queries)
    public let estimatedGroups: Int

    /// I/O cost estimate
    public let ioCost: Double

    /// CPU cost estimate
    public let cpuCost: Double

    /// Total cost (I/O + CPU)
    public var totalCost: Double {
        ioCost + cpuCost
    }

    public init(
        isIndexBacked: Bool,
        estimatedRowsToScan: Int = 0,
        estimatedGroups: Int = 1,
        ioCost: Double = 0,
        cpuCost: Double = 0
    ) {
        self.isIndexBacked = isIndexBacked
        self.estimatedRowsToScan = estimatedRowsToScan
        self.estimatedGroups = estimatedGroups
        self.ioCost = ioCost
        self.cpuCost = cpuCost
    }

    /// Index-backed constant-time lookup
    public static let indexBacked = AggregationPlanCost(
        isIndexBacked: true,
        estimatedRowsToScan: 0,
        estimatedGroups: 1,
        ioCost: 1.0,
        cpuCost: 0.1
    )

    /// Full table scan required
    public static func tableScan(estimatedRows: Int) -> AggregationPlanCost {
        AggregationPlanCost(
            isIndexBacked: false,
            estimatedRowsToScan: estimatedRows,
            estimatedGroups: 1,
            ioCost: Double(estimatedRows) * 0.1,
            cpuCost: Double(estimatedRows) * 0.01
        )
    }
}

/// Execution strategy for an aggregation
public enum AggregationStrategy: Sendable {
    /// Use pre-computed aggregation index (O(1) lookup)
    case indexLookup(indexName: String)

    /// Scan and compute on-the-fly
    case scanAndCompute

    /// Use HyperLogLog for approximate distinct count
    case hyperLogLog(indexName: String)

    /// Use percentile index
    case percentileIndex(indexName: String)
}

/// Plan for executing an aggregation query
///
/// Aggregation queries differ from record queries in that they return
/// computed scalar values rather than records.
///
/// **Supported Index Types**:
/// - `SumIndexKind`, `MinMaxIndexKind`: O(1) lookup for SUM/MIN/MAX
/// - `DistinctIndexKind`: O(1) approximate COUNT DISTINCT via HyperLogLog
/// - `PercentileIndexKind`: O(1) percentile queries via T-Digest
///
/// **GROUP BY Support**:
/// When `groupByFields` is non-empty, the aggregation is computed per group.
public struct AggregationPlan<T: Persistable>: Sendable {
    /// The aggregations to compute
    public let aggregations: [AggregationSpec]

    /// Fields to group by (empty for global aggregation)
    public let groupByFields: [String]

    /// Execution strategies for each aggregation
    public let strategies: [AggregationStrategy]

    /// Available aggregation indexes that can be used
    public let availableIndexes: [IndexDescriptor]

    /// Estimated cost of this plan
    public let estimatedCost: AggregationPlanCost

    /// Optional filter predicate (WHERE clause)
    public let filterPredicate: Predicate<T>?

    public init(
        aggregations: [AggregationSpec],
        groupByFields: [String] = [],
        strategies: [AggregationStrategy],
        availableIndexes: [IndexDescriptor] = [],
        estimatedCost: AggregationPlanCost,
        filterPredicate: Predicate<T>? = nil
    ) {
        self.aggregations = aggregations
        self.groupByFields = groupByFields
        self.strategies = strategies
        self.availableIndexes = availableIndexes
        self.estimatedCost = estimatedCost
        self.filterPredicate = filterPredicate
    }
}

// Note: AggregationResult is defined in AggregationExecution.swift
// This file only defines plan-related types

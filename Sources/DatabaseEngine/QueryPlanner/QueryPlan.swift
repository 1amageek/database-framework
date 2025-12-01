// QueryPlan.swift
// QueryPlanner - Query execution plan representation

import Foundation
import Core

/// Represents an executable query plan
public struct QueryPlan<T: Persistable>: @unchecked Sendable {
    /// Unique identifier for this plan
    public let id: UUID

    /// Root operator of the plan tree
    public let rootOperator: PlanOperator<T>

    /// Estimated cost metrics
    public let estimatedCost: PlanCost

    /// Fields used in the plan
    public let usedFields: Set<String>

    /// Indexes used in the plan
    public let usedIndexes: [IndexDescriptor]

    /// Whether ordering is satisfied by the plan
    public let orderingSatisfied: Bool

    /// Post-filter predicate (if any conditions couldn't use indexes)
    public let postFilterPredicate: Predicate<T>?

    /// Create a new query plan
    public init(
        id: UUID = UUID(),
        rootOperator: PlanOperator<T>,
        estimatedCost: PlanCost,
        usedFields: Set<String>,
        usedIndexes: [IndexDescriptor],
        orderingSatisfied: Bool,
        postFilterPredicate: Predicate<T>? = nil
    ) {
        self.id = id
        self.rootOperator = rootOperator
        self.estimatedCost = estimatedCost
        self.usedFields = usedFields
        self.usedIndexes = usedIndexes
        self.orderingSatisfied = orderingSatisfied
        self.postFilterPredicate = postFilterPredicate
    }

    /// Human-readable explanation
    public var explanation: String {
        PlanExplanation(plan: self).description
    }
}

// MARK: - Plan Cost

/// Cost metrics for a query plan
public struct PlanCost: Sendable {
    /// Estimated number of index entries to read
    public let indexReads: Double

    /// Estimated number of records to fetch
    public let recordFetches: Double

    /// Estimated number of records to post-filter
    public let postFilterCount: Double

    /// Whether in-memory sorting is required
    public let requiresSort: Bool

    /// Pre-weighted additional costs (deduplication, intersection, range initiation, etc.)
    ///
    /// These costs have already been weighted by their respective weights in `CostModel`
    /// and should NOT be multiplied by any additional weight in `totalCost`.
    ///
    /// Examples of costs stored here:
    /// - Deduplication: `records * deduplicationWeight`
    /// - Intersection ID operations: `ids * intersectionWeight`
    /// - Range initiation: `count * rangeInitiationWeight`
    public let additionalCost: Double

    /// Cost model used for weight calculation
    private let costModel: CostModel

    /// Create plan cost with custom model
    public init(
        indexReads: Double,
        recordFetches: Double,
        postFilterCount: Double,
        requiresSort: Bool,
        additionalCost: Double = 0,
        costModel: CostModel = .default
    ) {
        self.indexReads = indexReads
        self.recordFetches = recordFetches
        self.postFilterCount = postFilterCount
        self.requiresSort = requiresSort
        self.additionalCost = additionalCost
        self.costModel = costModel
    }

    /// Total estimated cost (weighted sum)
    public var totalCost: Double {
        let indexReadCost = indexReads * costModel.indexReadWeight
        let recordFetchCost = recordFetches * costModel.recordFetchWeight
        let postFilterCost = postFilterCount * costModel.postFilterWeight
        let sortCost = requiresSort ? (recordFetches * costModel.sortWeight) : 0
        // additionalCost is already pre-weighted, add directly
        return indexReadCost + recordFetchCost + postFilterCost + sortCost + additionalCost
    }
}

// MARK: - Comparable

extension PlanCost: Comparable {
    public static func == (lhs: PlanCost, rhs: PlanCost) -> Bool {
        lhs.totalCost == rhs.totalCost
    }

    public static func < (lhs: PlanCost, rhs: PlanCost) -> Bool {
        lhs.totalCost < rhs.totalCost
    }

    /// Zero cost (for empty plans)
    public static var zero: PlanCost {
        PlanCost(indexReads: 0, recordFetches: 0, postFilterCount: 0, requiresSort: false, additionalCost: 0)
    }

    /// Add two costs
    public static func + (lhs: PlanCost, rhs: PlanCost) -> PlanCost {
        PlanCost(
            indexReads: lhs.indexReads + rhs.indexReads,
            recordFetches: lhs.recordFetches + rhs.recordFetches,
            postFilterCount: lhs.postFilterCount + rhs.postFilterCount,
            requiresSort: lhs.requiresSort || rhs.requiresSort,
            additionalCost: lhs.additionalCost + rhs.additionalCost,
            costModel: lhs.costModel
        )
    }
}

// MARK: - Query Hints

/// Hints to influence query planning
public struct QueryHints: Sendable {
    /// Prefer using this index
    public var preferredIndex: String?

    /// Force table scan instead of index
    public var forceTableScan: Bool

    /// Maximum cost before falling back to scan
    public var maxIndexCost: Double?

    /// Enable/disable specific optimizations
    public var disabledOptimizations: Set<String>

    public init(
        preferredIndex: String? = nil,
        forceTableScan: Bool = false,
        maxIndexCost: Double? = nil,
        disabledOptimizations: Set<String> = []
    ) {
        self.preferredIndex = preferredIndex
        self.forceTableScan = forceTableScan
        self.maxIndexCost = maxIndexCost
        self.disabledOptimizations = disabledOptimizations
    }

    /// Default hints (no modifications)
    public static let `default` = QueryHints()
}

// MARK: - Plan Description

extension PlanOperator: CustomStringConvertible {
    public var description: String {
        switch self {
        case .tableScan(let op):
            return "TableScan(rows: \(op.estimatedRows))"
        case .indexScan(let op):
            return "IndexScan(\(op.index.name), entries: \(op.estimatedEntries))"
        case .indexSeek(let op):
            return "IndexSeek(\(op.index.name), keys: \(op.seekValues.count))"
        case .union(let op):
            return "Union(\(op.children.count) children, dedup: \(op.deduplicate))"
        case .intersection(let op):
            return "Intersection(\(op.children.count) children)"
        case .filter(let op):
            return "Filter(selectivity: \(String(format: "%.2f", op.selectivity)))"
        case .sort(let op):
            let fields = op.sortDescriptors.map { $0.fieldName }
            return "Sort(\(fields.joined(separator: ", ")))"
        case .limit(let op):
            return "Limit(\(op.limit ?? -1), offset: \(op.offset ?? 0))"
        case .project(let op):
            return "Project(\(op.fields.count) fields)"
        case .fullTextScan(let op):
            return "FullTextScan(\(op.index.name), terms: \(op.searchTerms.count))"
        case .vectorSearch(let op):
            return "VectorSearch(\(op.index.name), k: \(op.k))"
        case .spatialScan(let op):
            return "SpatialScan(\(op.index.name))"
        case .aggregation(let op):
            return "Aggregation(\(op.index.name), type: \(op.aggregationType))"
        }
    }
}

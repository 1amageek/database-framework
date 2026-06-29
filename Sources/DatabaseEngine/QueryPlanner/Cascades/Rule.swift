// Rule.swift
// Cascades Optimizer - Rule-based transformation and implementation
//
// Rules transform logical expressions into equivalent logical expressions
// (transformation rules) or into physical implementations (implementation rules).
//
// Reference: Graefe, G. "The Cascades Framework for Query Optimization", 1995

import Foundation
import Core

// MARK: - Rule Protocol

/// Base protocol for Cascades optimization rules
public protocol CascadesRule: Sendable {
    /// Rule name for debugging
    var name: String { get }

    /// Pattern to match (describes which expressions this rule applies to)
    var pattern: RulePattern { get }

    /// Promise: estimated benefit of applying this rule
    /// Higher values are explored first
    var promise: Int { get }
}

/// Pattern for matching expressions
public indirect enum RulePattern: Sendable, Equatable {
    /// Match any expression
    case any

    /// Match a specific operator type
    case scan
    case filter(child: RulePattern)
    case project(child: RulePattern)
    case join(left: RulePattern, right: RulePattern)
    case union(children: RulePattern)
    case intersection(children: RulePattern)
    case sort(child: RulePattern)
    case limit(child: RulePattern)
    case aggregate(child: RulePattern)
    case indexScan

    /// Match logical filter on scan
    case filterOnScan

    /// Match logical filter on index scan
    case filterOnIndexScan
}

// MARK: - Transformation Rule

/// Transformation rules generate equivalent logical expressions
///
/// These rules don't change the semantics, just the representation.
/// Examples: push-down predicates, join reordering, etc.
public protocol TransformationRule: CascadesRule {
    /// Apply the rule to generate new equivalent expressions
    ///
    /// - Parameters:
    ///   - expression: The matched expression
    ///   - memo: The memo structure
    /// - Returns: New equivalent logical operators (added to same group)
    func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator]
}

// MARK: - Implementation Rule

/// Implementation rules convert logical operators to physical operators
///
/// These rules choose specific algorithms for execution.
/// Cost is estimated for each physical operator.
public protocol ImplementationRule: CascadesRule {
    /// Apply the rule to generate physical implementations
    ///
    /// - Parameters:
    ///   - expression: The matched logical expression
    ///   - requiredProperties: Properties required by parent
    ///   - memo: The memo structure
    ///   - context: Optimization context with statistics
    /// - Returns: Physical operators with their costs
    func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)]
}

// MARK: - Optimization Context

/// Context for Cascades optimization with schema and statistics
public struct CascadesOptimizationContext: Sendable {
    /// Available indexes
    public let indexes: [Index]

    /// Table statistics (cardinality, histograms, etc.)
    public let statistics: CascadesTableStatistics

    /// Cost model parameters
    public let costModel: CascadesCostModel

    public init(
        indexes: [Index] = [],
        statistics: CascadesTableStatistics = CascadesTableStatistics(),
        costModel: CascadesCostModel = CascadesCostModel()
    ) {
        self.indexes = indexes
        self.statistics = statistics
        self.costModel = costModel
    }

    /// Find indexes for a given type
    public func indexesFor(type: String) -> [Index] {
        indexes.filter { index in
            // If itemTypes is nil, index applies to all types
            // Otherwise, check if the type is in the itemTypes set
            guard let itemTypes = index.itemTypes else { return true }
            return itemTypes.contains(type)
        }
    }
}

/// Table statistics for Cascades cost estimation
public struct CascadesTableStatistics: Sendable {
    /// Row counts per table
    public var rowCounts: [String: Int]

    /// Column statistics
    public var columnStats: [String: CascadesColumnStatistics]

    public init(
        rowCounts: [String: Int] = [:],
        columnStats: [String: CascadesColumnStatistics] = [:]
    ) {
        self.rowCounts = rowCounts
        self.columnStats = columnStats
    }
}

/// Column-level statistics for Cascades
public struct CascadesColumnStatistics: Sendable {
    /// Number of distinct values
    public let distinctCount: Int

    /// Null fraction (0.0 to 1.0)
    public let nullFraction: Double

    /// Histogram buckets for range estimation
    public let histogram: [FieldValue]?

    /// Most common values and their frequencies
    public let mostCommonValues: [(FieldValue, Double)]?

    public init(
        distinctCount: Int = 100,
        nullFraction: Double = 0.0,
        histogram: [FieldValue]? = nil,
        mostCommonValues: [(FieldValue, Double)]? = nil
    ) {
        self.distinctCount = distinctCount
        self.nullFraction = nullFraction
        self.histogram = histogram
        self.mostCommonValues = mostCommonValues
    }
}

/// Cost model parameters for Cascades optimizer
///
/// Uses PostgreSQL-like cost parameters for estimation.
/// Reference: PostgreSQL src/backend/optimizer/path/costsize.c
public struct CascadesCostModel: Sendable {
    /// Cost per sequential I/O page
    public let seqPageCost: Double

    /// Cost per random I/O page
    public let randomPageCost: Double

    /// Cost per CPU operation
    public let cpuOperatorCost: Double

    /// Cost per tuple processed
    public let cpuTupleCost: Double

    /// Cost per index tuple
    public let cpuIndexTupleCost: Double

    /// Page size in bytes
    public let pageSize: Int

    // MARK: - Merge-Sort Operation Costs

    /// Cost per key comparison in merge operations
    /// Reference: FDB Record Layer RecordQueryUnionPlan cost model
    public let mergeComparisonWeight: Double

    /// Cost per heap operation (insert/extract) in K-way merge
    public let heapOperationWeight: Double

    /// Maximum number of children before merge-sort becomes less attractive
    /// Above this threshold, hash-based operators may be preferred
    public let mergeSortChildThreshold: Int

    /// Default values based on PostgreSQL
    public init(
        seqPageCost: Double = 1.0,
        randomPageCost: Double = 4.0,
        cpuOperatorCost: Double = 0.0025,
        cpuTupleCost: Double = 0.01,
        cpuIndexTupleCost: Double = 0.005,
        pageSize: Int = 8192,
        mergeComparisonWeight: Double = 0.02,
        heapOperationWeight: Double = 0.05,
        mergeSortChildThreshold: Int = 10
    ) {
        self.seqPageCost = seqPageCost
        self.randomPageCost = randomPageCost
        self.cpuOperatorCost = cpuOperatorCost
        self.cpuTupleCost = cpuTupleCost
        self.cpuIndexTupleCost = cpuIndexTupleCost
        self.pageSize = pageSize
        self.mergeComparisonWeight = mergeComparisonWeight
        self.heapOperationWeight = heapOperationWeight
        self.mergeSortChildThreshold = mergeSortChildThreshold
    }
}

// MARK: - Built-in Transformation Rules

/// Push filter below project
public struct FilterPushDownRule: TransformationRule {
    public let name = "FilterPushDown"
    public let pattern = RulePattern.filter(child: .project(child: .any))
    public let promise = 10

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let projectGroup, let predicate) = logicalOp else {
            return []
        }

        // Get the project expression
        guard let projectExpr = memo.getLogicalExpressions(projectGroup).first,
              case .logical(let projectOp) = projectExpr.op,
              case .project(let inputGroup, let fields) = projectOp else {
            return []
        }

        // Push filter below project
        let newFilter = LogicalOperator.filter(input: inputGroup, predicate: predicate)
        let newFilterGroup = memo.addLogicalExpression(newFilter)
        let newProject = LogicalOperator.project(input: newFilterGroup, fields: fields)

        return [newProject]
    }
}

/// Convert filter + scan to index scan when applicable
public struct FilterToIndexScanRule: TransformationRule {
    public let name = "FilterToIndexScan"
    public let pattern = RulePattern.filterOnScan
    public let promise = 20

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        // This rule generates IndexScan hints that will be implemented later
        // The implementation rule will check for available indexes
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let scanGroup, let predicate) = logicalOp else {
            return []
        }

        guard let scanExpr = memo.getLogicalExpressions(scanGroup).first,
              case .logical(let scanOp) = scanExpr.op,
              case .scan(let typeName) = scanOp else {
            return []
        }

        // Extract bounds from predicate
        let bounds = extractBounds(from: predicate)

        // Generate index scan alternatives (will be validated in implementation)
        return [LogicalOperator.indexScan(typeName: typeName, indexName: "", bounds: bounds)]
    }

    private func extractBounds(from predicate: PredicateExpr) -> IndexBoundsExpr? {
        switch predicate {
        case .comparison(_, let op, let value):
            switch op {
            case .eq:
                return IndexBoundsExpr(
                    lowerBound: [value],
                    lowerInclusive: true,
                    upperBound: [value],
                    upperInclusive: true
                )
            case .lt:
                return IndexBoundsExpr(upperBound: [value], upperInclusive: false)
            case .le:
                return IndexBoundsExpr(upperBound: [value], upperInclusive: true)
            case .gt:
                return IndexBoundsExpr(lowerBound: [value], lowerInclusive: false)
            case .ge:
                return IndexBoundsExpr(lowerBound: [value], lowerInclusive: true)
            default:
                return nil
            }
        default:
            return nil
        }
    }
}

/// Join commutativity: A JOIN B = B JOIN A
public struct JoinCommutativityRule: TransformationRule {
    public let name = "JoinCommutativity"
    public let pattern = RulePattern.join(left: .any, right: .any)
    public let promise = 5

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .join(let left, let right, let condition, let joinType) = logicalOp else {
            return []
        }

        // Only commute inner joins
        guard joinType == .inner else { return [] }

        return [LogicalOperator.join(left: right, right: left, condition: condition, type: joinType)]
    }
}

// MARK: - Built-in Implementation Rules

/// Implement scan as sequential scan
public struct SeqScanImplementationRule: ImplementationRule {
    public let name = "SeqScanImpl"
    public let pattern = RulePattern.scan
    public let promise = 1

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .scan(let typeName) = logicalOp else {
            return []
        }

        // Estimate cost
        let rowCount = Double(context.statistics.rowCounts[typeName] ?? 1000)
        let cost = rowCount * context.costModel.cpuTupleCost +
                   (rowCount / Double(context.costModel.pageSize)) * context.costModel.seqPageCost

        return [(.seqScan(typeName: typeName, filter: nil), cost)]
    }
}

/// Implement filter as physical filter
public struct FilterImplementationRule: ImplementationRule {
    public let name = "FilterImpl"
    public let pattern = RulePattern.filter(child: .any)
    public let promise = 1

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let input, let predicate) = logicalOp else {
            return []
        }

        // Estimate selectivity (simple heuristic)
        let selectivity = estimateSelectivity(predicate)

        // Base cost for filter
        let cost = selectivity * context.costModel.cpuTupleCost

        return [(.filter(input: input, predicate: predicate), cost)]
    }

    private func estimateSelectivity(_ predicate: PredicateExpr) -> Double {
        switch predicate {
        case .comparison(_, let op, _):
            switch op {
            case .eq: return 0.01  // 1% selectivity for equality
            case .lt, .le, .gt, .ge: return 0.33  // 33% for range
            default: return 0.5
            }
        case .and(let predicates):
            return predicates.reduce(1.0) { $0 * estimateSelectivity($1) }
        case .or(let predicates):
            return min(1.0, predicates.reduce(0.0) { $0 + estimateSelectivity($1) })
        case .not(let inner):
            return 1.0 - estimateSelectivity(inner)
        case .isNull: return 0.01
        case .isNotNull: return 0.99
        case .true: return 1.0
        case .false: return 0.0
        }
    }
}

/// Implement index scan using available indexes
public struct IndexScanImplementationRule: ImplementationRule {
    public let name = "IndexScanImpl"
    public let pattern = RulePattern.indexScan
    public let promise = 10

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .indexScan(let typeName, _, let bounds) = logicalOp else {
            return []
        }

        var results: [(PhysicalOperator, Double)] = []

        // Try each available index
        for index in context.indexesFor(type: typeName) {
            // Estimate index scan cost
            let rowCount = Double(context.statistics.rowCounts[typeName] ?? 1000)
            let selectivity = estimateIndexSelectivity(bounds)
            let indexRows = rowCount * selectivity

            // Index scan cost: random I/O for index + sequential for heap
            let indexCost = indexRows * context.costModel.cpuIndexTupleCost +
                           (indexRows / 100) * context.costModel.randomPageCost

            let op = PhysicalOperator.indexScan(
                typeName: typeName,
                indexName: index.name,
                bounds: bounds,
                filter: nil
            )

            results.append((op, indexCost))
        }

        return results
    }

    private func estimateIndexSelectivity(_ bounds: IndexBoundsExpr?) -> Double {
        guard let bounds = bounds else { return 1.0 }

        if bounds.lowerBound == bounds.upperBound && bounds.lowerBound != nil {
            return 0.01  // Point lookup
        }

        return 0.33  // Range scan
    }
}

/// Implement sort
public struct SortImplementationRule: ImplementationRule {
    public let name = "SortImpl"
    public let pattern = RulePattern.sort(child: .any)
    public let promise = 1

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .sort(let input, let keys) = logicalOp else {
            return []
        }

        // Estimate sort cost (N log N)
        let cardinality = 1000.0  // Would get from group statistics
        let cost = cardinality * log2(max(2, cardinality)) * context.costModel.cpuOperatorCost

        return [(.sort(input: input, keys: keys, limit: nil), cost)]
    }
}

/// Implement join as hash join
public struct HashJoinImplementationRule: ImplementationRule {
    public let name = "HashJoinImpl"
    public let pattern = RulePattern.join(left: .any, right: .any)
    public let promise = 5

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .join(let left, let right, let condition, let joinType) = logicalOp else {
            return []
        }

        // Extract join keys from condition
        let (leftKeys, rightKeys) = extractJoinKeys(condition)

        guard !leftKeys.isEmpty else { return [] }

        // Estimate hash join cost
        // Build cost + probe cost
        let leftCard = 1000.0  // Would get from statistics
        let rightCard = 1000.0
        let buildCost = leftCard * context.costModel.cpuTupleCost
        let probeCost = rightCard * context.costModel.cpuTupleCost
        let totalCost = buildCost + probeCost

        return [(.hashJoin(
            build: left,
            probe: right,
            buildKeys: leftKeys,
            probeKeys: rightKeys,
            type: joinType
        ), totalCost)]
    }

    private func extractJoinKeys(_ condition: PredicateExpr) -> ([String], [String]) {
        switch condition {
        case .comparison(let field, .eq, _):
            // Simplified: assume field is in format "left.field = right.field"
            return ([field], [field])
        case .and(let predicates):
            var leftKeys: [String] = []
            var rightKeys: [String] = []
            for pred in predicates {
                let (l, r) = extractJoinKeys(pred)
                leftKeys.append(contentsOf: l)
                rightKeys.append(contentsOf: r)
            }
            return (leftKeys, rightKeys)
        default:
            return ([], [])
        }
    }
}

// MARK: - Union/Intersection Implementation Rules

/// Implement union as hash union (with deduplication)
public struct HashUnionImplementationRule: ImplementationRule {
    public let name = "HashUnionImpl"
    public let pattern = RulePattern.union(children: .any)
    public let promise = 5

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .union(let inputs, let deduplicate) = logicalOp else {
            return []
        }

        if deduplicate {
            // Hash union with deduplication
            // Cost: build hash table + probe
            let totalCard = Double(inputs.count) * 1000.0
            let cost = totalCard * context.costModel.cpuTupleCost * 1.5

            return [(.hashUnion(inputs: inputs), cost)]
        } else {
            // Simple concatenation
            let cost = Double(inputs.count) * context.costModel.cpuTupleCost

            return [(.unionAll(inputs: inputs), cost)]
        }
    }
}

/// Implement union as merge-sort union (for sorted inputs)
///
/// This is preferred when:
/// - Inputs are already sorted by the same key
/// - Result needs to maintain sort order
/// - Deduplication is needed
///
/// **Time**: O(N log K) where N = total elements, K = number of inputs
/// **Space**: O(K) for the heap
///
/// **Reference**: FDB Record Layer RecordQueryUnionPlan
public struct MergeSortUnionImplementationRule: ImplementationRule {
    public let name = "MergeSortUnionImpl"
    public let pattern = RulePattern.union(children: .any)
    public let promise = 15  // Higher than hash union when applicable

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .union(let inputs, let deduplicate) = logicalOp else {
            return []
        }

        // Skip if too many children (hash may be better)
        if inputs.count > context.costModel.mergeSortChildThreshold {
            return []
        }

        // Generate merge-sort union unconditionally with a default sort key.
        // The optimizer's satisfiesProperties will filter if sort order doesn't match.
        // If requiredProperties has a sortOrder, use it; otherwise use a placeholder.
        // The key insight: merge-sort union produces sorted output by its key,
        // so it only makes sense when sorted output is needed.
        let sortKeys: [SortKeyExpr]
        if let sortOrder = requiredProperties.sortOrder, !sortOrder.isEmpty {
            sortKeys = sortOrder
        } else {
            // Generate with a default primary key sort - actual usage will be
            // filtered by satisfiesProperties if sort doesn't match requirements
            sortKeys = [SortKeyExpr(field: "_id", ascending: true)]
        }

        // Merge-sort union cost:
        // N * log(K) comparisons for K-way merge
        // Add sort cost for children if they aren't already sorted
        let k = Double(inputs.count)
        let totalCard = k * 1000.0  // Simplified cardinality estimate
        let mergeComparisonCost = totalCard * log2(max(2, k)) * context.costModel.cpuOperatorCost * context.costModel.mergeComparisonWeight
        let heapOperationCost = totalCard * context.costModel.cpuOperatorCost * context.costModel.heapOperationWeight
        // Add estimated child sort cost (will be refined by child optimization)
        let childSortCost = totalCard * log2(max(2, totalCard / k)) * context.costModel.cpuOperatorCost * 0.01

        let cost = mergeComparisonCost + heapOperationCost + childSortCost

        return [(.mergeSortUnion(inputs: inputs, keys: sortKeys, deduplicate: deduplicate), cost)]
    }
}

/// Implement intersection as hash intersection
public struct HashIntersectionImplementationRule: ImplementationRule {
    public let name = "HashIntersectionImpl"
    public let pattern = RulePattern.intersection(children: .any)
    public let promise = 5

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .intersection(let inputs) = logicalOp else {
            return []
        }

        // Hash intersection cost:
        // Build hash table from first input, probe with others
        let firstCard = 1000.0
        let totalCard = Double(inputs.count) * 1000.0
        let buildCost = firstCard * context.costModel.cpuTupleCost
        let probeCost = (totalCard - firstCard) * context.costModel.cpuTupleCost * 0.5

        let cost = buildCost + probeCost

        return [(.hashIntersection(inputs: inputs), cost)]
    }
}

/// Implement intersection as merge-sort intersection (for sorted inputs)
///
/// Skip-ahead optimization for sorted streams.
/// Uses smallest stream as driver and seeks in others.
///
/// Preferred when:
/// - Inputs are sorted by the same key
/// - Inputs have varying cardinalities (skip-ahead helps)
/// - Result needs to maintain sort order
///
/// **Time**: O(N * K) where N = smallest stream size
/// **Space**: O(K) for iterators
///
/// **Reference**: FDB Record Layer RecordQueryIntersectionPlan
public struct MergeSortIntersectionImplementationRule: ImplementationRule {
    public let name = "MergeSortIntersectionImpl"
    public let pattern = RulePattern.intersection(children: .any)
    public let promise = 15  // Higher than hash intersection when applicable

    public init() {}

    public func apply(
        to expression: MemoExpression,
        requiredProperties: PropertySet,
        memo: Memo,
        context: CascadesOptimizationContext
    ) -> [(PhysicalOperator, Double)] {
        guard case .logical(let logicalOp) = expression.op,
              case .intersection(let inputs) = logicalOp else {
            return []
        }

        // Skip if too many children (hash may be better)
        if inputs.count > context.costModel.mergeSortChildThreshold {
            return []
        }

        // Generate merge-sort intersection with appropriate sort keys.
        // If requiredProperties has a sortOrder, use it; otherwise use a placeholder.
        let sortKeys: [SortKeyExpr]
        if let sortOrder = requiredProperties.sortOrder, !sortOrder.isEmpty {
            sortKeys = sortOrder
        } else {
            // Generate with a default primary key sort
            sortKeys = [SortKeyExpr(field: "_id", ascending: true)]
        }

        // Merge-sort intersection cost:
        // Skip-ahead reduces comparisons for sparse intersections
        let k = Double(inputs.count)
        let smallestCard = 500.0  // Simplified: assume smallest stream
        // Use mergeComparisonWeight for skip-ahead comparisons (similar to merge comparison)
        let skipAheadCost = smallestCard * k * context.costModel.cpuOperatorCost * context.costModel.mergeComparisonWeight * 5.0  // 5x for seek overhead
        // Add estimated child sort cost
        let childSortCost = smallestCard * k * log2(max(2, smallestCard)) * context.costModel.cpuOperatorCost * 0.01

        let cost = skipAheadCost + childSortCost

        return [(.mergeSortIntersection(inputs: inputs, keys: sortKeys), cost)]
    }
}

// MARK: - Union/Intersection Transformation Rules

/// Transform IN predicate to union of equality predicates
///
/// WHERE x IN (1, 2, 3) → WHERE x = 1 OR x = 2 OR x = 3
/// This enables using multiple index seeks combined with union.
///
/// **Reference**: PostgreSQL "Transforming IN to OR"
public struct INToUnionRule: TransformationRule {
    public let name = "INToUnion"
    public let pattern = RulePattern.filter(child: .scan)
    public let promise = 25

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let scanGroup, let predicate) = logicalOp else {
            return []
        }

        // Check if predicate contains IN clause
        guard case .comparison(let field, .in, let value) = predicate,
              case .array(let values) = value else {
            return []
        }

        // Don't transform if too many values (hash join would be better)
        guard values.count <= 10 else { return [] }

        // Create union of equality filters
        var filterGroups: [GroupID] = []
        for v in values {
            let eqPredicate = PredicateExpr.comparison(field: field, op: .eq, value: v)
            let filterOp = LogicalOperator.filter(input: scanGroup, predicate: eqPredicate)
            let filterGroup = memo.addLogicalExpression(filterOp)
            filterGroups.append(filterGroup)
        }

        // Create union of all filters
        return [LogicalOperator.union(inputs: filterGroups, deduplicate: true)]
    }
}

/// Push filter below union
///
/// Filter(Union(A, B)) → Union(Filter(A), Filter(B))
/// This enables pushing predicates to index scans within each branch.
public struct FilterPushBelowUnionRule: TransformationRule {
    public let name = "FilterPushBelowUnion"
    public let pattern = RulePattern.filter(child: .union(children: .any))
    public let promise = 12

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let unionGroup, let predicate) = logicalOp else {
            return []
        }

        guard let unionExpr = memo.getLogicalExpressions(unionGroup).first,
              case .logical(let unionOp) = unionExpr.op,
              case .union(let inputs, let deduplicate) = unionOp else {
            return []
        }

        // Push filter into each union branch
        var filteredInputs: [GroupID] = []
        for input in inputs {
            let filterOp = LogicalOperator.filter(input: input, predicate: predicate)
            let filterGroup = memo.addLogicalExpression(filterOp)
            filteredInputs.append(filterGroup)
        }

        return [LogicalOperator.union(inputs: filteredInputs, deduplicate: deduplicate)]
    }
}

/// Push filter below intersection
///
/// Filter(Intersect(A, B)) → Intersect(Filter(A), Filter(B))
public struct FilterPushBelowIntersectionRule: TransformationRule {
    public let name = "FilterPushBelowIntersection"
    public let pattern = RulePattern.filter(child: .intersection(children: .any))
    public let promise = 12

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let intersectGroup, let predicate) = logicalOp else {
            return []
        }

        guard let intersectExpr = memo.getLogicalExpressions(intersectGroup).first,
              case .logical(let intersectOp) = intersectExpr.op,
              case .intersection(let inputs) = intersectOp else {
            return []
        }

        // Push filter into each intersection branch
        var filteredInputs: [GroupID] = []
        for input in inputs {
            let filterOp = LogicalOperator.filter(input: input, predicate: predicate)
            let filterGroup = memo.addLogicalExpression(filterOp)
            filteredInputs.append(filterGroup)
        }

        return [LogicalOperator.intersection(inputs: filteredInputs)]
    }
}

/// Convert OR predicates to union
///
/// WHERE a = 1 OR b = 2 → Union(Filter(a=1), Filter(b=2))
/// This enables using different indexes for each branch.
public struct ORToUnionRule: TransformationRule {
    public let name = "ORToUnion"
    public let pattern = RulePattern.filter(child: .scan)
    public let promise = 15

    public init() {}

    public func apply(to expression: MemoExpression, memo: Memo) -> [LogicalOperator] {
        guard case .logical(let logicalOp) = expression.op,
              case .filter(let scanGroup, let predicate) = logicalOp else {
            return []
        }

        // Check if predicate is OR
        guard case .or(let predicates) = predicate, predicates.count >= 2 else {
            return []
        }

        // Don't transform if too many branches
        guard predicates.count <= 5 else { return [] }

        // Create union of filters
        var filterGroups: [GroupID] = []
        for pred in predicates {
            let filterOp = LogicalOperator.filter(input: scanGroup, predicate: pred)
            let filterGroup = memo.addLogicalExpression(filterOp)
            filterGroups.append(filterGroup)
        }

        return [LogicalOperator.union(inputs: filterGroups, deduplicate: true)]
    }
}

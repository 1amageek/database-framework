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
    public let histogram: [CascadesValue]?

    /// Most common values and their frequencies
    public let mostCommonValues: [(CascadesValue, Double)]?

    public init(
        distinctCount: Int = 100,
        nullFraction: Double = 0.0,
        histogram: [CascadesValue]? = nil,
        mostCommonValues: [(CascadesValue, Double)]? = nil
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

    /// Default values based on PostgreSQL
    public init(
        seqPageCost: Double = 1.0,
        randomPageCost: Double = 4.0,
        cpuOperatorCost: Double = 0.0025,
        cpuTupleCost: Double = 0.01,
        cpuIndexTupleCost: Double = 0.005,
        pageSize: Int = 8192
    ) {
        self.seqPageCost = seqPageCost
        self.randomPageCost = randomPageCost
        self.cpuOperatorCost = cpuOperatorCost
        self.cpuTupleCost = cpuTupleCost
        self.cpuIndexTupleCost = cpuIndexTupleCost
        self.pageSize = pageSize
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

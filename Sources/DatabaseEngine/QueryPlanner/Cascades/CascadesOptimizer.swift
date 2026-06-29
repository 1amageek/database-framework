// CascadesOptimizer.swift
// Cascades Optimizer - Main optimization driver
//
// Implements top-down, rule-based query optimization using the Cascades framework.
//
// Reference: Graefe, G. "The Cascades Framework for Query Optimization", 1995
// https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/graefe-ieee1995.pdf

import Foundation
import Core

// MARK: - Cascades Optimizer

/// The main Cascades optimizer
///
/// **Overview**:
/// The Cascades optimizer uses a top-down, goal-driven search strategy:
/// 1. Start with the root expression and required properties
/// 2. Apply transformation rules to generate equivalent logical expressions
/// 3. Apply implementation rules to generate physical alternatives
/// 4. Use branch-and-bound to prune the search space
/// 5. Memoize results to avoid redundant work
///
/// **Key Features**:
/// - Rule-based: Easy to add new transformations
/// - Memoization: Efficient handling of shared subexpressions
/// - Cost-based: Selects lowest-cost physical plan
/// - Extensible: Custom rules can be added
///
/// **Usage**:
/// ```swift
/// let optimizer = CascadesOptimizer(context: optimizationContext)
///
/// // Add custom rules
/// optimizer.addTransformationRule(MyCustomRule())
///
/// // Optimize a logical expression
/// let physicalPlan = try optimizer.optimize(logicalPlan)
/// ```
public final class CascadesOptimizer: @unchecked Sendable {
    // MARK: - Properties

    /// The memo structure
    private let memo: Memo

    /// Optimization context (indexes, statistics, cost model)
    private let context: CascadesOptimizationContext

    /// Transformation rules
    private var transformationRules: [any TransformationRule]

    /// Implementation rules
    private var implementationRules: [any ImplementationRule]

    /// Upper bound on cost (for pruning)
    private var upperBound: Double

    /// Maximum optimization time in seconds
    private let timeout: TimeInterval

    /// Start time for timeout checking
    private var startTime: Date?

    // MARK: - Initialization

    /// Create a new optimizer
    ///
    /// - Parameters:
    ///   - context: Optimization context with indexes and statistics
    ///   - timeout: Maximum optimization time (default: 30 seconds)
    public init(context: CascadesOptimizationContext, timeout: TimeInterval = 30.0) {
        self.memo = Memo()
        self.context = context
        self.transformationRules = []
        self.implementationRules = []
        self.upperBound = Double.infinity
        self.timeout = timeout
        self.startTime = nil

        // Register default rules
        registerDefaultRules()
    }

    // MARK: - Rule Registration

    /// Add a transformation rule
    public func addTransformationRule(_ rule: any TransformationRule) {
        transformationRules.append(rule)
    }

    /// Add an implementation rule
    public func addImplementationRule(_ rule: any ImplementationRule) {
        implementationRules.append(rule)
    }

    /// Register default built-in rules
    private func registerDefaultRules() {
        // Transformation rules
        transformationRules = [
            FilterPushDownRule(),
            FilterToIndexScanRule(),
            JoinCommutativityRule(),
            // Union/Intersection transformation rules
            INToUnionRule(),
            ORToUnionRule(),
            FilterPushBelowUnionRule(),
            FilterPushBelowIntersectionRule(),
        ]

        // Implementation rules
        implementationRules = [
            SeqScanImplementationRule(),
            FilterImplementationRule(),
            IndexScanImplementationRule(),
            SortImplementationRule(),
            HashJoinImplementationRule(),
            // Union/Intersection implementation rules
            HashUnionImplementationRule(),
            MergeSortUnionImplementationRule(),
            HashIntersectionImplementationRule(),
            MergeSortIntersectionImplementationRule(),
        ]

        // Sort by promise (higher first)
        transformationRules.sort(by: { $0.promise > $1.promise })
        implementationRules.sort(by: { $0.promise > $1.promise })
    }

    // MARK: - Optimization Entry Point

    /// Optimize a logical expression
    ///
    /// - Parameters:
    ///   - logical: The root logical operator
    ///   - requiredProperties: Properties required of the result
    /// - Returns: The optimal physical plan
    /// - Throws: If optimization fails or times out
    public func optimize(
        _ logical: LogicalOperator,
        requiredProperties: PropertySet = .none
    ) throws -> OptimizedPlan {
        startTime = Date()
        upperBound = Double.infinity

        // Add root expression to memo
        let rootGroup = memo.addLogicalExpression(logical)
        memo.setRootGroup(rootGroup)

        // Optimize the root group
        guard let winner = try optimizeGroup(rootGroup, requiredProperties: requiredProperties) else {
            throw CascadesError.noValidPlan
        }

        // Build the physical plan from the memo
        return try buildPlan(from: winner, groupId: rootGroup)
    }

    // MARK: - Group Optimization

    /// Optimize a group to find the best physical expression
    ///
    /// - Parameters:
    ///   - groupId: The group to optimize
    ///   - requiredProperties: Required physical properties
    /// - Returns: The best expression ID, or nil if no valid plan
    private func optimizeGroup(
        _ groupId: GroupID,
        requiredProperties: PropertySet
    ) throws -> ExpressionID? {
        // Check for cached winner
        if let winner = memo.getWinner(groupId: groupId, properties: requiredProperties) {
            return winner
        }

        // Check timeout
        try checkTimeout()

        // Explore the group if not already done
        if !memo.isExplored(groupId) {
            try exploreGroup(groupId)
        }

        // Find best physical expression
        var bestCost = upperBound
        var bestExpr: ExpressionID?

        for physicalExpr in memo.getPhysicalExpressions(groupId) {
            if let cost = physicalExpr.cost, cost < bestCost {
                // Check if properties are satisfied
                if satisfiesProperties(physicalExpr.op, requiredProperties) {
                    // Optimize children and compute total cost
                    if let totalCost = try optimizeChildren(physicalExpr, properties: requiredProperties) {
                        let fullCost = cost + totalCost
                        if fullCost < bestCost {
                            bestCost = fullCost
                            bestExpr = physicalExpr.id
                            // Update upper bound for branch-and-bound pruning
                            upperBound = fullCost
                        }
                    }
                }
            }
        }

        // Record winner
        if let bestExpr = bestExpr {
            memo.recordWinner(groupId: groupId, properties: requiredProperties, expressionId: bestExpr)
        }

        return bestExpr
    }

    /// Explore a group by applying rules
    ///
    /// This iteratively applies transformation and implementation rules
    /// until no new expressions are generated.
    private func exploreGroup(_ groupId: GroupID) throws {
        var previousExprCount = 0
        var currentExprCount = memo.getLogicalExpressions(groupId).count

        // Iterate until no new expressions are added (fixed-point)
        while currentExprCount > previousExprCount {
            previousExprCount = currentExprCount

            // Apply transformation rules to all logical expressions
            for expr in memo.getLogicalExpressions(groupId) {
                for rule in transformationRules {
                    if matches(rule.pattern, expr.op) {
                        let newExprs = rule.apply(to: expr, memo: memo)
                        for newExpr in newExprs {
                            memo.addLogicalExpressionToGroup(newExpr, groupId: groupId)
                        }
                    }
                }
            }

            currentExprCount = memo.getLogicalExpressions(groupId).count
        }

        // Apply implementation rules to ALL logical expressions (including newly added ones)
        for expr in memo.getLogicalExpressions(groupId) {
            for rule in implementationRules {
                if matches(rule.pattern, expr.op) {
                    let physicals = rule.apply(
                        to: expr,
                        requiredProperties: .none,
                        memo: memo,
                        context: context
                    )
                    for (physical, cost) in physicals {
                        memo.addPhysicalExpression(physical, groupId: groupId, cost: cost)
                    }
                }
            }
        }

        memo.markExplored(groupId)
    }

    /// Optimize children of an expression and compute total cost
    private func optimizeChildren(
        _ expr: MemoExpression,
        properties: PropertySet
    ) throws -> Double? {
        var totalCost: Double = 0

        for childGroup in expr.op.childGroups {
            // Determine required properties for child
            let childProperties = deriveChildProperties(expr.op, parentProperties: properties)

            guard let _ = try optimizeGroup(childGroup, requiredProperties: childProperties) else {
                return nil  // No valid plan for child
            }

            // Get cost of best child plan
            if let winner = memo.getWinner(groupId: childGroup, properties: childProperties),
               let physicalExpr = memo.getPhysicalExpressions(childGroup).first(where: { $0.id == winner }),
               let cost = physicalExpr.cost {
                totalCost += cost
            }
        }

        return totalCost
    }

    // MARK: - Pattern Matching

    /// Check if an operator matches a pattern
    private func matches(_ pattern: RulePattern, _ op: MemoOperator) -> Bool {
        switch (pattern, op) {
        case (.any, _):
            return true

        case (.scan, .logical(.scan)):
            return true

        case (.filter, .logical(.filter)):
            return true

        case (.project, .logical(.project)):
            return true

        case (.join, .logical(.join)):
            return true

        case (.sort, .logical(.sort)):
            return true

        case (.limit, .logical(.limit)):
            return true

        case (.aggregate, .logical(.aggregate)):
            return true

        case (.indexScan, .logical(.indexScan)):
            return true

        case (.filterOnScan, .logical(.filter(let input, _))):
            // Check if child is a scan
            if let childExpr = memo.getLogicalExpressions(input).first {
                if case .logical(.scan) = childExpr.op {
                    return true
                }
            }
            return false

        default:
            return false
        }
    }

    /// Check if an operator satisfies required properties
    private func satisfiesProperties(_ op: MemoOperator, _ properties: PropertySet) -> Bool {
        guard let sortOrder = properties.sortOrder, !sortOrder.isEmpty else {
            return true  // No sort requirement
        }

        // Check if operator provides required sort order
        switch op {
        case .physical(.sort(_, let keys, _)):
            return keys == sortOrder
        case .physical(.indexScan):
            // Index scans can provide sorted output
            return true  // Simplified check
        case .physical(.mergeSortUnion(_, let keys, _)):
            // Merge-sort union provides sorted output by its keys
            return keys == sortOrder
        case .physical(.mergeSortIntersection(_, let keys)):
            // Merge-sort intersection provides sorted output by its keys
            return keys == sortOrder
        default:
            return false
        }
    }

    /// Derive child properties from parent operator
    private func deriveChildProperties(
        _ op: MemoOperator,
        parentProperties: PropertySet
    ) -> PropertySet {
        // Operators that require sorted input propagate sort requirements to children
        switch op {
        case .physical(.mergeJoin):
            return parentProperties
        case .physical(.mergeSortUnion(_, let keys, _)):
            // Merge-sort union requires sorted children
            return PropertySet(sortOrder: keys)
        case .physical(.mergeSortIntersection(_, let keys)):
            // Merge-sort intersection requires sorted children
            return PropertySet(sortOrder: keys)
        default:
            return .none
        }
    }

    // MARK: - Plan Building

    /// Build the final physical plan from the memo
    private func buildPlan(
        from exprId: ExpressionID,
        groupId: GroupID
    ) throws -> OptimizedPlan {
        guard let expr = memo.getPhysicalExpressions(groupId).first(where: { $0.id == exprId }) else {
            throw CascadesError.expressionNotFound
        }

        return OptimizedPlan(
            rootOperator: expr.op,
            cost: expr.cost ?? 0,
            memo: memo
        )
    }

    // MARK: - Timeout

    private func checkTimeout() throws {
        guard let startTime = startTime else { return }

        if Date().timeIntervalSince(startTime) > timeout {
            throw CascadesError.timeout
        }
    }
}

// MARK: - Optimized Plan

/// The result of Cascades optimization
public struct OptimizedPlan: Sendable {
    /// The root physical operator
    public let rootOperator: MemoOperator

    /// Estimated total cost
    public let cost: Double

    /// The memo (for debugging and plan extraction)
    public let memo: Memo

    public init(rootOperator: MemoOperator, cost: Double, memo: Memo) {
        self.rootOperator = rootOperator
        self.cost = cost
        self.memo = memo
    }
}

// MARK: - Errors

/// Errors from the Cascades optimizer
public enum CascadesError: Error, CustomStringConvertible, Sendable {
    case noValidPlan
    case expressionNotFound
    case timeout
    case invalidExpression(String)

    public var description: String {
        switch self {
        case .noValidPlan:
            return "No valid physical plan found"
        case .expressionNotFound:
            return "Expression not found in memo"
        case .timeout:
            return "Optimization timed out"
        case .invalidExpression(let msg):
            return "Invalid expression: \(msg)"
        }
    }
}

// MARK: - Debug Support

extension CascadesOptimizer {
    /// Get optimization statistics
    public var statistics: OptimizerStatistics {
        OptimizerStatistics(
            groupCount: memo.groupCount,
            expressionCount: memo.expressionCount,
            transformationRuleCount: transformationRules.count,
            implementationRuleCount: implementationRules.count
        )
    }

    /// Print the memo for debugging
    public func printMemo() {
        print(memo)
    }
}

/// Statistics about the optimization process
public struct OptimizerStatistics: Sendable {
    public let groupCount: Int
    public let expressionCount: Int
    public let transformationRuleCount: Int
    public let implementationRuleCount: Int
}

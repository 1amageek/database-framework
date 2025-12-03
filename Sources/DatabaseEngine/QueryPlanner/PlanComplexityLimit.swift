// PlanComplexityLimit.swift
// DatabaseEngine - Query plan complexity limiting and configuration
//
// Reference: FDB Record Layer RecordQueryPlannerConfiguration.java
// Prevents resource exhaustion from over-complex query plans.

import Foundation
import Core

// MARK: - QueryPlannerConfiguration

/// Configuration for the query planner
///
/// Controls resource limits and behavior of the query planner to prevent
/// excessive resource consumption from complex queries.
///
/// **Usage**:
/// ```swift
/// let config = QueryPlannerConfiguration(
///     complexityThreshold: 500,
///     maxPlanEnumerations: 50,
///     timeoutSeconds: 10.0
/// )
///
/// let planner = QueryPlanner(configuration: config, ...)
/// ```
public struct QueryPlannerConfiguration: Sendable, Equatable {
    // MARK: - Complexity Limits

    /// Maximum allowed plan complexity score
    ///
    /// Plans exceeding this threshold will be rejected with
    /// `PlanComplexityExceededError`.
    ///
    /// Reference: Based on PostgreSQL's geqo_threshold and
    /// FDB Record Layer's complexity tracking.
    public let complexityThreshold: Int

    /// Maximum number of plan alternatives to enumerate
    ///
    /// Limits the search space during plan enumeration.
    /// When reached, returns the best plan found so far.
    public let maxPlanEnumerations: Int

    /// Maximum number of transformation rule applications
    ///
    /// Prevents infinite loops in rule-based optimizers.
    public let maxRuleApplications: Int

    /// Maximum optimization time in seconds
    ///
    /// Optimization will abort with the best plan found when
    /// this timeout is reached.
    public let timeoutSeconds: Double

    // MARK: - Feature Flags

    /// Whether to enable cost-based optimization
    ///
    /// When false, uses heuristics only.
    public let enableCostBasedOptimization: Bool

    /// Whether to enable plan caching
    public let enablePlanCaching: Bool

    /// Whether to allow index intersection (AND with multiple indexes)
    public let enableIndexIntersection: Bool

    /// Whether to allow index union (OR with multiple indexes)
    public let enableIndexUnion: Bool

    /// Whether to enable IN predicate optimization
    public let enableInPredicateOptimization: Bool

    // MARK: - IN Predicate Optimization

    /// Maximum number of values for IN → UNION transformation
    ///
    /// IN predicates with more values than this will use IN-JOIN
    /// instead of UNION of index scans.
    public let inUnionThreshold: Int

    /// Maximum number of values for IN-JOIN
    ///
    /// IN predicates with more values than this will not be optimized.
    public let inJoinThreshold: Int

    // MARK: - Presets

    /// Default configuration
    ///
    /// Balanced settings suitable for most use cases.
    public static let `default` = QueryPlannerConfiguration(
        complexityThreshold: 1000,
        maxPlanEnumerations: 100,
        maxRuleApplications: 10000,
        timeoutSeconds: 30.0,
        enableCostBasedOptimization: true,
        enablePlanCaching: true,
        enableIndexIntersection: true,
        enableIndexUnion: true,
        enableInPredicateOptimization: true,
        inUnionThreshold: 10,
        inJoinThreshold: 1000
    )

    /// Conservative configuration for resource-constrained environments
    ///
    /// Lower limits to prevent resource exhaustion.
    public static let conservative = QueryPlannerConfiguration(
        complexityThreshold: 100,
        maxPlanEnumerations: 20,
        maxRuleApplications: 1000,
        timeoutSeconds: 5.0,
        enableCostBasedOptimization: true,
        enablePlanCaching: true,
        enableIndexIntersection: false,
        enableIndexUnion: true,
        enableInPredicateOptimization: true,
        inUnionThreshold: 5,
        inJoinThreshold: 100
    )

    /// Aggressive configuration for complex analytical queries
    ///
    /// Higher limits for thorough optimization.
    public static let aggressive = QueryPlannerConfiguration(
        complexityThreshold: 10000,
        maxPlanEnumerations: 1000,
        maxRuleApplications: 100000,
        timeoutSeconds: 120.0,
        enableCostBasedOptimization: true,
        enablePlanCaching: true,
        enableIndexIntersection: true,
        enableIndexUnion: true,
        enableInPredicateOptimization: true,
        inUnionThreshold: 20,
        inJoinThreshold: 5000
    )

    /// Minimal configuration for simple queries
    ///
    /// Disables most optimizations for fast planning.
    public static let minimal = QueryPlannerConfiguration(
        complexityThreshold: 50,
        maxPlanEnumerations: 5,
        maxRuleApplications: 100,
        timeoutSeconds: 1.0,
        enableCostBasedOptimization: false,
        enablePlanCaching: false,
        enableIndexIntersection: false,
        enableIndexUnion: false,
        enableInPredicateOptimization: false,
        inUnionThreshold: 0,
        inJoinThreshold: 0
    )

    // MARK: - Initialization

    public init(
        complexityThreshold: Int = 1000,
        maxPlanEnumerations: Int = 100,
        maxRuleApplications: Int = 10000,
        timeoutSeconds: Double = 30.0,
        enableCostBasedOptimization: Bool = true,
        enablePlanCaching: Bool = true,
        enableIndexIntersection: Bool = true,
        enableIndexUnion: Bool = true,
        enableInPredicateOptimization: Bool = true,
        inUnionThreshold: Int = 10,
        inJoinThreshold: Int = 1000
    ) {
        precondition(complexityThreshold > 0, "complexityThreshold must be positive")
        precondition(maxPlanEnumerations > 0, "maxPlanEnumerations must be positive")
        precondition(maxRuleApplications > 0, "maxRuleApplications must be positive")
        precondition(timeoutSeconds > 0, "timeoutSeconds must be positive")

        self.complexityThreshold = complexityThreshold
        self.maxPlanEnumerations = maxPlanEnumerations
        self.maxRuleApplications = maxRuleApplications
        self.timeoutSeconds = timeoutSeconds
        self.enableCostBasedOptimization = enableCostBasedOptimization
        self.enablePlanCaching = enablePlanCaching
        self.enableIndexIntersection = enableIndexIntersection
        self.enableIndexUnion = enableIndexUnion
        self.enableInPredicateOptimization = enableInPredicateOptimization
        self.inUnionThreshold = inUnionThreshold
        self.inJoinThreshold = inJoinThreshold
    }
}

// MARK: - PlanComplexityCalculator

/// Calculator for query plan complexity
///
/// Computes a complexity score for a query plan based on the structure
/// and operators used. This score is used to prevent overly complex
/// plans from consuming excessive resources.
///
/// **Complexity Formula**:
/// ```
/// complexity = Σ(operator_cost)
///
/// where operator_cost:
///   - TableScan: 1
///   - IndexScan, IndexSeek, IndexOnlyScan: 1
///   - Filter: input_complexity + 1
///   - Sort: input_complexity + 1
///   - Limit: input_complexity
///   - Project: input_complexity
///   - Union: Σ(child_complexity) + children_count
///   - Intersection: Σ(child_complexity) × 2
///   - FullTextScan, VectorSearch, SpatialScan: 2
///   - Aggregation: 2
/// ```
///
/// **Reference**:
/// - PostgreSQL: src/backend/optimizer/path/costsize.c
/// - FDB Record Layer: complexity tracking in RecordQueryPlanner
public struct PlanComplexityCalculator<T: Persistable>: Sendable {

    /// Default cost for leaf operators
    private static var leafCost: Int { 1 }

    /// Cost multiplier for specialized indexes
    private static var specializedIndexCost: Int { 2 }

    // MARK: - Complexity Calculation

    /// Calculate the complexity of a plan
    ///
    /// - Parameter plan: The plan to analyze
    /// - Returns: The complexity score
    public static func calculateComplexity(_ plan: PlanOperator<T>) -> Int {
        switch plan {
        // Leaf operators
        case .tableScan:
            return leafCost

        case .indexScan, .indexSeek, .indexOnlyScan:
            return leafCost

        // Specialized index operators (slightly higher cost)
        case .fullTextScan, .vectorSearch, .spatialScan, .aggregation:
            return specializedIndexCost

        // Transform operators (add 1 to input complexity)
        case .filter(let op):
            return calculateComplexity(op.input) + 1

        case .sort(let op):
            return calculateComplexity(op.input) + 1

        // Pass-through operators (same as input)
        case .limit(let op):
            return calculateComplexity(op.input)

        case .project(let op):
            return calculateComplexity(op.input)

        // Set operators
        case .union(let op):
            let childComplexity = op.children.reduce(0) { $0 + calculateComplexity($1) }
            return childComplexity + op.children.count  // Add overhead for union

        case .intersection(let op):
            let childComplexity = op.children.reduce(0) { $0 + calculateComplexity($1) }
            return childComplexity * 2  // Intersection is more expensive
        }
    }

    /// Check if a plan exceeds the complexity threshold
    ///
    /// - Parameters:
    ///   - plan: The plan to check
    ///   - threshold: The complexity threshold
    /// - Returns: Whether the plan exceeds the threshold
    public static func exceedsThreshold(_ plan: PlanOperator<T>, threshold: Int) -> Bool {
        calculateComplexity(plan) > threshold
    }

    /// Validate a plan against configuration limits
    ///
    /// - Parameters:
    ///   - plan: The plan to validate
    ///   - configuration: The planner configuration
    /// - Throws: `PlanComplexityExceededError` if complexity exceeds threshold
    public static func validate(_ plan: PlanOperator<T>, configuration: QueryPlannerConfiguration) throws {
        let complexity = calculateComplexity(plan)
        if complexity > configuration.complexityThreshold {
            throw PlanComplexityExceededError(
                complexity: complexity,
                threshold: configuration.complexityThreshold,
                planDescription: describePlan(plan)
            )
        }
    }

    /// Get complexity breakdown for debugging
    ///
    /// - Parameter plan: The plan to analyze
    /// - Returns: Breakdown of complexity by operator type
    public static func complexityBreakdown(_ plan: PlanOperator<T>) -> ComplexityBreakdown {
        var breakdown = ComplexityBreakdown()
        analyzeBreakdown(plan, into: &breakdown)
        return breakdown
    }

    // MARK: - Private Helpers

    /// Recursively analyze breakdown
    private static func analyzeBreakdown(_ plan: PlanOperator<T>, into breakdown: inout ComplexityBreakdown) {
        switch plan {
        case .tableScan:
            breakdown.tableScanCount += 1
            breakdown.totalComplexity += leafCost

        case .indexScan:
            breakdown.indexScanCount += 1
            breakdown.totalComplexity += leafCost

        case .indexSeek:
            breakdown.indexSeekCount += 1
            breakdown.totalComplexity += leafCost

        case .indexOnlyScan:
            breakdown.indexOnlyScanCount += 1
            breakdown.totalComplexity += leafCost

        case .fullTextScan:
            breakdown.fullTextScanCount += 1
            breakdown.totalComplexity += specializedIndexCost

        case .vectorSearch:
            breakdown.vectorSearchCount += 1
            breakdown.totalComplexity += specializedIndexCost

        case .spatialScan:
            breakdown.spatialScanCount += 1
            breakdown.totalComplexity += specializedIndexCost

        case .aggregation:
            breakdown.aggregationCount += 1
            breakdown.totalComplexity += specializedIndexCost

        case .filter(let op):
            breakdown.filterCount += 1
            breakdown.totalComplexity += 1
            analyzeBreakdown(op.input, into: &breakdown)

        case .sort(let op):
            breakdown.sortCount += 1
            breakdown.totalComplexity += 1
            analyzeBreakdown(op.input, into: &breakdown)

        case .limit(let op):
            breakdown.limitCount += 1
            analyzeBreakdown(op.input, into: &breakdown)

        case .project(let op):
            breakdown.projectCount += 1
            analyzeBreakdown(op.input, into: &breakdown)

        case .union(let op):
            breakdown.unionCount += 1
            breakdown.totalComplexity += op.children.count
            for child in op.children {
                analyzeBreakdown(child, into: &breakdown)
            }

        case .intersection(let op):
            breakdown.intersectionCount += 1
            // Track complexity before processing children
            let beforeTotal = breakdown.totalComplexity
            for child in op.children {
                analyzeBreakdown(child, into: &breakdown)
            }
            // Double only the children's complexity, not everything before
            let childComplexity = breakdown.totalComplexity - beforeTotal
            breakdown.totalComplexity += childComplexity  // Add again to double it
        }
    }

    /// Describe a plan for error messages
    private static func describePlan(_ plan: PlanOperator<T>) -> String {
        switch plan {
        case .tableScan:
            return "TableScan"
        case .indexScan(let op):
            return "IndexScan(\(op.index.name))"
        case .indexSeek(let op):
            return "IndexSeek(\(op.index.name))"
        case .indexOnlyScan(let op):
            return "IndexOnlyScan(\(op.index.name))"
        case .filter(let op):
            return "Filter(\(describePlan(op.input)))"
        case .sort(let op):
            return "Sort(\(describePlan(op.input)))"
        case .limit(let op):
            return "Limit(\(describePlan(op.input)))"
        case .project(let op):
            return "Project(\(describePlan(op.input)))"
        case .union(let op):
            return "Union(\(op.children.count) children)"
        case .intersection(let op):
            return "Intersection(\(op.children.count) children)"
        case .fullTextScan(let op):
            return "FullTextScan(\(op.index.name))"
        case .vectorSearch(let op):
            return "VectorSearch(\(op.index.name), k=\(op.k))"
        case .spatialScan(let op):
            return "SpatialScan(\(op.index.name))"
        case .aggregation(let op):
            return "Aggregation(\(op.index.name))"
        }
    }
}

// MARK: - ComplexityBreakdown

/// Breakdown of plan complexity by operator type
public struct ComplexityBreakdown: Sendable {
    /// Total complexity score
    public var totalComplexity: Int = 0

    /// Count of table scan operators
    public var tableScanCount: Int = 0

    /// Count of index scan operators
    public var indexScanCount: Int = 0

    /// Count of index seek operators
    public var indexSeekCount: Int = 0

    /// Count of index-only scan operators
    public var indexOnlyScanCount: Int = 0

    /// Count of filter operators
    public var filterCount: Int = 0

    /// Count of sort operators
    public var sortCount: Int = 0

    /// Count of limit operators
    public var limitCount: Int = 0

    /// Count of project operators
    public var projectCount: Int = 0

    /// Count of union operators
    public var unionCount: Int = 0

    /// Count of intersection operators
    public var intersectionCount: Int = 0

    /// Count of full-text scan operators
    public var fullTextScanCount: Int = 0

    /// Count of vector search operators
    public var vectorSearchCount: Int = 0

    /// Count of spatial scan operators
    public var spatialScanCount: Int = 0

    /// Count of aggregation operators
    public var aggregationCount: Int = 0

    /// Total number of operators
    public var totalOperators: Int {
        tableScanCount + indexScanCount + indexSeekCount + indexOnlyScanCount +
        filterCount + sortCount + limitCount + projectCount +
        unionCount + intersectionCount +
        fullTextScanCount + vectorSearchCount + spatialScanCount + aggregationCount
    }
}

// MARK: - PlanComplexityExceededError

/// Error thrown when a plan exceeds the complexity threshold
public struct PlanComplexityExceededError: Error, CustomStringConvertible, Sendable {
    /// The computed complexity
    public let complexity: Int

    /// The configured threshold
    public let threshold: Int

    /// Description of the plan
    public let planDescription: String

    /// Suggestions for reducing complexity
    public var suggestions: [String] {
        var suggestions: [String] = []

        if complexity > threshold * 10 {
            suggestions.append("Consider breaking the query into smaller parts")
        }

        if planDescription.contains("Union") {
            suggestions.append("Reduce the number of OR conditions")
        }

        if planDescription.contains("Intersection") {
            suggestions.append("Reduce the number of AND conditions using different indexes")
        }

        suggestions.append("Add more specific predicates to reduce result set")
        suggestions.append("Consider creating a compound index for the query pattern")

        return suggestions
    }

    public var description: String {
        """
        Plan complexity (\(complexity)) exceeds threshold (\(threshold)).
        Plan: \(planDescription)
        Suggestions:
        \(suggestions.enumerated().map { "  \($0.offset + 1). \($0.element)" }.joined(separator: "\n"))
        """
    }
}

// MARK: - Planning State Tracker

/// Tracks resource usage during query planning
///
/// Used to enforce limits on plan enumeration and rule applications.
public final class PlanningStateTracker: @unchecked Sendable {
    private var planEnumerations: Int = 0
    private var ruleApplications: Int = 0
    private let startTime: Date
    private let configuration: QueryPlannerConfiguration

    /// Create a new tracker
    public init(configuration: QueryPlannerConfiguration) {
        self.configuration = configuration
        self.startTime = Date()
    }

    /// Record a plan enumeration
    ///
    /// - Throws: `PlanningLimitExceededError` if limit is reached
    public func recordPlanEnumeration() throws {
        planEnumerations += 1
        if planEnumerations > configuration.maxPlanEnumerations {
            throw PlanningLimitExceededError.planEnumerationsExceeded(
                count: planEnumerations,
                limit: configuration.maxPlanEnumerations
            )
        }
    }

    /// Record a rule application
    ///
    /// - Throws: `PlanningLimitExceededError` if limit is reached
    public func recordRuleApplication() throws {
        ruleApplications += 1
        if ruleApplications > configuration.maxRuleApplications {
            throw PlanningLimitExceededError.ruleApplicationsExceeded(
                count: ruleApplications,
                limit: configuration.maxRuleApplications
            )
        }
    }

    /// Check if timeout has been reached
    ///
    /// - Throws: `PlanningLimitExceededError` if timeout is exceeded
    public func checkTimeout() throws {
        let elapsed = Date().timeIntervalSince(startTime)
        if elapsed > configuration.timeoutSeconds {
            throw PlanningLimitExceededError.timeoutExceeded(
                elapsed: elapsed,
                limit: configuration.timeoutSeconds
            )
        }
    }

    /// Get current statistics
    public var statistics: PlanningStatistics {
        PlanningStatistics(
            planEnumerations: planEnumerations,
            ruleApplications: ruleApplications,
            elapsedSeconds: Date().timeIntervalSince(startTime)
        )
    }
}

// MARK: - PlanningStatistics

/// Statistics about the planning process
public struct PlanningStatistics: Sendable {
    /// Number of plans enumerated
    public let planEnumerations: Int

    /// Number of rules applied
    public let ruleApplications: Int

    /// Elapsed planning time in seconds
    public let elapsedSeconds: Double
}

// MARK: - PlanningLimitExceededError

/// Error thrown when planning limits are exceeded
public enum PlanningLimitExceededError: Error, CustomStringConvertible, Sendable {
    case planEnumerationsExceeded(count: Int, limit: Int)
    case ruleApplicationsExceeded(count: Int, limit: Int)
    case timeoutExceeded(elapsed: Double, limit: Double)

    public var description: String {
        switch self {
        case .planEnumerationsExceeded(let count, let limit):
            return "Plan enumerations (\(count)) exceeded limit (\(limit)). Consider simplifying the query or increasing maxPlanEnumerations."

        case .ruleApplicationsExceeded(let count, let limit):
            return "Rule applications (\(count)) exceeded limit (\(limit)). Consider simplifying the query or increasing maxRuleApplications."

        case .timeoutExceeded(let elapsed, let limit):
            return String(format: "Planning timeout (%.2fs) exceeded limit (%.2fs). Consider simplifying the query or increasing timeoutSeconds.", elapsed, limit)
        }
    }
}

// MARK: - Extension to CascadesOptimizer

extension CascadesError {
    /// Create error for complexity exceeded
    public static func complexityExceeded(complexity: Int, threshold: Int) -> CascadesError {
        .invalidExpression("Plan complexity (\(complexity)) exceeds threshold (\(threshold))")
    }

    /// Create error for plan enumeration limit
    public static func planEnumerationLimit(count: Int, limit: Int) -> CascadesError {
        .invalidExpression("Plan enumeration count (\(count)) exceeds limit (\(limit))")
    }

    /// Create error for rule application limit
    public static func ruleApplicationLimit(count: Int, limit: Int) -> CascadesError {
        .invalidExpression("Rule application count (\(count)) exceeds limit (\(limit))")
    }
}

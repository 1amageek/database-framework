// InPredicateOptimizer.swift
// DatabaseEngine - Optimizer for IN predicates in queries
//
// Reference: FDB Record Layer InExtractor.java
// Transforms IN predicates into efficient join or union operations.

import Foundation
import FoundationDB
import Core

// MARK: - InOptimizationStrategy

/// Strategy for handling IN predicates
public enum InOptimizationStrategy<T: Persistable>: Sendable {
    /// Expand IN into a union of index scans (one scan per value)
    /// Best for small number of values with index support
    case indexUnion(fieldPath: String, values: [AnySendable])

    /// Use a nested loop join with the IN values as the inner table
    /// Best for larger number of values
    case inJoin(fieldPath: String, values: [AnySendable])

    /// Convert to OR conditions (disjunction)
    /// Used when no better strategy is available
    case orExpansion(conditions: QueryCondition<T>)

    /// Keep as-is (no optimization)
    case noOptimization
}

// MARK: - InPredicateOptimizer

/// Optimizer for IN predicates in query conditions
///
/// Extracts IN predicates from query conditions and determines the
/// optimal strategy for executing them.
///
/// **Optimization Strategies**:
/// 1. **Index Union**: When an index exists on the IN field and there are
///    few values, expand into a union of index scans.
/// 2. **In Join**: When there are many values, use a nested loop join
///    with the values as a virtual table.
/// 3. **OR Expansion**: Convert to equivalent OR conditions when other
///    strategies are not applicable.
///
/// **Usage**:
/// ```swift
/// let optimizer = InPredicateOptimizer<User>(configuration: .default)
///
/// let (optimizedCondition, strategy) = optimizer.optimize(
///     condition: originalCondition,
///     availableIndexes: indexes,
///     statistics: stats
/// )
///
/// switch strategy {
/// case .indexUnion(let field, let values):
///     // Plan union of index scans
/// case .inJoin(let field, let values):
///     // Plan nested loop join
/// case .orExpansion(let conditions):
///     // Plan OR of conditions
/// case .noOptimization:
///     // Use original condition
/// }
/// ```
public struct InPredicateOptimizer<T: Persistable>: Sendable {
    // MARK: - Configuration

    public struct Configuration: Sendable {
        /// Maximum number of values for index union strategy
        public let unionThreshold: Int

        /// Maximum number of values for in-join strategy
        public let joinThreshold: Int

        /// Minimum selectivity improvement to apply optimization
        public let minSelectivityImprovement: Double

        /// Default configuration
        public static var `default`: Configuration {
            Configuration(
                unionThreshold: 10,
                joinThreshold: 1000,
                minSelectivityImprovement: 0.1
            )
        }

        public init(
            unionThreshold: Int = 10,
            joinThreshold: Int = 1000,
            minSelectivityImprovement: Double = 0.1
        ) {
            self.unionThreshold = unionThreshold
            self.joinThreshold = joinThreshold
            self.minSelectivityImprovement = minSelectivityImprovement
        }
    }

    // MARK: - Properties

    private let configuration: Configuration

    // MARK: - Initialization

    public init(configuration: Configuration = .default) {
        self.configuration = configuration
    }

    // MARK: - Optimization

    /// Optimize a query condition containing IN predicates
    ///
    /// - Parameters:
    ///   - condition: The query condition to optimize
    ///   - availableIndexes: Available indexes that could be used
    ///   - statistics: Statistics provider for cost estimation
    /// - Returns: Optimized condition and the strategy used
    public func optimize(
        condition: QueryCondition<T>,
        availableIndexes: [IndexDescriptor],
        statistics: StatisticsProvider? = nil
    ) -> (optimizedCondition: QueryCondition<T>, strategy: InOptimizationStrategy<T>) {
        // Extract IN predicates from the condition
        let inPredicates = extractInPredicates(from: condition)

        guard !inPredicates.isEmpty else {
            return (condition, .noOptimization)
        }

        // Find the best IN predicate to optimize
        var bestPredicate: InPredicate<T>?
        var bestStrategy: InOptimizationStrategy<T> = .noOptimization
        var bestScore: Double = 0

        for predicate in inPredicates {
            let (strategy, score) = evaluateStrategy(
                for: predicate,
                availableIndexes: availableIndexes,
                statistics: statistics
            )

            if score > bestScore {
                bestPredicate = predicate
                bestStrategy = strategy
                bestScore = score
            }
        }

        guard let predicate = bestPredicate, bestScore > 0 else {
            return (condition, .noOptimization)
        }

        // Apply the optimization
        let optimizedCondition = applyOptimization(
            to: condition,
            predicate: predicate,
            strategy: bestStrategy
        )

        return (optimizedCondition, bestStrategy)
    }

    /// Extract all IN predicates from a condition
    public func extractInPredicates(from condition: QueryCondition<T>) -> [InPredicate<T>] {
        var predicates: [InPredicate<T>] = []
        collectInPredicates(from: condition, into: &predicates)
        return predicates
    }

    // MARK: - Private Methods

    /// Recursively collect IN predicates from a condition tree
    private func collectInPredicates(from condition: QueryCondition<T>, into predicates: inout [InPredicate<T>]) {
        switch condition {
        case .field(let fieldCondition):
            if case .in(let values) = fieldCondition.constraint {
                predicates.append(InPredicate(
                    fieldPath: fieldCondition.field.fieldName,
                    values: values,
                    originalCondition: fieldCondition
                ))
            }

        case .conjunction(let conditions):
            for cond in conditions {
                collectInPredicates(from: cond, into: &predicates)
            }

        case .disjunction(let conditions):
            for cond in conditions {
                collectInPredicates(from: cond, into: &predicates)
            }

        case .alwaysTrue, .alwaysFalse:
            break
        }
    }

    /// Evaluate the best strategy for an IN predicate
    ///
    /// **Strategy Selection** (based on FDB Record Layer InExtractor):
    /// - indexUnion: Small IN lists (<= unionThreshold) with index support
    /// - inJoin: Medium IN lists (<= joinThreshold)
    /// - orExpansion: Fallback for very small lists (2-5 values) without better options
    /// - noOptimization: Very large IN lists or when no optimization benefits
    private func evaluateStrategy(
        for predicate: InPredicate<T>,
        availableIndexes: [IndexDescriptor],
        statistics: StatisticsProvider?
    ) -> (strategy: InOptimizationStrategy<T>, score: Double) {
        let valueCount = predicate.values.count

        // Check for index support by comparing field names
        let hasIndex = availableIndexes.contains { index in
            guard let firstKeyPath = index.keyPaths.first else { return false }
            let indexFieldName = T.fieldName(for: firstKeyPath)
            return indexFieldName == predicate.fieldPath
        }

        // Determine strategy based on value count and index availability
        if hasIndex && valueCount <= configuration.unionThreshold {
            // Use index union for small number of values with index
            let score = Double(configuration.unionThreshold - valueCount + 1) / Double(configuration.unionThreshold)
            return (.indexUnion(fieldPath: predicate.fieldPath, values: predicate.values), score)
        } else if valueCount <= configuration.joinThreshold {
            // Use in-join for larger number of values
            let score = 0.5  // Medium priority
            return (.inJoin(fieldPath: predicate.fieldPath, values: predicate.values), score)
        } else if valueCount <= 5 {
            // For very small value sets without index, OR expansion can be effective
            // This creates: field == val1 OR field == val2 OR ...
            // Reference: PostgreSQL converts small IN lists to OR disjunctions
            let orCondition = buildOrExpansion(from: predicate)
            let score = 0.3  // Lower priority than join strategies
            return (.orExpansion(conditions: orCondition), score)
        }

        // Very large IN lists: no optimization (will be handled by filter)
        return (.noOptimization, 0)
    }

    /// Build an OR expansion from an IN predicate
    ///
    /// Converts: field IN (v1, v2, v3)
    /// To:       field == v1 OR field == v2 OR field == v3
    ///
    /// Reference: PostgreSQL ScalarArrayOpExpr expansion for small arrays
    private func buildOrExpansion(from predicate: InPredicate<T>) -> QueryCondition<T> {
        let fieldRef = predicate.originalCondition.field

        // Create equals conditions for each value
        let equalsConditions: [QueryCondition<T>] = predicate.values.map { value in
            let newFieldCondition = FieldCondition(
                field: fieldRef,
                constraint: .equals(value)
            )
            return QueryCondition.field(newFieldCondition)
        }

        // Combine into disjunction
        if equalsConditions.count == 1 {
            return equalsConditions[0]
        } else {
            return .disjunction(equalsConditions)
        }
    }

    /// Apply the optimization strategy to the condition
    private func applyOptimization(
        to condition: QueryCondition<T>,
        predicate: InPredicate<T>,
        strategy: InOptimizationStrategy<T>
    ) -> QueryCondition<T> {
        switch strategy {
        case .orExpansion(let disjunction):
            // Replace the IN predicate with OR of equals
            return replaceInWithOr(in: condition, predicate: predicate, replacement: disjunction)

        case .indexUnion, .inJoin:
            // Keep the condition as-is; the planner will handle the strategy
            return condition

        case .noOptimization:
            return condition
        }
    }

    /// Replace an IN predicate with disjunction of equals conditions
    private func replaceInWithOr(
        in condition: QueryCondition<T>,
        predicate: InPredicate<T>,
        replacement: QueryCondition<T>
    ) -> QueryCondition<T> {
        switch condition {
        case .field(let fieldCondition):
            if fieldCondition.field.fieldName == predicate.fieldPath,
               case .in = fieldCondition.constraint {
                return replacement
            }
            return condition

        case .conjunction(let conditions):
            let newConditions = conditions.map { cond in
                replaceInWithOr(in: cond, predicate: predicate, replacement: replacement)
            }
            return .conjunction(newConditions)

        case .disjunction(let conditions):
            let newConditions = conditions.map { cond in
                replaceInWithOr(in: cond, predicate: predicate, replacement: replacement)
            }
            return .disjunction(newConditions)

        case .alwaysTrue, .alwaysFalse:
            return condition
        }
    }
}

// MARK: - InPredicate

/// Represents an extracted IN predicate
public struct InPredicate<T: Persistable>: Sendable {
    /// Field path for the IN predicate
    public let fieldPath: String

    /// Values in the IN list
    public let values: [AnySendable]

    /// Original field condition (stored for OR expansion)
    /// This contains the FieldReference needed to create equals conditions
    public let originalCondition: FieldCondition<T>

    public init(fieldPath: String, values: [AnySendable], originalCondition: FieldCondition<T>) {
        self.fieldPath = fieldPath
        self.values = values
        self.originalCondition = originalCondition
    }
}

// MARK: - InPlanOperator

/// Plan operators for IN optimization
public enum InPlanOperator<T: Persistable>: @unchecked Sendable {
    /// Union of index scans
    case indexUnion(
        fieldPath: String,
        values: [AnySendable],
        indexName: String
    )

    /// Nested loop join with IN values
    case inJoin(
        fieldPath: String,
        values: [AnySendable],
        innerPlan: PlanOperator<T>
    )
}

// MARK: - Extension to QueryCondition

extension QueryCondition {
    /// Check if this condition contains any IN predicates
    public var containsInPredicate: Bool {
        switch self {
        case .field(let fieldCondition):
            if case .in = fieldCondition.constraint {
                return true
            }
            return false

        case .conjunction(let conditions), .disjunction(let conditions):
            return conditions.contains { $0.containsInPredicate }

        case .alwaysTrue, .alwaysFalse:
            return false
        }
    }

    /// Count the number of IN predicates in this condition
    public var inPredicateCount: Int {
        switch self {
        case .field(let fieldCondition):
            if case .in = fieldCondition.constraint {
                return 1
            }
            return 0

        case .conjunction(let conditions), .disjunction(let conditions):
            return conditions.reduce(0) { $0 + $1.inPredicateCount }

        case .alwaysTrue, .alwaysFalse:
            return 0
        }
    }
}

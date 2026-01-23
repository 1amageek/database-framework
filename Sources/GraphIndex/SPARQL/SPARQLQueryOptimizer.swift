// SPARQLQueryOptimizer.swift
// GraphIndex - Enhanced SPARQL query optimization
//
// Provides cardinality estimation, filter pushdown, and index selection.
//
// Reference: Neumann, T., Weikum, G. (2010). "x-RDF-3X: Fast Querying, Strong Consistency, and Versatile Updates in RDF Databases"

import Foundation

/// SPARQL Query Optimizer
///
/// Provides advanced optimization strategies for SPARQL-like graph queries:
/// - Cardinality estimation using statistics
/// - Filter pushdown for early filtering
/// - Optimal index selection based on bound positions
/// - Join order optimization using dynamic programming
///
/// **Reference**: RDF-3X, Apache Jena ARQ optimizer
public struct SPARQLQueryOptimizer: Sendable {

    // MARK: - Configuration

    /// Optimizer configuration
    public struct Configuration: Sendable {
        /// Enable cardinality-based join ordering
        public let useCardinalityEstimation: Bool

        /// Enable filter pushdown optimization
        public let enableFilterPushdown: Bool

        /// Maximum patterns for exhaustive search (beyond this, use greedy)
        public let exhaustiveSearchLimit: Int

        /// Default selectivity for unknown predicates
        public let defaultSelectivity: Double

        public init(
            useCardinalityEstimation: Bool = true,
            enableFilterPushdown: Bool = true,
            exhaustiveSearchLimit: Int = 6,
            defaultSelectivity: Double = 0.1
        ) {
            self.useCardinalityEstimation = useCardinalityEstimation
            self.enableFilterPushdown = enableFilterPushdown
            self.exhaustiveSearchLimit = exhaustiveSearchLimit
            self.defaultSelectivity = defaultSelectivity
        }

        public static let `default` = Configuration()

        /// Configuration optimized for small datasets
        public static let small = Configuration(
            useCardinalityEstimation: false,
            enableFilterPushdown: true,
            exhaustiveSearchLimit: 8
        )

        /// Configuration for large-scale queries
        public static let large = Configuration(
            useCardinalityEstimation: true,
            enableFilterPushdown: true,
            exhaustiveSearchLimit: 4
        )
    }

    // MARK: - Properties

    private let configuration: Configuration
    private let statistics: QueryStatistics?

    // MARK: - Initialization

    public init(
        configuration: Configuration = .default,
        statistics: QueryStatistics? = nil
    ) {
        self.configuration = configuration
        self.statistics = statistics
    }

    // MARK: - Main Optimization Entry Point

    /// Optimize a graph pattern
    ///
    /// - Parameter pattern: The pattern to optimize
    /// - Returns: Optimized pattern with reordered joins and pushed-down filters
    public func optimize(_ pattern: GraphPattern) -> GraphPattern {
        var optimized = pattern

        // Step 1: Filter pushdown
        if configuration.enableFilterPushdown {
            optimized = pushDownFilters(optimized)
        }

        // Step 2: Join reordering
        optimized = optimizeJoins(optimized)

        return optimized
    }

    // MARK: - Filter Pushdown

    /// Push filter expressions down as close to the base patterns as possible
    ///
    /// This reduces intermediate result sizes by filtering early.
    ///
    /// **Example**:
    /// Before: FILTER(?x > 5) applied after all joins
    /// After: FILTER(?x > 5) applied immediately after ?x is bound
    private func pushDownFilters(_ pattern: GraphPattern) -> GraphPattern {
        switch pattern {
        case .basic:
            return pattern

        case .join(let left, let right):
            return .join(
                pushDownFilters(left),
                pushDownFilters(right)
            )

        case .optional(let left, let right):
            return .optional(
                pushDownFilters(left),
                pushDownFilters(right)
            )

        case .union(let left, let right):
            return .union(
                pushDownFilters(left),
                pushDownFilters(right)
            )

        case .filter(let innerPattern, let expression):
            // Try to push filter into the inner pattern
            return pushFilterInto(innerPattern, filter: expression)

        case .groupBy(let sourcePattern, let groupVars, let aggs, let having):
            // Optimize the source pattern, preserve groupBy structure
            return .groupBy(
                pushDownFilters(sourcePattern),
                groupVariables: groupVars,
                aggregates: aggs,
                having: having
            )

        case .propertyPath:
            // Property paths are atomic, cannot push filters into them
            return pattern
        }
    }

    /// Push a filter expression into a pattern
    private func pushFilterInto(_ pattern: GraphPattern, filter: FilterExpression) -> GraphPattern {
        let filterVariables = extractVariables(from: filter)

        switch pattern {
        case .basic(let patterns):
            // Can push filter here if all variables are bound by these patterns
            let patternVariables = patterns.reduce(into: Set<String>()) { vars, p in
                vars.formUnion(p.variables)
            }
            if filterVariables.isSubset(of: patternVariables) {
                return .filter(.basic(patterns), filter)
            } else {
                return .filter(pattern, filter)
            }

        case .join(let left, let right):
            let leftVars = extractPatternVariables(left)
            let rightVars = extractPatternVariables(right)

            // Can push to left if all filter vars are in left
            if filterVariables.isSubset(of: leftVars) {
                return .join(
                    pushFilterInto(left, filter: filter),
                    pushDownFilters(right)
                )
            }
            // Can push to right if all filter vars are in right
            else if filterVariables.isSubset(of: rightVars) {
                return .join(
                    pushDownFilters(left),
                    pushFilterInto(right, filter: filter)
                )
            }
            // Filter spans both sides, apply after join
            else {
                return .filter(
                    .join(pushDownFilters(left), pushDownFilters(right)),
                    filter
                )
            }

        case .optional(let left, let right):
            // For OPTIONAL, can only safely push to left (required) side
            let leftVars = extractPatternVariables(left)
            if filterVariables.isSubset(of: leftVars) {
                return .optional(
                    pushFilterInto(left, filter: filter),
                    pushDownFilters(right)
                )
            }
            return .filter(.optional(pushDownFilters(left), pushDownFilters(right)), filter)

        case .union(let left, let right):
            // For UNION, must push to both sides or neither
            let leftVars = extractPatternVariables(left)
            let rightVars = extractPatternVariables(right)

            if filterVariables.isSubset(of: leftVars) && filterVariables.isSubset(of: rightVars) {
                return .union(
                    pushFilterInto(left, filter: filter),
                    pushFilterInto(right, filter: filter)
                )
            }
            return .filter(.union(pushDownFilters(left), pushDownFilters(right)), filter)

        case .filter(let innerPattern, let existingFilter):
            // Combine with existing filter
            return pushFilterInto(
                innerPattern,
                filter: .and(existingFilter, filter)
            )

        case .groupBy(let sourcePattern, let groupVars, let aggs, let having):
            // Can't push filter into groupBy source (would change semantics)
            // But we can optimize the source pattern
            return .filter(
                .groupBy(
                    pushDownFilters(sourcePattern),
                    groupVariables: groupVars,
                    aggregates: aggs,
                    having: having
                ),
                filter
            )

        case .propertyPath:
            // Property paths are atomic, wrap with filter
            return .filter(pattern, filter)
        }
    }

    // MARK: - Join Optimization

    /// Optimize join ordering in a pattern
    private func optimizeJoins(_ pattern: GraphPattern) -> GraphPattern {
        switch pattern {
        case .basic(let patterns) where patterns.count > 1:
            let optimized = optimizePatternOrder(patterns)
            return .basic(optimized)

        case .join(let left, let right):
            // Flatten nested joins and reorder
            let flattened = flattenJoins(pattern)
            if flattened.count > 1 {
                return buildOptimalJoinTree(flattened)
            }
            return .join(optimizeJoins(left), optimizeJoins(right))

        case .optional(let left, let right):
            return .optional(optimizeJoins(left), optimizeJoins(right))

        case .union(let left, let right):
            return .union(optimizeJoins(left), optimizeJoins(right))

        case .filter(let innerPattern, let expression):
            return .filter(optimizeJoins(innerPattern), expression)

        case .groupBy(let sourcePattern, let groupVars, let aggs, let having):
            return .groupBy(
                optimizeJoins(sourcePattern),
                groupVariables: groupVars,
                aggregates: aggs,
                having: having
            )

        default:
            return pattern
        }
    }

    /// Flatten nested join patterns
    private func flattenJoins(_ pattern: GraphPattern) -> [GraphPattern] {
        switch pattern {
        case .join(let left, let right):
            return flattenJoins(left) + flattenJoins(right)
        default:
            return [pattern]
        }
    }

    /// Build optimal join tree from flattened patterns
    private func buildOptimalJoinTree(_ patterns: [GraphPattern]) -> GraphPattern {
        guard !patterns.isEmpty else {
            return .basic([])
        }
        guard patterns.count > 1 else {
            return patterns[0]
        }

        // Use greedy algorithm for now
        // TODO: Implement DP-based optimization for small pattern counts
        var remaining = patterns
        var result = remaining.removeFirst()
        var boundVars = extractPatternVariables(result)

        while !remaining.isEmpty {
            // Find pattern with best join selectivity
            let (idx, _) = remaining.enumerated().map { (idx, p) -> (Int, Double) in
                let patternVars = extractPatternVariables(p)
                let sharedVars = patternVars.intersection(boundVars)

                // Score based on shared variables and estimated cardinality
                let card = estimateCardinality(p)
                let joinScore = sharedVars.isEmpty ? card * 1000 : card / Double(max(1, sharedVars.count))

                return (idx, joinScore)
            }.min { $0.1 < $1.1 }!

            let next = remaining.remove(at: idx)
            boundVars.formUnion(extractPatternVariables(next))
            result = .join(result, next)
        }

        return result
    }

    /// Optimize ordering of basic triple patterns
    private func optimizePatternOrder(_ patterns: [TriplePattern]) -> [TriplePattern] {
        guard patterns.count > 1 else { return patterns }

        if configuration.useCardinalityEstimation {
            // Use cardinality-based ordering
            return orderByCardinality(patterns)
        } else {
            // Use simple selectivity-based ordering
            return orderBySelectivity(patterns)
        }
    }

    /// Order patterns by estimated cardinality (lowest first)
    private func orderByCardinality(_ patterns: [TriplePattern]) -> [TriplePattern] {
        var remaining = patterns
        var ordered: [TriplePattern] = []
        var boundVariables = Set<String>()

        while !remaining.isEmpty {
            let (idx, _) = remaining.enumerated().map { (idx, pattern) -> (Int, Double) in
                // Substitute bound variables
                let effectivePattern = substituteWithBound(pattern, boundVars: boundVariables)
                var card = estimatePatternCardinality(effectivePattern)

                // Bonus for patterns that share variables (join will filter)
                let sharedVars = pattern.variables.intersection(boundVariables)
                if !sharedVars.isEmpty {
                    card /= Double(1 + sharedVars.count)
                }

                return (idx, card)
            }.min { $0.1 < $1.1 }!

            let selected = remaining.remove(at: idx)
            ordered.append(selected)
            boundVariables.formUnion(selected.variables)
        }

        return ordered
    }

    /// Order patterns by selectivity score (highest first)
    private func orderBySelectivity(_ patterns: [TriplePattern]) -> [TriplePattern] {
        var remaining = patterns
        var ordered: [TriplePattern] = []
        var boundVariables = Set<String>()

        while !remaining.isEmpty {
            let (idx, _) = remaining.enumerated().map { (idx, pattern) -> (Int, Int) in
                var score = pattern.selectivityScore

                // Bonus for shared variables
                let sharedVars = pattern.variables.intersection(boundVariables)
                score += sharedVars.count * 10

                return (idx, score)
            }.max { $0.1 < $1.1 }!

            let selected = remaining.remove(at: idx)
            ordered.append(selected)
            boundVariables.formUnion(selected.variables)
        }

        return ordered
    }

    // MARK: - Cardinality Estimation

    /// Estimate cardinality for a graph pattern
    public func estimateCardinality(_ pattern: GraphPattern) -> Double {
        switch pattern {
        case .basic(let patterns):
            guard !patterns.isEmpty else { return 0 }
            // Multiply individual cardinalities with join factor
            let cards = patterns.map { estimatePatternCardinality($0) }
            return cards.reduce(1.0) { acc, card in
                acc * card * 0.1 // Simple join selectivity factor
            }

        case .join(let left, let right):
            let leftCard = estimateCardinality(left)
            let rightCard = estimateCardinality(right)
            let leftVars = extractPatternVariables(left)
            let rightVars = extractPatternVariables(right)
            let sharedVars = leftVars.intersection(rightVars)

            // Join selectivity based on shared variables
            let joinSelectivity = sharedVars.isEmpty ? 1.0 : pow(0.1, Double(sharedVars.count))
            return leftCard * rightCard * joinSelectivity

        case .optional(let left, let right):
            let leftCard = estimateCardinality(left)
            let rightCard = estimateCardinality(right)
            // OPTIONAL preserves all left results, adds optional right matches
            return leftCard + leftCard * rightCard * 0.01

        case .union(let left, let right):
            return estimateCardinality(left) + estimateCardinality(right)

        case .filter(let innerPattern, _):
            // Filter typically reduces by ~50%
            return estimateCardinality(innerPattern) * 0.5

        case .groupBy(let sourcePattern, let groupVars, _, _):
            // GROUP BY reduces cardinality to number of distinct group values
            let sourceCard = estimateCardinality(sourcePattern)
            // Estimate distinct groups as sqrt of source
            let groupFactor = pow(0.5, Double(groupVars.count))
            return max(1, sourceCard * groupFactor)

        case .propertyPath(let subject, let path, let object):
            // Property paths vary widely based on the path type
            let baseCardinality: Double = 10000
            let subjectBound = subject.isBound
            let objectBound = object.isBound

            // Higher complexity paths have higher cardinality
            let pathMultiplier = Double(path.complexityEstimate)

            switch (subjectBound, objectBound) {
            case (true, true):
                return pathMultiplier  // Checking specific path
            case (true, false), (false, true):
                return pathMultiplier * 100  // One end bound
            case (false, false):
                return baseCardinality * pathMultiplier  // Full scan
            }
        }
    }

    /// Estimate cardinality for a single triple pattern
    private func estimatePatternCardinality(_ pattern: TriplePattern) -> Double {
        // If statistics available, use them
        if let stats = statistics {
            return stats.estimateCardinality(pattern)
        }

        // Fall back to heuristic based on bound positions
        let baseCardinality: Double = 10000 // Assume 10k triples

        switch (pattern.subject.isBound, pattern.predicate.isBound, pattern.object.isBound) {
        case (true, true, true):
            return 1 // Point lookup
        case (true, true, false):
            return 10 // Subject + predicate bound
        case (true, false, true):
            return 100 // Subject + object bound (rare)
        case (false, true, true):
            return 10 // Predicate + object bound
        case (true, false, false):
            return 100 // Only subject bound
        case (false, true, false):
            return baseCardinality * configuration.defaultSelectivity // Only predicate
        case (false, false, true):
            return 100 // Only object bound
        case (false, false, false):
            return baseCardinality // Full scan
        }
    }

    /// Create effective pattern with bound variables substituted
    private func substituteWithBound(_ pattern: TriplePattern, boundVars: Set<String>) -> TriplePattern {
        // This is a simplification - in reality we'd substitute actual values
        // Here we just check if the variable is bound
        let subjectBound = pattern.subject.isBound ||
            (pattern.subject.variableName.map { boundVars.contains($0) } ?? false)
        let predicateBound = pattern.predicate.isBound ||
            (pattern.predicate.variableName.map { boundVars.contains($0) } ?? false)
        let objectBound = pattern.object.isBound ||
            (pattern.object.variableName.map { boundVars.contains($0) } ?? false)

        return TriplePattern(
            subject: subjectBound ? .value("_bound_") : pattern.subject,
            predicate: predicateBound ? .value("_bound_") : pattern.predicate,
            object: objectBound ? .value("_bound_") : pattern.object
        )
    }

    // MARK: - Variable Extraction

    /// Extract variables from a filter expression
    ///
    /// Uses FilterExpression's built-in `variables` property.
    private func extractVariables(from expression: FilterExpression) -> Set<String> {
        expression.variables
    }

    /// Extract all variables from a graph pattern
    private func extractPatternVariables(_ pattern: GraphPattern) -> Set<String> {
        switch pattern {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { vars, p in
                vars.formUnion(p.variables)
            }
        case .join(let left, let right), .union(let left, let right), .optional(let left, let right):
            return extractPatternVariables(left).union(extractPatternVariables(right))
        case .filter(let innerPattern, _):
            return extractPatternVariables(innerPattern)
        case .groupBy(_, let groupVars, let aggs, _):
            // GROUP BY output variables are group variables + aggregate aliases
            var vars = Set(groupVars)
            for agg in aggs {
                vars.insert(agg.alias)
            }
            return vars

        case .propertyPath(let subject, _, let object):
            var vars = Set<String>()
            if case .variable(let name) = subject { vars.insert(name) }
            if case .variable(let name) = object { vars.insert(name) }
            return vars
        }
    }
}

// MARK: - Query Statistics

/// Statistics for cardinality estimation
///
/// Can be populated from actual index statistics or samples.
public struct QueryStatistics: Sendable {

    /// Total number of triples
    public let totalTriples: Int

    /// Distinct subject count
    public let distinctSubjects: Int

    /// Distinct predicate count
    public let distinctPredicates: Int

    /// Distinct object count
    public let distinctObjects: Int

    /// Per-predicate triple counts
    public let predicateCounts: [String: Int]

    /// Per-predicate distinct subject counts
    public let predicateSubjectCounts: [String: Int]

    /// Per-predicate distinct object counts
    public let predicateObjectCounts: [String: Int]

    public init(
        totalTriples: Int,
        distinctSubjects: Int,
        distinctPredicates: Int,
        distinctObjects: Int,
        predicateCounts: [String: Int] = [:],
        predicateSubjectCounts: [String: Int] = [:],
        predicateObjectCounts: [String: Int] = [:]
    ) {
        self.totalTriples = totalTriples
        self.distinctSubjects = distinctSubjects
        self.distinctPredicates = distinctPredicates
        self.distinctObjects = distinctObjects
        self.predicateCounts = predicateCounts
        self.predicateSubjectCounts = predicateSubjectCounts
        self.predicateObjectCounts = predicateObjectCounts
    }

    /// Estimate cardinality for a triple pattern
    public func estimateCardinality(_ pattern: TriplePattern) -> Double {
        let s = pattern.subject.isBound
        let p = pattern.predicate.isBound
        let o = pattern.object.isBound

        // Get predicate-specific stats if predicate is bound
        let predStats: (count: Int, subjects: Int, objects: Int)?
        if case .value(let pred) = pattern.predicate {
            predStats = (
                predicateCounts[pred] ?? totalTriples / max(1, distinctPredicates),
                predicateSubjectCounts[pred] ?? distinctSubjects / 10,
                predicateObjectCounts[pred] ?? distinctObjects / 10
            )
        } else {
            predStats = nil
        }

        switch (s, p, o) {
        case (true, true, true):
            return 1.0 // Point lookup

        case (true, true, false):
            // Subject + predicate: avg objects per (s,p) pair
            if let ps = predStats {
                return Double(ps.count) / Double(max(1, ps.subjects))
            }
            return 10.0

        case (true, false, true):
            // Subject + object bound (rare pattern)
            return Double(totalTriples) / Double(max(1, distinctSubjects * distinctObjects)) * 100

        case (false, true, true):
            // Predicate + object: avg subjects per (p,o) pair
            if let ps = predStats {
                return Double(ps.count) / Double(max(1, ps.objects))
            }
            return 10.0

        case (true, false, false):
            // Only subject: all predicates/objects for subject
            return Double(totalTriples) / Double(max(1, distinctSubjects))

        case (false, true, false):
            // Only predicate: all triples with that predicate
            if let ps = predStats {
                return Double(ps.count)
            }
            return Double(totalTriples) / Double(max(1, distinctPredicates))

        case (false, false, true):
            // Only object: all triples pointing to object
            return Double(totalTriples) / Double(max(1, distinctObjects))

        case (false, false, false):
            // Full scan
            return Double(totalTriples)
        }
    }
}

// MARK: - Index Selection

/// Index selection strategy for triple patterns
public enum IndexStrategy: Sendable {
    /// SPO index: Subject, Predicate, Object ordering
    case spo
    /// POS index: Predicate, Object, Subject ordering
    case pos
    /// OSP index: Object, Subject, Predicate ordering
    case osp
    /// PSO index: Predicate, Subject, Object ordering
    case pso
    /// SOP index: Subject, Object, Predicate ordering
    case sop
    /// OPS index: Object, Predicate, Subject ordering
    case ops

    /// Select optimal index for a triple pattern
    public static func selectIndex(for pattern: TriplePattern) -> IndexStrategy {
        let s = pattern.subject.isBound
        let p = pattern.predicate.isBound
        let o = pattern.object.isBound

        switch (s, p, o) {
        case (true, true, true):   return .spo  // Any index works
        case (true, true, false):  return .spo  // S,P prefix
        case (true, false, true):  return .sop  // S,O prefix
        case (true, false, false): return .spo  // S prefix
        case (false, true, true):  return .pos  // P,O prefix
        case (false, true, false): return .pso  // P prefix
        case (false, false, true): return .osp  // O prefix
        case (false, false, false): return .spo // Full scan, any index
        }
    }

    /// Get the key order for this index
    public var keyOrder: (first: String, second: String, third: String) {
        switch self {
        case .spo: return ("subject", "predicate", "object")
        case .pos: return ("predicate", "object", "subject")
        case .osp: return ("object", "subject", "predicate")
        case .pso: return ("predicate", "subject", "object")
        case .sop: return ("subject", "object", "predicate")
        case .ops: return ("object", "predicate", "subject")
        }
    }
}

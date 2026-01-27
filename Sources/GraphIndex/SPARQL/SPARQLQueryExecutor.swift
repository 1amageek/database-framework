// SPARQLQueryExecutor.swift
// GraphIndex - SPARQL-like query execution engine
//
// Executes graph patterns against hexastore indexes.

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

/// SPARQL query execution engine
///
/// Evaluates graph patterns against hexastore indexes with:
/// - Greedy join order optimization
/// - Variable substitution for efficient index lookups
/// - OPTIONAL (left outer join), UNION, and FILTER support
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 18.5 (SPARQL Algebra Evaluation)
internal struct SPARQLQueryExecutor<T: Persistable>: Sendable {

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    // MARK: - Initialization

    init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
    }

    // MARK: - Result Type

    private struct EvaluationResult: Sendable {
        var bindings: [VariableBinding]
        var stats: ExecutionStatistics

        static var empty: EvaluationResult {
            EvaluationResult(bindings: [], stats: ExecutionStatistics())
        }

        static func single(_ bindings: [VariableBinding], stats: ExecutionStatistics) -> EvaluationResult {
            EvaluationResult(bindings: bindings, stats: stats)
        }

        func mergedStats(with other: ExecutionStatistics) -> EvaluationResult {
            var merged = stats
            merged.indexScans += other.indexScans
            merged.joinOperations += other.joinOperations
            merged.intermediateResults += other.intermediateResults
            merged.patternsEvaluated += other.patternsEvaluated
            merged.optionalMisses += other.optionalMisses
            return EvaluationResult(bindings: bindings, stats: merged)
        }
    }

    // MARK: - Execution

    /// Execute a graph pattern and return bindings
    ///
    /// - Parameters:
    ///   - pattern: The graph pattern to evaluate
    ///   - limit: Maximum number of results (nil for unlimited)
    ///   - offset: Number of results to skip
    /// - Returns: Tuple of bindings and execution statistics
    func execute(
        pattern: GraphPattern,
        limit: Int?,
        offset: Int
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let indexName = buildIndexName()
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? GraphIndexKind<T> else {
            throw SPARQLQueryError.indexNotFound(indexName)
        }

        let strategy = kind.strategy
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let evalResult = try await queryContext.withTransaction { transaction in
            try await self.evaluate(
                pattern: pattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
        }

        // Apply offset and limit
        var bindings = evalResult.bindings
        if offset > 0 {
            bindings = Array(bindings.dropFirst(offset))
        }
        if let limit = limit {
            bindings = Array(bindings.prefix(limit))
        }

        return (bindings, evalResult.stats)
    }

    // MARK: - Pattern Evaluation

    /// Evaluate a graph pattern recursively
    private func evaluate(
        pattern: GraphPattern,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> EvaluationResult {
        var stats = ExecutionStatistics()
        stats.patternsEvaluated = 1

        switch pattern {
        case .basic(let patterns):
            let (bindings, basicStats) = try await evaluateBasic(
                patterns: patterns,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            stats.indexScans = basicStats.indexScans
            stats.joinOperations = basicStats.joinOperations
            stats.intermediateResults = basicStats.intermediateResults
            return EvaluationResult(bindings: bindings, stats: stats)

        case .join(let left, let right):
            stats.joinOperations = 1
            let leftResult = try await evaluate(
                pattern: left,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            let (bindings, joinStats) = try await evaluateJoin(
                leftBindings: leftResult.bindings,
                rightPattern: right,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            return EvaluationResult(bindings: bindings, stats: stats)
                .mergedStats(with: leftResult.stats)
                .mergedStats(with: joinStats)

        case .optional(let left, let right):
            let leftResult = try await evaluate(
                pattern: left,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            let (bindings, optionalStats) = try await evaluateOptional(
                leftBindings: leftResult.bindings,
                rightPattern: right,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            return EvaluationResult(bindings: bindings, stats: stats)
                .mergedStats(with: leftResult.stats)
                .mergedStats(with: optionalStats)

        case .union(let left, let right):
            let leftResult = try await evaluate(
                pattern: left,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            let rightResult = try await evaluate(
                pattern: right,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            let bindings = leftResult.bindings + rightResult.bindings
            return EvaluationResult(bindings: bindings, stats: stats)
                .mergedStats(with: leftResult.stats)
                .mergedStats(with: rightResult.stats)

        case .filter(let innerPattern, let expression):
            let innerResult = try await evaluate(
                pattern: innerPattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            let filtered = innerResult.bindings.filter { expression.evaluate($0) }
            return EvaluationResult(bindings: filtered, stats: stats)
                .mergedStats(with: innerResult.stats)

        case .groupBy(let sourcePattern, let groupVars, let aggs, let havingExpr):
            // Note: groupBy is typically handled by executeGrouped, not evaluate.
            // For completeness, we implement basic handling here.
            let sourceResult = try await evaluate(
                pattern: sourcePattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )

            // Group the results using GroupValue for proper null handling
            var groups: [[String: GroupValue]: [VariableBinding]] = [:]
            for binding in sourceResult.bindings {
                var key: [String: GroupValue] = [:]
                for variable in groupVars {
                    key[variable] = GroupValue(from: binding[variable])
                }
                if groups[key] == nil {
                    groups[key] = []
                }
                groups[key]?.append(binding)
            }

            // Compute aggregates for each group
            var resultBindings: [VariableBinding] = []
            for (groupKey, groupBindingsInGroup) in groups {
                var binding = VariableBinding()
                for (variable, groupValue) in groupKey {
                    // Only bind if not unbound (preserve SPARQL null semantics)
                    if let fieldValue = groupValue.fieldValue {
                        binding = binding.binding(variable, to: fieldValue)
                    }
                }
                for agg in aggs {
                    if let aggValue = agg.evaluate(groupBindingsInGroup) {
                        binding = binding.binding(agg.alias, to: aggValue)
                    }
                }
                resultBindings.append(binding)
            }

            // Apply HAVING
            if let having = havingExpr {
                resultBindings = resultBindings.filter { having.evaluate($0) }
            }

            return EvaluationResult(bindings: resultBindings, stats: stats)
                .mergedStats(with: sourceResult.stats)

        case .propertyPath(let subject, let path, let object):
            // Execute property path
            let pathResult = try await evaluatePropertyPath(
                subject: subject,
                path: path,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            return EvaluationResult(bindings: pathResult.bindings, stats: stats)
                .mergedStats(with: pathResult.stats)
        }
    }

    /// Evaluate a Basic Graph Pattern (BGP)
    ///
    /// Optimizes join order based on selectivity and executes patterns
    /// using nested loop join with variable substitution.
    private func evaluateBasic(
        patterns: [TriplePattern],
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        var stats = ExecutionStatistics()

        guard !patterns.isEmpty else {
            return ([VariableBinding()], stats)
        }

        // Optimize join order using greedy algorithm
        let orderedPatterns = optimizeJoinOrder(patterns)

        // Start with empty binding
        var currentBindings: [VariableBinding] = [VariableBinding()]

        // Execute patterns in optimized order
        for pattern in orderedPatterns {
            stats.joinOperations += 1
            var newBindings: [VariableBinding] = []

            for binding in currentBindings {
                // Substitute variables in pattern
                let substituted = pattern.substitute(binding)

                // Execute pattern
                let (matches, scanStats) = try await executePattern(
                    substituted,
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    transaction: transaction
                )
                stats.indexScans += scanStats.indexScans

                // Merge bindings
                for match in matches {
                    if let merged = binding.merged(with: match) {
                        newBindings.append(merged)
                    }
                }
            }

            currentBindings = newBindings
            stats.intermediateResults += currentBindings.count

            // Early termination if no results
            if currentBindings.isEmpty {
                break
            }
        }

        return (currentBindings, stats)
    }

    /// Evaluate a join of left bindings with right pattern
    private func evaluateJoin(
        leftBindings: [VariableBinding],
        rightPattern: GraphPattern,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        var results: [VariableBinding] = []
        var combinedStats = ExecutionStatistics()

        for leftBinding in leftBindings {
            // Substitute variables in right pattern
            let substitutedPattern = substitutePattern(rightPattern, with: leftBinding)

            let rightResult = try await evaluate(
                pattern: substitutedPattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            combinedStats = combinedStats.merged(with: rightResult.stats)

            for rightBinding in rightResult.bindings {
                if let merged = leftBinding.merged(with: rightBinding) {
                    results.append(merged)
                }
            }
        }

        return (results, combinedStats)
    }

    /// Evaluate OPTIONAL (left outer join)
    ///
    /// For each left binding, try to match the right pattern.
    /// If right pattern doesn't match, keep the left binding as-is.
    private func evaluateOptional(
        leftBindings: [VariableBinding],
        rightPattern: GraphPattern,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        var results: [VariableBinding] = []
        var combinedStats = ExecutionStatistics()

        for leftBinding in leftBindings {
            let substitutedPattern = substitutePattern(rightPattern, with: leftBinding)

            let rightResult = try await evaluate(
                pattern: substitutedPattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            combinedStats = combinedStats.merged(with: rightResult.stats)

            if rightResult.bindings.isEmpty {
                // OPTIONAL didn't match - keep left binding as-is
                results.append(leftBinding)
                combinedStats.optionalMisses += 1
            } else {
                // Merge each right binding with left
                // SPARQL LeftJoin: if right is non-empty but ALL merges fail
                // (incompatible bindings), the left binding must still be preserved
                var anyMerged = false
                for rightBinding in rightResult.bindings {
                    if let merged = leftBinding.merged(with: rightBinding) {
                        results.append(merged)
                        anyMerged = true
                    }
                }
                if !anyMerged {
                    // All right bindings were incompatible → preserve left binding
                    results.append(leftBinding)
                    combinedStats.optionalMisses += 1
                }
            }
        }

        return (results, combinedStats)
    }

    // MARK: - Pattern Execution

    /// Execute a single triple pattern against the index
    private func executePattern(
        _ pattern: TriplePattern,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        var stats = ExecutionStatistics()
        stats.indexScans = 1

        let ordering = selectOptimalOrdering(pattern: pattern, strategy: strategy)
        let orderingSubspace = subspaceForOrdering(ordering, base: indexSubspace)

        let (beginKey, endKey) = buildScanRange(pattern: pattern, ordering: ordering, subspace: orderingSubspace)

        var results: [VariableBinding] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let binding = try parseKeyToBinding(key, pattern: pattern, ordering: ordering, subspace: orderingSubspace) {
                results.append(binding)
            }
        }

        return (results, stats)
    }

    /// Select optimal index ordering based on bound terms
    ///
    /// Uses `isBound` to check for concrete values. This correctly handles:
    /// - `.value` → bound (use as index prefix)
    /// - `.variable` → unbound (scan needed)
    /// - `.wildcard` → unbound (scan needed)
    private func selectOptimalOrdering(pattern: TriplePattern, strategy: GraphIndexStrategy) -> GraphIndexOrdering {
        let subjectBound = pattern.subject.isBound
        let predicateBound = pattern.predicate.isBound
        let objectBound = pattern.object.isBound

        switch (subjectBound, predicateBound, objectBound) {
        case (true, true, true):
            return strategy == .adjacency ? .out : .spo
        case (true, true, false):
            return strategy == .adjacency ? .out : .spo
        case (true, false, true):
            return strategy == .hexastore ? .sop : .osp
        case (false, true, true):
            return strategy == .adjacency ? .in : .pos
        case (true, false, false):
            return strategy == .adjacency ? .out : .spo
        case (false, true, false):
            return strategy == .hexastore ? .pso : .pos
        case (false, false, true):
            return strategy == .adjacency ? .in : .osp
        case (false, false, false):
            return strategy == .adjacency ? .out : .spo
        }
    }

    /// Get subspace for a specific index ordering
    private func subspaceForOrdering(_ ordering: GraphIndexOrdering, base: Subspace) -> Subspace {
        let key: Int64
        switch ordering {
        case .out: key = 0
        case .in: key = 1
        case .spo: key = 2
        case .pos: key = 3
        case .osp: key = 4
        case .sop: key = 5
        case .pso: key = 6
        case .ops: key = 7
        }
        return base.subspace(key)
    }

    /// Build scan range for a pattern
    ///
    /// Constructs begin/end keys for range scan based on bound terms.
    /// Uses `subspace.pack()` to correctly construct keys in the subspace's keyspace.
    ///
    /// **Important**: Tuple packing is NOT additive. `prefix + tuple.pack()` produces
    /// invalid keys. Must use `subspace.pack(tuple)` instead.
    ///
    /// **Fully-bound case**: When all 3 elements are bound, we need to find the EXACT key,
    /// not keys with additional suffixes. Subspace.range() adds 0x00/0xFF which excludes
    /// the exact key. Use `[key, strinc(key))` instead to include the exact key.
    private func buildScanRange(pattern: TriplePattern, ordering: GraphIndexOrdering, subspace: Subspace) -> (begin: FDB.Bytes, end: FDB.Bytes) {
        var prefixElements: [any TupleElement] = []
        let elementOrder = ordering.elementOrder
        let terms = [pattern.subject, pattern.predicate, pattern.object]

        // Build prefix from bound terms in index order
        // Stop at first unbound term (variable or wildcard)
        for idx in elementOrder {
            let term = terms[idx]
            guard term.isBound, case .value(let fieldValue) = term else {
                break  // Stop at first unbound term
            }
            prefixElements.append(fieldValue.toTupleElement())
        }

        if prefixElements.isEmpty {
            // No bound elements: scan entire subspace
            return subspace.range()
        } else {
            let prefixKey = subspace.pack(Tuple(prefixElements))

            if prefixElements.count == 3 {
                // Fully bound: exact key lookup
                // Subspace.range() returns (prefix+0x00, prefix+0xFF) which EXCLUDES the exact key
                // Use [prefixKey, strinc(prefixKey)) to include the exact key
                do {
                    let endKey = try FDB.strinc(prefixKey)
                    return (prefixKey, endKey)
                } catch {
                    // Edge case: key is all 0xFF bytes (extremely rare)
                    // Fall back to prefix range which won't match, but is safe
                    let prefixSubspace = Subspace(prefix: prefixKey)
                    return prefixSubspace.range()
                }
            } else {
                // Partial prefix: look for keys with this prefix and additional elements
                // Subspace.range() correctly returns keys that START with prefix
                let prefixSubspace = Subspace(prefix: prefixKey)
                return prefixSubspace.range()
            }
        }
    }

    /// Parse a key into variable bindings
    ///
    /// Extracts triple components from FDB key and creates typed variable bindings.
    /// Uses `FieldValue(tupleElement:)` to preserve the native type from the hexastore,
    /// eliminating the need for string-based type inference.
    private func parseKeyToBinding(
        _ key: FDB.Bytes,
        pattern: TriplePattern,
        ordering: GraphIndexOrdering,
        subspace: Subspace
    ) throws -> VariableBinding? {
        let tuple = try subspace.unpack(key)

        guard tuple.count >= 3 else {
            return nil
        }

        let elementOrder = ordering.elementOrder
        var fromValue: FieldValue?
        var edgeValue: FieldValue?
        var toValue: FieldValue?

        for (tupleIdx, componentIdx) in elementOrder.enumerated() {
            guard tupleIdx < tuple.count, let element = tuple[tupleIdx] else {
                continue
            }

            let fieldValue = FieldValue(tupleElement: element)
                ?? .string(String(describing: element))

            switch componentIdx {
            case 0: fromValue = fieldValue
            case 1: edgeValue = fieldValue
            case 2: toValue = fieldValue
            default: break
            }
        }

        guard let from = fromValue, let edge = edgeValue, let to = toValue else {
            return nil
        }

        // Check if values match bound terms (FieldValue == with cross-type support)
        if case .value(let v) = pattern.subject, v != from { return nil }
        if case .value(let v) = pattern.predicate, v != edge { return nil }
        if case .value(let v) = pattern.object, v != to { return nil }

        // Build binding from variables (direct FieldValue, no inference needed)
        var binding = VariableBinding()

        if case .variable(let name) = pattern.subject {
            binding = binding.binding(name, to: from)
        }
        if case .variable(let name) = pattern.predicate {
            binding = binding.binding(name, to: edge)
        }
        if case .variable(let name) = pattern.object {
            binding = binding.binding(name, to: to)
        }

        return binding
    }

    // MARK: - Join Order Optimization

    /// Optimize join order using greedy algorithm
    ///
    /// Selects patterns in order of:
    /// 1. Most selective (most bound terms) first
    /// 2. Patterns sharing variables with already-bound variables
    ///
    /// **Reference**: "Optimal Ordering of BGP Evaluation"
    /// W3C SPARQL 1.1 Query Language, Section 18.4
    private func optimizeJoinOrder(_ patterns: [TriplePattern]) -> [TriplePattern] {
        guard patterns.count > 1 else { return patterns }

        var remaining = patterns
        var ordered: [TriplePattern] = []
        var boundVariables = Set<String>()

        while !remaining.isEmpty {
            // Score each remaining pattern
            let scores = remaining.map { pattern -> (pattern: TriplePattern, score: Int) in
                var score = pattern.selectivityScore

                // Bonus for sharing variables with already-bound variables
                let sharedVars = pattern.variables.intersection(boundVariables)
                score += sharedVars.count * 10

                return (pattern, score)
            }

            // Select highest scoring pattern
            let best = scores.max { $0.score < $1.score }!
            ordered.append(best.pattern)
            boundVariables.formUnion(best.pattern.variables)

            // Remove from remaining
            if let idx = remaining.firstIndex(of: best.pattern) {
                remaining.remove(at: idx)
            }
        }

        return ordered
    }

    // MARK: - Pattern Substitution

    /// Substitute variables in a pattern with bound values
    private func substitutePattern(_ pattern: GraphPattern, with binding: VariableBinding) -> GraphPattern {
        switch pattern {
        case .basic(let patterns):
            return .basic(patterns.map { $0.substitute(binding) })

        case .join(let left, let right):
            return .join(
                substitutePattern(left, with: binding),
                substitutePattern(right, with: binding)
            )

        case .optional(let left, let right):
            return .optional(
                substitutePattern(left, with: binding),
                substitutePattern(right, with: binding)
            )

        case .union(let left, let right):
            return .union(
                substitutePattern(left, with: binding),
                substitutePattern(right, with: binding)
            )

        case .filter(let innerPattern, let expression):
            return .filter(
                substitutePattern(innerPattern, with: binding),
                expression
            )

        case .groupBy(let sourcePattern, let groupVars, let aggs, let havingExpr):
            return .groupBy(
                substitutePattern(sourcePattern, with: binding),
                groupVariables: groupVars,
                aggregates: aggs,
                having: havingExpr
            )

        case .propertyPath(let subject, let path, let object):
            return .propertyPath(
                subject: subject.substitute(binding),
                path: path,
                object: object.substitute(binding)
            )
        }
    }

    // MARK: - Helpers

    private func buildIndexName() -> String {
        "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"
    }

    // MARK: - GROUP BY Execution

    /// Execute a grouped query with aggregation
    ///
    /// - Parameters:
    ///   - pattern: The graph pattern (expected to be groupBy)
    ///   - groupVariables: Variables to group by
    ///   - aggregates: Aggregate expressions to compute
    ///   - having: Optional HAVING filter
    /// - Returns: Grouped bindings and statistics
    func executeGrouped(
        pattern: GraphPattern,
        groupVariables: [String],
        aggregates: [AggregateExpression],
        having: FilterExpression?
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        // Extract the source pattern
        let sourcePattern: GraphPattern
        switch pattern {
        case .groupBy(let source, _, _, _):
            sourcePattern = source
        default:
            sourcePattern = pattern
        }

        let indexName = buildIndexName()
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? GraphIndexKind<T> else {
            throw SPARQLQueryError.indexNotFound(indexName)
        }

        let strategy = kind.strategy
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute source pattern
        let evalResult = try await queryContext.withTransaction { transaction in
            try await self.evaluate(
                pattern: sourcePattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
        }

        let stats = evalResult.stats

        // Group the bindings using GroupValue for proper null handling
        let groups = groupBindings(evalResult.bindings, by: groupVariables)

        // Sort by group key for deterministic output using GroupValue ordering
        // GroupValue.Comparable ensures: bound("") < bound("a") < unbound
        let sortedGroups = groups.keys.sorted { lhs, rhs in
            for variable in groupVariables {
                let lhsVal = lhs[variable] ?? .unbound
                let rhsVal = rhs[variable] ?? .unbound
                if lhsVal != rhsVal {
                    return lhsVal < rhsVal
                }
            }
            return false
        }

        // Rebuild result bindings in sorted order
        var sortedResultBindings: [VariableBinding] = []
        for groupKey in sortedGroups {
            guard let groupBindingsInGroup = groups[groupKey] else { continue }

            var binding = VariableBinding()
            for (variable, groupValue) in groupKey {
                if let fieldValue = groupValue.fieldValue {
                    binding = binding.binding(variable, to: fieldValue)
                }
            }
            for aggregate in aggregates {
                if let value = aggregate.evaluate(groupBindingsInGroup) {
                    binding = binding.binding(aggregate.alias, to: value)
                }
            }
            sortedResultBindings.append(binding)
        }

        // Apply HAVING filter to sorted results
        if let having = having {
            sortedResultBindings = sortedResultBindings.filter { having.evaluate($0) }
        }

        return (sortedResultBindings, stats)
    }

    /// Group bindings by specified variables
    ///
    /// Returns groups as dictionary mapping group keys to bindings.
    /// Group keys use `GroupValue` to properly distinguish unbound from empty string.
    ///
    /// **SPARQL 1.1 Compliance**:
    /// Per Section 11.2, unbound values must be distinct from any bound value.
    /// Using `GroupValue.unbound` vs `GroupValue.bound("")` ensures proper separation.
    private func groupBindings(
        _ bindings: [VariableBinding],
        by groupVariables: [String]
    ) -> [[String: GroupValue]: [VariableBinding]] {
        var groups: [[String: GroupValue]: [VariableBinding]] = [:]

        for binding in bindings {
            // Build group key using GroupValue to preserve null distinction
            var key: [String: GroupValue] = [:]
            for variable in groupVariables {
                key[variable] = GroupValue(from: binding[variable])
            }

            if groups[key] == nil {
                groups[key] = []
            }
            groups[key]?.append(binding)
        }

        return groups
    }

    // MARK: - Property Path Evaluation

    /// Evaluate a property path pattern
    ///
    /// Handles all SPARQL 1.1 property path operators:
    /// - Simple IRI: direct edge lookup
    /// - Inverse: reverse direction lookup
    /// - Sequence: multi-hop paths
    /// - Alternative: union of paths
    /// - ZeroOrMore/OneOrMore: transitive closure
    /// - ZeroOrOne: optional single hop
    private func evaluatePropertyPath(
        subject: SPARQLTerm,
        path: PropertyPath,
        object: SPARQLTerm,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol,
        config: PropertyPathConfiguration = .default,
        depth: Int = 0
    ) async throws -> EvaluationResult {
        var stats = ExecutionStatistics()
        stats.patternsEvaluated = 1

        // Check max depth for recursive paths
        if depth > config.maxDepth {
            return EvaluationResult(bindings: [], stats: stats)
        }

        switch path {
        case .iri(let predicate):
            // Simple predicate - equivalent to a triple pattern
            let pattern = TriplePattern(subject: subject, predicate: .value(.string(predicate)), object: object)
            let (results, patternStats) = try await executePattern(
                pattern,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction
            )
            return EvaluationResult(bindings: results, stats: stats).mergedStats(with: patternStats)

        case .inverse(let innerPath):
            // Normalize inverse paths per SPARQL 1.1, Section 18.4
            // Push inverse inside quantifiers so BFS expands from the correct end.
            switch innerPath {
            case .oneOrMore(let p):
                // ^(p+) = (^p)+
                return try await evaluatePropertyPath(
                    subject: subject, path: .oneOrMore(.inverse(p)), object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            case .zeroOrMore(let p):
                // ^(p*) = (^p)*
                return try await evaluatePropertyPath(
                    subject: subject, path: .zeroOrMore(.inverse(p)), object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            case .zeroOrOne(let p):
                // ^(p?) = (^p)?
                return try await evaluatePropertyPath(
                    subject: subject, path: .zeroOrOne(.inverse(p)), object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            case .sequence(let p1, let p2):
                // ^(p1/p2) = (^p2)/(^p1) — SPARQL 1.1 Section 18.4
                // Reverse order AND invert each component
                return try await evaluatePropertyPath(
                    subject: subject, path: .sequence(.inverse(p2), .inverse(p1)), object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            case .alternative(let p1, let p2):
                // ^(p1|p2) = (^p1)|(^p2) — distribute inverse over alternative
                return try await evaluatePropertyPath(
                    subject: subject, path: .alternative(.inverse(p1), .inverse(p2)), object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            case .inverse(let p):
                // ^^p = p — double inverse cancels
                return try await evaluatePropertyPath(
                    subject: subject, path: p, object: object,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            default:
                // Simple inverse (iri, negatedPropertySet): swap subject and object
                return try await evaluatePropertyPath(
                    subject: object, path: innerPath, object: subject,
                    indexSubspace: indexSubspace, strategy: strategy,
                    transaction: transaction, config: config, depth: depth
                )
            }

        case .sequence(let path1, let path2):
            // Execute first path, then chain with second
            let intermediateVar = "?_intermediate_\(depth)"
            let firstResult = try await evaluatePropertyPath(
                subject: subject,
                path: path1,
                object: .variable(intermediateVar),
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                depth: depth + 1
            )

            var allBindings: [VariableBinding] = []
            for binding in firstResult.bindings {
                // Get intermediate value as FieldValue (preserving type)
                guard let intermediateValue = binding[intermediateVar] else { continue }

                // Execute second path with intermediate as subject
                let secondResult = try await evaluatePropertyPath(
                    subject: .value(intermediateValue),
                    path: path2,
                    object: object,
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    transaction: transaction,
                    config: config,
                    depth: depth + 1
                )

                // Merge bindings
                for secondBinding in secondResult.bindings {
                    if let merged = binding.merged(with: secondBinding) {
                        // Remove intermediate variable from result
                        let projectionVars = merged.boundVariables.subtracting([intermediateVar])
                        let cleanBinding = merged.project(projectionVars)
                        allBindings.append(cleanBinding)
                    }
                }
            }

            return EvaluationResult(bindings: allBindings, stats: stats).mergedStats(with: firstResult.stats)

        case .alternative(let path1, let path2):
            // Union of both paths
            let result1 = try await evaluatePropertyPath(
                subject: subject,
                path: path1,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                depth: depth + 1
            )

            let result2 = try await evaluatePropertyPath(
                subject: subject,
                path: path2,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                depth: depth + 1
            )

            // Combine and deduplicate
            var seen = Set<VariableBinding>()
            var combined: [VariableBinding] = []
            for binding in result1.bindings + result2.bindings {
                if seen.insert(binding).inserted {
                    combined.append(binding)
                }
            }

            return EvaluationResult(bindings: combined, stats: stats)
                .mergedStats(with: result1.stats)
                .mergedStats(with: result2.stats)

        case .zeroOrMore(let innerPath):
            // Transitive closure including self-loop (0 hops)
            return try await evaluateTransitivePath(
                subject: subject,
                path: innerPath,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                includeZeroHop: true
            )

        case .oneOrMore(let innerPath):
            // Transitive closure (at least 1 hop)
            return try await evaluateTransitivePath(
                subject: subject,
                path: innerPath,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                includeZeroHop: false
            )

        case .zeroOrOne(let innerPath):
            // Union of zero-hop (self) and one-hop
            var results: [VariableBinding] = []
            var seen = Set<VariableBinding>()

            // Zero-hop: subject == object
            if case .variable(let subjVar) = subject, case .variable(let objVar) = object {
                // Both endpoints are unbound variables - generate identity bindings for all nodes
                // SPARQL 1.1: For p?, when both ?s and ?o are unbound, return (?s=x, ?o=x) for all nodes x
                let allNodes = try await getAllNodesForPropertyPath(
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    maxNodes: config.maxResults,
                    transaction: transaction
                )

                for node in allNodes {
                    if results.count >= config.maxResults { break }

                    var binding = VariableBinding()
                    binding = binding.binding(subjVar, to: node)
                    if subjVar != objVar {
                        binding = binding.binding(objVar, to: node)
                    }
                    if seen.insert(binding).inserted {
                        results.append(binding)
                    }
                }
            } else if case .value(let value) = subject, case .variable(let varName) = object {
                // Subject bound, object variable - include identity
                let identityBinding = VariableBinding().binding(varName, to: value)
                results.append(identityBinding)
                seen.insert(identityBinding)
            } else if case .variable(let varName) = subject, case .value(let value) = object {
                // Object bound, subject variable - include identity
                let identityBinding = VariableBinding().binding(varName, to: value)
                results.append(identityBinding)
                seen.insert(identityBinding)
            } else if case .value(let subjVal) = subject, case .value(let objVal) = object {
                // Both bound - check equality
                if subjVal == objVal {
                    results.append(VariableBinding())
                }
            }

            // One-hop
            let oneHopResult = try await evaluatePropertyPath(
                subject: subject,
                path: innerPath,
                object: object,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                config: config,
                depth: depth + 1
            )

            for binding in oneHopResult.bindings {
                if seen.insert(binding).inserted {
                    results.append(binding)
                }
            }

            return EvaluationResult(bindings: results, stats: stats).mergedStats(with: oneHopResult.stats)

        case .negatedPropertySet(let iris):
            // Match any edge NOT in the given set
            // This requires scanning all edges and filtering
            let irisSet = Set(iris)

            // Scan all edges from/to the subject/object
            var results: [VariableBinding] = []

            if case .value(let subjValue) = subject {
                // Scan outgoing edges from subject
                let scanResult = try await scanOutgoingEdges(
                    from: subjValue,
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    transaction: transaction
                )

                for (edge, target) in scanResult {
                    // Edge labels are strings in the graph model
                    guard case .string(let edgeStr) = edge else { continue }
                    if !irisSet.contains(edgeStr) {
                        var binding = VariableBinding()
                        if case .variable(let varName) = object {
                            binding = binding.binding(varName, to: target)
                        } else if case .value(let objValue) = object, objValue == target {
                            // Match
                        } else {
                            continue  // No match
                        }
                        results.append(binding)
                    }
                }
            }

            return EvaluationResult(bindings: results, stats: stats)
        }
    }

    /// Evaluate transitive closure paths (*, +)
    ///
    /// BFS exploration uses an unbound variable for each hop to discover
    /// intermediate nodes. When the original object is bound, results are
    /// filtered at the end. This ensures multi-hop paths are traversable.
    ///
    /// Reference: SPARQL 1.1, Section 9.1 (Property Paths)
    private func evaluateTransitivePath(
        subject: SPARQLTerm,
        path: PropertyPath,
        object: SPARQLTerm,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol,
        config: PropertyPathConfiguration,
        includeZeroHop: Bool
    ) async throws -> EvaluationResult {
        var stats = ExecutionStatistics()
        var allResults: [VariableBinding] = []
        var seen = Set<VariableBinding>()
        var visitedNodes = Set<FieldValue>()

        // Determine exploration variable:
        // If object is bound, use a temporary unbound variable for BFS exploration
        // so we can discover intermediate nodes, not just direct matches.
        let explorationObject: SPARQLTerm
        let explorationVarName: String
        if case .variable(let varName) = object {
            // Already unbound → use as-is
            explorationObject = object
            explorationVarName = varName
        } else {
            // Bound object → create temporary exploration variable
            let tempVar = "?_bfs_explore_"
            explorationObject = .variable(tempVar)
            explorationVarName = tempVar
        }

        // Handle zero-hop case for p* and p?
        if includeZeroHop {
            if case .variable(let subjVar) = subject, case .variable(let objVar) = object {
                // Both endpoints are unbound variables - generate identity bindings for all nodes
                // SPARQL 1.1: For p*, when both ?s and ?o are unbound, zero-hop returns (?s=x, ?o=x) for all nodes x
                let allNodes = try await getAllNodesForPropertyPath(
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    maxNodes: config.maxResults,
                    transaction: transaction
                )

                for node in allNodes {
                    if allResults.count >= config.maxResults { break }

                    var binding = VariableBinding()
                    binding = binding.binding(subjVar, to: node)
                    if subjVar != objVar {
                        binding = binding.binding(objVar, to: node)
                    }
                    if seen.insert(binding).inserted {
                        allResults.append(binding)
                    }
                }
            } else if case .value(let subjValue) = subject, case .value(let objValue) = object {
                // Both bound: zero-hop matches only if subject == object
                if subjValue == objValue {
                    let emptyBinding = VariableBinding()
                    if seen.insert(emptyBinding).inserted {
                        allResults.append(emptyBinding)
                    }
                }
            } else if case .value(let value) = subject, case .variable(let varName) = object {
                let zeroHopBinding = VariableBinding().binding(varName, to: value)
                if seen.insert(zeroHopBinding).inserted {
                    allResults.append(zeroHopBinding)
                }
            } else if case .variable(let varName) = subject, case .value(let value) = object {
                let zeroHopBinding = VariableBinding().binding(varName, to: value)
                if seen.insert(zeroHopBinding).inserted {
                    allResults.append(zeroHopBinding)
                }
            }
        }

        // BFS/iterative deepening for transitive closure
        var frontier: [SPARQLTerm] = [subject]
        var depth = 0

        while !frontier.isEmpty && depth < config.maxDepth && allResults.count < config.maxResults {
            var nextFrontier: [SPARQLTerm] = []
            depth += 1

            for currentSubject in frontier {
                // Skip if we've visited this node (cycle detection)
                if config.detectCycles, case .value(let nodeValue) = currentSubject {
                    if visitedNodes.contains(nodeValue) {
                        continue
                    }
                    visitedNodes.insert(nodeValue)
                }

                // Execute one hop with exploration variable (always unbound)
                // This discovers ALL reachable nodes, not just the target
                let result = try await evaluatePropertyPath(
                    subject: currentSubject,
                    path: path,
                    object: explorationObject,
                    indexSubspace: indexSubspace,
                    strategy: strategy,
                    transaction: transaction,
                    config: config,
                    depth: depth
                )

                for binding in result.bindings {
                    // Extract the reached node for frontier expansion
                    guard let reachedValue = binding[explorationVarName] else { continue }
                    nextFrontier.append(.value(reachedValue))

                    // Build result binding based on original object type
                    if case .value(let targetValue) = object {
                        // Object is bound → only emit result if reached the target
                        if reachedValue == targetValue {
                            // Construct binding with only the original variables (not exploration var)
                            var resultBinding = VariableBinding()
                            if case .variable(let subjVar) = subject {
                                if let subjVal = binding[subjVar] {
                                    resultBinding = resultBinding.binding(subjVar, to: subjVal)
                                } else if case .value(let currentVal) = currentSubject {
                                    // Subject was bound at start, propagate through BFS
                                    resultBinding = resultBinding.binding(subjVar, to: currentVal)
                                }
                            }
                            if seen.insert(resultBinding).inserted {
                                allResults.append(resultBinding)
                            }
                        }
                        // If not the target, don't add to results (but still explored via frontier)
                    } else {
                        // Object is a variable → all reached nodes are results
                        if seen.insert(binding).inserted {
                            allResults.append(binding)
                        }
                    }
                }

                stats = stats.merged(with: result.stats)
            }

            frontier = nextFrontier
        }

        return EvaluationResult(bindings: allResults, stats: stats)
    }

    /// Scan all outgoing edges from a node
    private func scanOutgoingEdges(
        from source: FieldValue,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionProtocol
    ) async throws -> [(edge: FieldValue, target: FieldValue)] {
        let ordering: GraphIndexOrdering = strategy == .adjacency ? .out : .spo
        let orderingSubspace = subspaceForOrdering(ordering, base: indexSubspace)

        // Build prefix for subject scan using typed TupleElement
        let prefix: [any TupleElement] = [source.toTupleElement()]

        let prefixKey = orderingSubspace.pack(Tuple(prefix))
        let prefixSubspace = Subspace(prefix: prefixKey)
        let range = prefixSubspace.range()

        var results: [(FieldValue, FieldValue)] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in stream {
            // Parse key to get edge and target
            guard let tuple = try? prefixSubspace.unpack(key) else {
                continue
            }

            // After unpacking with prefixSubspace, we get just (edge, to) or (predicate, object)
            // The source/subject was part of the prefix
            guard tuple.count >= 2,
                  let edgeElement = tuple[0],
                  let targetElement = tuple[1] else {
                continue
            }

            let edge = FieldValue(tupleElement: edgeElement)
                ?? .string(String(describing: edgeElement))
            let target = FieldValue(tupleElement: targetElement)
                ?? .string(String(describing: targetElement))

            results.append((edge, target))
        }

        return results
    }

    /// Get all unique nodes in the graph for Property Path evaluation
    ///
    /// Used when both subject and object are unbound variables (var, var case).
    /// Returns identity bindings for zero-hop paths.
    ///
    /// - Parameters:
    ///   - indexSubspace: The graph index subspace
    ///   - strategy: The graph index strategy
    ///   - maxNodes: Maximum number of nodes to return
    ///   - transaction: FDB transaction
    /// - Returns: Set of all unique node FieldValues
    private func getAllNodesForPropertyPath(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        maxNodes: Int,
        transaction: any TransactionProtocol
    ) async throws -> Set<FieldValue> {
        let scanner = GraphEdgeScanner(indexSubspace: indexSubspace, strategy: strategy)
        let stringNodes = try await scanner.getAllNodes(edgeLabel: nil, maxNodes: maxNodes, transaction: transaction)
        // Convert String node IDs to FieldValue at the boundary
        return Set(stringNodes.map { FieldValue.string($0) })
    }
}

// MARK: - ExecutionStatistics Extension

extension ExecutionStatistics {
    fileprivate func merged(with other: ExecutionStatistics) -> ExecutionStatistics {
        var result = self
        result.indexScans += other.indexScans
        result.joinOperations += other.joinOperations
        result.intermediateResults += other.intermediateResults
        result.patternsEvaluated += other.patternsEvaluated
        result.optionalMisses += other.optionalMisses
        return result
    }
}

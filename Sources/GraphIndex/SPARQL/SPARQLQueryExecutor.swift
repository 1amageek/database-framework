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
                for rightBinding in rightResult.bindings {
                    if let merged = leftBinding.merged(with: rightBinding) {
                        results.append(merged)
                    }
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
            guard term.isBound, case .value(let value) = term else {
                break  // Stop at first unbound term
            }
            prefixElements.append(value)
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
        var fromValue: String?
        var edgeValue: String?
        var toValue: String?

        for (tupleIdx, componentIdx) in elementOrder.enumerated() {
            guard tupleIdx < tuple.count, let element = tuple[tupleIdx] else {
                continue
            }

            let stringValue: String
            if let str = element as? String {
                stringValue = str
            } else {
                stringValue = String(describing: element)
            }

            switch componentIdx {
            case 0: fromValue = stringValue
            case 1: edgeValue = stringValue
            case 2: toValue = stringValue
            default: break
            }
        }

        guard let from = fromValue, let edge = edgeValue, let to = toValue else {
            return nil
        }

        // Check if values match bound terms
        if case .value(let v) = pattern.subject, v != from { return nil }
        if case .value(let v) = pattern.predicate, v != edge { return nil }
        if case .value(let v) = pattern.object, v != to { return nil }

        // Build binding from variables
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
        }
    }

    // MARK: - Helpers

    private func buildIndexName() -> String {
        "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"
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

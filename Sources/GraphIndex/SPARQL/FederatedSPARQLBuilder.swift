// FederatedSPARQLBuilder.swift
// GraphIndex - Graph-scoped federated SPARQL query builder
//
// Type-erased counterpart of `SPARQLQueryBuilder<T>`. Evaluates a SPARQL
// pattern against the union of all triple-producing indexes bound to a named
// graph — OWL materialized individuals (per-type OWLTripleIndexKind subspaces)
// plus free-form GraphIndexKind tables with a graph column.
//
// Design highlights:
// - `TripleSourcePlanner` enumerates and statically prunes candidate sources
//   using BGP predicate and subject-IRI analysis, so typical recall queries
//   (`?e rdfs:label ?l FILTER contains(?l, ...)`) fan out to only the handful
//   of OWL types that actually declare `rdfs:label`.
// - Multi-source execution runs per-source `SPARQLQueryExecutor` calls in a
//   single shared transaction via `TaskGroup`, giving snapshot isolation and
//   parallel I/O with no pre-compute barrier.
// - LIMIT is pushed down to each executor whenever it's safe (no DISTINCT and
//   no ORDER BY); the builder still applies the final LIMIT/OFFSET at the
//   union level because we conservatively oversample.
// - Single-source queries take a fast path that bypasses the union machinery.

import Foundation
import Core
import DatabaseEngine
import Graph
import StorageKit

/// Fluent builder for graph-scoped federated SPARQL queries.
///
/// Prefer this over `sparql(T.self)` when the query is graph-scoped rather
/// than type-scoped — e.g., memory recall over a knowledge graph that spans
/// multiple entity types.
public struct FederatedSPARQLBuilder: Sendable {

    // MARK: - Configuration

    private let queryContext: IndexQueryContext
    private let graph: String

    // MARK: - Query State

    private var graphPattern: ExecutionPattern
    private var projectedVariables: [String]?
    private var limitCount: Int?
    private var offsetCount: Int
    private var isDistinct: Bool
    private var sortKeys: [BindingSortKey]

    // MARK: - Initialization

    internal init(queryContext: IndexQueryContext, graph: String) {
        self.queryContext = queryContext
        self.graph = graph
        self.graphPattern = .basic([])
        self.projectedVariables = nil
        self.limitCount = nil
        self.offsetCount = 0
        self.isDistinct = false
        self.sortKeys = []
    }

    // MARK: - Pattern Building

    /// Add a triple pattern to the WHERE clause.
    ///
    /// Strings starting with "?" are interpreted as variables.
    public func `where`(
        _ subject: String,
        _ predicate: String,
        _ object: String
    ) -> Self {
        `where`(
            ExecutionTerm(stringLiteral: subject),
            ExecutionTerm(stringLiteral: predicate),
            ExecutionTerm(stringLiteral: object)
        )
    }

    /// Add a triple pattern using ExecutionTerm values.
    public func `where`(
        _ subject: ExecutionTerm,
        _ predicate: ExecutionTerm,
        _ object: ExecutionTerm
    ) -> Self {
        var copy = self
        let triple = ExecutionTriple(subject: subject, predicate: predicate, object: object)
        switch copy.graphPattern {
        case .basic(var triples):
            triples.append(triple)
            copy.graphPattern = .basic(triples)
        default:
            copy.graphPattern = .join(copy.graphPattern, .basic([triple]))
        }
        return copy
    }

    /// Add a property path pattern to the WHERE clause.
    public func wherePath(
        _ subject: String,
        path: ExecutionPropertyPath,
        _ object: String
    ) -> Self {
        wherePath(
            ExecutionTerm(stringLiteral: subject),
            path: path,
            ExecutionTerm(stringLiteral: object)
        )
    }

    /// Add a property path pattern using ExecutionTerm values.
    public func wherePath(
        _ subject: ExecutionTerm,
        path: ExecutionPropertyPath,
        _ object: ExecutionTerm
    ) -> Self {
        var copy = self
        let pathPattern = ExecutionPattern.propertyPath(subject: subject, path: path, object: object)
        switch copy.graphPattern {
        case .basic(let triples) where triples.isEmpty:
            copy.graphPattern = pathPattern
        default:
            copy.graphPattern = .join(copy.graphPattern, pathPattern)
        }
        return copy
    }

    // MARK: - OPTIONAL / UNION

    public func optional(
        _ configure: (FederatedSPARQLBuilder) -> FederatedSPARQLBuilder
    ) -> Self {
        var copy = self
        let inner = configure(FederatedSPARQLBuilder(queryContext: queryContext, graph: graph))
        copy.graphPattern = .optional(copy.graphPattern, inner.graphPattern)
        return copy
    }

    public func union(
        _ configure: (FederatedSPARQLBuilder) -> FederatedSPARQLBuilder
    ) -> Self {
        var copy = self
        let inner = configure(FederatedSPARQLBuilder(queryContext: queryContext, graph: graph))
        copy.graphPattern = .union(copy.graphPattern, inner.graphPattern)
        return copy
    }

    // MARK: - FILTER

    public func filter(_ expression: FilterExpression) -> Self {
        var copy = self
        copy.graphPattern = .filter(copy.graphPattern, expression)
        return copy
    }

    public func filter(_ variable: String, equals value: String) -> Self {
        filter(.equals(variable, .string(value)))
    }

    public func filter(_ variable: String, notEquals value: String) -> Self {
        filter(.notEquals(variable, .string(value)))
    }

    public func filter(_ variable: String, matches regex: String) -> Self {
        filter(.regex(variable, regex))
    }

    public func filter(_ variable: String, contains substring: String) -> Self {
        filter(.contains(variable, substring))
    }

    public func filter(_ variable: String, startsWith prefix: String) -> Self {
        filter(.startsWith(variable, prefix))
    }

    public func filter(_ variable: String, endsWith suffix: String) -> Self {
        filter(.endsWith(variable, suffix))
    }

    public func filter(_ variable: String, similarTo pattern: String, threshold: Double = 0.45) -> Self {
        filter(.similarTo(variable, pattern, threshold))
    }

    public func filterBound(_ variable: String) -> Self {
        filter(.bound(variable))
    }

    public func filterNotBound(_ variable: String) -> Self {
        filter(.notBound(variable))
    }

    public func filter(_ variable1: String, equalsVariable variable2: String) -> Self {
        filter(.variableEquals(variable1, variable2))
    }

    public func filter(_ variable1: String, notEqualsVariable variable2: String) -> Self {
        filter(.variableNotEquals(variable1, variable2))
    }

    // MARK: - Projection / Modifiers

    public func select(_ variables: String...) -> Self {
        var copy = self
        copy.projectedVariables = variables
        return copy
    }

    public func select(_ variables: [String]) -> Self {
        var copy = self
        copy.projectedVariables = variables
        return copy
    }

    public func distinct(_ enabled: Bool = true) -> Self {
        var copy = self
        copy.isDistinct = enabled
        return copy
    }

    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    public func offset(_ count: Int) -> Self {
        var copy = self
        copy.offsetCount = count
        return copy
    }

    public func orderBy(_ variable: String, ascending: Bool = true) -> Self {
        var copy = self
        copy.sortKeys.append(.variable(variable, ascending: ascending))
        return copy
    }

    public func orderByDesc(_ variable: String) -> Self {
        orderBy(variable, ascending: false)
    }

    // MARK: - Execution

    /// Execute the query, unioning results from every candidate `TripleSource`.
    ///
    /// Follows W3C SPARQL 1.1 §15 execution order: pattern → ORDER BY →
    /// projection → DISTINCT → OFFSET/LIMIT.
    public func execute() async throws -> SPARQLResult {
        if graphPattern.isEmpty {
            throw SPARQLQueryError.noPatterns
        }

        let sources = try await TripleSourcePlanner.plan(
            pattern: graphPattern,
            graph: graph,
            queryContext: queryContext
        )

        let startTime = DispatchTime.now()
        let projectedVars = resolveProjection()

        if sources.isEmpty {
            return SPARQLResult(
                bindings: [],
                projectedVariables: projectedVars,
                isComplete: true,
                limitReason: nil,
                statistics: ExecutionStatistics(durationNs: elapsed(since: startTime))
            )
        }

        let (bindings, stats) = try await evaluate(sources: sources)

        return finalize(
            bindings: bindings,
            stats: stats,
            projectedVars: projectedVars,
            startTime: startTime
        )
    }

    /// Execute and return just the first result (or nil).
    public func first() async throws -> VariableBinding? {
        try await limit(1).execute().bindings.first
    }

    /// Execute and return the total count.
    public func count() async throws -> Int {
        try await execute().count
    }

    /// Check if any results exist.
    public func exists() async throws -> Bool {
        try await first() != nil
    }

    // MARK: - Query Info

    public var variables: Set<String> {
        graphPattern.variables
    }

    public var pattern: ExecutionPattern {
        graphPattern
    }

    // MARK: - Internals

    private func resolveProjection() -> [String] {
        if let projectedVariables {
            return projectedVariables
        }
        return Array(graphPattern.variables).sorted()
    }

    /// Run each source's executor in parallel under a shared transaction and
    /// return the concatenated bindings and merged statistics.
    private func evaluate(
        sources: [TripleSource]
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let engine = queryContext.context.container.engine
        let pattern = graphPattern
        let graph = graph
        let hasOrderBy = !sortKeys.isEmpty
        let needsAllResults = hasOrderBy || isDistinct
        let pushdownLimit: Int? = needsAllResults ? nil : limitCount.map { $0 + offsetCount }

        return try await engine.withTransaction { sharedTxn in
            if sources.count == 1 {
                let source = sources[0]
                let executor = Self.makeExecutor(
                    for: source,
                    graph: graph,
                    engine: engine
                )
                return try await executor.executeInTransaction(
                    pattern: pattern,
                    transaction: sharedTxn,
                    limit: pushdownLimit,
                    offset: 0
                )
            }

            return try await withThrowingTaskGroup(
                of: ([VariableBinding], ExecutionStatistics).self
            ) { group in
                for source in sources {
                    group.addTask {
                        let executor = Self.makeExecutor(
                            for: source,
                            graph: graph,
                            engine: engine
                        )
                        return try await executor.executeInTransaction(
                            pattern: pattern,
                            transaction: sharedTxn,
                            limit: pushdownLimit,
                            offset: 0
                        )
                    }
                }

                var allBindings: [VariableBinding] = []
                var mergedStats = ExecutionStatistics()
                for try await (bindings, stats) in group {
                    allBindings.append(contentsOf: bindings)
                    Self.merge(&mergedStats, with: stats)
                }
                return (allBindings, mergedStats)
            }
        }
    }

    private static func makeExecutor(
        for source: TripleSource,
        graph: String,
        engine: any StorageEngine
    ) -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            database: engine,
            indexSubspace: source.indexSubspace,
            strategy: source.strategy,
            fromFieldName: source.fromField.isEmpty ? "subject" : source.fromField,
            edgeFieldName: source.edgeField.isEmpty ? "predicate" : source.edgeField,
            toFieldName: source.toField.isEmpty ? "object" : source.toField,
            graphFieldName: source.graphFieldName,
            storedFieldNames: source.storedFieldNames,
            ontologyContext: nil,
            defaultGraph: source.graphFieldName != nil ? graph : nil
        )
    }

    private static func merge(_ base: inout ExecutionStatistics, with other: ExecutionStatistics) {
        base.indexScans += other.indexScans
        base.joinOperations += other.joinOperations
        base.intermediateResults += other.intermediateResults
        base.patternsEvaluated += other.patternsEvaluated
        base.optionalMisses += other.optionalMisses
        base.joinStrategies.append(contentsOf: other.joinStrategies)
        base.joinFallbackReasons.append(contentsOf: other.joinFallbackReasons)
    }

    private func finalize(
        bindings: [VariableBinding],
        stats: ExecutionStatistics,
        projectedVars: [String],
        startTime: DispatchTime
    ) -> SPARQLResult {
        var ordered = bindings

        if !sortKeys.isEmpty {
            ordered = BindingSorter.sort(ordered, by: sortKeys)
        }

        let projectionSet = Set(projectedVars)
        var projected = ordered.map { $0.project(projectionSet) }

        if isDistinct {
            var seen = Set<VariableBinding>()
            projected = projected.filter { seen.insert($0).inserted }
        }

        if offsetCount > 0 {
            projected = Array(projected.dropFirst(offsetCount))
        }
        if let limitCount {
            projected = Array(projected.prefix(limitCount))
        }

        var finalStats = stats
        finalStats.durationNs = elapsed(since: startTime)

        let resultCount = projected.count
        let isComplete = limitCount == nil || resultCount < limitCount!
        let limitReason: SPARQLLimitReason? = (limitCount != nil && resultCount >= limitCount!)
            ? .explicitLimit
            : nil

        return SPARQLResult(
            bindings: projected,
            projectedVariables: projectedVars,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: finalStats
        )
    }

    private func elapsed(since start: DispatchTime) -> UInt64 {
        DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
    }
}

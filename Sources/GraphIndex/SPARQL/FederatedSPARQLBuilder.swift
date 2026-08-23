// FederatedSPARQLBuilder.swift
// GraphIndex - Graph-scoped federated SPARQL query builder
//
// Type-erased counterpart of `SPARQLQueryBuilder<T>`. Evaluates a SPARQL
// pattern against the union of all triple-producing indexes bound to a named
// graph — OWL class projections plus canonical RDF quad indexes.
//
// Atomic triple scans are evaluated against the union of every participating
// RDF source. This permits joins and property paths to cross physical indexes
// while retaining one transaction snapshot and one active named graph.

import DatabaseKit
import DatabaseEngine
import StorageKit

/// Fluent builder for graph-scoped federated SPARQL queries.
///
/// Prefer this over `sparql(T.self)` when the query is graph-scoped rather
/// than type-scoped — e.g., memory recall over a knowledge graph that spans
/// multiple entity types.
public struct FederatedSPARQLBuilder: Sendable {

    // MARK: - Configuration

    private let queryContext: IndexQueryContext
    private let namedGraph: RDFGraphName

    // MARK: - Query State

    private var graphPattern: ExecutionPattern
    private var projectedVariables: [String]?
    private var limitCount: Int?
    private var offsetCount: Int
    private var isDistinct: Bool
    private var sortKeys: [BindingSortKey]

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        namedGraph: RDFGraphName
    ) {
        self.queryContext = queryContext
        self.namedGraph = namedGraph
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
        let inner = configure(
            FederatedSPARQLBuilder(
                queryContext: queryContext,
                namedGraph: namedGraph
            )
        )
        copy.graphPattern = .optional(copy.graphPattern, inner.graphPattern)
        return copy
    }

    public func union(
        _ configure: (FederatedSPARQLBuilder) -> FederatedSPARQLBuilder
    ) -> Self {
        var copy = self
        let inner = configure(
            FederatedSPARQLBuilder(
                queryContext: queryContext,
                namedGraph: namedGraph
            )
        )
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
        filter(.equals(variable, .rdfTerm(.string(value))))
    }

    public func filter(_ variable: String, notEquals value: String) -> Self {
        filter(.notEquals(variable, .rdfTerm(.string(value))))
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

    /// Execute the query against one logical RDF dataset.
    ///
    /// Follows W3C SPARQL 1.1 §15 execution order: pattern → ORDER BY →
    /// projection → DISTINCT → OFFSET/LIMIT.
    public func execute(
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        guard offsetCount >= 0, limitCount.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let startTime = queryContext.graphClock.now()
        let projectedVars = resolveProjection()
        let workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: queryContext.context.container.monotonicClock
        )

        let evaluation = try await queryContext.withTransaction {
            transaction -> ([VariableBinding], ExecutionStatistics)? in
            let sources = try await RDFDatasetSourcePlanner.plan(
                namedGraph: namedGraph,
                queryContext: queryContext,
                transaction: transaction
            )
            guard !sources.isEmpty else {
                return nil
            }
            return try await evaluate(
                sources: sources,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        guard let (bindings, stats) = evaluation else {
            return SPARQLResult(
                bindings: [],
                projectedVariables: projectedVars,
                isComplete: true,
                limitReason: nil,
                statistics: ExecutionStatistics(durationNs: elapsed(since: startTime))
            )
        }

        let result = try finalize(
            bindings: bindings,
            stats: stats,
            projectedVars: projectedVars,
            startTime: startTime,
            workMeter: workMeter
        )
        guard let outputRows = UInt32(exactly: result.bindings.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(outputRows)
        return result
    }

    /// Execute and return just the first result (or nil).
    public func first(
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> VariableBinding? {
        try await limit(1).execute(budget: budget).bindings.first
    }

    /// Execute and return the total count.
    public func count(
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> Int {
        try await execute(budget: budget).count
    }

    /// Check if any results exist.
    public func exists(
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> Bool {
        try await first(budget: budget) != nil
    }

    // MARK: - Query Info

    public var variables: Set<String> {
        graphPattern.outputVariables
    }

    public var pattern: ExecutionPattern {
        graphPattern
    }

    // MARK: - Internals

    private func resolveProjection() -> [String] {
        if let projectedVariables {
            return projectedVariables
        }
        return Array(graphPattern.outputVariables).sorted()
    }

    /// Evaluate algebra once while atomic scans fan out across all sources.
    private func evaluate(
        sources: [RDFDatasetSource],
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let engine = queryContext.context.container.engine
        let pattern = ExecutionPattern.graph(
            .named(namedGraph),
            graphPattern
        )
        let hasOrderBy = !sortKeys.isEmpty
        let needsAllResults = hasOrderBy || isDistinct
        let pushdownLimit: Int?
        if needsAllResults {
            pushdownLimit = nil
        } else if let limitCount {
            let (combined, overflow) = limitCount.addingReportingOverflow(
                offsetCount
            )
            guard !overflow else {
                throw SPARQLQueryError.invalidPagination
            }
            pushdownLimit = combined
        } else {
            pushdownLimit = nil
        }

        let executor = SPARQLQueryExecutor(
            database: engine,
            monotonicClock: queryContext.context.container.monotonicClock,
            wallClock: queryContext.context.container.wallClock,
            sources: sources
        )
        return try await executor.executeInTransaction(
            pattern: pattern,
            transaction: transaction,
            limit: pushdownLimit,
            offset: 0,
            workMeter: workMeter
        )
    }

    private func finalize(
        bindings: [VariableBinding],
        stats: ExecutionStatistics,
        projectedVars: [String],
        startTime: MonotonicTimestamp,
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLResult {
        var ordered = bindings

        if !sortKeys.isEmpty {
            ordered = try BindingSorter.sort(
                ordered,
                by: sortKeys,
                workMeter: workMeter
            )
        }

        let projectionSet = Set(projectedVars)
        var projected = try ordered.map { binding in
            try workMeter.consume(at: .projection)
            return binding.project(projectionSet)
        }

        if isDistinct {
            var seen = Set<VariableBinding>()
            projected = try projected.filter { binding in
                try workMeter.consume(at: .deduplication)
                return seen.insert(binding).inserted
            }
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
        let reachedLimit = limitCount.map { resultCount >= $0 } ?? false
        let isComplete = !reachedLimit
        let limitReason: SPARQLLimitReason? = reachedLimit ? .explicitLimit : nil

        return SPARQLResult(
            bindings: projected,
            projectedVariables: projectedVars,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: finalStats
        )
    }

    private func elapsed(since start: MonotonicTimestamp) -> UInt64 {
        queryContext.graphClock.now().uptimeNanoseconds - start.uptimeNanoseconds
    }
}

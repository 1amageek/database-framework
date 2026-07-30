// SPARQLEntryPoint.swift
// GraphIndex - Entry point for SPARQL-like queries
//
// Provides the DatabaseContext extension and entry point for SPARQL queries.

import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import DatabaseWire
import StorageKit

// MARK: - SPARQL Entry Point

/// Entry point for SPARQL-like queries on graph indexes
///
/// **Usage**:
/// ```swift
/// // Using explicit index fields
/// let results = try await context.sparql(Statement.self)
///     .index(\.subject, \.predicate, \.object)
///     .where("?person", "knows", "Alice")
///     .execute()
///
/// // Using default index
/// let results = try await context.sparql(Statement.self)
///     .defaultIndex()
///     .where("Alice", "knows", "?friend")
///     .select("?friend")
///     .execute()
/// ```
public struct SPARQLEntryPoint<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    // MARK: - Index Specification

    /// Specify the graph index fields
    ///
    /// - Parameters:
    ///   - from: KeyPath to the source/subject field
    ///   - edge: KeyPath to the edge/predicate field
    ///   - to: KeyPath to the target/object field
    /// - Returns: SPARQL query builder
    public func index(
        _ subject: Field<T, RDFTerm>,
        _ predicate: Field<T, RDFTerm>,
        _ object: Field<T, RDFTerm>
    ) throws -> SPARQLQueryBuilder<T> {
        let subjectField = subject.name
        let predicateField = predicate.name
        let objectField = object.name
        let matches = try T.indexDescriptors.compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
            .filter {
                $0.metadata.subjectFieldName == subjectField
                    && $0.metadata.predicateFieldName == predicateField
                    && $0.metadata.objectFieldName == objectField
            }
        return SPARQLQueryBuilder(
            queryContext: queryContext,
            selection: matches.count == 1 ? matches[0] : nil
        )
    }

    /// Use the only RDF dataset index declared by this entity type.
    ///
    /// - Returns: SPARQL query builder configured with the default index
    public func defaultIndex() throws -> SPARQLQueryBuilder<T> {
        let candidates = try T.indexDescriptors.compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        return SPARQLQueryBuilder(
            queryContext: queryContext,
            selection: candidates.count == 1 ? candidates[0] : nil
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Start a SPARQL-like graph query
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Find all people Alice knows
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("Alice", "knows", "?friend")
    ///     .select("?friend")
    ///     .execute()
    ///
    /// // Multi-pattern query with join
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("?person", "knows", "Bob")
    ///     .where("?person", "name", "?name")
    ///     .select("?person", "?name")
    ///     .execute()
    ///
    /// // Friends of friends
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("Alice", "knows", "?friend")
    ///     .where("?friend", "knows", "?fof")
    ///     .filter(.variableNotEquals("?fof", "Alice"))
    ///     .select("?fof")
    ///     .distinct()
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the SPARQL query
    public func sparql<T: Persistable>(_ type: T.Type) -> SPARQLEntryPoint<T> {
        SPARQLEntryPoint(queryContext: indexQueryContext)
    }

    /// Start a graph-scoped SPARQL query that unions all canonical RDF indexes
    /// bound to a validated named graph.
    ///
    /// Prefer this API over `sparql(T.self)` when the query is graph-scoped
    /// rather than type-scoped. OWL class projections whose fixed graph
    /// matches and RDF quad indexes with a graph field participate in the
    /// union.
    ///
    /// **Usage**:
    /// ```swift
    /// let graph = try RDFGraphName(iri: "https://example.com/graphs/memory")
    /// let result = try await context.sparql(namedGraph: graph)
    ///     .where(
    ///         .variable("?entity"),
    ///         .value(.rdfTerm(.iri("http://www.w3.org/2000/01/rdf-schema#label"))),
    ///         .variable("?label")
    ///     )
    ///     .select("?entity")
    ///     .execute()
    /// ```
    ///
    /// - Parameter namedGraph: Validated RDF graph name used to scope the query.
    /// - Returns: A `FederatedSPARQLBuilder` ready to accept patterns.
    public func sparql(namedGraph: RDFGraphName) -> FederatedSPARQLBuilder {
        FederatedSPARQLBuilder(
            queryContext: indexQueryContext,
            namedGraph: namedGraph
        )
    }

    /// Execute a pre-built ExecutionPattern directly
    ///
    /// Used by higher-level modules (e.g., Database) that convert parsed
    /// SPARQL or SQL queries into ExecutionPattern before execution.
    ///
    /// Follows W3C SPARQL 1.1 Section 15 execution order:
    /// 1. Pattern evaluation (WHERE + GROUP BY / HAVING)
    /// 2. ORDER BY
    /// 3. Projection (SELECT)
    /// 4. DISTINCT / REDUCED
    /// 5. OFFSET / LIMIT (Slice)
    ///
    /// - Parameters:
    ///   - pattern: The pre-built execution pattern to evaluate
    ///   - type: The Persistable type to query against
    ///   - projection: Variables to include in results (nil = all)
    ///   - distinct: Whether to deduplicate results
    ///   - limit: Maximum number of results
    ///   - offset: Number of results to skip
    ///   - orderBy: Sort keys for ORDER BY (empty = no sorting)
    /// - Returns: Query results
    public func executeSPARQLPattern<T: Persistable>(
        _ pattern: ExecutionPattern,
        on type: T.Type,
        projection: [String]? = nil,
        distinct: Bool = false,
        limit: Int? = nil,
        offset: Int = 0,
        orderBy: [BindingSortKey] = [],
        datasetScope: SPARQLDatasetExecutionScope = .implicit,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        guard offset >= 0, limit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let candidates = try T.indexDescriptors.compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        guard candidates.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = candidates[0]

        let typeSubspace = try await indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(selection.indexName)
        let source = try RDFDatasetSource(
            entityName: T.persistableType,
            selection: selection,
            indexSubspace: indexSubspace
        )

        let executor = SPARQLQueryExecutor(
            database: container.engine,
            wallClock: container.wallClock,
            sources: [source],
            datasetScope: datasetScope
        )
        let workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: container.monotonicClock
        )

        // Determine projected variables
        let projectedVars: [String]
        if let projection = projection {
            // User specified projection: use as-is (do not auto-add property variables)
            projectedVars = projection
        } else {
            // No projection: include pattern variables + property variables (SELECT * equivalent)
            var allVariables = pattern.outputVariables
            for fieldName in selection.storedFieldNames {
                allVariables.insert("?\(fieldName)")
            }
            projectedVars = Array(allVariables).sorted()
        }
        let startTime = indexQueryContext.graphClock.now()

        let hasOrderBy = !orderBy.isEmpty
        let needsAllResults = hasOrderBy || distinct

        // Step 1: Pattern evaluation (WHERE + GROUP BY / HAVING)
        var (bindings, stats) = try await executor.execute(
            pattern: pattern,
            limit: needsAllResults ? nil : limit,
            offset: needsAllResults ? 0 : offset,
            workMeter: workMeter
        )

        // Step 2: ORDER BY (before projection, per W3C Section 15)
        if hasOrderBy {
            bindings = try BindingSorter.sort(
                bindings,
                by: orderBy,
                workMeter: workMeter
            )
        }

        // Step 3: Projection (SELECT)
        let projectionSet = Set(projectedVars)
        var projected = try bindings.map { binding in
            try workMeter.consume(at: .projection)
            return binding.project(projectionSet)
        }

        // Step 4: DISTINCT
        if distinct {
            var seen = Set<VariableBinding>()
            projected = try projected.filter { binding in
                try workMeter.consume(at: .deduplication)
                return seen.insert(binding).inserted
            }
        }

        // Step 5: OFFSET / LIMIT (Slice)
        if needsAllResults {
            if offset > 0 {
                projected = Array(projected.dropFirst(offset))
            }
            if let lim = limit {
                projected = Array(projected.prefix(lim))
            }
        }

        let endTime = indexQueryContext.graphClock.now()
        stats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        let resultCount = projected.count
        let reachedLimit = limit.map { resultCount >= $0 } ?? false
        let isComplete = !reachedLimit
        let limitReason: SPARQLLimitReason? = reachedLimit ? .explicitLimit : nil

        guard let outputRows = UInt32(exactly: resultCount) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(outputRows)

        return SPARQLResult(
            bindings: projected,
            projectedVariables: projectedVars,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: stats
        )
    }

    public func executeSPARQLSelectPlan<T: Persistable>(
        _ plan: SPARQLSelectExecutionPlan,
        on type: T.Type,
        datasetScope: SPARQLDatasetExecutionScope = .implicit,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        let candidates = try T.indexDescriptors.compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        guard candidates.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = candidates[0]
        let typeSubspace = try await indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(selection.indexName)
        let source = try RDFDatasetSource(
            entityName: T.persistableType,
            selection: selection,
            indexSubspace: indexSubspace
        )
        let executor = SPARQLQueryExecutor(
            database: container.engine,
            wallClock: container.wallClock,
            sources: [source],
            datasetScope: datasetScope
        )
        let workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: container.monotonicClock
        )
        let startTime = indexQueryContext.graphClock.now()
        var (bindings, statistics) = try await executor.execute(
            selectPlan: plan,
            workMeter: workMeter
        )
        statistics.durationNs = indexQueryContext.graphClock.now().uptimeNanoseconds
            - startTime.uptimeNanoseconds

        guard let outputRows = UInt32(exactly: bindings.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(outputRows)
        let reachedLimit = plan.slice.limit.map {
            bindings.count >= $0
        } ?? false
        return SPARQLResult(
            bindings: consume bindings,
            projectedVariables: plan.projectionVariables,
            isComplete: !reachedLimit,
            limitReason: reachedLimit ? .explicitLimit : nil,
            statistics: statistics
        )
    }
}

// MARK: - SPARQL Query Error

/// Errors for SPARQL query operations
public enum SPARQLQueryError: Error, CustomStringConvertible {
    /// Index not configured
    case indexNotConfigured

    /// Index not found
    case indexNotFound(String)

    /// Invalid pattern
    case invalidPattern(String)

    /// Execution failed
    case executionFailed(String)

    /// Variable conflict in join
    case variableConflict(variable: String, existingValue: String, newValue: String)

    /// No patterns specified
    case noPatterns

    /// Invalid GROUP BY
    case invalidGroupBy(String)

    /// A runtime-builder variable does not use canonical SPARQL syntax.
    case invalidVariable(String)

    /// OFFSET and LIMIT must be non-negative.
    case invalidPagination

    /// An aggregate result cannot be represented by its canonical value type.
    case aggregateResultOutOfRange

    /// A bound SPARQL term is not represented by a canonical RDF term.
    case invalidRDFTerm(String)

    /// A GRAPH variable is bound to a value that cannot name an RDF graph.
    case invalidGraphBinding(String)

    /// Ontology data exposed a predicate that is not an absolute RDF IRI.
    case invalidOntologyPredicateIRI(String)

    /// Property-path expression nesting exceeded the configured limit.
    case propertyPathExpressionDepthLimitExceeded(maximum: Int)

    /// Recursive property-path traversal exceeded the configured graph depth.
    case propertyPathTraversalDepthLimitExceeded(maximum: Int)

    /// Property-path evaluation exceeded the configured result budget.
    case propertyPathResultLimitExceeded(maximum: Int)

    /// Property-path execution limits must be non-negative.
    case invalidPropertyPathConfiguration(
        maximumExpressionDepth: Int,
        maximumTraversalDepth: Int,
        maximumResults: Int
    )

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .invalidPattern(let reason):
            return "Invalid query pattern: \(reason)"
        case .executionFailed(let reason):
            return "Query execution failed: \(reason)"
        case .variableConflict(let variable, let existing, let new):
            return "Variable \(variable) conflict: existing='\(existing)', new='\(new)'"
        case .noPatterns:
            return "No patterns specified in query"
        case .invalidGroupBy(let reason):
            return "Invalid GROUP BY: \(reason)"
        case .invalidVariable(let variable):
            return "Invalid SPARQL variable: \(variable)"
        case .invalidPagination:
            return "SPARQL OFFSET and LIMIT must be non-negative"
        case .aggregateResultOutOfRange:
            return "SPARQL aggregate result is outside the canonical value range"
        case .invalidRDFTerm(let value):
            return "Expected a canonical RDF term, got: \(value)"
        case .invalidGraphBinding(let variable):
            return "GRAPH variable is not bound to a valid graph name: \(variable)"
        case .invalidOntologyPredicateIRI(let value):
            return "Ontology predicate is not an absolute RDF IRI: \(value)"
        case .propertyPathExpressionDepthLimitExceeded(let maximum):
            return "Property path expression depth limit exceeded: \(maximum)"
        case .propertyPathTraversalDepthLimitExceeded(let maximum):
            return "Property path traversal depth limit exceeded: \(maximum)"
        case .propertyPathResultLimitExceeded(let maximum):
            return "Property path result limit exceeded: \(maximum)"
        case .invalidPropertyPathConfiguration(
            let expressionDepth,
            let traversalDepth,
            let results
        ):
            return "Invalid property path configuration: maximumExpressionDepth=\(expressionDepth), maximumTraversalDepth=\(traversalDepth), maximumResults=\(results)"
        }
    }
}

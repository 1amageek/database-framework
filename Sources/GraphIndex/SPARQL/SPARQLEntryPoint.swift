// SPARQLEntryPoint.swift
// GraphIndex - Entry point for SPARQL-like queries
//
// Provides the DatabaseContext extension and entry point for SPARQL queries.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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

    /// Bind this query to one dynamic entity partition.
    public func partition<Value: Sendable & FieldValueRepresentable>(
        _ field: Field<T, Value>,
        equals value: Value
    ) throws -> SPARQLEntryPoint<T> {
        SPARQLEntryPoint(
            queryContext: try queryContext.withPartition(
                field,
                equals: value
            )
        )
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
        let matches = try queryContext.indexDescriptors(
            for: T.self
        ).compactMap {
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
        let candidates = try queryContext.indexDescriptors(
            for: T.self
        ).compactMap {
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
        dataset: SPARQLExecutionDataset = .implicit,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        guard offset >= 0, limit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let candidates = try indexQueryContext.indexDescriptors(
            for: T.self
        ).compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        guard candidates.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = candidates[0]

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
            for fieldName in selection.includedFieldNames {
                allVariables.insert("?\(fieldName)")
            }
            projectedVars = Array(allVariables).sorted()
        }
        let startTime = indexQueryContext.graphClock.now()

        let possibleBindingVariables = pattern.outputVariables.union(
            selection.includedFieldNames.map { "?\($0)" }
        )
        let projectionIsIdentity = possibleBindingVariables.isSubset(
            of: Set(projectedVars)
        )

        let executionResult = try await indexQueryContext
            .withReadableIndex(
                named: selection.indexName,
                indexType: selection.indexType,
                for: T.self
            ) {
                readableIndex,
                transaction -> SPARQLResult in
                let sources: [RDFDatasetSource]
                if let readableIndex {
                    sources = [
                        try RDFDatasetSource(
                            entityName: T.persistableType,
                            selection: selection,
                            indexSubspace: readableIndex.subspace
                        )
                    ]
                } else {
                    sources = []
                }
                let executor = SPARQLQueryExecutor(
                    monotonicClock: self.container.monotonicClock,
                    wallClock: self.container.wallClock,
                    datasetScanner: IndexedRDFDatasetScanner(
                        sources: sources
                    ),
                    dataset: dataset
                )
                let retained = try await executor
                    .executeRetainedProjectedInTransaction(
                    pattern: pattern,
                    transaction: transaction,
                    orderBy: orderBy,
                    projectionVariables: projectedVars,
                    projectionIsIdentity: projectionIsIdentity,
                    duplicatePolicy: distinct ? .distinct : .preserve,
                    offset: offset,
                    limit: limit,
                    workMeter: workMeter
                )
                return retained.promoteToResult()
            }

        let executionStats = executionResult.statistics
        let isComplete = executionResult.isComplete
        let limitReason = executionResult.limitReason
        let projected = executionResult.bindings
        let endTime = indexQueryContext.graphClock.now()
        var stats = executionStats
        stats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        let resultCount = projected.count

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
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        let candidates = try indexQueryContext.indexDescriptors(
            for: T.self
        ).compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        guard candidates.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = candidates[0]
        let workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: container.monotonicClock
        )
        let startTime = indexQueryContext.graphClock.now()
        let executionResult = try await indexQueryContext
            .withReadableIndex(
                named: selection.indexName,
                indexType: selection.indexType,
                for: T.self
            ) {
                readableIndex,
                transaction -> SPARQLResult in
                let sources: [RDFDatasetSource]
                if let readableIndex {
                    sources = [
                        try RDFDatasetSource(
                            entityName: T.persistableType,
                            selection: selection,
                            indexSubspace: readableIndex.subspace
                        )
                    ]
                } else {
                    sources = []
                }
                let executor = SPARQLQueryExecutor(
                    monotonicClock: self.container.monotonicClock,
                    wallClock: self.container.wallClock,
                    datasetScanner: IndexedRDFDatasetScanner(
                        sources: sources
                    )
                )
                let retained = try await executor.executeRetainedInTransaction(
                    selectPlan: plan,
                    transaction: transaction,
                    workMeter: workMeter
                )
                return retained.promoteToResult()
            }

        let executionStats = executionResult.statistics
        let projectedVariables = executionResult.projectedVariables
        let isComplete = executionResult.isComplete
        let limitReason = executionResult.limitReason
        let bindings = executionResult.bindings
        var statistics = executionStats
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
        return SPARQLResult(
            bindings: consume bindings,
            projectedVariables: projectedVariables,
            isComplete: isComplete,
            limitReason: limitReason,
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

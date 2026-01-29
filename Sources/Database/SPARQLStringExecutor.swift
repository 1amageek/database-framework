// SPARQLStringExecutor.swift
// Database - Execute SPARQL query strings against the database
//
// End-to-end pipeline: SPARQL string → parse → convert → execute → results

import Foundation
import QueryAST
import QueryIR
import GraphIndex
import DatabaseEngine
import Core
import Graph
import FoundationDB

// MARK: - FDBContext Extension

extension FDBContext {

    /// Execute a SPARQL query string and return results
    ///
    /// Parses the SPARQL string, converts the graph pattern to the internal
    /// execution representation, and executes it via the GraphIndex engine.
    ///
    /// **Usage**:
    /// ```swift
    /// let result = try await context.executeSPARQL("""
    ///     SELECT ?person ?name
    ///     WHERE {
    ///         ?person <knows> "Alice" .
    ///         ?person <name> ?name .
    ///     }
    ///     LIMIT 10
    /// """, on: RDFTriple.self)
    ///
    /// for binding in result.bindings {
    ///     print("\(binding["?person"]) - \(binding["?name"])")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - sparql: A SPARQL SELECT query string
    ///   - type: The Persistable type with a GraphIndex to query against
    ///   - prefixes: Additional prefix map for resolving prefixed names
    /// - Returns: Query results as SPARQLResult
    /// - Throws: `SPARQLStringError` for parse/conversion errors,
    ///           `SPARQLQueryError` for execution errors
    public func executeSPARQL<T: Persistable>(
        _ sparql: String,
        on type: T.Type,
        prefixes: [String: String] = [:]
    ) async throws -> SPARQLResult {
        // 1. Parse SPARQL string
        let parser = SPARQLParser()
        let statement = try parser.parse(sparql)

        // 2. Extract SelectQuery
        guard case .select(let selectQuery) = statement else {
            throw SPARQLStringError.unsupportedQueryForm(statement)
        }

        // 3. Extract GraphPattern from data source
        let graphPattern: QueryIR.GraphPattern
        switch selectQuery.source {
        case .graphPattern(let gp):
            graphPattern = gp
        case .namedGraph(_, let gp):
            graphPattern = gp
        default:
            throw SPARQLStringError.unsupportedDataSource
        }

        // 4. Convert GraphPattern → ExecutionPattern
        var executionPattern = GraphPatternConverter.convert(graphPattern, prefixes: prefixes)

        // 5. Apply top-level filter (if any)
        if let filter = selectQuery.filter {
            executionPattern = .filter(executionPattern, GraphPatternConverter.convertFilter(filter))
        }

        // 6. Apply GROUP BY + aggregation + HAVING
        //    SPARQLParser places groupBy/having on SelectQuery, not inside GraphPattern.
        //    We must wire them here.
        if let groupByExprs = selectQuery.groupBy, !groupByExprs.isEmpty {
            let groupVars = groupByExprs.compactMap { expr -> String? in
                if case .variable(let v) = expr {
                    return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
                }
                return nil
            }

            let aggBindings = extractAggregateBindings(selectQuery)
            let aggExprs = aggBindings.map { GraphPatternConverter.convertAggregate($0) }

            let havingFilter: FilterExpression? = selectQuery.having.map {
                GraphPatternConverter.convertFilter($0)
            }

            executionPattern = .groupBy(
                executionPattern,
                groupVariables: groupVars,
                aggregates: aggExprs,
                having: havingFilter
            )
        }

        // 7. Extract projection variables
        let (projectionVars, isDistinct) = extractProjection(selectQuery)

        // 8. Convert ORDER BY → BindingSortKey array
        let sortKeys: [BindingSortKey] = (selectQuery.orderBy ?? []).map { sortKey in
            BindingSortKey(
                ascending: sortKey.direction == .ascending,
                nullsLast: sortKey.nulls == .last
            ) { binding in
                ExpressionEvaluator.evaluate(sortKey.expression, binding: binding)
            }
        }

        // 9. Execute via GraphIndex (W3C Section 15 order)
        let result = try await executeSPARQLPattern(
            executionPattern,
            on: type,
            projection: projectionVars,
            distinct: isDistinct || selectQuery.distinct,
            limit: selectQuery.limit,
            offset: selectQuery.offset ?? 0,
            orderBy: sortKeys
        )

        return result
    }

    // MARK: - Projection Extraction

    /// Extract projection variable names from a SelectQuery
    private func extractProjection(
        _ query: QueryIR.SelectQuery
    ) -> (variables: [String]?, isDistinct: Bool) {
        switch query.projection {
        case .all, .allFrom:
            return (nil, false)

        case .items(let items):
            let vars = items.compactMap { extractVariableName($0) }
            return (vars.isEmpty ? nil : vars, false)

        case .distinctItems(let items):
            let vars = items.compactMap { extractVariableName($0) }
            return (vars.isEmpty ? nil : vars, true)
        }
    }

    /// Extract a variable name from a ProjectionItem
    private func extractVariableName(_ item: QueryIR.ProjectionItem) -> String? {
        // Alias takes priority
        if let alias = item.alias {
            return alias.hasPrefix("?") ? alias : "?\(alias)"
        }
        // Otherwise extract from expression
        switch item.expression {
        case .variable(let v):
            return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
        case .column(let col):
            return "?\(col.column)"
        default:
            return nil
        }
    }

    // MARK: - Aggregate Extraction

    /// Extract aggregate bindings from projection items
    ///
    /// Finds projection items with aggregate expressions (e.g., `COUNT(?x) AS ?cnt`)
    /// and converts them to `AggregateBinding` for the execution engine.
    private func extractAggregateBindings(
        _ query: QueryIR.SelectQuery
    ) -> [QueryIR.AggregateBinding] {
        let items: [QueryIR.ProjectionItem]
        switch query.projection {
        case .items(let i), .distinctItems(let i):
            items = i
        default:
            return []
        }
        return items.compactMap { item -> QueryIR.AggregateBinding? in
            guard case .aggregate(let agg) = item.expression,
                  let alias = item.alias else { return nil }
            let varName = alias.hasPrefix("?") ? alias : "?\(alias)"
            return QueryIR.AggregateBinding(variable: varName, aggregate: agg)
        }
    }
}

// MARK: - Non-generic SPARQL String Execution

/// Execute a SPARQL query string without compiled types
///
/// Uses pre-resolved index metadata from `TypeCatalog` / `IndexCatalog`.
/// This enables CLI and dynamic tools to execute SPARQL queries without
/// requiring compiled `Persistable` Swift types.
///
/// Follows W3C SPARQL 1.1 Section 15 execution order:
/// 1. Pattern evaluation (WHERE + GROUP BY / HAVING)
/// 2. ORDER BY
/// 3. Projection (SELECT)
/// 4. DISTINCT / REDUCED
/// 5. OFFSET / LIMIT (Slice)
///
/// - Parameters:
///   - sparql: A SPARQL SELECT query string
///   - database: Database for transaction execution
///   - indexSubspace: Pre-resolved `[I]/[indexName]` subspace
///   - strategy: Graph index strategy (adjacency/tripleStore/hexastore)
///   - fromFieldName: Name of the from/subject field
///   - edgeFieldName: Name of the edge/predicate field
///   - toFieldName: Name of the to/object field
///   - prefixes: Additional prefix map for resolving prefixed names
/// - Returns: Query results as SPARQLResult
/// - Throws: `SPARQLStringError` for parse/conversion errors,
///           `SPARQLQueryError` for execution errors
public func executeSPARQLString(
    _ sparql: String,
    database: any DatabaseProtocol,
    indexSubspace: Subspace,
    strategy: GraphIndexStrategy,
    fromFieldName: String,
    edgeFieldName: String,
    toFieldName: String,
    prefixes: [String: String] = [:]
) async throws -> SPARQLResult {
    // 1. Parse SPARQL string
    let parser = SPARQLParser()
    let statement = try parser.parse(sparql)

    // 2. Extract SelectQuery
    guard case .select(let selectQuery) = statement else {
        throw SPARQLStringError.unsupportedQueryForm(statement)
    }

    // 3. Extract GraphPattern from data source
    let graphPattern: QueryIR.GraphPattern
    switch selectQuery.source {
    case .graphPattern(let gp):
        graphPattern = gp
    case .namedGraph(_, let gp):
        graphPattern = gp
    default:
        throw SPARQLStringError.unsupportedDataSource
    }

    // 4. Convert GraphPattern → ExecutionPattern
    var executionPattern = GraphPatternConverter.convert(graphPattern, prefixes: prefixes)

    // 5. Apply top-level filter (if any)
    if let filter = selectQuery.filter {
        executionPattern = .filter(executionPattern, GraphPatternConverter.convertFilter(filter))
    }

    // 6. Apply GROUP BY + aggregation + HAVING
    if let groupByExprs = selectQuery.groupBy, !groupByExprs.isEmpty {
        let groupVars = groupByExprs.compactMap { expr -> String? in
            if case .variable(let v) = expr {
                return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
            }
            return nil
        }

        let aggBindings = _extractAggregateBindings(selectQuery)
        let aggExprs = aggBindings.map { GraphPatternConverter.convertAggregate($0) }

        let havingFilter: FilterExpression? = selectQuery.having.map {
            GraphPatternConverter.convertFilter($0)
        }

        executionPattern = .groupBy(
            executionPattern,
            groupVariables: groupVars,
            aggregates: aggExprs,
            having: havingFilter
        )
    }

    // 7. Extract projection variables
    let (projectionVars, isProjectionDistinct) = _extractProjection(selectQuery)

    // 8. Convert ORDER BY → BindingSortKey array
    let sortKeys: [BindingSortKey] = (selectQuery.orderBy ?? []).map { sortKey in
        BindingSortKey(
            ascending: sortKey.direction == .ascending,
            nullsLast: sortKey.nulls == .last
        ) { binding in
            ExpressionEvaluator.evaluate(sortKey.expression, binding: binding)
        }
    }

    // 9. Execute pattern (W3C Section 15 order)
    let executor = SPARQLQueryExecutor(
        database: database,
        indexSubspace: indexSubspace,
        strategy: strategy,
        fromFieldName: fromFieldName,
        edgeFieldName: edgeFieldName,
        toFieldName: toFieldName
    )

    let isDistinct = isProjectionDistinct || selectQuery.distinct
    let hasOrderBy = !sortKeys.isEmpty
    let needsAllResults = hasOrderBy || isDistinct

    // Step 1: Pattern evaluation
    let startTime = DispatchTime.now()
    var (bindings, stats) = try await executor.execute(
        pattern: executionPattern,
        limit: needsAllResults ? nil : selectQuery.limit,
        offset: needsAllResults ? 0 : (selectQuery.offset ?? 0)
    )

    // Step 2: ORDER BY
    if hasOrderBy {
        bindings = BindingSorter.sort(bindings, by: sortKeys)
    }

    // Step 3: Projection (SELECT)
    let allVariables = executionPattern.variables
    let projectedVars = projectionVars ?? Array(allVariables).sorted()
    let projectionSet = Set(projectedVars)
    var projected = bindings.map { $0.project(projectionSet) }

    // Step 4: DISTINCT
    if isDistinct {
        var seen = Set<VariableBinding>()
        projected = projected.filter { seen.insert($0).inserted }
    }

    // Step 5: OFFSET / LIMIT (Slice)
    let offset = selectQuery.offset ?? 0
    if needsAllResults {
        if offset > 0 {
            projected = Array(projected.dropFirst(offset))
        }
        if let lim = selectQuery.limit {
            projected = Array(projected.prefix(lim))
        }
    }

    let endTime = DispatchTime.now()
    stats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

    let resultCount = projected.count
    let limit = selectQuery.limit
    let isComplete = limit == nil || resultCount < limit!
    let limitReason: SPARQLLimitReason? = (limit != nil && resultCount >= limit!) ? .explicitLimit : nil

    return SPARQLResult(
        bindings: projected,
        projectedVariables: projectedVars,
        isComplete: isComplete,
        limitReason: limitReason,
        statistics: stats
    )
}

// MARK: - Private Helpers for Non-generic Execution

/// Extract projection variable names from a SelectQuery (free function variant)
private func _extractProjection(
    _ query: QueryIR.SelectQuery
) -> (variables: [String]?, isDistinct: Bool) {
    switch query.projection {
    case .all, .allFrom:
        return (nil, false)

    case .items(let items):
        let vars = items.compactMap { _extractVariableName($0) }
        return (vars.isEmpty ? nil : vars, false)

    case .distinctItems(let items):
        let vars = items.compactMap { _extractVariableName($0) }
        return (vars.isEmpty ? nil : vars, true)
    }
}

/// Extract a variable name from a ProjectionItem (free function variant)
private func _extractVariableName(_ item: QueryIR.ProjectionItem) -> String? {
    if let alias = item.alias {
        return alias.hasPrefix("?") ? alias : "?\(alias)"
    }
    switch item.expression {
    case .variable(let v):
        return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
    case .column(let col):
        return "?\(col.column)"
    default:
        return nil
    }
}

/// Extract aggregate bindings from projection items (free function variant)
private func _extractAggregateBindings(
    _ query: QueryIR.SelectQuery
) -> [QueryIR.AggregateBinding] {
    let items: [QueryIR.ProjectionItem]
    switch query.projection {
    case .items(let i), .distinctItems(let i):
        items = i
    default:
        return []
    }
    return items.compactMap { item -> QueryIR.AggregateBinding? in
        guard case .aggregate(let agg) = item.expression,
              let alias = item.alias else { return nil }
        let varName = alias.hasPrefix("?") ? alias : "?\(alias)"
        return QueryIR.AggregateBinding(variable: varName, aggregate: agg)
    }
}

// MARK: - Errors

/// Errors specific to SPARQL string execution
public enum SPARQLStringError: Error, CustomStringConvertible {
    /// Only SELECT queries are currently supported
    case unsupportedQueryForm(QueryIR.QueryStatement)

    /// The data source must be a graph pattern
    case unsupportedDataSource

    /// Parse error (wraps underlying SPARQLParser error)
    case parseError(Error)

    public var description: String {
        switch self {
        case .unsupportedQueryForm(let stmt):
            return "Unsupported query form: only SELECT queries are supported, got: \(stmt)"
        case .unsupportedDataSource:
            return "Unsupported data source: only graph pattern sources are supported"
        case .parseError(let error):
            return "SPARQL parse error: \(error)"
        }
    }
}

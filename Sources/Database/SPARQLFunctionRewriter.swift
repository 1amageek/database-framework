// SPARQLFunctionRewriter.swift
// Database - Rewrite SelectQuery by executing SPARQL() functions

#if DATABASE_GRAPH_INDEXES
import DatabaseKit
import QueryAST
@_spi(DatabaseExecution) import GraphIndex
import DatabaseEngine
import DatabaseTypes
import StorageKit
import Synchronization

/// Rewrites SelectQuery by executing SPARQL() subqueries
///
/// **Design**: Pre-execution rewrite at DatabaseContext level.
/// - Finds SPARQL() function calls in Expression tree
/// - Executes SPARQL queries within parent transaction
/// - Inlines results as literal arrays
/// - Returns rewritten SelectQuery for standard execution
///
/// **Usage**:
/// ```swift
/// let rewriter = SPARQLFunctionRewriter(
///     context: context,
///     workMeter: workMeter,
///     transaction: transaction
/// )
/// let rewritten = try await rewriter.rewrite(selectQuery)
/// // Execute rewritten query through normal path
/// ```
internal struct SPARQLFunctionRewriter: Sendable {
    private final class InliningStructureMeter: Sendable {
        private let cumulativeCount = Mutex<UInt64>(0)

        func admit(count: Int, limits: QueryStructuralLimits) throws {
            guard let amount = UInt64(exactly: count) else {
                throw QueryStructuralValidationError.resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: UInt64.max,
                    maximum: limits.maximumCollectionElements
                )
            }
            guard amount <= limits.maximumCollectionElements else {
                throw QueryStructuralValidationError.resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: amount,
                    maximum: limits.maximumCollectionElements
                )
            }
            try cumulativeCount.withLock { current in
                let addition = current.addingReportingOverflow(amount)
                let actual = addition.overflow
                    ? UInt64.max
                    : addition.partialValue
                guard !addition.overflow,
                      actual <= limits.maximumTotalNodes else {
                    throw QueryStructuralValidationError
                        .resourceLimitExceeded(
                            resource: .totalNodes,
                            actual: actual,
                            maximum: limits.maximumTotalNodes
                        )
                }
                current = actual
            }
        }
    }

    private let context: DatabaseContext
    private let workMeter: DatabaseWorkMeter
    private let transaction: any TransactionAccess
    private let structuralLimits: QueryStructuralLimits
    private let inliningStructureMeter = InliningStructureMeter()

    /// Initialize with DatabaseContext
    ///
    /// - Parameters:
    ///   - context: Context for schema and index access.
    ///   - workMeter: Shared resource budget for graph and SQL execution.
    ///   - transaction: Parent SQL read transaction.
    ///   - structuralLimits: Limits shared by graph compilation and rewritten SQL.
    internal init(
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter,
        transaction: any TransactionAccess,
        structuralLimits: QueryStructuralLimits = .default
    ) {
        self.context = context
        self.workMeter = workMeter
        self.transaction = transaction
        self.structuralLimits = structuralLimits
    }

    /// Returns whether any query expression contains a SPARQL SQL function
    /// that requires transaction-bound rewriting before canonical execution.
    internal static func containsSPARQLFunction(
        in query: SelectQuery
    ) -> Bool {
        var pendingExpressions: [DatabaseKit.Expression] = []
        var pendingQueries: [SelectQuery] = [query]
        var pendingSources: [DataSource] = []
        var pendingPatterns: [GraphPattern] = []
        while !pendingExpressions.isEmpty
                || !pendingQueries.isEmpty
                || !pendingSources.isEmpty
                || !pendingPatterns.isEmpty {
            if let nestedQuery = pendingQueries.popLast() {
                switch nestedQuery.projection {
                case .items(let items), .distinctItems(let items):
                    pendingExpressions.append(
                        contentsOf: items.map(\.expression)
                    )
                case .all, .allFrom:
                    break
                }
                if let filter = nestedQuery.filter {
                    pendingExpressions.append(filter)
                }
                pendingExpressions.append(
                    contentsOf: nestedQuery.groupBy ?? []
                )
                if let having = nestedQuery.having {
                    pendingExpressions.append(having)
                }
                pendingExpressions.append(
                    contentsOf: (nestedQuery.orderBy ?? []).map(\.expression)
                )
                for subquery in nestedQuery.subqueries ?? [] {
                    pendingQueries.append(subquery.query)
                }
                pendingSources.append(nestedQuery.source)
                continue
            }
            if let source = pendingSources.popLast() {
                switch source {
                case .subquery(let nestedQuery, _):
                    pendingQueries.append(nestedQuery)
                case .join(let join):
                    pendingSources.append(join.left)
                    pendingSources.append(join.right)
                    if case .on(let expression) = join.condition {
                        pendingExpressions.append(expression)
                    }
                case .union(let sources), .unionAll(let sources),
                        .intersect(let sources):
                    pendingSources.append(contentsOf: sources)
                case .except(let lhs, let rhs):
                    pendingSources.append(lhs)
                    pendingSources.append(rhs)
                case .graphTable(let graphTable):
                    if let filter = graphTable.matchPattern.where {
                        pendingExpressions.append(filter)
                    }
                    for column in graphTable.columns ?? [] {
                        pendingExpressions.append(column.expression)
                    }
                #if DATABASE_MULTI_BASE
                case .base(_, let nested):
                    pendingSources.append(nested)
                #endif
                case .graphPattern(let pattern),
                        .namedGraph(_, let pattern),
                        .service(_, let pattern, _):
                    pendingPatterns.append(pattern)
                case .table, .logical, .values:
                    break
                }
                continue
            }
            if let pattern = pendingPatterns.popLast() {
                switch pattern {
                case .join(let lhs, let rhs), .optional(let lhs, let rhs),
                        .union(let lhs, let rhs), .minus(let lhs, let rhs),
                        .lateral(let lhs, let rhs):
                    pendingPatterns.append(lhs)
                    pendingPatterns.append(rhs)
                case .filter(let nested, let expression),
                        .bind(let nested, _, let expression):
                    pendingPatterns.append(nested)
                    pendingExpressions.append(expression)
                case .graph(_, let nested), .service(_, let nested, _):
                    pendingPatterns.append(nested)
                case .subquery(let query):
                    pendingQueries.append(query)
                case .groupBy(let nested, let expressions, let aggregates):
                    pendingPatterns.append(nested)
                    pendingExpressions.append(contentsOf: expressions)
                    for aggregate in aggregates {
                        switch aggregate.aggregate {
                        case .count(let value, _):
                            if let value { pendingExpressions.append(value) }
                        case .sum(let value, _), .avg(let value, _),
                                .min(let value), .max(let value),
                                .groupConcat(let value, _, _),
                                .sample(let value):
                            pendingExpressions.append(value)
                        case .arrayAgg(let value, let orderBy, _):
                            pendingExpressions.append(value)
                            pendingExpressions.append(
                                contentsOf: (orderBy ?? []).map(\.expression)
                            )
                        }
                    }
                case .basic, .values:
                    break
                }
                continue
            }
            guard let expression = pendingExpressions.popLast() else {
                continue
            }
            switch expression {
            case .function(let call):
                if call.name.uppercased() == "SPARQL" {
                    return true
                }
                pendingExpressions.append(contentsOf: call.arguments)

            case .add(let left, let right),
                 .subtract(let left, let right),
                 .multiply(let left, let right),
                 .divide(let left, let right),
                 .modulo(let left, let right),
                 .equal(let left, let right),
                 .notEqual(let left, let right),
                 .lessThan(let left, let right),
                 .lessThanOrEqual(let left, let right),
                 .greaterThan(let left, let right),
                 .greaterThanOrEqual(let left, let right),
                 .and(let left, let right),
                 .or(let left, let right),
                 .nullIf(let left, let right):
                pendingExpressions.append(left)
                pendingExpressions.append(right)

            case .negate(let inner),
                 .not(let inner),
                 .isNull(let inner),
                 .isNotNull(let inner),
                 .like(let inner, _),
                 .regex(let inner, _, _),
                 .cast(let inner, _),
                 .isTriple(let inner),
                 .subject(let inner),
                 .predicate(let inner),
                 .object(let inner):
                pendingExpressions.append(inner)

            case .between(let value, let low, let high):
                pendingExpressions.append(value)
                pendingExpressions.append(low)
                pendingExpressions.append(high)

            case .inList(let value, let values),
                 .notInList(let value, let values):
                pendingExpressions.append(value)
                pendingExpressions.append(contentsOf: values)

            case .inSubquery(let value, let subquery):
                pendingExpressions.append(value)
                pendingQueries.append(subquery)

            case .caseWhen(let cases, let elseResult):
                for pair in cases {
                    pendingExpressions.append(pair.condition)
                    pendingExpressions.append(pair.result)
                }
                if let elseResult {
                    pendingExpressions.append(elseResult)
                }

            case .coalesce(let expressions):
                pendingExpressions.append(contentsOf: expressions)

            case .triple(let subject, let predicate, let object):
                pendingExpressions.append(subject)
                pendingExpressions.append(predicate)
                pendingExpressions.append(object)

            case .subquery(let subquery), .exists(let subquery):
                pendingQueries.append(subquery)

            case .aggregate(let aggregate):
                switch aggregate {
                case .count(let value, _):
                    if let value { pendingExpressions.append(value) }
                case .sum(let value, _),
                     .avg(let value, _),
                     .min(let value),
                     .max(let value),
                     .sample(let value):
                    pendingExpressions.append(value)
                case .groupConcat(let value, _, _):
                    pendingExpressions.append(value)
                case .arrayAgg(let value, let orderBy, _):
                    pendingExpressions.append(value)
                    if let orderBy {
                        for ordering in orderBy {
                            pendingExpressions.append(ordering.expression)
                        }
                    }
                }

            case .literal, .column, .variable, .parameter, .bound:
                break
            }
        }
        return false
    }

    // MARK: - Rewrite Entry Point

    /// Rewrite SelectQuery by executing SPARQL subqueries
    ///
    /// Recursively traverses every expression-bearing query clause, executing
    /// SPARQL() functions and inlining their results.
    ///
    /// - Parameter query: The SelectQuery to rewrite
    /// - Returns: Rewritten SelectQuery with SPARQL() calls replaced
    /// - Throws: `SPARQLFunctionError` for execution errors
    internal func rewrite(_ query: SelectQuery) async throws -> SelectQuery {
        let rewrittenFilter: DatabaseKit.Expression?
        if let filter = query.filter {
            rewrittenFilter = try await rewriteExpression(filter)
        } else {
            rewrittenFilter = nil
        }
        var rewrittenSubqueries: [NamedSubquery] = []
        rewrittenSubqueries.reserveCapacity(query.subqueries?.count ?? 0)
        for subquery in query.subqueries ?? [] {
            rewrittenSubqueries.append(
                NamedSubquery(
                    name: subquery.name,
                    columns: subquery.columns,
                    query: try await rewrite(subquery.query),
                    materialized: subquery.materialized
                )
            )
        }

        return SelectQuery(
            projection: try await rewriteProjection(query.projection),
            source: try await rewriteSource(query.source),
            accessPath: query.accessPath,
            filter: rewrittenFilter,
            groupBy: try await rewriteOptionalExpressions(query.groupBy),
            having: try await rewriteOptionalExpression(query.having),
            orderBy: try await rewriteOptionalSortKeys(query.orderBy),
            limit: query.limit,
            offset: query.offset,
            distinct: query.distinct,
            subqueries: query.subqueries == nil ? nil : rewrittenSubqueries,
            reduced: query.reduced,
            dataset: query.dataset
        )
    }

    private func rewriteSource(_ source: DataSource) async throws -> DataSource {
        switch source {
        case .subquery(let query, let alias):
            return .subquery(try await rewrite(query), alias: alias)
        case .join(let join):
            let condition: JoinCondition?
            switch join.condition {
            case .on(let expression):
                condition = .on(try await rewriteExpression(expression))
            case .using(let columns):
                condition = .using(columns)
            case nil:
                condition = nil
            }
            return .join(
                JoinClause(
                    type: join.type,
                    left: try await rewriteSource(join.left),
                    right: try await rewriteSource(join.right),
                    condition: condition
                )
            )
        case .union(let sources):
            return .union(try await rewriteSources(sources))
        case .unionAll(let sources):
            return .unionAll(try await rewriteSources(sources))
        case .intersect(let sources):
            return .intersect(try await rewriteSources(sources))
        case .except(let lhs, let rhs):
            return .except(
                try await rewriteSource(lhs),
                try await rewriteSource(rhs)
            )
        case .graphTable(let graphTable):
            let rewrittenFilter: DatabaseKit.Expression?
            if let filter = graphTable.matchPattern.where {
                rewrittenFilter = try await rewriteExpression(filter)
            } else {
                rewrittenFilter = nil
            }
            var columns: [GraphTableColumn] = []
            columns.reserveCapacity(graphTable.columns?.count ?? 0)
            for column in graphTable.columns ?? [] {
                columns.append(
                    GraphTableColumn(
                        expression: try await rewriteExpression(
                            column.expression
                        ),
                        alias: column.alias
                    )
                )
            }
            return .graphTable(
                GraphTableSource(
                    graphName: graphTable.graphName,
                    matchPattern: MatchPattern(
                        paths: graphTable.matchPattern.paths,
                        where: rewrittenFilter
                    ),
                    columns: graphTable.columns == nil ? nil : columns,
                    alias: graphTable.alias
                )
            )
        #if DATABASE_MULTI_BASE
        case .base(let base, let nested):
            return .base(base, try await rewriteSource(nested))
        #endif
        case .graphPattern(let pattern):
            return .graphPattern(try await rewritePattern(pattern))
        case .namedGraph(let name, let pattern):
            return .namedGraph(
                name: name,
                pattern: try await rewritePattern(pattern)
            )
        case .service(let endpoint, let pattern, let silent):
            return .service(
                endpoint: endpoint,
                pattern: try await rewritePattern(pattern),
                silent: silent
            )
        case .table, .logical, .values:
            return source
        }
    }

    private func rewritePattern(
        _ pattern: GraphPattern
    ) async throws -> GraphPattern {
        switch pattern {
        case .basic, .values:
            return pattern
        case .join(let lhs, let rhs):
            return .join(
                try await rewritePattern(lhs),
                try await rewritePattern(rhs)
            )
        case .optional(let lhs, let rhs):
            return .optional(
                try await rewritePattern(lhs),
                try await rewritePattern(rhs)
            )
        case .union(let lhs, let rhs):
            return .union(
                try await rewritePattern(lhs),
                try await rewritePattern(rhs)
            )
        case .minus(let lhs, let rhs):
            return .minus(
                try await rewritePattern(lhs),
                try await rewritePattern(rhs)
            )
        case .lateral(let lhs, let rhs):
            return .lateral(
                try await rewritePattern(lhs),
                try await rewritePattern(rhs)
            )
        case .filter(let nested, let expression):
            return .filter(
                try await rewritePattern(nested),
                try await rewriteExpression(expression)
            )
        case .graph(let name, let nested):
            return .graph(
                name: name,
                pattern: try await rewritePattern(nested)
            )
        case .service(let endpoint, let nested, let silent):
            return .service(
                endpoint: endpoint,
                pattern: try await rewritePattern(nested),
                silent: silent
            )
        case .bind(let nested, let variable, let expression):
            return .bind(
                try await rewritePattern(nested),
                variable: variable,
                expression: try await rewriteExpression(expression)
            )
        case .subquery(let query):
            return .subquery(try await rewrite(query))
        case .groupBy(let nested, let expressions, let aggregates):
            var rewrittenAggregates: [AggregateBinding] = []
            rewrittenAggregates.reserveCapacity(aggregates.count)
            for binding in aggregates {
                rewrittenAggregates.append(
                    AggregateBinding(
                        variable: binding.variable,
                        aggregate: try await rewriteAggregate(
                            binding.aggregate
                        )
                    )
                )
            }
            return .groupBy(
                try await rewritePattern(nested),
                expressions: try await rewriteExpressions(expressions),
                aggregates: rewrittenAggregates
            )
        }
    }

    private func rewriteExpressions(
        _ expressions: [DatabaseKit.Expression]
    ) async throws -> [DatabaseKit.Expression] {
        var rewritten: [DatabaseKit.Expression] = []
        rewritten.reserveCapacity(expressions.count)
        for expression in expressions {
            rewritten.append(try await rewriteExpression(expression))
        }
        return rewritten
    }

    private func rewriteSources(
        _ sources: [DataSource]
    ) async throws -> [DataSource] {
        var rewritten: [DataSource] = []
        rewritten.reserveCapacity(sources.count)
        for source in sources {
            rewritten.append(try await rewriteSource(source))
        }
        return rewritten
    }

    private func rewriteProjection(
        _ projection: Projection
    ) async throws -> Projection {
        switch projection {
        case .all:
            return .all
        case .allFrom(let sourceName):
            return .allFrom(sourceName)
        case .items(let items):
            return .items(try await rewriteProjectionItems(items))
        case .distinctItems(let items):
            return .distinctItems(try await rewriteProjectionItems(items))
        }
    }

    private func rewriteProjectionItems(
        _ items: [ProjectionItem]
    ) async throws -> [ProjectionItem] {
        var rewritten: [ProjectionItem] = []
        rewritten.reserveCapacity(items.count)
        for item in items {
            rewritten.append(
                ProjectionItem(
                    try await rewriteExpression(item.expression),
                    alias: item.alias
                )
            )
        }
        return rewritten
    }

    private func rewriteOptionalExpressions(
        _ expressions: [DatabaseKit.Expression]?
    ) async throws -> [DatabaseKit.Expression]? {
        guard let expressions else { return nil }
        var rewritten: [DatabaseKit.Expression] = []
        rewritten.reserveCapacity(expressions.count)
        for expression in expressions {
            rewritten.append(try await rewriteExpression(expression))
        }
        return rewritten
    }

    private func rewriteOptionalExpression(
        _ expression: DatabaseKit.Expression?
    ) async throws -> DatabaseKit.Expression? {
        guard let expression else { return nil }
        return try await rewriteExpression(expression)
    }

    private func rewriteOptionalSortKeys(
        _ sortKeys: [SortKey]?
    ) async throws -> [SortKey]? {
        guard let sortKeys else { return nil }
        var rewritten: [SortKey] = []
        rewritten.reserveCapacity(sortKeys.count)
        for sortKey in sortKeys {
            rewritten.append(
                SortKey(
                    try await rewriteExpression(sortKey.expression),
                    direction: sortKey.direction,
                    nulls: sortKey.nulls
                )
            )
        }
        return rewritten
    }

    // MARK: - Expression Rewriting

    /// Recursively rewrite expressions, executing SPARQL() calls
    ///
    /// Traverses the expression tree and replaces `.function("SPARQL", ...)` nodes
    /// with `.inList(lhs, [literal values...])`.
    ///
    /// - Parameter expr: Expression to rewrite
    /// - Returns: Rewritten expression
    /// - Throws: `SPARQLFunctionError` for execution errors
    private func rewriteExpression(
        _ expr: DatabaseKit.Expression
    ) async throws -> DatabaseKit.Expression {
        switch expr {
        case .inList(let lhs, let values):
            // Check if any value is a SPARQL() function
            var rewrittenValues: [DatabaseKit.Expression] = []
            for value in values {
                if case .function(let call) = value, call.name.uppercased() == "SPARQL" {
                    // Execute SPARQL and inline results
                    rewrittenValues.append(
                        contentsOf: try await executeSPARQLFunctionAsExpressions(
                            call
                        )
                    )
                } else {
                    rewrittenValues.append(try await rewriteExpression(value))
                }
            }
            return .inList(try await rewriteExpression(lhs), values: rewrittenValues)

        case .notInList(let lhs, let values):
            var rewrittenValues: [DatabaseKit.Expression] = []
            for value in values {
                if case .function(let call) = value, call.name.uppercased() == "SPARQL" {
                    rewrittenValues.append(
                        contentsOf: try await executeSPARQLFunctionAsExpressions(
                            call
                        )
                    )
                } else {
                    rewrittenValues.append(try await rewriteExpression(value))
                }
            }
            return .notInList(try await rewriteExpression(lhs), values: rewrittenValues)

        case .inSubquery(let lhs, let subquery):
            // Check if subquery contains SPARQL() - recursively rewrite
            let rewrittenSubquery = try await rewrite(subquery)
            return .inSubquery(try await rewriteExpression(lhs), subquery: rewrittenSubquery)

        // Logical operators - recurse
        case .and(let left, let right):
            return .and(try await rewriteExpression(left), try await rewriteExpression(right))

        case .or(let left, let right):
            return .or(try await rewriteExpression(left), try await rewriteExpression(right))

        case .not(let inner):
            return .not(try await rewriteExpression(inner))

        // Comparison operators - recurse on both sides
        case .equal(let left, let right):
            return .equal(try await rewriteExpression(left), try await rewriteExpression(right))

        case .notEqual(let left, let right):
            return .notEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        case .lessThan(let left, let right):
            return .lessThan(try await rewriteExpression(left), try await rewriteExpression(right))

        case .lessThanOrEqual(let left, let right):
            return .lessThanOrEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        case .greaterThan(let left, let right):
            return .greaterThan(try await rewriteExpression(left), try await rewriteExpression(right))

        case .greaterThanOrEqual(let left, let right):
            return .greaterThanOrEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        // Arithmetic operators - recurse
        case .add(let left, let right):
            return .add(try await rewriteExpression(left), try await rewriteExpression(right))

        case .subtract(let left, let right):
            return .subtract(try await rewriteExpression(left), try await rewriteExpression(right))

        case .multiply(let left, let right):
            return .multiply(try await rewriteExpression(left), try await rewriteExpression(right))

        case .divide(let left, let right):
            return .divide(try await rewriteExpression(left), try await rewriteExpression(right))

        case .modulo(let left, let right):
            return .modulo(try await rewriteExpression(left), try await rewriteExpression(right))

        case .negate(let inner):
            return .negate(try await rewriteExpression(inner))

        // Other cases that might contain expressions
        case .between(let expr, let low, let high):
            return .between(
                try await rewriteExpression(expr),
                low: try await rewriteExpression(low),
                high: try await rewriteExpression(high)
            )

        case .isNull(let inner):
            return .isNull(try await rewriteExpression(inner))

        case .isNotNull(let inner):
            return .isNotNull(try await rewriteExpression(inner))

        case .like(let inner, let pattern):
            return .like(try await rewriteExpression(inner), pattern: pattern)

        case .regex(let inner, let pattern, let flags):
            return .regex(try await rewriteExpression(inner), pattern: pattern, flags: flags)

        case .cast(let inner, let targetType):
            return .cast(try await rewriteExpression(inner), targetType: targetType)

        case .caseWhen(let cases, let elseResult):
            var rewrittenCases: [CaseWhenPair] = []
            for pair in cases {
                rewrittenCases.append(CaseWhenPair(
                    condition: try await rewriteExpression(pair.condition),
                    result: try await rewriteExpression(pair.result)
                ))
            }
            let rewrittenElse = try await elseResult.asyncMap { try await rewriteExpression($0) }
            return .caseWhen(cases: rewrittenCases, elseResult: rewrittenElse)

        case .coalesce(let exprs):
            var rewrittenExprs: [DatabaseKit.Expression] = []
            for expr in exprs {
                rewrittenExprs.append(try await rewriteExpression(expr))
            }
            return .coalesce(rewrittenExprs)

        case .nullIf(let left, let right):
            return .nullIf(try await rewriteExpression(left), try await rewriteExpression(right))

        // Function call - check if it's SPARQL()
        case .function(let call):
            if call.name.uppercased() == "SPARQL" {
                throw SPARQLFunctionError.invalidArguments(
                    "SPARQL() must be used as an IN-list item"
                )
            }
            // Other functions - recurse on arguments
            var rewrittenArgs: [DatabaseKit.Expression] = []
            for arg in call.arguments {
                rewrittenArgs.append(try await rewriteExpression(arg))
            }
            return .function(FunctionCall(name: call.name, arguments: rewrittenArgs, distinct: call.distinct))

        case .aggregate(let aggregate):
            return .aggregate(try await rewriteAggregate(aggregate))

        // Terminal cases - no recursion needed
        case .literal, .column, .variable, .parameter, .bound:
            return expr

        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try await rewriteExpression(subject),
                predicate: try await rewriteExpression(predicate),
                object: try await rewriteExpression(object)
            )
        case .isTriple(let inner):
            return .isTriple(try await rewriteExpression(inner))
        case .subject(let inner):
            return .subject(try await rewriteExpression(inner))
        case .predicate(let inner):
            return .predicate(try await rewriteExpression(inner))
        case .object(let inner):
            return .object(try await rewriteExpression(inner))

        // Subquery expression cases
        case .exists(let subquery):
            return .exists(try await rewrite(subquery))

        case .subquery(let subquery):
            return .subquery(try await rewrite(subquery))
        }
    }

    private func rewriteAggregate(
        _ aggregate: AggregateFunction
    ) async throws -> AggregateFunction {
        switch aggregate {
        case .count(let expression, let distinct):
            return .count(
                try await rewriteOptionalExpression(expression),
                distinct: distinct
            )
        case .sum(let expression, let distinct):
            return .sum(
                try await rewriteExpression(expression),
                distinct: distinct
            )
        case .avg(let expression, let distinct):
            return .avg(
                try await rewriteExpression(expression),
                distinct: distinct
            )
        case .min(let expression):
            return .min(try await rewriteExpression(expression))
        case .max(let expression):
            return .max(try await rewriteExpression(expression))
        case .groupConcat(let expression, let separator, let distinct):
            return .groupConcat(
                try await rewriteExpression(expression),
                separator: separator,
                distinct: distinct
            )
        case .sample(let expression):
            return .sample(try await rewriteExpression(expression))
        case .arrayAgg(let expression, let orderBy, let distinct):
            return .arrayAgg(
                try await rewriteExpression(expression),
                orderBy: try await rewriteOptionalSortKeys(orderBy),
                distinct: distinct
            )
        }
    }

    // MARK: - SPARQL Execution

    /// Execute a SPARQL function and return scalar literal expressions.
    ///
    /// - Parameter call: The SPARQL() function call
    /// - Returns: Literal expressions for a single-variable projection.
    /// - Throws: `SPARQLFunctionError` for invalid arguments or execution errors
    private func executeSPARQLFunctionAsExpressions(
        _ call: FunctionCall
    ) async throws -> [DatabaseKit.Expression] {
        // 1. Extract arguments (type name, query string, optional variable)
        let (typeName, sparqlQuery, extractVar) = try extractArguments(call)

        // 2. Resolve type via TypeResolver
        let resolver = TypeResolver(schema: context.container.schema)
        let entity = try resolver.resolve(typeName: typeName)
        guard !entity.hasDynamicDirectory else {
            throw SPARQLFunctionError.invalidArguments(
                "SPARQL() requires an explicit partition for dynamic entity '\(entity.name)'"
            )
        }
        guard let dataset = try RDFDatasetReadResolver.resolve(entity: entity) else {
            throw SPARQLFunctionError.invalidGraphIndex(entity.name)
        }
        let graphIndex = dataset.indexDescriptor
        try context.authorizeIndexFieldRead(
            entity: entity,
            descriptor: graphIndex
        )

        // 4. Admit and execute the schema-declared index in the parent read
        // transaction. The rewritten SQL query consumes the same snapshot.
        let result = try await executeSPARQL(
            sparqlQuery: sparqlQuery,
            entityName: entity.name,
            indexDescriptor: graphIndex,
            metadata: dataset.metadata,
            includedFieldNames: graphIndex.includedFieldNames
        )
        try admitInlinedLiterals(result.bindings.count)

        // 5. Extract single-variable values
        let varToExtract = extractVar ?? result.projectedVariables.first
        guard let variable = varToExtract else {
            throw SPARQLFunctionError.multipleVariablesNotSupported
        }

        // Validate single-variable projection
        if result.projectedVariables.count > 1 && extractVar == nil {
            throw SPARQLFunctionError.multipleVariablesNotSupported
        }

        // IN-list order and duplicates are not observable SQL semantics. A
        // canonical value order keeps the rewritten query fingerprint stable
        // when a historical continuation repeats this rewrite at the same
        // read version without an explicit SPARQL ORDER BY.
        var values = try DatabaseRetainedArrayBuilder<FieldValue>(
            workMeter: workMeter,
            stage: .expressionEvaluation,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: FieldValue.self),
            expectedCount: result.bindings.count
        )
        for binding in result.bindings {
            try workMeter.consume(at: .expressionEvaluation)
            guard let fieldValue = binding[variable] else {
                throw SPARQLFunctionError.missingVariable(variable)
            }
            try values.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: QueryRow(fields: ["value": fieldValue]),
                    workMeter: workMeter
                ),
                make: { fieldValue }
            )
        }
        let sortedValues = try values.finish().sortingElements { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return lhs < rhs
        }

        var expressions: [DatabaseKit.Expression] = []
        expressions.reserveCapacity(sortedValues.count)
        try sortedValues.withSpan { values in
            var previous: FieldValue?
            for value in values {
                guard value != previous else { continue }
                try workMeter.consume(at: .expressionEvaluation)
                expressions.append(.literal(try fieldValueToLiteral(value)))
                previous = value
            }
        }
        return expressions
    }

    // MARK: - Argument Extraction

    /// Extract arguments from SPARQL() function call
    ///
    /// Expected formats:
    /// - SPARQL(TypeName, 'query string')
    /// - SPARQL(TypeName, 'query string', '?variable')
    ///
    /// - Parameter call: The function call
    /// - Returns: Tuple of (typeName, sparqlQuery, optionalVariable)
    /// - Throws: `SPARQLFunctionError.invalidArguments` if format is incorrect
    private func extractArguments(_ call: FunctionCall) throws -> (typeName: String, sparqlQuery: String, variable: String?) {
        guard call.arguments.count >= 2, call.arguments.count <= 3 else {
            throw SPARQLFunctionError.invalidArguments(
                "SPARQL() requires 2-3 arguments: SPARQL(TypeName, 'query', ['?variable'])"
            )
        }

        // First argument: type name (column reference or literal)
        let typeName: String
        switch call.arguments[0] {
        case .column(let col):
            typeName = col.column
        case .literal(.string(let str)):
            typeName = str
        default:
            throw SPARQLFunctionError.invalidArguments(
                "First argument must be a type name (column or string literal)"
            )
        }

        // Second argument: SPARQL query string
        guard case .literal(.string(let sparqlQuery)) = call.arguments[1] else {
            throw SPARQLFunctionError.invalidArguments(
                "Second argument must be a SPARQL query string literal"
            )
        }

        // Third argument (optional): variable name
        let extractVar: String?
        if call.arguments.count == 3 {
            guard case .literal(.string(let varName)) = call.arguments[2] else {
                throw SPARQLFunctionError.invalidArguments(
                    "Third argument must be a variable name string literal (e.g., '?s')"
                )
            }
            extractVar = varName.hasPrefix("?") ? varName : "?\(varName)"
        } else {
            extractVar = nil
        }

        return (typeName, sparqlQuery, extractVar)
    }

    // MARK: - SPARQL Execution

    /// Execute SPARQL within the current transaction scope
    ///
    /// - Parameters:
    ///   - sparqlQuery: SPARQL query string
    ///   - indexSubspace: Resolved index subspace
    ///   - metadata: RDF dataset index metadata
    ///   - includedFieldNames: Stored field names for the index
    /// - Returns: SPARQL result
    /// - Throws: SPARQL execution errors
    private func executeSPARQL(
        sparqlQuery: String,
        entityName: String,
        indexDescriptor: IndexDescriptor,
        metadata: RDFDatasetIndexMetadata,
        includedFieldNames: [String]
    ) async throws -> SPARQLResult {
        let readableIndex = try await context.indexQueryContext
            .readableIndex(
                named: indexDescriptor.name,
                indexType: indexDescriptor.type,
                forEntityName: entityName,
                partitions: FieldObject(),
                transaction: transaction
            )
        let sources: [RDFDatasetSource]
        if let readableIndex {
            sources = [
                RDFDatasetSource(
                    entityName: entityName,
                    indexName: indexDescriptor.name,
                    indexSubspace: readableIndex.subspace,
                    coverage: try metadata.graphMapping.sourceCoverage,
                    includedFieldNames: includedFieldNames
                )
            ]
        } else {
            sources = []
        }
        return try await _executeSPARQLString(
            sparqlQuery,
            database: context.container.engine,
            sources: sources,
            monotonicClock: context.container.monotonicClock,
            wallClock: context.container.wallClock,
            transaction: transaction,
            compilationLimits: SPARQLExpressionCompilationLimits(
                structuralLimits: structuralLimits
            ),
            workMeter: workMeter
        )
    }

    private func admitInlinedLiterals(_ count: Int) throws {
        try inliningStructureMeter.admit(
            count: count,
            limits: structuralLimits
        )
    }

    // MARK: - FieldValue Conversion

    private func fieldValueToLiteral(
        _ fieldValue: FieldValue
    ) throws -> Literal {
        do {
            return try fieldValue.toLiteral()
        } catch {
            throw SPARQLFunctionError.invalidArguments(error.description)
        }
    }
}

// MARK: - Optional async map helper

extension Optional {
    fileprivate func asyncMap<T>(_ transform: (Wrapped) async throws -> T) async rethrows -> T? {
        switch self {
        case .some(let value):
            return try await transform(value)
        case .none:
            return nil
        }
    }
}
#endif

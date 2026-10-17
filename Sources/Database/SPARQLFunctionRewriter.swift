// SPARQLFunctionRewriter.swift
// Database - Rewrite SelectQuery by executing SPARQL() functions

#if DATABASE_GRAPH_INDEXES
import DatabaseKit
import QueryAST
@_spi(DatabaseExecution) import GraphIndex
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseTypes
import StorageKit
import Synchronization

/// Rewrites SelectQuery by executing SPARQL() subqueries
///
/// **Design**: Pre-execution rewrite at DatabaseContext level.
/// - Finds SPARQL() function calls in Expression tree
/// - Executes SPARQL queries within parent transaction
/// - Inlines results as literal arrays
/// - Returns a prepared query that retains literal storage and reservations
///
/// **Usage**:
/// ```swift
/// let retainedStorage = try DatabasePreparedSQLSelectStorage(
///     workMeter: workMeter
/// )
/// let rewriter = SPARQLFunctionRewriter(
///     context: context,
///     workMeter: workMeter,
///     transaction: transaction,
///     retainedStorage: retainedStorage
/// )
/// let prepared = try await rewriter.rewritePrepared(selectQuery)
/// // Execute through DatabasePreparedSQLSelect.execute.
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

    private final class OccurrenceRewrite: Sendable {
        struct Snapshot: Sendable {
            let occurrences: QueryOccurrenceArena
            let root: QueryOccurrenceID
            let origins: [QueryOccurrenceID: QueryOccurrenceID]
        }

        private struct State {
            var occurrences: QueryOccurrenceArena
            var origins: [QueryOccurrenceID: QueryOccurrenceID] = [:]
            var ledger: QueryStructuralResourceLedger
        }

        let original: QueryOccurrenceArena
        private let state: Mutex<State>
        private let reservation: DatabaseIntermediateReservation

        init(original: QueryOccurrenceArena, limits: QueryStructuralLimits,
             reservation: DatabaseIntermediateReservation) {
            self.original = original
            self.reservation = reservation
            self.state = Mutex(State(occurrences: QueryOccurrenceArena(limits: limits),
                                     ledger: QueryStructuralResourceLedger(limits: limits)))
        }

        func append(original origin: QueryOccurrenceID, parent: QueryOccurrenceID?,
                    field: QueryOccurrenceField, index: UInt32? = nil) throws -> Cursor {
            let rewritten = try state.withLock { state in
                try state.ledger.consume(.collectionElements)
                let bytes = MemoryLayout<QueryOccurrence>.stride
                    + MemoryLayout<QueryOccurrenceID>.stride * 4
                    + MemoryLayout<QueryOccurrenceField>.stride + MemoryLayout<UInt32?>.stride
                try state.ledger.consume(.retainedBytes, amount: UInt64(bytes))
                try reservation.reserveAdditional(bytes: UInt64(bytes), at: .bindingCandidate)
                let rewritten = try state.occurrences.append(parent: parent, field: field, index: index)
                state.origins[rewritten] = origin
                return rewritten
            }
            return Cursor(owner: self, original: origin, rewritten: rewritten)
        }

        func snapshot(root: QueryOccurrenceID) -> Snapshot {
            state.withLock { Snapshot(occurrences: $0.occurrences, root: root, origins: $0.origins) }
        }

        struct Cursor: Sendable {
            let owner: OccurrenceRewrite
            let original: QueryOccurrenceID
            let rewritten: QueryOccurrenceID

            func child(_ field: QueryOccurrenceField, _ index: Int? = nil,
                       rewrittenIndex: Int? = nil) throws -> Cursor? {
                guard let origin = try originalChild(field, index) else { return nil }
                return try owner.append(original: origin, parent: rewritten, field: field,
                                        index: try (rewrittenIndex ?? index).map(Self.ordinal))
            }

            func originalChild(_ field: QueryOccurrenceField, _ index: Int? = nil) throws -> QueryOccurrenceID? {
                owner.original.child(of: original, field: field, index: try index.map(Self.ordinal))
            }

            func generatedChild(origin: QueryOccurrenceID, index: Int) throws {
                _ = try owner.append(original: origin, parent: rewritten, field: .argument, index: Self.ordinal(index))
            }

            func copyTypeDescendants() throws {
                if let child = try child(.elementType) {
                    try child.copyTypeDescendants()
                }
            }

            private static func ordinal(_ index: Int) throws -> UInt32 {
                guard let ordinal = UInt32(exactly: index) else { throw QueryOccurrenceError.identifierOverflow }
                return ordinal
            }
        }
    }

    private let context: DatabaseContext
    private let workMeter: DatabaseWorkMeter
    private let transaction: DatabaseReadTransaction
    private let structuralLimits: QueryStructuralLimits
    private let retainedStorage: DatabasePreparedSQLSelectStorage
    private let inliningStructureMeter = InliningStructureMeter()
    private let parameters: [QueryParameter]

    /// Initialize with DatabaseContext
    ///
    /// - Parameters:
    ///   - context: Context for schema and index access.
    ///   - workMeter: Shared resource budget for graph and SQL execution.
    ///   - transaction: Parent SQL read transaction.
    ///   - retainedStorage: Owner for rewritten literal buffers.
    ///   - structuralLimits: Limits shared by graph compilation and rewritten SQL.
    internal init(
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseReadTransaction,
        retainedStorage: DatabasePreparedSQLSelectStorage,
        structuralLimits: QueryStructuralLimits = .default,
        parameters: [QueryParameter] = []
    ) {
        self.context = context
        self.workMeter = workMeter
        self.transaction = transaction
        self.retainedStorage = retainedStorage
        self.structuralLimits = structuralLimits
        self.parameters = parameters
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
                for item in nestedQuery.projection {
                    if case .expression(let expression, _) = item { pendingExpressions.append(expression) }
                }
                if case .distinctOn(let expressions) = nestedQuery.quantifier {
                    pendingExpressions.append(contentsOf: expressions)
                }
                if case .expression(let expression) = nestedQuery.pagination.offset {
                    pendingExpressions.append(expression)
                }
                if case .count(.expression(let expression), _) = nestedQuery.pagination.limit {
                    pendingExpressions.append(expression)
                }
                if let filter = nestedQuery.filter {
                    pendingExpressions.append(filter)
                }
                for grouping in nestedQuery.groupBy ?? [] {
                    appendGroupingExpressions(grouping, to: &pendingExpressions)
                }
                for window in nestedQuery.windows {
                    appendWindowExpressions(window.specification, to: &pendingExpressions)
                }
                if let having = nestedQuery.having {
                    pendingExpressions.append(having)
                }
                pendingExpressions.append(
                    contentsOf: (nestedQuery.orderBy ?? []).map {
                        $0.expression
                    }
                )
                for subquery in nestedQuery.subqueries ?? [] {
                    pendingQueries.append(subquery.query)
                }
                pendingSources.append(nestedQuery.source)
                continue
            }
            if let source = pendingSources.popLast() {
                switch source {
                case .subquery(let nestedQuery):
                    pendingQueries.append(nestedQuery)
                case .join(let join):
                    pendingSources.append(join.left)
                    pendingSources.append(join.right)
                    if case .on(let expression) = join.condition {
                        pendingExpressions.append(expression)
                    }
                case .aliased(let inner, _, _): pendingSources.append(inner)
                case .setOperation(_, _, let left, let right):
                    pendingQueries.append(left)
                    pendingQueries.append(right)
                case .values(let rows):
                    for row in rows { pendingExpressions.append(contentsOf: row) }
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
                case .table, .logical:
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
                        case .sql(let call): appendAggregateExpressions(call, to: &pendingExpressions)
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
                                contentsOf: (orderBy ?? []).map {
                                    $0.expression
                                }
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
                if call.schema == nil && call.name == "sparql" {
                    return true
                }
                pendingExpressions.append(contentsOf: call.arguments)

            case .collectionOperation(_, let left, let right),
                 .add(let left, let right),
                 .subtract(let left, let right),
                 .multiply(let left, let right),
                 .divide(let left, let right),
                 .modulo(let left, let right),
                 .concatenate(let left, let right),
                 .equal(let left, let right),
                 .notEqual(let left, let right),
                 .lessThan(let left, let right),
                 .lessThanOrEqual(let left, let right),
                 .greaterThan(let left, let right),
                 .greaterThanOrEqual(let left, let right),
                 .and(let left, let right),
                 .or(let left, let right),
                 .nullIf(let left, let right), .isDistinctFrom(let left, let right, _):
                pendingExpressions.append(left)
                pendingExpressions.append(right)

            case .collate(let inner, _), .field(let inner, _), .fieldExpansion(let inner),
                 .negate(let inner),
                 .not(let inner),
                 .truthTest(let inner, _, _),
                 .isNull(let inner),
                 .isNotNull(let inner),
                 .regex(let inner, _, _),
                 .cast(let inner, _),
                 .isTriple(let inner),
                 .subject(let inner),
                 .predicate(let inner),
                 .object(let inner):
                pendingExpressions.append(inner)

            case .arrayAccess(let base, let subscripts):
                pendingExpressions.append(base)
                for subscriptValue in subscripts {
                    switch subscriptValue {
                    case .index(let value): pendingExpressions.append(value)
                    case .slice(let lower, let upper):
                        if let lower { pendingExpressions.append(lower) }
                        if let upper { pendingExpressions.append(upper) }
                    }
                }
            case .patternMatch(let subject, let pattern, let escape, _):
                pendingExpressions.append(subject)
                pendingExpressions.append(pattern)
                if let escape { pendingExpressions.append(escape) }

            case .between(let value, let low, let high):
                pendingExpressions.append(value)
                pendingExpressions.append(low)
                pendingExpressions.append(high)

            case .inList(let value, let values),
                 .notInList(let value, let values):
                pendingExpressions.append(value)
                pendingExpressions.append(contentsOf: values)

            case .quantifiedComparison(let lhs, _, _, let source):
                pendingExpressions.append(lhs)
                switch source {
                case .array(let value): pendingExpressions.append(value)
                case .subquery(let query): pendingQueries.append(query)
                }
            case .inSubquery(let value, let subquery):
                pendingExpressions.append(value)
                pendingQueries.append(subquery)

            case .caseWhen(let operand, let cases, let elseResult):
                if let operand { pendingExpressions.append(operand) }
                for pair in cases {
                    pendingExpressions.append(pair.condition)
                    pendingExpressions.append(pair.result)
                }
                if let elseResult {
                    pendingExpressions.append(elseResult)
                }

            case .coalesce(let expressions), .array(let expressions), .row(let expressions), .greatest(let expressions), .least(let expressions), .grouping(let expressions):
                pendingExpressions.append(contentsOf: expressions)

            case .triple(let subject, let predicate, let object):
                pendingExpressions.append(subject)
                pendingExpressions.append(predicate)
                pendingExpressions.append(object)

            case .subquery(let subquery), .exists(let subquery), .arraySubquery(let subquery):
                pendingQueries.append(subquery)

            case .window(let call):
                switch call.function {
                case .aggregate(let aggregate): appendAggregateExpressions(aggregate, to: &pendingExpressions)
                case .analytic(_, let arguments): pendingExpressions.append(contentsOf: arguments)
                }
                if case .specification(let specification) = call.reference {
                    appendWindowExpressions(specification, to: &pendingExpressions)
                }
            case .aggregate(let aggregate):
                switch aggregate {
                case .sql(let call): appendAggregateExpressions(call, to: &pendingExpressions)
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

            case .literal, .currentValue, .column, .variable, .parameter, .bound, .wholeRow:
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
    internal func rewritePrepared(
        _ query: SelectQuery,
        parameters: [QueryParameter] = [],
        metadataReservation: DatabaseIntermediateReservation? = nil,
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> DatabasePreparedSQLSelect {
        let reservation = try metadataReservation ?? workMeter.reserveIntermediate(
            bytes: 128, at: .bindingCandidate
        )
        let cursor: OccurrenceRewrite.Cursor?
        if let occurrences, let occurrence {
            let rewrite = OccurrenceRewrite(original: occurrences, limits: structuralLimits,
                                            reservation: reservation)
            cursor = try rewrite.append(original: occurrence, parent: nil, field: .statement)
        } else {
            cursor = nil
        }
        let rewrittenQuery = try await rewrite(query, provenance: cursor)
        let snapshot = cursor.map { $0.owner.snapshot(root: $0.rewritten) }
        return DatabasePreparedSQLSelect(
            query: rewrittenQuery,
            workMeter: workMeter,
            parameters: parameters,
            metadataReservation: reservation,
            occurrences: snapshot?.occurrences,
            occurrence: snapshot?.root,
            originalOccurrences: occurrences,
            originalOccurrence: occurrence,
            occurrenceOrigins: snapshot?.origins ?? [:],
            retainedStorage: retainedStorage
        )
    }

    private func rewrite(_ query: SelectQuery, provenance: OccurrenceRewrite.Cursor? = nil) async throws -> SelectQuery {
        let rewrittenFilter: DatabaseKit.Expression?
        if let filter = query.filter {
            rewrittenFilter = try await rewriteExpression(filter, provenance: try provenance?.child(.filter))
        } else {
            rewrittenFilter = nil
        }
        var rewrittenSubqueries: [NamedSubquery] = []
        rewrittenSubqueries.reserveCapacity(query.subqueries?.count ?? 0)
        for (index, subquery) in (query.subqueries ?? []).enumerated() {
            rewrittenSubqueries.append(
                NamedSubquery(
                    name: subquery.name,
                    columns: subquery.columns,
                    query: try await rewrite(subquery.query, provenance: try provenance?.child(.cte, index)),
                    materialized: subquery.materialized
                )
            )
        }

        return SelectQuery(
            projection: try await rewriteProjection(query.projection, provenance: provenance),
            source: try await rewriteSource(query.source, provenance: try provenance?.child(.source)),
            accessPath: query.accessPath,
            filter: rewrittenFilter,
            groupBy: try await rewriteGroupingList(query.groupBy, provenance: provenance),
            having: try await rewriteOptionalExpression(query.having, provenance: try provenance?.child(.having)),
            windows: try await rewriteNamedWindows(query.windows, provenance: provenance),
            orderBy: try await rewriteOptionalSortKeys(query.orderBy, provenance: provenance, field: .orderBy),
            quantifier: try await rewriteQuantifier(query.quantifier, provenance: provenance),
            pagination: try await rewritePagination(query.pagination, provenance: provenance),
            subqueries: query.subqueries == nil ? nil : rewrittenSubqueries,
            reduced: query.reduced,
            dataset: query.dataset
        )
    }

    private func rewriteSource(_ source: DataSource, provenance: OccurrenceRewrite.Cursor? = nil) async throws -> DataSource {
        switch source {
        case .subquery(let query):
            return .subquery(try await rewrite(query, provenance: try provenance?.child(.subquery)))
        case .aliased(let inner, let name, let columns):
            return .aliased(try await rewriteSource(inner, provenance: try provenance?.child(.source)),
                            name: name, columns: columns)
        case .values(let rows):
            var rewritten: [[DatabaseKit.Expression]] = []
            for (rowIndex, row) in rows.enumerated() {
                let rowProvenance = try provenance?.child(.valuesRow, rowIndex)
                var cells: [DatabaseKit.Expression] = []
                for (columnIndex, expression) in row.enumerated() {
                    cells.append(try await rewriteExpression(expression,
                        provenance: try rowProvenance?.child(.valuesCell, columnIndex)))
                }
                rewritten.append(cells)
            }
            return .values(rewritten)
        case .join(let join):
            let condition: JoinCondition?
            switch join.condition {
            case .on(let expression):
                condition = .on(try await rewriteExpression(expression, provenance: try provenance?.child(.joinCondition)))
            case .using(let columns):
                condition = .using(columns)
            case nil:
                condition = nil
            }
            return .join(
                JoinClause(
                    type: join.type,
                    left: try await rewriteSource(join.left, provenance: try provenance?.child(.joinLeft)),
                    right: try await rewriteSource(join.right, provenance: try provenance?.child(.joinRight)),
                    condition: condition
                )
            )
        case .setOperation(let operation, let quantifier, let left, let right):
            return .setOperation(operator: operation, quantifier: quantifier,
                left: try await rewrite(left, provenance: try provenance?.child(.setLeft)),
                right: try await rewrite(right, provenance: try provenance?.child(.setRight)))
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
                    columns: graphTable.columns == nil ? nil : columns
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
        case .table, .logical:
            return source
        }
    }

    private func rewritePattern(
        _ pattern: GraphPattern,
        provenance: OccurrenceRewrite.Cursor? = nil
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
        _ expressions: [DatabaseKit.Expression],
        provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> [DatabaseKit.Expression] {
        var rewritten: [DatabaseKit.Expression] = []
        rewritten.reserveCapacity(expressions.count)
        for expression in expressions {
            rewritten.append(try await rewriteExpression(expression))
        }
        return rewritten
    }

    private func rewriteProjection(
        _ projection: [ProjectionItem], provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> [ProjectionItem] {
        var rewritten: [ProjectionItem] = []
        for (index, item) in projection.enumerated() {
            switch item {
            case .wildcard: rewritten.append(item)
            case .expression(let expression, let alias):
                rewritten.append(.expression(try await rewriteExpression(expression,
                    provenance: try provenance?.child(.projection, index)), alias: alias))
            }
        }
        return rewritten
    }

    private func rewriteQuantifier(
        _ quantifier: SelectQuantifier, provenance: OccurrenceRewrite.Cursor?
    ) async throws -> SelectQuantifier {
        guard case .distinctOn(let expressions) = quantifier else { return quantifier }
        var rewritten: [DatabaseKit.Expression] = []
        for (index, expression) in expressions.enumerated() {
            rewritten.append(try await rewriteExpression(expression,
                provenance: try provenance?.child(.distinctOn, index)))
        }
        return .distinctOn(rewritten)
    }

    private func rewritePagination(
        _ pagination: QueryPagination, provenance: OccurrenceRewrite.Cursor?
    ) async throws -> QueryPagination {
        let offset: QueryCount?
        if case .expression(let expression) = pagination.offset {
            offset = .expression(try await rewriteExpression(expression,
                provenance: try provenance?.child(.offset)))
        } else { offset = pagination.offset }
        let limit: QueryLimit?
        if case .count(.expression(let expression), let withTies) = pagination.limit {
            limit = .count(.expression(try await rewriteExpression(expression,
                provenance: try provenance?.child(.limit))), withTies: withTies)
        } else { limit = pagination.limit }
        return QueryPagination(limit: limit, offset: offset)
    }

    private func rewriteOptionalExpressions(
        _ expressions: [DatabaseKit.Expression]?,
        provenance: OccurrenceRewrite.Cursor? = nil,
        field: QueryOccurrenceField = .argument
    ) async throws -> [DatabaseKit.Expression]? {
        guard let expressions else { return nil }
        var rewritten: [DatabaseKit.Expression] = []
        rewritten.reserveCapacity(expressions.count)
        for (index, expression) in expressions.enumerated() {
            rewritten.append(try await rewriteExpression(expression, provenance: try provenance?.child(field, index)))
        }
        return rewritten
    }

    private func rewriteOptionalExpression(
        _ expression: DatabaseKit.Expression?,
        provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> DatabaseKit.Expression? {
        guard let expression else { return nil }
        return try await rewriteExpression(expression, provenance: provenance)
    }

    private func rewriteOptionalSortKeys(
        _ sortKeys: [SortKey]?,
        provenance: OccurrenceRewrite.Cursor? = nil,
        field: QueryOccurrenceField = .aggregateOrderBy
    ) async throws -> [SortKey]? {
        guard let sortKeys else { return nil }
        var rewritten: [SortKey] = []
        rewritten.reserveCapacity(sortKeys.count)
        for (index, sortKey) in sortKeys.enumerated() {
            rewritten.append(
                SortKey(
                    try await rewriteExpression(sortKey.expression, provenance: try provenance?.child(field, index)),
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
        _ expr: DatabaseKit.Expression,
        provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> DatabaseKit.Expression {
        switch expr {
        case .collate(let expression, let collation):
            return .collate(try await rewriteExpression(expression, provenance: try provenance?.child(.operand, 0)), collation: collation)
        case .field(let expression, let name):
            return .field(try await rewriteExpression(expression, provenance: try provenance?.child(.operand, 0)), name: name)
        case .fieldExpansion(let expression):
            return .fieldExpansion(try await rewriteExpression(expression, provenance: try provenance?.child(.operand, 0)))
        case .arraySubquery(let query):
            return .arraySubquery(try await rewrite(query, provenance: try provenance?.child(.subquery)))
        case .wholeRow: return expr
        case .inList(let lhs, let values):
            return .inList(
                try await rewriteExpression(lhs, provenance: try provenance?.child(.operand, 0)),
                values: try await rewriteMembershipValues(values, provenance: provenance)
            )

        case .notInList(let lhs, let values):
            return .notInList(
                try await rewriteExpression(lhs, provenance: try provenance?.child(.operand, 0)),
                values: try await rewriteMembershipValues(values, provenance: provenance)
            )

        case .inSubquery(let lhs, let subquery):
            // Check if subquery contains SPARQL() - recursively rewrite
            let rewrittenSubquery = try await rewrite(subquery, provenance: try provenance?.child(.subquery))
            return .inSubquery(try await rewriteExpression(lhs, provenance: try provenance?.child(.operand, 0)), subquery: rewrittenSubquery)

        // Logical operators - recurse
        case .and(let left, let right):
            return .and(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .or(let left, let right):
            return .or(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .not(let inner):
            return .not(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))

        // Comparison operators - recurse on both sides
        case .equal(let left, let right):
            return .equal(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .notEqual(let left, let right):
            return .notEqual(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .lessThan(let left, let right):
            return .lessThan(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .lessThanOrEqual(let left, let right):
            return .lessThanOrEqual(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .greaterThan(let left, let right):
            return .greaterThan(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .greaterThanOrEqual(let left, let right):
            return .greaterThanOrEqual(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        // Arithmetic operators - recurse
        case .add(let left, let right):
            return .add(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .subtract(let left, let right):
            return .subtract(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .multiply(let left, let right):
            return .multiply(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .divide(let left, let right):
            return .divide(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .modulo(let left, let right):
            return .modulo(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .negate(let inner):
            return .negate(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))

        // Other cases that might contain expressions
        case .between(let expr, let low, let high):
            return .between(
                try await rewriteExpression(expr, provenance: try provenance?.child(.operand, 0)),
                low: try await rewriteExpression(low, provenance: try provenance?.child(.operand, 1)),
                high: try await rewriteExpression(high, provenance: try provenance?.child(.operand, 2))
            )

        case .isNull(let inner):
            return .isNull(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))

        case .isNotNull(let inner):
            return .isNotNull(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))

        case .patternMatch(let subject, let pattern, let escape, let kind):
            let rewrittenSubject = try await rewriteExpression(subject, provenance: try provenance?.child(.operand, 0))
            let rewrittenPattern = try await rewriteExpression(pattern, provenance: try provenance?.child(.operand, 1))
            let rewrittenEscape = try await rewriteOptionalExpression(escape, provenance: try provenance?.child(.operand, 2))
            return .patternMatch(subject: rewrittenSubject, pattern: rewrittenPattern,
                                 escape: rewrittenEscape, kind: kind)

        case .concatenate(let left, let right):
            return .concatenate(
                try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)),
                try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        case .regex(let inner, let pattern, let flags):
            return .regex(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)), pattern: pattern, flags: flags)

        case .cast(let inner, let targetType):
            try provenance?.child(.castType)?.copyTypeDescendants()
            return .cast(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)), targetType: targetType)

        case .caseWhen(let operand, let cases, let elseResult):
            let rewrittenOperand = try await rewriteOptionalExpression(operand, provenance: try provenance?.child(.operand, 0))
            var rewrittenCases: [CaseWhenPair] = []
            for (index, pair) in cases.enumerated() {
                rewrittenCases.append(CaseWhenPair(
                    condition: try await rewriteExpression(pair.condition, provenance: try provenance?.child(.caseCondition, index)),
                    result: try await rewriteExpression(pair.result, provenance: try provenance?.child(.caseResult, index))
                ))
            }
            let rewrittenElse = try await elseResult.asyncMap { try await rewriteExpression($0, provenance: try provenance?.child(.caseElse)) }
            return .caseWhen(operand: rewrittenOperand, cases: rewrittenCases, elseResult: rewrittenElse)

        case .arrayAccess(let base, let subscripts):
            let rewrittenBase = try await rewriteExpression(base, provenance: try provenance?.child(.operand))
            var rewrittenSubscripts: [ArraySubscript] = []
            for (dimension, subscriptValue) in subscripts.enumerated() {
                switch subscriptValue {
                case .index(let value):
                    rewrittenSubscripts.append(.index(try await rewriteExpression(value,
                        provenance: try provenance?.child(.argument, 2 * dimension))))
                case .slice(let lower, let upper):
                    let rewrittenLower = try await rewriteOptionalExpression(lower,
                        provenance: try provenance?.child(.argument, 2 * dimension))
                    let rewrittenUpper = try await rewriteOptionalExpression(upper,
                        provenance: try provenance?.child(.argument, 2 * dimension + 1))
                    rewrittenSubscripts.append(.slice(lower: rewrittenLower, upper: rewrittenUpper))
                }
            }
            return .arrayAccess(rewrittenBase, subscripts: rewrittenSubscripts)
        case .collectionOperation(let operation, let left, let right):
            return .collectionOperation(operation,
                left: try await rewriteExpression(left, provenance: try provenance?.child(.left)),
                right: try await rewriteExpression(right, provenance: try provenance?.child(.right)))
        case .array(let exprs), .row(let exprs):
            var rewrittenExprs: [DatabaseKit.Expression] = []
            for (index, expr) in exprs.enumerated() { rewrittenExprs.append(try await rewriteExpression(expr, provenance: try provenance?.child(.argument, index))) }
            if case .array = expr { return .array(rewrittenExprs) }
            return .row(rewrittenExprs)

        case .greatest(let exprs), .least(let exprs):
            var rewritten: [Expression] = []
            for (index, value) in exprs.enumerated() {
                rewritten.append(try await rewriteExpression(value, provenance: try provenance?.child(.argument, index)))
            }
            if case .greatest = expr { return .greatest(rewritten) }
            return .least(rewritten)
        case .truthTest(let value, let truth, let negated):
            return .truthTest(try await rewriteExpression(value, provenance: try provenance?.child(.operand, 0)), value: truth, negated: negated)
        case .isDistinctFrom(let lhs, let rhs, let negated):
            return .isDistinctFrom(try await rewriteExpression(lhs, provenance: try provenance?.child(.operand, 0)),
                try await rewriteExpression(rhs, provenance: try provenance?.child(.operand, 1)), negated: negated)
        case .quantifiedComparison(let lhs, let comparison, let quantifier, let source):
            let left = try await rewriteExpression(lhs, provenance: try provenance?.child(.operand, 0))
            let rewrittenSource: QuantifiedComparisonSource
            switch source {
            case .array(let value): rewrittenSource = .array(try await rewriteExpression(value, provenance: try provenance?.child(.operand, 1)))
            case .subquery(let query): rewrittenSource = .subquery(try await rewrite(query, provenance: try provenance?.child(.subquery)))
            }
            return .quantifiedComparison(left, comparison: comparison, quantifier: quantifier, source: rewrittenSource)

        case .coalesce(let exprs):
            var rewrittenExprs: [DatabaseKit.Expression] = []
            for (index, expr) in exprs.enumerated() {
                rewrittenExprs.append(try await rewriteExpression(expr, provenance: try provenance?.child(.argument, index)))
            }
            return .coalesce(rewrittenExprs)

        case .nullIf(let left, let right):
            return .nullIf(try await rewriteExpression(left, provenance: try provenance?.child(.operand, 0)), try await rewriteExpression(right, provenance: try provenance?.child(.operand, 1)))

        // Function call - check if it's SPARQL()
        case .function(let call):
            if call.schema == nil && call.name == "sparql" {
                throw SPARQLFunctionError.invalidArguments(
                    "SPARQL() must be used as an IN-list item"
                )
            }
            // Other functions - recurse on arguments
            var rewrittenArgs: [DatabaseKit.Expression] = []
            for (index, arg) in call.arguments.enumerated() {
                rewrittenArgs.append(try await rewriteExpression(arg, provenance: try provenance?.child(.argument, index)))
            }
            return .function(FunctionCall(name: call.name, arguments: rewrittenArgs, distinct: call.distinct, schema: call.schema, finalArrayIsVariadic: call.finalArrayIsVariadic))

        case .grouping(let expressions):
            var rewritten: [Expression] = []
            for (index, expression) in expressions.enumerated() {
                rewritten.append(try await rewriteExpression(expression, provenance: try provenance?.child(.argument, index)))
            }
            return .grouping(rewritten)
        case .window(let call):
            let functionProvenance = try provenance?.child(.windowFunction)
            let function: WindowFunction
            switch call.function {
            case .aggregate(let aggregate):
                function = .aggregate(try await rewriteSQLAggregate(aggregate, provenance: functionProvenance))
            case .analytic(let name, let arguments):
                var rewritten: [Expression] = []
                for (index, argument) in arguments.enumerated() {
                    rewritten.append(try await rewriteExpression(argument, provenance: try functionProvenance?.child(.argument, index)))
                }
                function = .analytic(name, arguments: rewritten)
            }
            let referenceProvenance = try provenance?.child(.windowReference)
            let reference: WindowReference
            switch call.reference {
            case .named: reference = call.reference
            case .specification(let specification):
                reference = .specification(try await rewriteWindowSpecification(specification, provenance: referenceProvenance))
            }
            return .window(WindowCall(function: function, reference: reference))
        case .aggregate(let aggregate):
            return .aggregate(try await rewriteAggregate(aggregate, provenance: provenance))

        // Terminal cases - no recursion needed
        case .literal, .currentValue, .column, .variable, .parameter, .bound:
            return expr

        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try await rewriteExpression(subject, provenance: try provenance?.child(.operand, 0)),
                predicate: try await rewriteExpression(predicate, provenance: try provenance?.child(.operand, 1)),
                object: try await rewriteExpression(object, provenance: try provenance?.child(.operand, 2))
            )
        case .isTriple(let inner):
            return .isTriple(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))
        case .subject(let inner):
            return .subject(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))
        case .predicate(let inner):
            return .predicate(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))
        case .object(let inner):
            return .object(try await rewriteExpression(inner, provenance: try provenance?.child(.operand, 0)))

        // Subquery expression cases
        case .exists(let subquery):
            return .exists(try await rewrite(subquery, provenance: try provenance?.child(.subquery)))

        case .subquery(let subquery):
            return .subquery(try await rewrite(subquery, provenance: try provenance?.child(.subquery)))
        }
    }

    private func rewriteAggregate(
        _ aggregate: AggregateFunction,
        provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> AggregateFunction {
        switch aggregate {
        case .sql(let call): return .sql(try await rewriteSQLAggregate(call, provenance: provenance))
        case .count(let expression, let distinct):
            return .count(
                try await rewriteOptionalExpression(expression, provenance: try provenance?.child(.aggregateArgument)),
                distinct: distinct
            )
        case .sum(let expression, let distinct):
            return .sum(
                try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)),
                distinct: distinct
            )
        case .avg(let expression, let distinct):
            return .avg(
                try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)),
                distinct: distinct
            )
        case .min(let expression):
            return .min(try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)))
        case .max(let expression):
            return .max(try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)))
        case .groupConcat(let expression, let separator, let distinct):
            return .groupConcat(
                try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)),
                separator: separator,
                distinct: distinct
            )
        case .sample(let expression):
            return .sample(try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)))
        case .arrayAgg(let expression, let orderBy, let distinct):
            return .arrayAgg(
                try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument)),
                orderBy: try await rewriteOptionalSortKeys(orderBy, provenance: provenance),
                distinct: distinct
            )
        }
    }

    private static func appendAggregateExpressions(_ call: SQLAggregateCall, to expressions: inout [Expression]) {
        expressions.append(contentsOf: call.arguments)
        for key in call.orderBy { expressions.append(key.expression) }
        if let filter = call.filter { expressions.append(filter) }
        for key in call.withinGroup ?? [] { expressions.append(key.expression) }
    }

    private static func appendGroupingExpressions(_ grouping: GroupingElement, to expressions: inout [Expression]) {
        var pending = [grouping]
        while let element = pending.popLast() {
            switch element {
            case .expression(let expression): expressions.append(expression)
            case .rollup(let components), .cube(let components):
                for component in components { expressions.append(contentsOf: component) }
            case .groupingSets(let sets):
                for set in sets { pending.append(contentsOf: set) }
            }
        }
    }

    private static func appendWindowExpressions(_ specification: WindowSpecification, to expressions: inout [Expression]) {
        expressions.append(contentsOf: specification.partitionBy)
        for key in specification.orderBy { expressions.append(key.expression) }
        if let frame = specification.frame {
            for bound in [frame.start, frame.end] {
                switch bound {
                case .preceding(let expression), .following(let expression): expressions.append(expression)
                default: break
                }
            }
        }
    }

    private func rewriteSQLAggregate(_ call: SQLAggregateCall, provenance: OccurrenceRewrite.Cursor?) async throws -> SQLAggregateCall {
        var arguments: [Expression] = []
        for (index, expression) in call.arguments.enumerated() {
            arguments.append(try await rewriteExpression(expression, provenance: try provenance?.child(.aggregateArgument, index)))
        }
        return SQLAggregateCall(function: call.function, arguments: arguments, distinct: call.distinct,
            orderBy: try await rewriteOptionalSortKeys(call.orderBy, provenance: provenance, field: .aggregateOrderBy) ?? [],
            filter: try await rewriteOptionalExpression(call.filter, provenance: try provenance?.child(.aggregateFilter)),
            withinGroup: try await rewriteOptionalSortKeys(call.withinGroup, provenance: provenance, field: .aggregateWithinGroup), schema: call.schema)
    }

    private func rewriteGroupingList(_ elements: [GroupingElement]?, provenance: OccurrenceRewrite.Cursor?) async throws -> [GroupingElement]? {
        guard let elements else { return nil }
        var rewritten: [GroupingElement] = []
        for (index, element) in elements.enumerated() {
            rewritten.append(try await rewriteGrouping(element, provenance: try provenance?.child(.groupBy, index)))
        }
        return rewritten
    }

    private func rewriteGrouping(_ element: GroupingElement, provenance: OccurrenceRewrite.Cursor?) async throws -> GroupingElement {
        switch element {
        case .expression(let expression):
            return .expression(try await rewriteExpression(expression, provenance: try provenance?.child(.operand)))
        case .rollup(let components), .cube(let components):
            var rewritten: [[Expression]] = []
            for (index, component) in components.enumerated() {
                let child = try provenance?.child(.groupingComponent, index)
                var expressions: [Expression] = []
                for (ordinal, expression) in component.enumerated() {
                    expressions.append(try await rewriteExpression(expression, provenance: try child?.child(.argument, ordinal)))
                }
                rewritten.append(expressions)
            }
            if case .rollup = element { return .rollup(rewritten) }
            return .cube(rewritten)
        case .groupingSets(let sets):
            var rewritten: [[GroupingElement]] = []
            for (index, set) in sets.enumerated() {
                let child = try provenance?.child(.groupingSet, index)
                var elements: [GroupingElement] = []
                for (ordinal, element) in set.enumerated() {
                    elements.append(try await rewriteGrouping(element, provenance: try child?.child(.groupingElement, ordinal)))
                }
                rewritten.append(elements)
            }
            return .groupingSets(rewritten)
        }
    }

    private func rewriteNamedWindows(_ windows: [NamedWindow], provenance: OccurrenceRewrite.Cursor?) async throws -> [NamedWindow] {
        var rewritten: [NamedWindow] = []
        for (index, window) in windows.enumerated() {
            let child = try provenance?.child(.namedWindow, index)
            rewritten.append(NamedWindow(name: window.name, specification: try await rewriteWindowSpecification(
                window.specification, provenance: try child?.child(.windowReference))))
        }
        return rewritten
    }

    private func rewriteWindowSpecification(_ specification: WindowSpecification, provenance: OccurrenceRewrite.Cursor?) async throws -> WindowSpecification {
        var partition: [Expression] = []
        for (index, expression) in specification.partitionBy.enumerated() {
            partition.append(try await rewriteExpression(expression, provenance: try provenance?.child(.windowPartitionBy, index)))
        }
        let frame: WindowFrame?
        if let original = specification.frame {
            frame = WindowFrame(mode: original.mode,
                start: try await rewriteFrameBound(original.start, provenance: try provenance?.child(.windowFrameStart)),
                end: try await rewriteFrameBound(original.end, provenance: try provenance?.child(.windowFrameEnd)), exclusion: original.exclusion)
        } else { frame = nil }
        return WindowSpecification(inheritedName: specification.inheritedName, partitionBy: partition,
            orderBy: try await rewriteOptionalSortKeys(specification.orderBy, provenance: provenance, field: .windowOrderBy) ?? [], frame: frame)
    }

    private func rewriteFrameBound(_ bound: WindowFrameBound, provenance: OccurrenceRewrite.Cursor?) async throws -> WindowFrameBound {
        switch bound {
        case .preceding(let expression): return .preceding(try await rewriteExpression(expression, provenance: provenance))
        case .following(let expression): return .following(try await rewriteExpression(expression, provenance: provenance))
        default: return bound
        }
    }

    // MARK: - SPARQL Execution

    private func rewriteMembershipValues(
        _ values: [DatabaseKit.Expression],
        provenance: OccurrenceRewrite.Cursor? = nil
    ) async throws -> [DatabaseKit.Expression] {
        var rewritten = try DatabaseRetainedArrayBuilder<
            DatabaseKit.Expression
        >(
            workMeter: workMeter,
            stage: .expressionEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(DatabaseKit.Expression.self),
            expectedCount: values.count
        )
        var rewrittenIndex = 0
        for (index, value) in values.enumerated() {
            if case .function(let call) = value,
               call.schema == nil && call.name == "sparql" {
                let inlinedValues = try await executeSPARQLFunctionValues(call)
                let origin = try provenance?.originalChild(.argument, index)
                for fieldValue in inlinedValues {
                    if let origin { try provenance?.generatedChild(origin: origin, index: rewrittenIndex) }
                    rewrittenIndex += 1
                    try workMeter.consume(at: .expressionEvaluation)
                    try rewritten.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .valueFootprint(
                                of: QueryValue.primitive(fieldValue),
                                workMeter: workMeter,
                                stage: .expressionEvaluation
                            ),
                        make: {
                            .literal(try fieldValueToLiteral(fieldValue))
                        }
                    )
                }
            } else {
                let rewrittenValue = try await rewriteExpression(
                    value, provenance: try provenance?.child(.argument, index, rewrittenIndex: rewrittenIndex)
                )
                rewrittenIndex += 1
                try rewritten.append(
                    footprint: DatabaseIntermediateFootprint(),
                    make: { rewrittenValue }
                )
            }
        }
        let retained = rewritten.finish().moveRetainingReservation()
        let queryElements = retained.elements
        try retainedStorage.retain(
            elements: retained.elements,
            reservation: retained.reservation
        )
        return queryElements
    }

    /// Execute a SPARQL function and return retained canonical values.
    ///
    /// - Parameter call: The SPARQL() function call
    /// - Returns: Canonical values for a single-variable projection.
    /// - Throws: `SPARQLFunctionError` for invalid arguments or execution errors
    private func executeSPARQLFunctionValues(
        _ call: FunctionCall
    ) async throws -> DatabaseSharedRetainedArray<FieldValue> {
        // 1. Extract arguments (type name, query string, optional variable)
        let producer = try QueryParameterBinder(
            parameters: parameters,
            structuralLimits: structuralLimits
        ).bind(.select(SelectQuery(
            projection: [ProjectionItem(.function(call))],
            source: .values([[]])
        )))
        guard case .select(let query) = producer,
              let item = query.projection.first,
              case .expression(.function(let substitutedCall), _) = item else {
            throw QueryParameterBindingError.invalidTraversalState
        }
        let (typeName, sparqlQuery, extractVar) = try extractArguments(substitutedCall)

        // 2. Resolve type via TypeResolver
        let resolver = TypeResolver(schema: context.container.schema)
        let entity = try resolver.resolve(typeName: typeName)
        guard !entity.hasDynamicDirectory else {
            throw SPARQLFunctionError.invalidArguments(
                "SPARQL() requires an explicit partition for dynamic entity '\(entity.name)'"
            )
        }
        let dataset: RDFDatasetReadResolution
        do {
            dataset = try RDFDatasetReadResolver.resolveRequired(
                entity: entity
            )
        } catch let error as RDFDatasetReadResolutionError {
            switch error {
            case .missing:
                throw SPARQLFunctionError.graphIndexNotFound(entity.name)
            case .ambiguous(let candidates):
                throw SPARQLFunctionError.ambiguousGraphIndexes(
                    typeName: entity.name,
                    candidates: candidates
                )
            }
        }
        let graphIndex = dataset.indexDescriptor

        // 4. Admit and execute the schema-declared index in the parent read
        // transaction. The rewritten SQL query consumes the same snapshot.
        let result = try await executeSPARQL(
            sparqlQuery: sparqlQuery,
            entityName: entity.name,
            indexDescriptor: graphIndex,
            metadata: dataset.metadata,
            includedFieldNames: graphIndex.includedFieldNames
        )
        try admitInlinedLiterals(result.count)

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
        let values: DatabaseRetainedBuffer<FieldValue>
        do {
            values = try result.retainedValues(
                for: variable,
                workMeter: workMeter
            )
        } catch let error as SPARQLRetainedResultError {
            switch error {
            case .missingVariable:
                throw SPARQLFunctionError.missingVariable(variable)
            case .workMeterMismatch:
                throw DatabasePreparedSQLSelectError.workMeterMismatch
            }
        }
        let sortedValues = try values.sortingElements { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return lhs < rhs
        }
        let uniqueValues = try sortedValues.removingAdjacentDuplicates {
            lhs,
            rhs in
            try workMeter.consume(at: .expressionEvaluation)
            return lhs == rhs
        }
        return try uniqueValues.moveToSharedOwnership(
            at: .expressionEvaluation
        )
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
        guard !call.finalArrayIsVariadic else {
            throw SPARQLFunctionError.invalidArguments("SPARQL() does not support SQL VARIADIC")
        }
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
        case .literal(.string(let str)), .literal(.sqlUnknownString(let str)),
             .literal(.queryValue(.primitive(.string(let str)), _)):
            typeName = str
        default:
            throw SPARQLFunctionError.invalidArguments(
                "First argument must be a type name (column or string literal)"
            )
        }

        // Second argument: SPARQL query string
        let sparqlQuery: String
        switch call.arguments[1] {
        case .literal(.string(let value)), .literal(.sqlUnknownString(let value)),
             .literal(.queryValue(.primitive(.string(let value)), _)):
            sparqlQuery = value
        default:
            throw SPARQLFunctionError.invalidArguments(
                "Second argument must be a SPARQL query string literal"
            )
        }

        // Third argument (optional): variable name
        let extractVar: String?
        if call.arguments.count == 3 {
            let variable: String
            switch call.arguments[2] {
            case .literal(.string(let value)), .literal(.sqlUnknownString(let value)),
                 .literal(.queryValue(.primitive(.string(let value)), _)):
                variable = value
            default:
                throw SPARQLFunctionError.invalidArguments(
                    "Third argument must be a variable name string literal (e.g., '?s')"
                )
            }
            extractVar = variable.hasPrefix("?") ? variable : "?\(variable)"
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
    ) async throws -> SPARQLRetainedResult {
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
        return try await _executeRetainedSPARQLString(
            sparqlQuery,
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

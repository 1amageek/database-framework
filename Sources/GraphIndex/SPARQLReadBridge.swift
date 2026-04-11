import Foundation
import Core
import QueryIR
import DatabaseEngine
import DatabaseClientProtocol

public enum SPARQLReadBridge {
    public static func registerReadExecutors() {
        LogicalSourceExecutorRegistry.shared.register(RuntimeSPARQLSourceExecutor())
    }
}

private struct RuntimeSPARQLSourceExecutor: SPARQLSourceExecutor {
    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        guard selectQuery.subqueries == nil || selectQuery.subqueries?.isEmpty == true else {
            throw QueryBridgeError.unsupportedSelectQuery(
                "SPARQL canonical execution does not yet support WITH bindings"
            )
        }
        guard selectQuery.from == nil, selectQuery.fromNamed == nil, selectQuery.reduced == false else {
            throw QueryBridgeError.unsupportedSelectQuery(
                "SPARQL canonical execution does not yet support dataset clauses or REDUCED"
            )
        }

        switch selectQuery.source {
        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        case .graphPattern, .namedGraph:
            break
        default:
            throw CanonicalReadError.unsupportedSource("Expected SPARQL source")
        }

        let resolution = try resolveGraphResolution(for: selectQuery, schema: context.container.schema)
        guard let type = resolution.entity.persistableType else {
            throw ServiceError(
                code: "UNKNOWN_GRAPH",
                message: GraphReadResolver.errorMessage(
                    graphName: declaredGraphName(for: selectQuery.source),
                    schema: context.container.schema
                )
            )
        }

        return try await execute(
            context: context,
            type: type,
            resolution: resolution,
            selectQuery: selectQuery,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func execute(
        context: FDBContext,
        type: any Persistable.Type,
        resolution: GraphReadResolution,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        try await _execute(
            context: context,
            type: type,
            resolution: resolution,
            selectQuery: selectQuery,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func _execute<T: Persistable>(
        context: FDBContext,
        type: T.Type,
        resolution: GraphReadResolution,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: type)
        let typeSubspace = try await queryContext.indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(resolution.indexDescriptor.name)

        var executionPattern = try buildExecutionPattern(from: selectQuery.source)
        if let filter = selectQuery.filter {
            executionPattern = .filter(executionPattern, GraphPatternConverter.convertFilter(filter))
        }
        if let groupBy = selectQuery.groupBy, !groupBy.isEmpty {
            let groupVariables = groupBy.compactMap { expression -> String? in
                guard case .variable(let variable) = expression else {
                    return nil
                }
                return prefixedVariable(variable.name)
            }

            let aggregates = extractAggregateBindings(from: selectQuery)
                .map(GraphPatternConverter.convertAggregate(_:))
            let having = selectQuery.having.map(GraphPatternConverter.convertFilter(_:))
            executionPattern = .groupBy(
                executionPattern,
                groupVariables: groupVariables,
                aggregates: aggregates,
                having: having
            )
        }

        if executionPattern.isEmpty {
            throw SPARQLQueryError.noPatterns
        }

        let executor = SPARQLQueryExecutor(
            database: context.container.engine,
            indexSubspace: indexSubspace,
            strategy: resolution.kind.strategy,
            fromFieldName: resolution.kind.fromFieldName,
            edgeFieldName: resolution.kind.edgeFieldName,
            toFieldName: resolution.kind.toFieldName,
            graphFieldName: resolution.kind.graphFieldName,
            storedFieldNames: resolution.indexDescriptor.storedFieldNames
        )

        let (projectionVariables, distinctFromProjection) = extractProjection(from: selectQuery)
        let orderBy = makeSortKeys(from: selectQuery.orderBy)
        let projectedVariables = projectedVariables(
            for: executionPattern,
            projectionVariables: projectionVariables,
            storedFieldNames: resolution.indexDescriptor.storedFieldNames
        )

        let hasOrderBy = !orderBy.isEmpty
        let distinct = distinctFromProjection || selectQuery.distinct
        var (bindings, _) = try await executor.execute(
            pattern: executionPattern,
            limit: nil,
            offset: 0
        )

        if hasOrderBy {
            bindings = BindingSorter.sort(bindings, by: orderBy)
        }

        let projectionSet = Set(projectedVariables)
        var projected = bindings.map { $0.project(projectionSet) }

        if distinct {
            var seen = Set<VariableBinding>()
            projected = projected.filter { seen.insert($0).inserted }
        }

        let rows = projected.map { binding in
            QueryRow(fields: rowFields(from: binding, projectedVariables: projectedVariables))
        }
        let page = try CanonicalOffsetPagination.window(
            items: rows,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func resolveGraphResolution(
        for selectQuery: SelectQuery,
        schema: Schema
    ) throws -> GraphReadResolution {
        let graphName = declaredGraphName(for: selectQuery.source)
        guard let resolution = GraphReadResolver.resolve(graphName: graphName, schema: schema) else {
            throw ServiceError(
                code: "UNKNOWN_GRAPH",
                message: GraphReadResolver.errorMessage(graphName: graphName, schema: schema)
            )
        }
        return resolution
    }

    private func declaredGraphName(for source: DataSource) -> String? {
        switch source {
        case .namedGraph(let name, _):
            return name
        default:
            return nil
        }
    }

    private func buildExecutionPattern(from source: DataSource) throws -> ExecutionPattern {
        switch source {
        case .graphPattern(let pattern):
            return GraphPatternConverter.convert(pattern)
        case .namedGraph(let name, let pattern):
            return GraphPatternConverter.convert(
                .graph(name: .iri(name), pattern: pattern)
            )
        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        default:
            throw CanonicalReadError.unsupportedSource("Expected SPARQL source")
        }
    }

    private func extractProjection(
        from selectQuery: SelectQuery
    ) -> (variables: [String]?, isDistinct: Bool) {
        switch selectQuery.projection {
        case .all, .allFrom:
            return (nil, false)
        case .items(let items):
            let variables = items.compactMap(extractProjectionVariable(from:))
            return (variables.isEmpty ? nil : variables, false)
        case .distinctItems(let items):
            let variables = items.compactMap(extractProjectionVariable(from:))
            return (variables.isEmpty ? nil : variables, true)
        }
    }

    private func extractProjectionVariable(from item: ProjectionItem) -> String? {
        if let alias = item.alias {
            return prefixedVariable(alias)
        }

        switch item.expression {
        case .variable(let variable):
            return prefixedVariable(variable.name)
        case .column(let column):
            return prefixedVariable(column.column)
        default:
            return nil
        }
    }

    private func extractAggregateBindings(from selectQuery: SelectQuery) -> [AggregateBinding] {
        let items: [ProjectionItem]
        switch selectQuery.projection {
        case .items(let projectionItems), .distinctItems(let projectionItems):
            items = projectionItems
        default:
            return []
        }

        return items.compactMap { item in
            guard case .aggregate(let aggregate) = item.expression,
                  let alias = item.alias else {
                return nil
            }
            return AggregateBinding(variable: prefixedVariable(alias), aggregate: aggregate)
        }
    }

    private func makeSortKeys(from sortKeys: [SortKey]?) -> [BindingSortKey] {
        (sortKeys ?? []).map { sortKey in
            BindingSortKey(
                ascending: sortKey.direction == .ascending,
                nullsLast: sortKey.nulls == .last
            ) { binding in
                ExpressionEvaluator.evaluate(sortKey.expression, binding: binding)
            }
        }
    }

    private func projectedVariables(
        for pattern: ExecutionPattern,
        projectionVariables: [String]?,
        storedFieldNames: [String]
    ) -> [String] {
        if let projectionVariables {
            return projectionVariables
        }

        var allVariables = pattern.variables
        for fieldName in storedFieldNames {
            allVariables.insert(prefixedVariable(fieldName))
        }
        return Array(allVariables).sorted()
    }

    private func rowFields(
        from binding: VariableBinding,
        projectedVariables: [String]
    ) -> [String: FieldValue] {
        var fields: [String: FieldValue] = [:]
        for variable in projectedVariables {
            guard let value = binding[variable] else {
                continue
            }
            fields[unprefixedVariable(variable)] = value
        }
        return fields
    }

    private func prefixedVariable(_ name: String) -> String {
        if name.hasPrefix("?") || name.hasPrefix("$") {
            return "?\(name.dropFirst())"
        }
        return "?\(name)"
    }

    private func unprefixedVariable(_ name: String) -> String {
        if name.hasPrefix("?") || name.hasPrefix("$") {
            return String(name.dropFirst())
        }
        return name
    }
}

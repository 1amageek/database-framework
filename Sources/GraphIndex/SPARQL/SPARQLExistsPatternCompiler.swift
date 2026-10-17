import DatabaseKit

/// Compiles EXISTS graph algebra once as part of its owning expression plan.
enum SPARQLExistsPatternCompiler {
    static func compile(
        _ query: SelectQuery,
        limits: SPARQLExpressionCompilationLimits
    ) throws -> ExecutionPattern {
        guard query.projection == [.wildcard()],
              query.accessPath == nil,
              query.groupBy == nil,
              query.having == nil,
              query.windows.isEmpty,
              query.orderBy == nil,
              query.pagination.limit == nil,
              query.pagination.offset == nil,
              query.quantifier == .all,
              !query.withRecursive,
              query.subqueries == nil || query.subqueries?.isEmpty == true,
              query.reduced == false,
              query.dataset == .implicit else {
            throw SPARQLExpressionCompilationError.invalidExistsSource
        }
        let sourcePattern: GraphPattern
        switch query.source {
        case .graphPattern(let pattern):
            sourcePattern = pattern
        case .namedGraph(let name, let pattern):
            sourcePattern = .graph(name: .iri(name), pattern: pattern)
        default:
            throw SPARQLExpressionCompilationError.invalidExistsSource
        }

        var context = SPARQLAlgebraCompilationContext(
            expressionLimits: limits
        )
        var pattern = try GraphPatternConverter.convert(
            sourcePattern,
            prefixes: [:],
            context: &context,
            subqueryInputPolicy: .isolated,
            inputVariables: []
        )
        if let filter = query.filter {
            pattern = .filter(
                pattern,
                try GraphPatternConverter.convertFilter(
                    filter,
                    limits: limits
                )
            )
        }
        return pattern
    }
}

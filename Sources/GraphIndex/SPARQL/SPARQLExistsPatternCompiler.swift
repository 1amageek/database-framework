import DatabaseKit

/// Compiles EXISTS graph algebra once as part of its owning expression plan.
enum SPARQLExistsPatternCompiler {
    static func compile(_ query: SelectQuery) throws -> ExecutionPattern {
        let sourcePattern: GraphPattern
        switch query.source {
        case .graphPattern(let pattern):
            sourcePattern = pattern
        case .namedGraph(let name, let pattern):
            sourcePattern = .graph(name: .iri(name), pattern: pattern)
        default:
            throw SPARQLExpressionCompilationError.invalidExistsSource
        }

        do {
            var pattern = try GraphPatternConverter.convert(sourcePattern)
            if let filter = query.filter {
                pattern = .filter(
                    pattern,
                    try GraphPatternConverter.convertFilter(filter)
                )
            }
            return pattern
        } catch {
            throw SPARQLExpressionCompilationError.unsupportedExpression(
                "EXISTS graph pattern conversion failed"
            )
        }
    }
}

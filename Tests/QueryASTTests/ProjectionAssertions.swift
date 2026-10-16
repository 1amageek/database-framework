import DatabaseKit
import Testing

func projectionExpression(_ item: ProjectionItem) throws -> Expression {
    guard case .expression(let expression, _) = item else {
        Issue.record("Expected an expression projection, received a wildcard")
        throw ProjectionAssertionError.expectedExpression
    }
    return expression
}

func projectionAlias(_ item: ProjectionItem) throws -> String? {
    guard case .expression(_, let alias) = item else {
        Issue.record("Expected an aliased expression projection, received a wildcard")
        throw ProjectionAssertionError.expectedExpression
    }
    return alias
}

func projectionExpressions(_ items: [ProjectionItem]) throws -> [Expression] {
    try items.map { try projectionExpression($0) }
}

private enum ProjectionAssertionError: Error {
    case expectedExpression
}

import DatabaseKit

/// Associates each compiled GROUP BY expression with its internal variable.
struct SPARQLGroupedExpressionBindings {
    private var storage: [Expression: String] = [:]

    subscript(expression: Expression) -> String? {
        storage[expression]
    }

    mutating func insert(
        variable: String,
        for expression: Expression
    ) {
        storage[expression] = variable
    }
}

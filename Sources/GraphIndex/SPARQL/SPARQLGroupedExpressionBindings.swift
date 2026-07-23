import QueryIR

/// Associates each compiled GROUP BY expression with its internal variable.
struct SPARQLGroupedExpressionBindings {
    private var storage: [QueryIR.Expression: String] = [:]

    subscript(expression: QueryIR.Expression) -> String? {
        storage[expression]
    }

    mutating func insert(
        variable: String,
        for expression: QueryIR.Expression
    ) {
        storage[expression] = variable
    }
}

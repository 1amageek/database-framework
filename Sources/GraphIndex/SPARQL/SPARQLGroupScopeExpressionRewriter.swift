import DatabaseKit

/// Applies the aggregate-scope translation defined by SPARQL 1.1 section
/// 18.2.4.1 without entering nested query scopes.
struct SPARQLGroupScopeExpressionRewriter {
    let groupedExpressions: SPARQLGroupedExpressionBindings
    let directlyGroupedVariables: Set<String>
    let eligibleVariables: Set<String>
    let protectedVariables: Set<String>

    private var sampledVariables: [String: String] = [:]

    mutating func rewrite(
        _ expression: Expression,
        aggregateRewriter: inout SPARQLAggregateRewriter
    ) throws -> Expression {
        if let variable = groupedExpressions[expression] {
            return .variable(Variable(variable))
        }

        switch expression {
        case .literal, .parameter:
            return expression
        case .column(let column):
            return sampleIfRequired(
                variable: column.column,
                original: expression,
                aggregateRewriter: &aggregateRewriter
            )
        case .variable(let variable):
            return sampleIfRequired(
                variable: variable.name,
                original: expression,
                aggregateRewriter: &aggregateRewriter
            )
        case .bound(let variable):
            let normalized = Self.prefixed(variable.name)
            guard shouldSample(normalized) else { return expression }
            return .bound(
                Variable(
                    sampledAlias(
                        for: normalized,
                        aggregateRewriter: &aggregateRewriter
                    )
                )
            )

        case .add(let lhs, let rhs):
            return .add(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .subtract(let lhs, let rhs):
            return .subtract(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .multiply(let lhs, let rhs):
            return .multiply(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .divide(let lhs, let rhs):
            return .divide(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .modulo(let lhs, let rhs):
            return .modulo(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .equal(let lhs, let rhs):
            return .equal(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .notEqual(let lhs, let rhs):
            return .notEqual(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .lessThan(let lhs, let rhs):
            return .lessThan(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .lessThanOrEqual(let lhs, let rhs):
            return .lessThanOrEqual(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .greaterThan(let lhs, let rhs):
            return .greaterThan(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .greaterThanOrEqual(let lhs, let rhs):
            return .greaterThanOrEqual(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .and(let lhs, let rhs):
            return .and(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .or(let lhs, let rhs):
            return .or(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )
        case .nullIf(let lhs, let rhs):
            return .nullIf(
                try rewrite(lhs, aggregateRewriter: &aggregateRewriter),
                try rewrite(rhs, aggregateRewriter: &aggregateRewriter)
            )

        case .negate(let value):
            return .negate(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .not(let value):
            return .not(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .isNull(let value):
            return .isNull(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .isNotNull(let value):
            return .isNotNull(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .isTriple(let value):
            return .isTriple(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .subject(let value):
            return .subject(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .predicate(let value):
            return .predicate(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .object(let value):
            return .object(
                try rewrite(value, aggregateRewriter: &aggregateRewriter)
            )
        case .cast(let value, let targetType):
            return .cast(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                targetType: targetType
            )

        case .like(let value, let pattern):
            return .like(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                pattern: pattern
            )
        case .regex(let value, let pattern, let flags):
            return .regex(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                pattern: pattern,
                flags: flags
            )
        case .between(let value, let low, let high):
            return .between(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                low: try rewrite(low, aggregateRewriter: &aggregateRewriter),
                high: try rewrite(high, aggregateRewriter: &aggregateRewriter)
            )
        case .inList(let value, let values):
            return .inList(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                values: try values.map {
                    try rewrite($0, aggregateRewriter: &aggregateRewriter)
                }
            )
        case .notInList(let value, let values):
            return .notInList(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                values: try values.map {
                    try rewrite($0, aggregateRewriter: &aggregateRewriter)
                }
            )
        case .inSubquery(let value, let subquery):
            return .inSubquery(
                try rewrite(value, aggregateRewriter: &aggregateRewriter),
                subquery: subquery
            )

        case .aggregate:
            return try aggregateRewriter.rewrite(expression)
        case .function(let call):
            return .function(
                FunctionCall(
                    name: call.name,
                    arguments: try call.arguments.map {
                        try rewrite($0, aggregateRewriter: &aggregateRewriter)
                    },
                    distinct: call.distinct
                )
            )
        case .caseWhen(let pairs, let elseResult):
            return .caseWhen(
                cases: try pairs.map {
                    CaseWhenPair(
                        condition: try rewrite(
                            $0.condition,
                            aggregateRewriter: &aggregateRewriter
                        ),
                        result: try rewrite(
                            $0.result,
                            aggregateRewriter: &aggregateRewriter
                        )
                    )
                },
                elseResult: try elseResult.map {
                    try rewrite($0, aggregateRewriter: &aggregateRewriter)
                }
            )
        case .coalesce(let values):
            return .coalesce(
                try values.map {
                    try rewrite($0, aggregateRewriter: &aggregateRewriter)
                }
            )
        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try rewrite(
                    subject,
                    aggregateRewriter: &aggregateRewriter
                ),
                predicate: try rewrite(
                    predicate,
                    aggregateRewriter: &aggregateRewriter
                ),
                object: try rewrite(
                    object,
                    aggregateRewriter: &aggregateRewriter
                )
            )

        case .subquery, .exists:
            return expression
        }
    }

    private mutating func sampleIfRequired(
        variable: String,
        original: Expression,
        aggregateRewriter: inout SPARQLAggregateRewriter
    ) -> Expression {
        let normalized = Self.prefixed(variable)
        guard shouldSample(normalized) else { return original }
        return .variable(
            Variable(
                sampledAlias(
                    for: normalized,
                    aggregateRewriter: &aggregateRewriter
                )
            )
        )
    }

    private func shouldSample(_ variable: String) -> Bool {
        eligibleVariables.contains(variable)
            && !directlyGroupedVariables.contains(variable)
            && !protectedVariables.contains(variable)
    }

    private mutating func sampledAlias(
        for variable: String,
        aggregateRewriter: inout SPARQLAggregateRewriter
    ) -> String {
        if let alias = sampledVariables[variable] {
            return alias
        }
        let alias = aggregateRewriter.registerImplicitSample(of: variable)
        sampledVariables[variable] = alias
        return alias
    }

    private static func prefixed(_ variable: String) -> String {
        if variable.hasPrefix("?") || variable.hasPrefix("$") {
            return "?" + variable.dropFirst()
        }
        return "?" + variable
    }
}

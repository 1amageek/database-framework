import DatabaseKit

struct SPARQLAggregateRewriter {
    private(set) var aggregateBindings: [AggregateBinding] = []
    private(set) var aggregateOccurrenceCount: UInt64 = 0

    mutating func rewrite(
        _ expression: Expression
    ) throws -> Expression {
        switch expression {
        case .literal, .column, .variable, .parameter, .bound:
            return expression

        case .add(let lhs, let rhs):
            return .add(try rewrite(lhs), try rewrite(rhs))
        case .subtract(let lhs, let rhs):
            return .subtract(try rewrite(lhs), try rewrite(rhs))
        case .multiply(let lhs, let rhs):
            return .multiply(try rewrite(lhs), try rewrite(rhs))
        case .divide(let lhs, let rhs):
            return .divide(try rewrite(lhs), try rewrite(rhs))
        case .modulo(let lhs, let rhs):
            return .modulo(try rewrite(lhs), try rewrite(rhs))
        case .equal(let lhs, let rhs):
            return .equal(try rewrite(lhs), try rewrite(rhs))
        case .notEqual(let lhs, let rhs):
            return .notEqual(try rewrite(lhs), try rewrite(rhs))
        case .lessThan(let lhs, let rhs):
            return .lessThan(try rewrite(lhs), try rewrite(rhs))
        case .lessThanOrEqual(let lhs, let rhs):
            return .lessThanOrEqual(try rewrite(lhs), try rewrite(rhs))
        case .greaterThan(let lhs, let rhs):
            return .greaterThan(try rewrite(lhs), try rewrite(rhs))
        case .greaterThanOrEqual(let lhs, let rhs):
            return .greaterThanOrEqual(try rewrite(lhs), try rewrite(rhs))
        case .and(let lhs, let rhs):
            return .and(try rewrite(lhs), try rewrite(rhs))
        case .or(let lhs, let rhs):
            return .or(try rewrite(lhs), try rewrite(rhs))
        case .nullIf(let lhs, let rhs):
            return .nullIf(try rewrite(lhs), try rewrite(rhs))

        case .negate(let value):
            return .negate(try rewrite(value))
        case .not(let value):
            return .not(try rewrite(value))
        case .isNull(let value):
            return .isNull(try rewrite(value))
        case .isNotNull(let value):
            return .isNotNull(try rewrite(value))
        case .isTriple(let value):
            return .isTriple(try rewrite(value))
        case .subject(let value):
            return .subject(try rewrite(value))
        case .predicate(let value):
            return .predicate(try rewrite(value))
        case .object(let value):
            return .object(try rewrite(value))
        case .cast(let value, let targetType):
            return .cast(try rewrite(value), targetType: targetType)

        case .like(let value, let pattern):
            return .like(try rewrite(value), pattern: pattern)
        case .regex(let value, let pattern, let flags):
            return .regex(
                try rewrite(value),
                pattern: pattern,
                flags: flags
            )
        case .between(let value, let low, let high):
            return .between(
                try rewrite(value),
                low: try rewrite(low),
                high: try rewrite(high)
            )
        case .inList(let value, let values):
            return .inList(
                try rewrite(value),
                values: try values.map { try rewrite($0) }
            )
        case .notInList(let value, let values):
            return .notInList(
                try rewrite(value),
                values: try values.map { try rewrite($0) }
            )
        case .inSubquery(let value, let subquery):
            return .inSubquery(
                try rewrite(value),
                subquery: subquery
            )

        case .aggregate(let aggregate):
            aggregateOccurrenceCount += 1
            let variable = register(aggregate)
            return .variable(Variable(variable))

        case .function(let call):
            return .function(
                FunctionCall(
                    name: call.name,
                    arguments: try call.arguments.map { try rewrite($0) },
                    distinct: call.distinct
                )
            )
        case .caseWhen(let pairs, let elseResult):
            return .caseWhen(
                cases: try pairs.map {
                    CaseWhenPair(
                        condition: try rewrite($0.condition),
                        result: try rewrite($0.result)
                    )
                },
                elseResult: try elseResult.map { try rewrite($0) }
            )
        case .coalesce(let values):
            return .coalesce(try values.map { try rewrite($0) })
        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try rewrite(subject),
                predicate: try rewrite(predicate),
                object: try rewrite(object)
            )

        case .subquery, .exists:
            // Each nested Select is an independent aggregate scope and is
            // compiled at its own algebra boundary.
            return expression
        }
    }

    private mutating func register(
        _ aggregate: AggregateFunction
    ) -> String {
        let identifier = UInt64(aggregateBindings.count)
        let variable = SPARQLInternalVariable.aggregateRaw(identifier)
        aggregateBindings.append(
            AggregateBinding(variable: variable, aggregate: aggregate)
        )
        return variable
    }

    mutating func registerImplicitSample(
        of variable: String
    ) -> String {
        register(
            .sample(
                .variable(Variable(Self.unprefixed(variable)))
            )
        )
    }

    private static func unprefixed(_ variable: String) -> String {
        if variable.hasPrefix("?") || variable.hasPrefix("$") {
            return String(variable.dropFirst())
        }
        return variable
    }
}

import DatabaseTypes
import DatabaseKit

/// Fail-closed validator for the SPARQL subset of the shared QueryIR model.
/// SQL-only nodes and resource-exhausting in-memory expressions are rejected
/// before physical execution begins.
public enum SPARQLExpressionValidator {
    public static func validate(
        _ expression: Expression,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws {
        var state = ValidationState(limits: limits)
        try validate(expression, state: &state, depth: 1)
    }

    public static func validateAggregate(
        _ aggregate: AggregateFunction,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws {
        var state = ValidationState(limits: limits)
        try state.enter(depth: 1)
        switch aggregate {
        case .count(let expression, _):
            if let expression {
                try validate(expression, state: &state, depth: 2)
            }
        case .sum(let expression, _), .avg(let expression, _),
             .min(let expression), .max(let expression),
             .sample(let expression):
            try validate(expression, state: &state, depth: 2)
        case .groupConcat(let expression, let separator, _):
            if let separator {
                try state.checkString(separator)
            }
            try validate(expression, state: &state, depth: 2)
        case .arrayAgg:
            throw SPARQLExpressionCompilationError.unsupportedExpression(
                "ARRAY_AGG"
            )
        }
    }

    private static func validate(
        _ expression: Expression,
        state: inout ValidationState,
        depth: Int
    ) throws {
        try state.enter(depth: depth)
        let childDepth = depth + 1

        switch expression {
        case .literal(let literal):
            switch literal {
            case .null, .array, .binary:
                throw unsupported(expression)
            case .string(let value), .iri(let value), .blankNode(let value):
                try state.checkString(value)
            case .typedLiteral(let value, let datatype):
                try state.checkString(value)
                try state.checkString(datatype)
            case .langLiteral(let value, let language):
                try state.checkString(value)
                try state.checkString(language)
            case .dirLangLiteral(let value, let language, let direction):
                try state.checkString(value)
                try state.checkString(language)
                try state.checkString(direction)
            case .rdfTerm(let term):
                try state.checkRDFTerm(term)
            default:
                return
            }

        case .variable(let variable), .bound(let variable):
            try state.checkString(variable.name)
            return

        case .column:
            throw unsupported(expression)
        case .parameter:
            throw SPARQLExpressionCompilationError.unboundParameter

        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .equal(let lhs, let rhs), .notEqual(let lhs, let rhs),
             .lessThan(let lhs, let rhs), .lessThanOrEqual(let lhs, let rhs),
             .greaterThan(let lhs, let rhs), .greaterThanOrEqual(let lhs, let rhs),
             .and(let lhs, let rhs), .or(let lhs, let rhs):
            try validate(lhs, state: &state, depth: childDepth)
            try validate(rhs, state: &state, depth: childDepth)

        case .negate(let value), .not(let value), .isTriple(let value),
             .subject(let value), .predicate(let value), .object(let value):
            try validate(value, state: &state, depth: childDepth)

        case .regex(let value, let pattern, let flags):
            try state.checkString(pattern)
            if let flags { try state.checkString(flags) }
            try validate(value, state: &state, depth: childDepth)

        case .inList(let value, let candidates),
             .notInList(let value, let candidates):
            try state.checkCollection(candidates.count)
            try validate(value, state: &state, depth: childDepth)
            for candidate in candidates {
                try validate(candidate, state: &state, depth: childDepth)
            }

        case .function(let function):
            try validate(
                function,
                state: &state,
                childDepth: childDepth
            )

        case .coalesce(let values):
            guard !values.isEmpty else {
                throw SPARQLExpressionCompilationError.invalidFunctionArity(
                    function: "COALESCE",
                    actual: 0,
                    expected: "at least one"
                )
            }
            try state.checkCollection(values.count)
            for value in values {
                try validate(value, state: &state, depth: childDepth)
            }

        case .triple(let subject, let predicate, let object):
            try validate(subject, state: &state, depth: childDepth)
            try validate(predicate, state: &state, depth: childDepth)
            try validate(object, state: &state, depth: childDepth)

        case .exists(let query):
            try validateExists(query, state: &state, depth: childDepth)

        case .aggregate:
            throw SPARQLExpressionCompilationError.aggregateNotAllowed

        case .modulo, .isNull, .isNotNull, .like, .between,
             .inSubquery, .caseWhen, .nullIf, .cast, .subquery:
            throw unsupported(expression)
        }
    }

    private static func validate(
        _ function: FunctionCall,
        state: inout ValidationState,
        childDepth: Int
    ) throws {
        guard !function.distinct else {
            throw SPARQLExpressionCompilationError.distinctScalarFunction(
                function.name
            )
        }
        try state.checkString(function.name)
        try state.checkFunctionArguments(function.arguments.count)

        let identifier = try SPARQLFunctionIdentifier.resolve(function.name)
        let arity: ClosedRange<Int>?
        switch identifier {
        case .builtIn(let builtIn):
            arity = builtIn.arity
            if builtIn == .bound,
               function.arguments.count == 1,
               case .variable = function.arguments[0] {
                break
            } else if builtIn == .bound, function.arguments.count == 1 {
                throw SPARQLExpressionCompilationError.unsupportedExpression(
                    "BOUND requires a variable argument"
                )
            }
        case .datatypeConstructor:
            arity = 1...1
        case .extensionFunction:
            arity = nil
        }

        if let arity, !arity.contains(function.arguments.count) {
            let expected = arity.lowerBound == arity.upperBound
                ? String(arity.lowerBound)
                : "\(arity.lowerBound)...\(arity.upperBound)"
            throw SPARQLExpressionCompilationError.invalidFunctionArity(
                function: function.name,
                actual: function.arguments.count,
                expected: expected
            )
        }
        for argument in function.arguments {
            try validate(argument, state: &state, depth: childDepth)
        }
    }

    private static func validateExists(
        _ query: SelectQuery,
        state: inout ValidationState,
        depth: Int
    ) throws {
        switch query.source {
        case .graphPattern(let pattern):
            try validate(pattern, state: &state, depth: depth)
        case .namedGraph(let name, let pattern):
            try state.checkString(name)
            try validate(pattern, state: &state, depth: depth)
        default:
            throw SPARQLExpressionCompilationError.invalidExistsSource
        }
        guard query.subqueries == nil || query.subqueries?.isEmpty == true,
              query.accessPath == nil,
              query.groupBy == nil,
              query.having == nil,
              query.orderBy == nil,
              query.limit == nil,
              query.offset == nil,
              query.reduced == false else {
            throw SPARQLExpressionCompilationError.invalidExistsSource
        }
        if let filter = query.filter {
            try validate(filter, state: &state, depth: depth)
        }
    }

    private static func validate(
        _ pattern: GraphPattern,
        state: inout ValidationState,
        depth: Int
    ) throws {
        try state.enter(depth: depth)
        let childDepth = depth + 1
        switch pattern {
        case .basic(let basicGraphPattern):
            try state.checkCollection(basicGraphPattern.count)
            for element in basicGraphPattern.elements {
                switch element {
                case .triple(let triple):
                    try validate(
                        triple.subject,
                        state: &state,
                        depth: childDepth
                    )
                    try validate(
                        triple.predicate,
                        state: &state,
                        depth: childDepth
                    )
                    try validate(
                        triple.object,
                        state: &state,
                        depth: childDepth
                    )
                case .propertyPath(let pathPattern):
                    try validate(
                        pathPattern.subject,
                        state: &state,
                        depth: childDepth
                    )
                    try validate(
                        pathPattern.path,
                        state: &state,
                        depth: childDepth
                    )
                    try validate(
                        pathPattern.object,
                        state: &state,
                        depth: childDepth
                    )
                }
            }

        case .join(let lhs, let rhs), .optional(let lhs, let rhs),
             .union(let lhs, let rhs), .minus(let lhs, let rhs),
             .lateral(let lhs, let rhs):
            try validate(lhs, state: &state, depth: childDepth)
            try validate(rhs, state: &state, depth: childDepth)

        case .filter(let inner, let expression):
            try validate(inner, state: &state, depth: childDepth)
            try validate(expression, state: &state, depth: childDepth)

        case .graph(let name, let inner):
            try validate(name, state: &state, depth: childDepth)
            try validate(inner, state: &state, depth: childDepth)

        case .bind(let inner, let variable, let expression):
            try state.checkString(variable)
            try validate(inner, state: &state, depth: childDepth)
            try validate(expression, state: &state, depth: childDepth)

        case .groupBy(let inner, let expressions, let aggregates):
            try state.checkCollection(expressions.count)
            try state.checkCollection(aggregates.count)
            try validate(inner, state: &state, depth: childDepth)
            for expression in expressions {
                try validate(expression, state: &state, depth: childDepth)
            }
            for binding in aggregates {
                try state.checkString(binding.variable)
                try validate(
                    binding.aggregate,
                    state: &state,
                    depth: childDepth
                )
            }

        case .service:
            throw SPARQLExpressionCompilationError.unsupportedExpression(
                "SERVICE inside EXISTS"
            )
        case .values(let variables, let bindings):
            try state.checkCollection(variables.count)
            try state.checkCollection(bindings.count)
            for variable in variables {
                try state.checkString(variable)
            }
            for row in bindings {
                try state.checkCollection(row.count)
                for literal in row {
                    guard let literal else { continue }
                    try validate(
                        Expression.literal(literal),
                        state: &state,
                        depth: childDepth
                    )
                }
            }
        case .subquery:
            throw SPARQLExpressionCompilationError.unsupportedExpression(
                "subquery inside EXISTS"
            )
        }
    }

    private static func validate(
        _ aggregate: AggregateFunction,
        state: inout ValidationState,
        depth: Int
    ) throws {
        try state.enter(depth: depth)
        let childDepth = depth + 1
        switch aggregate {
        case .count(let expression, _):
            if let expression {
                try validate(expression, state: &state, depth: childDepth)
            }
        case .sum(let expression, _), .avg(let expression, _),
             .min(let expression), .max(let expression),
             .sample(let expression):
            try validate(expression, state: &state, depth: childDepth)
        case .groupConcat(let expression, let separator, _):
            if let separator {
                try state.checkString(separator)
            }
            try validate(expression, state: &state, depth: childDepth)
        case .arrayAgg:
            throw SPARQLExpressionCompilationError.unsupportedExpression(
                "ARRAY_AGG"
            )
        }
    }

    private static func validate(
        _ term: SPARQLTerm,
        state: inout ValidationState,
        depth: Int
    ) throws {
        try state.enter(depth: depth)
        let childDepth = depth + 1
        switch term {
        case .variable(let value), .iri(let value), .blankNode(let value):
            try state.checkString(value)
        case .literal(let literal):
            try validate(
                Expression.literal(literal),
                state: &state,
                depth: childDepth
            )
        case .tripleTerm(let subject, let predicate, let object):
            try validate(subject, state: &state, depth: childDepth)
            try validate(predicate, state: &state, depth: childDepth)
            try validate(object, state: &state, depth: childDepth)
        case .reifiedTriple(let subject, let predicate, let object, let reifier):
            try validate(subject, state: &state, depth: childDepth)
            try validate(predicate, state: &state, depth: childDepth)
            try validate(object, state: &state, depth: childDepth)
            try validate(reifier, state: &state, depth: childDepth)
        }
    }

    private static func validate(
        _ path: PropertyPath,
        state: inout ValidationState,
        depth: Int
    ) throws {
        try state.enter(depth: depth)
        let childDepth = depth + 1
        switch path {
        case .iri(let iri):
            try state.checkString(iri.rawValue)
        case .inverse(let inner), .zeroOrMore(let inner),
             .oneOrMore(let inner), .zeroOrOne(let inner),
             .range(let inner, _):
            try validate(inner, state: &state, depth: childDepth)
        case .sequence(let lhs, let rhs), .alternative(let lhs, let rhs):
            try validate(lhs, state: &state, depth: childDepth)
            try validate(rhs, state: &state, depth: childDepth)
        case .negatedPropertySet(let values):
            let forward = values.forward ?? []
            let inverse = values.inverse ?? []
            try state.checkCollection(forward.count)
            try state.checkCollection(inverse.count)
            for iri in forward {
                try state.checkString(iri.rawValue)
            }
            for iri in inverse {
                try state.checkString(iri.rawValue)
            }
        }
    }

    private static func unsupported(
        _ expression: Expression
    ) -> SPARQLExpressionCompilationError {
        .unsupportedExpression(
            SPARQLExpressionSemanticName.describe(expression)
        )
    }

    private struct ValidationState {
        let limits: SPARQLExpressionCompilationLimits
        var nodes = 0

        mutating func enter(depth: Int) throws {
            guard limits.maximumDepth >= 0, depth <= limits.maximumDepth else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "expressionDepth",
                    actual: depth,
                    maximum: limits.maximumDepth
                )
            }
            let (next, overflow) = nodes.addingReportingOverflow(1)
            guard !overflow, limits.maximumNodes >= 0,
                  next <= limits.maximumNodes else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "expressionNodes",
                    actual: overflow ? Int.max : next,
                    maximum: limits.maximumNodes
                )
            }
            nodes = next
        }

        func checkFunctionArguments(_ count: Int) throws {
            guard limits.maximumFunctionArguments >= 0,
                  count <= limits.maximumFunctionArguments else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "functionArguments",
                    actual: count,
                    maximum: limits.maximumFunctionArguments
                )
            }
        }

        func checkCollection(_ count: Int) throws {
            guard limits.maximumCollectionElements >= 0,
                  count <= limits.maximumCollectionElements else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "expressionCollectionElements",
                    actual: count,
                    maximum: limits.maximumCollectionElements
                )
            }
        }

        func checkString(_ value: String) throws {
            let count = value.utf8.count
            guard limits.maximumStringUTF8Count >= 0,
                  count <= limits.maximumStringUTF8Count else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "expressionStringUTF8",
                    actual: count,
                    maximum: limits.maximumStringUTF8Count
                )
            }
        }

        func checkRDFTerm(_ term: RDFTerm) throws {
            do {
                try RDFTermValidation.validate(term)
            } catch {
                throw SPARQLExpressionCompilationError.unsupportedExpression(
                    "invalid canonical RDF term: \(error)"
                )
            }

            var pending = [term]
            var utf8Count = 0
            while let current = pending.popLast() {
                switch current {
                case .iri(let iri):
                    try addRDFString(iri.rawValue, to: &utf8Count)
                case .blankNode(let identifier):
                    try addRDFString(identifier.rawValue, to: &utf8Count)
                case .literal(let literal):
                    try addRDFString(literal.lexicalForm, to: &utf8Count)
                    switch literal.annotation {
                    case .typed(let datatype):
                        try addRDFString(
                            datatype.iri.rawValue,
                            to: &utf8Count
                        )
                    case .languageTagged(let language):
                        try addRDFString(language.rawValue, to: &utf8Count)
                    case .directionalLanguageTagged(
                        let language,
                        let direction
                    ):
                        try addRDFString(language.rawValue, to: &utf8Count)
                        try addRDFString(direction.rawValue, to: &utf8Count)
                    }
                case .tripleTerm(let subject, let predicate, let object):
                    pending.append(object)
                    pending.append(predicate.term)
                    pending.append(subject.term)
                }
            }
        }

        private func addRDFString(
            _ value: String,
            to count: inout Int
        ) throws {
            let (next, overflow) = count.addingReportingOverflow(
                value.utf8.count
            )
            guard limits.maximumStringUTF8Count >= 0,
                  !overflow,
                  next <= limits.maximumStringUTF8Count else {
                throw SPARQLExpressionCompilationError.resourceLimitExceeded(
                    resource: "expressionRDFTermUTF8",
                    actual: overflow ? Int.max : next,
                    maximum: limits.maximumStringUTF8Count
                )
            }
            count = next
        }
    }
}

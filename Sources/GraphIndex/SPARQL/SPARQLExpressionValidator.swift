import DatabaseTypes
import DatabaseKit

/// Fail-closed validator for the SPARQL subset of the shared QueryIR model.
///
/// Validation uses one explicit work stack. Admitted nesting therefore cannot
/// consume the process call stack before a typed resource error is produced.
public enum SPARQLExpressionValidator {
    public static func validate(
        _ expression: Expression,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws(SPARQLExpressionCompilationError) {
        do throws(QueryStructuralValidationError) {
            try QueryStructuralValidator.validate(
                expression,
                limits: limits.structuralLimits
            )
        } catch {
            throw SPARQLExpressionCompilationError.structural(error)
        }
        var traversal = ValidationTraversal(limits: limits)
        try traversal.validate([.expression(expression, depth: 0)])
    }

    public static func validateAggregate(
        _ aggregate: AggregateFunction,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws(SPARQLExpressionCompilationError) {
        do throws(QueryStructuralValidationError) {
            try QueryStructuralValidator.validate(
                aggregate,
                limits: limits.structuralLimits
            )
        } catch {
            throw SPARQLExpressionCompilationError.structural(error)
        }
        var traversal = ValidationTraversal(limits: limits)
        try traversal.validate([.aggregate(aggregate, depth: 0)])
    }
}

private extension SPARQLExpressionValidator {
    enum ValidationTask {
        case expression(Expression, depth: UInt64)
        case aggregate(AggregateFunction, depth: UInt64)
        case graphPattern(GraphPattern, depth: UInt64)
        case term(SPARQLTerm, depth: UInt64)
        case propertyPath(PropertyPath, depth: UInt64)
    }

    struct ValidationTraversal {
        var state: ValidationState

        init(limits: SPARQLExpressionCompilationLimits) {
            self.state = ValidationState(limits: limits)
        }

        mutating func validate(
            _ initialTasks: consuming [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            var tasks = consume initialTasks
            while let task = tasks.popLast() {
                switch consume task {
                case .expression(let expression, let depth):
                    try validateExpression(
                        consume expression,
                        depth: depth,
                        tasks: &tasks
                    )
                case .aggregate(let aggregate, let depth):
                    try validateAggregate(
                        consume aggregate,
                        depth: depth,
                        tasks: &tasks
                    )
                case .graphPattern(let pattern, let depth):
                    try validateGraphPattern(
                        consume pattern,
                        depth: depth,
                        tasks: &tasks
                    )
                case .term(let term, let depth):
                    try validateTerm(
                        consume term,
                        depth: depth,
                        tasks: &tasks
                    )
                case .propertyPath(let path, let depth):
                    try validatePropertyPath(
                        consume path,
                        depth: depth,
                        tasks: &tasks
                    )
                }
            }
        }

        private mutating func validateExpression(
            _ expression: consuming Expression,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            try state.enter(depth: depth)
            let childDepth = try state.childDepth(after: depth)
            let semanticName = SPARQLExpressionSemanticName.describe(
                expression
            )

            switch consume expression {
            case .literal(let literal):
                switch literal {
                case .null, .array, .binary:
                    throw SPARQLExpressionCompilationError
                        .unsupportedExpression(semanticName)
                case .string(let value), .iri(let value),
                     .blankNode(let value):
                    try state.checkString(value)
                case .typedLiteral(let value, let datatype):
                    try state.checkString(value)
                    try state.checkString(datatype)
                case .langLiteral(let value, let language):
                    try state.checkString(value)
                    try state.checkString(language)
                case .dirLangLiteral(
                    let value,
                    let language,
                    let direction
                ):
                    try state.checkString(value)
                    try state.checkString(language)
                    try state.checkString(direction)
                case .rdfTerm(let term):
                    try state.checkRDFTerm(term, depth: childDepth)
                default:
                    break
                }

            case .variable(let variable), .bound(let variable):
                try state.checkString(variable.name)

            case .column(let column):
                _ = column
                throw SPARQLExpressionCompilationError
                    .unsupportedExpression(semanticName)
            case .parameter:
                throw SPARQLExpressionCompilationError.unboundParameter

            case .add(let lhs, let rhs),
                 .subtract(let lhs, let rhs),
                 .multiply(let lhs, let rhs),
                 .divide(let lhs, let rhs),
                 .equal(let lhs, let rhs),
                 .notEqual(let lhs, let rhs),
                 .lessThan(let lhs, let rhs),
                 .lessThanOrEqual(let lhs, let rhs),
                 .greaterThan(let lhs, let rhs),
                 .greaterThanOrEqual(let lhs, let rhs),
                 .and(let lhs, let rhs),
                 .or(let lhs, let rhs):
                tasks.append(.expression(rhs, depth: childDepth))
                tasks.append(.expression(lhs, depth: childDepth))

            case .negate(let value), .not(let value),
                 .isTriple(let value), .subject(let value),
                 .predicate(let value), .object(let value):
                tasks.append(.expression(value, depth: childDepth))

            case .regex(let value, let pattern, let flags):
                try state.checkString(pattern)
                if let flags {
                    try state.checkString(flags)
                }
                tasks.append(.expression(value, depth: childDepth))

            case .like(let value, let pattern):
                try state.checkString(pattern)
                tasks.append(.expression(value, depth: childDepth))

            case .inList(let value, let candidates),
                 .notInList(let value, let candidates):
                try state.checkCollection(candidates.count)
                for candidate in candidates.reversed() {
                    tasks.append(.expression(candidate, depth: childDepth))
                }
                tasks.append(.expression(value, depth: childDepth))

            case .function(let function):
                try validateFunction(
                    function,
                    childDepth: childDepth,
                    tasks: &tasks
                )

            case .coalesce(let values):
                guard !values.isEmpty else {
                    throw SPARQLExpressionCompilationError
                        .invalidFunctionArity(
                            function: "COALESCE",
                            actual: 0,
                            expected: "at least one"
                        )
                }
                try state.checkCollection(values.count)
                for value in values.reversed() {
                    tasks.append(.expression(value, depth: childDepth))
                }

            case .triple(let subject, let predicate, let object):
                tasks.append(.expression(object, depth: childDepth))
                tasks.append(.expression(predicate, depth: childDepth))
                tasks.append(.expression(subject, depth: childDepth))

            case .exists(let query):
                try validateExists(
                    query,
                    depth: childDepth,
                    tasks: &tasks
                )

            case .aggregate:
                throw SPARQLExpressionCompilationError.aggregateNotAllowed

            case .modulo, .isNull, .isNotNull, .between,
                 .inSubquery, .caseWhen, .nullIf, .cast, .subquery:
                throw SPARQLExpressionCompilationError
                    .unsupportedExpression(semanticName)
            }
        }

        private mutating func validateFunction(
            _ function: FunctionCall,
            childDepth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            guard !function.distinct else {
                throw SPARQLExpressionCompilationError
                    .distinctScalarFunction(function.name)
            }
            try state.checkString(function.name)
            try state.checkFunctionArguments(function.arguments.count)

            let identifier = try SPARQLFunctionIdentifier.resolve(
                function.name
            )
            let arity: ClosedRange<Int>?
            switch identifier {
            case .builtIn(let builtIn):
                arity = builtIn.arity
                if builtIn == .bound,
                   function.arguments.count == 1,
                   case .variable = function.arguments[0] {
                    break
                }
                if builtIn == .bound, function.arguments.count == 1 {
                    throw SPARQLExpressionCompilationError
                        .invalidFunctionArity(
                            function: function.name,
                            actual: function.arguments.count,
                            expected: "one variable argument"
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
                throw SPARQLExpressionCompilationError
                    .invalidFunctionArity(
                        function: function.name,
                        actual: function.arguments.count,
                        expected: expected
                    )
            }
            for argument in function.arguments.reversed() {
                tasks.append(.expression(argument, depth: childDepth))
            }
        }

        private mutating func validateExists(
            _ query: SelectQuery,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            guard query.projection == .all,
                  query.subqueries == nil || query.subqueries?.isEmpty == true,
                  query.accessPath == nil,
                  query.groupBy == nil,
                  query.having == nil,
                  query.orderBy == nil,
                  query.limit == nil,
                  query.offset == nil,
                  query.distinct == false,
                  query.reduced == false,
                  query.dataset == .implicit else {
                throw SPARQLExpressionCompilationError.invalidExistsSource
            }

            if let filter = query.filter {
                tasks.append(.expression(filter, depth: depth))
            }
            switch query.source {
            case .graphPattern(let pattern):
                tasks.append(.graphPattern(pattern, depth: depth))
            case .namedGraph(let name, let pattern):
                try state.checkString(name)
                tasks.append(.graphPattern(pattern, depth: depth))
            default:
                throw SPARQLExpressionCompilationError.invalidExistsSource
            }
        }

        private mutating func validateGraphPattern(
            _ pattern: consuming GraphPattern,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            try state.enter(depth: depth)
            let childDepth = try state.childDepth(after: depth)

            switch consume pattern {
            case .basic(let basicGraphPattern):
                try state.checkCollection(basicGraphPattern.count)
                for element in basicGraphPattern.elements.reversed() {
                    switch element {
                    case .triple(let triple):
                        tasks.append(
                            .term(triple.object, depth: childDepth)
                        )
                        tasks.append(
                            .term(triple.predicate, depth: childDepth)
                        )
                        tasks.append(
                            .term(triple.subject, depth: childDepth)
                        )
                    case .propertyPath(let pathPattern):
                        tasks.append(
                            .term(pathPattern.object, depth: childDepth)
                        )
                        tasks.append(
                            .propertyPath(
                                pathPattern.path,
                                depth: childDepth
                            )
                        )
                        tasks.append(
                            .term(pathPattern.subject, depth: childDepth)
                        )
                    }
                }

            case .join(let lhs, let rhs),
                 .optional(let lhs, let rhs),
                 .union(let lhs, let rhs),
                 .minus(let lhs, let rhs),
                 .lateral(let lhs, let rhs):
                tasks.append(.graphPattern(rhs, depth: childDepth))
                tasks.append(.graphPattern(lhs, depth: childDepth))

            case .filter(let inner, let expression):
                tasks.append(.expression(expression, depth: childDepth))
                tasks.append(.graphPattern(inner, depth: childDepth))

            case .graph(let name, let inner):
                tasks.append(.graphPattern(inner, depth: childDepth))
                tasks.append(.term(name, depth: childDepth))

            case .bind(let inner, let variable, let expression):
                try state.checkString(variable)
                tasks.append(.expression(expression, depth: childDepth))
                tasks.append(.graphPattern(inner, depth: childDepth))

            case .groupBy(let inner, let expressions, let aggregates):
                try state.checkCollection(expressions.count)
                try state.checkCollection(aggregates.count)
                for binding in aggregates.reversed() {
                    try state.checkString(binding.variable)
                    tasks.append(
                        .aggregate(binding.aggregate, depth: childDepth)
                    )
                }
                for expression in expressions.reversed() {
                    tasks.append(
                        .expression(expression, depth: childDepth)
                    )
                }
                tasks.append(.graphPattern(inner, depth: childDepth))

            case .service:
                throw SPARQLExpressionCompilationError
                    .unsupportedExpression("SERVICE inside EXISTS")

            case .values(let variables, let bindings):
                try state.checkCollection(variables.count)
                try state.checkCollection(bindings.count)
                for variable in variables {
                    try state.checkString(variable)
                }
                for row in bindings.reversed() {
                    try state.checkCollection(row.count)
                    for literal in row.reversed() {
                        guard let literal else { continue }
                        tasks.append(
                            .expression(
                                .literal(literal),
                                depth: childDepth
                            )
                        )
                    }
                }

            case .subquery:
                throw SPARQLExpressionCompilationError
                    .unsupportedExpression("subquery inside EXISTS")
            }
        }

        private mutating func validateAggregate(
            _ aggregate: consuming AggregateFunction,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            try state.enter(depth: depth)
            let childDepth = try state.childDepth(after: depth)

            switch consume aggregate {
            case .count(let expression, _):
                if let expression {
                    tasks.append(
                        .expression(expression, depth: childDepth)
                    )
                }
            case .sum(let expression, _), .avg(let expression, _),
                 .min(let expression), .max(let expression),
                 .sample(let expression):
                tasks.append(.expression(expression, depth: childDepth))
            case .groupConcat(let expression, let separator, _):
                if let separator {
                    try state.checkString(separator)
                }
                tasks.append(.expression(expression, depth: childDepth))
            case .arrayAgg:
                throw SPARQLExpressionCompilationError
                    .unsupportedExpression("ARRAY_AGG")
            }
        }

        private mutating func validateTerm(
            _ term: consuming SPARQLTerm,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            try state.enter(depth: depth)
            let childDepth = try state.childDepth(after: depth)

            switch consume term {
            case .variable(let value), .iri(let value),
                 .blankNode(let value):
                try state.checkString(value)
            case .literal(let literal):
                tasks.append(
                    .expression(.literal(literal), depth: childDepth)
                )
            case .tripleTerm(let subject, let predicate, let object):
                tasks.append(.term(object, depth: childDepth))
                tasks.append(.term(predicate, depth: childDepth))
                tasks.append(.term(subject, depth: childDepth))
            case .reifiedTriple(
                let subject,
                let predicate,
                let object,
                let reifier
            ):
                tasks.append(.term(reifier, depth: childDepth))
                tasks.append(.term(object, depth: childDepth))
                tasks.append(.term(predicate, depth: childDepth))
                tasks.append(.term(subject, depth: childDepth))
            }
        }

        private mutating func validatePropertyPath(
            _ path: consuming PropertyPath,
            depth: UInt64,
            tasks: inout [ValidationTask]
        ) throws(SPARQLExpressionCompilationError) {
            try state.enter(depth: depth)
            let childDepth = try state.childDepth(after: depth)

            switch consume path {
            case .iri(let iri):
                try state.checkString(iri.rawValue)
            case .inverse(let inner), .zeroOrMore(let inner),
                 .oneOrMore(let inner), .zeroOrOne(let inner),
                 .range(let inner, _):
                tasks.append(.propertyPath(inner, depth: childDepth))
            case .sequence(let lhs, let rhs),
                 .alternative(let lhs, let rhs):
                tasks.append(.propertyPath(rhs, depth: childDepth))
                tasks.append(.propertyPath(lhs, depth: childDepth))
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
    }

    struct ValidationState {
        let limits: SPARQLExpressionCompilationLimits
        var nodes: UInt64 = 0

        mutating func enter(
            depth: UInt64
        ) throws(SPARQLExpressionCompilationError) {
            guard depth <= limits.maximumDepth else {
                throw SPARQLExpressionCompilationError
                    .structural(
                        .resourceLimitExceeded(
                            resource: .nestingDepth,
                            actual: depth,
                            maximum: limits.maximumDepth
                        )
                    )
            }
            let (next, overflow) = nodes.addingReportingOverflow(1)
            guard !overflow,
                  next <= limits.maximumNodes else {
                throw SPARQLExpressionCompilationError
                    .structural(
                        .resourceLimitExceeded(
                            resource: .totalNodes,
                            actual: overflow ? .max : next,
                            maximum: limits.maximumNodes
                        )
                    )
            }
            nodes = next
        }

        func childDepth(
            after depth: UInt64
        ) throws(SPARQLExpressionCompilationError) -> UInt64 {
            let (childDepth, overflow) = depth.addingReportingOverflow(1)
            guard !overflow else {
                throw SPARQLExpressionCompilationError
                    .structural(
                        .resourceLimitExceeded(
                            resource: .nestingDepth,
                            actual: .max,
                            maximum: limits.maximumDepth
                        )
                    )
            }
            return childDepth
        }

        func checkFunctionArguments(
            _ count: Int
        ) throws(SPARQLExpressionCompilationError) {
            let actual = UInt64(count)
            guard actual <= limits.maximumFunctionArguments else {
                throw SPARQLExpressionCompilationError
                    .structural(
                        .resourceLimitExceeded(
                            resource: .collectionElements,
                            actual: actual,
                            maximum: limits.maximumFunctionArguments
                        )
                    )
            }
        }

        func checkCollection(
            _ count: Int
        ) throws(SPARQLExpressionCompilationError) {
            let actual = UInt64(count)
            guard actual <= limits.maximumCollectionElements else {
                throw SPARQLExpressionCompilationError
                    .structural(
                        .resourceLimitExceeded(
                            resource: .collectionElements,
                            actual: actual,
                            maximum: limits.maximumCollectionElements
                        )
                    )
            }
        }

        func checkString(
            _ value: String
        ) throws(SPARQLExpressionCompilationError) {
            let count = UInt64(value.utf8.count)
            guard count <= limits.maximumStringUTF8Count else {
                throw SPARQLExpressionCompilationError
                    .resourceLimitExceeded(
                        resource: .stringUTF8,
                        actual: count,
                        maximum: limits.maximumStringUTF8Count
                    )
            }
        }

        mutating func checkRDFTerm(
            _ term: RDFTerm,
            depth: UInt64
        ) throws(SPARQLExpressionCompilationError) {
            var pending = [(term, depth)]
            var utf8Count: UInt64 = 0
            while let (current, currentDepth) = pending.popLast() {
                try enter(depth: currentDepth)
                switch current {
                case .iri(let iri):
                    try addRDFString(iri.rawValue, to: &utf8Count)
                case .blankNode(let identifier):
                    try addRDFString(identifier.rawValue, to: &utf8Count)
                case .literal(let literal):
                    try addRDFString(
                        literal.lexicalForm,
                        to: &utf8Count
                    )
                    switch literal.annotation {
                    case .typed(let datatype):
                        try addRDFString(
                            datatype.iri.rawValue,
                            to: &utf8Count
                        )
                    case .languageTagged(let language):
                        try addRDFString(
                            language.rawValue,
                            to: &utf8Count
                        )
                    case .directionalLanguageTagged(
                        let language,
                        let direction
                    ):
                        try addRDFString(
                            language.rawValue,
                            to: &utf8Count
                        )
                        try addRDFString(
                            direction.rawValue,
                            to: &utf8Count
                        )
                    }
                case .tripleTerm(
                    let subject,
                    let predicate,
                    let object
                ):
                    let nestedDepth = try childDepth(after: currentDepth)
                    pending.append((object, nestedDepth))
                    pending.append((predicate.term, nestedDepth))
                    pending.append((subject.term, nestedDepth))
                }
            }
        }

        private func addRDFString(
            _ value: String,
            to count: inout UInt64
        ) throws(SPARQLExpressionCompilationError) {
            let (next, overflow) = count.addingReportingOverflow(
                UInt64(value.utf8.count)
            )
            guard !overflow,
                  next <= limits.maximumStringUTF8Count else {
                throw SPARQLExpressionCompilationError
                    .resourceLimitExceeded(
                        resource: .rdfTermUTF8,
                        actual: overflow ? .max : next,
                        maximum: limits.maximumStringUTF8Count
                    )
            }
            count = next
        }
    }
}

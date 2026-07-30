import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Dataset-dependent expression resolution supplied by the query executor.
/// Thrown failures retain their original execution-domain type. SPARQL
/// expression failures travel through the outcome value.
struct SPARQLRuntimeExpressionResolver: Sendable {
    let exists: @Sendable (
        SelectQuery,
        VariableBinding
    ) async throws -> SPARQLExpressionEvaluationOutcome<Bool>
    let function: @Sendable (
        String,
        [FieldValue],
        VariableBinding
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue>
}

struct SPARQLRuntimeExpressionEvaluator: Sendable {
    private enum BooleanOutcome {
        case value(Bool)
        case expressionError(SPARQLExpressionEvaluationError)
    }

    static func evaluateAsBoolean(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> Bool {
        switch try await evaluate(plan, binding: binding, resolver: resolver) {
        case .value(let value):
            switch effectiveBooleanValue(value) {
            case .value(let boolean): return boolean
            case .expressionError(let error):
                if error.isSPARQLEvaluationError { return false }
                throw error
            }
        case .expressionError(let error):
            if error.isSPARQLEvaluationError { return false }
            throw error
        }
    }

    static func evaluate(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        guard plan.requiresDataset
                || plan.usesExtensionFunction
                || plan.volatility != .immutable else {
            return evaluateImmediate(plan.expression, binding: binding)
        }
        return try await evaluateExpression(
            plan.expression,
            binding: binding,
            resolver: resolver
        )
    }

    private static func evaluateExpression(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        switch expression {
        case .exists(let query):
            switch try await resolver.exists(query, binding) {
            case .value(let result):
                return canonicalBoolean(result, binding: binding)
            case .expressionError(let error):
                return .expressionError(error)
            }

        case .and(let lhs, let rhs):
            return try await evaluateLogicalAnd(
                lhs, rhs, binding: binding, resolver: resolver
            )
        case .or(let lhs, let rhs):
            return try await evaluateLogicalOr(
                lhs, rhs, binding: binding, resolver: resolver
            )
        case .not(let operand):
            switch try await booleanOutcome(
                operand, binding: binding, resolver: resolver
            ) {
            case .value(let value):
                return canonicalBoolean(!value, binding: binding)
            case .expressionError(let error):
                return .expressionError(error)
            }

        case .caseWhen(let pairs, let elseResult):
            for pair in pairs {
                switch try await booleanOutcome(
                    pair.condition, binding: binding, resolver: resolver
                ) {
                case .value(true):
                    return try await evaluateExpression(
                        pair.result, binding: binding, resolver: resolver
                    )
                case .value(false):
                    continue
                case .expressionError(let error):
                    return .expressionError(error)
                }
            }
            if let elseResult {
                return try await evaluateExpression(
                    elseResult, binding: binding, resolver: resolver
                )
            }
            return .value(.null)

        case .coalesce(let expressions):
            return try await evaluateCoalesce(
                expressions, binding: binding, resolver: resolver
            )
        case .inList(let operand, let values):
            return try await evaluateInList(
                operand,
                values: values,
                negated: false,
                binding: binding,
                resolver: resolver
            )
        case .notInList(let operand, let values):
            return try await evaluateInList(
                operand,
                values: values,
                negated: true,
                binding: binding,
                resolver: resolver
            )

        case .function(let call)
            where TextSearch.isEqualIgnoringASCIICase(call.name, "IF"):
            guard call.arguments.count == 3 else {
                return .expressionError(.invalidFunctionArguments(call.name))
            }
            let condition = try await booleanOutcome(
                call.arguments[0], binding: binding, resolver: resolver
            )
            switch condition {
            case .value(let value):
                return try await evaluateExpression(
                    value ? call.arguments[1] : call.arguments[2],
                    binding: binding,
                    resolver: resolver
                )
            case .expressionError(let error):
                return .expressionError(error)
            }

        case .function(let call)
            where TextSearch.isEqualIgnoringASCIICase(call.name, "COALESCE"):
            return try await evaluateCoalesce(
                call.arguments, binding: binding, resolver: resolver
            )

        case .function(let call):
            let identifier: SPARQLFunctionIdentifier
            do {
                identifier = try SPARQLFunctionIdentifier.resolve(call.name)
            } catch {
                return .expressionError(
                    .unsupportedExpression("function \(call.name)")
                )
            }
            switch identifier {
            case .extensionFunction,
                 .builtIn(.now), .builtIn(.rand), .builtIn(.uuid),
                 .builtIn(.strUUID), .builtIn(.blankNode):
                var arguments: [FieldValue] = []
                arguments.reserveCapacity(call.arguments.count)
                for argument in call.arguments {
                    switch try await evaluateExpression(
                        argument, binding: binding, resolver: resolver
                    ) {
                    case .value(let value): arguments.append(value)
                    case .expressionError(let error):
                        return .expressionError(error)
                    }
                }
                return try await resolver.function(call.name, arguments, binding)
            case .builtIn, .datatypeConstructor:
                return try await evaluateLowered(
                    expression, binding: binding, resolver: resolver
                )
            }

        case .subquery, .inSubquery:
            return .expressionError(
                .unsupportedExpression(
                    SPARQLExpressionSemanticName.describe(expression)
                )
            )
        case .literal, .variable, .bound:
            return evaluateImmediate(expression, binding: binding)
        default:
            return try await evaluateLowered(
                expression, binding: binding, resolver: resolver
            )
        }
    }

    private static func evaluateLogicalAnd(
        _ lhs: Expression,
        _ rhs: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        let left = try await booleanOutcome(
            lhs, binding: binding, resolver: resolver
        )
        if case .value(false) = left {
            return canonicalBoolean(false, binding: binding)
        }
        let right = try await booleanOutcome(
            rhs, binding: binding, resolver: resolver
        )
        switch (left, right) {
        case (.value(true), .value(let value)):
            return canonicalBoolean(value, binding: binding)
        case (.value(true), .expressionError(let error)):
            return .expressionError(error)
        case (.expressionError, .value(false)):
            return canonicalBoolean(false, binding: binding)
        case (.expressionError(let error), .value(true)),
             (.expressionError(let error), .expressionError):
            return .expressionError(error)
        case (.value(false), _):
            return canonicalBoolean(false, binding: binding)
        }
    }

    private static func evaluateLogicalOr(
        _ lhs: Expression,
        _ rhs: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        let left = try await booleanOutcome(
            lhs, binding: binding, resolver: resolver
        )
        if case .value(true) = left {
            return canonicalBoolean(true, binding: binding)
        }
        let right = try await booleanOutcome(
            rhs, binding: binding, resolver: resolver
        )
        switch (left, right) {
        case (.value(false), .value(let value)):
            return canonicalBoolean(value, binding: binding)
        case (.value(false), .expressionError(let error)):
            return .expressionError(error)
        case (.expressionError, .value(true)):
            return canonicalBoolean(true, binding: binding)
        case (.expressionError(let error), .value(false)),
             (.expressionError(let error), .expressionError):
            return .expressionError(error)
        case (.value(true), _):
            return canonicalBoolean(true, binding: binding)
        }
    }

    private static func booleanOutcome(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> BooleanOutcome {
        switch try await evaluateExpression(
            expression, binding: binding, resolver: resolver
        ) {
        case .value(let value):
            switch effectiveBooleanValue(value) {
            case .value(let boolean): return .value(boolean)
            case .expressionError(let error): return .expressionError(error)
            }
        case .expressionError(let error):
            return .expressionError(error)
        }
    }

    private static func evaluateCoalesce(
        _ expressions: [Expression],
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        for expression in expressions {
            switch try await evaluateExpression(
                expression, binding: binding, resolver: resolver
            ) {
            case .value(let value) where value != .null:
                return .value(value)
            case .value:
                continue
            case .expressionError(let error):
                if error.isSPARQLEvaluationError { continue }
                return .expressionError(error)
            }
        }
        return .expressionError(
            .typeError("COALESCE has no expression without an error")
        )
    }

    private static func evaluateInList(
        _ operand: Expression,
        values: [Expression],
        negated: Bool,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        let left: FieldValue
        switch try await evaluateExpression(
            operand, binding: binding, resolver: resolver
        ) {
        case .value(let value): left = value
        case .expressionError(let error): return .expressionError(error)
        }
        var firstError: SPARQLExpressionEvaluationError?
        for candidate in values {
            let right: FieldValue
            switch try await evaluateExpression(
                candidate, binding: binding, resolver: resolver
            ) {
            case .value(let value): right = value
            case .expressionError(let error):
                if error.isSPARQLEvaluationError {
                    if firstError == nil { firstError = error }
                    continue
                }
                return .expressionError(error)
            }
            switch fieldValuesEqual(left, right) {
            case .value(true):
                return canonicalBoolean(!negated, binding: binding)
            case .value(false):
                continue
            case .expressionError(let error):
                if error.isSPARQLEvaluationError {
                    if firstError == nil { firstError = error }
                } else {
                    return .expressionError(error)
                }
            }
        }
        if let firstError { return .expressionError(firstError) }
        return canonicalBoolean(negated, binding: binding)
    }

    private static func evaluateLowered(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        switch try await lowerImmediateOperands(
            expression, binding: binding, resolver: resolver
        ) {
        case .value(let lowered):
            return evaluateImmediate(lowered, binding: binding)
        case .expressionError(let error):
            return .expressionError(error)
        }
    }

    private static func lowerImmediateOperands(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> SPARQLExpressionEvaluationOutcome<Expression> {
        func lowered(
            _ child: Expression
        ) async throws -> SPARQLExpressionEvaluationOutcome<Expression> {
            switch try await evaluateExpression(
                child, binding: binding, resolver: resolver
            ) {
            case .value(let value): return literalExpression(value)
            case .expressionError(let error): return .expressionError(error)
            }
        }

        func binary(
            _ lhs: Expression,
            _ rhs: Expression,
            _ make: (Expression, Expression) -> Expression
        ) async throws -> SPARQLExpressionEvaluationOutcome<Expression> {
            let left: Expression
            switch try await lowered(lhs) {
            case .value(let value): left = value
            case .expressionError(let error): return .expressionError(error)
            }
            let right: Expression
            switch try await lowered(rhs) {
            case .value(let value): right = value
            case .expressionError(let error): return .expressionError(error)
            }
            return .value(make(left, right))
        }

        func unary(
            _ value: Expression,
            _ make: (Expression) -> Expression
        ) async throws -> SPARQLExpressionEvaluationOutcome<Expression> {
            switch try await lowered(value) {
            case .value(let loweredValue): return .value(make(loweredValue))
            case .expressionError(let error): return .expressionError(error)
            }
        }

        switch expression {
        case .add(let lhs, let rhs): return try await binary(lhs, rhs, Expression.add)
        case .subtract(let lhs, let rhs): return try await binary(lhs, rhs, Expression.subtract)
        case .multiply(let lhs, let rhs): return try await binary(lhs, rhs, Expression.multiply)
        case .divide(let lhs, let rhs): return try await binary(lhs, rhs, Expression.divide)
        case .modulo(let lhs, let rhs): return try await binary(lhs, rhs, Expression.modulo)
        case .equal(let lhs, let rhs): return try await binary(lhs, rhs, Expression.equal)
        case .notEqual(let lhs, let rhs): return try await binary(lhs, rhs, Expression.notEqual)
        case .lessThan(let lhs, let rhs): return try await binary(lhs, rhs, Expression.lessThan)
        case .lessThanOrEqual(let lhs, let rhs): return try await binary(lhs, rhs, Expression.lessThanOrEqual)
        case .greaterThan(let lhs, let rhs): return try await binary(lhs, rhs, Expression.greaterThan)
        case .greaterThanOrEqual(let lhs, let rhs): return try await binary(lhs, rhs, Expression.greaterThanOrEqual)
        case .nullIf(let lhs, let rhs): return try await binary(lhs, rhs, Expression.nullIf)
        case .negate(let value): return try await unary(value, Expression.negate)
        case .isNull(let value): return try await unary(value, Expression.isNull)
        case .isNotNull(let value): return try await unary(value, Expression.isNotNull)
        case .isTriple(let value): return try await unary(value, Expression.isTriple)
        case .subject(let value): return try await unary(value, Expression.subject)
        case .predicate(let value): return try await unary(value, Expression.predicate)
        case .object(let value): return try await unary(value, Expression.object)
        case .like(let value, let pattern):
            return try await unary(value) { .like($0, pattern: pattern) }
        case .regex(let value, let pattern, let flags):
            return try await unary(value) { .regex($0, pattern: pattern, flags: flags) }
        case .cast(let value, let targetType):
            return try await unary(value) { .cast($0, targetType: targetType) }
        case .between(let value, let low, let high):
            let loweredValue: Expression
            switch try await lowered(value) {
            case .value(let result): loweredValue = result
            case .expressionError(let error): return .expressionError(error)
            }
            let loweredLow: Expression
            switch try await lowered(low) {
            case .value(let result): loweredLow = result
            case .expressionError(let error): return .expressionError(error)
            }
            let loweredHigh: Expression
            switch try await lowered(high) {
            case .value(let result): loweredHigh = result
            case .expressionError(let error): return .expressionError(error)
            }
            return .value(.between(loweredValue, low: loweredLow, high: loweredHigh))
        case .function(let call):
            var arguments: [Expression] = []
            arguments.reserveCapacity(call.arguments.count)
            for argument in call.arguments {
                switch try await lowered(argument) {
                case .value(let value): arguments.append(value)
                case .expressionError(let error): return .expressionError(error)
                }
            }
            return .value(
                .function(
                    FunctionCall(
                        name: call.name,
                        arguments: arguments,
                        distinct: call.distinct
                    )
                )
            )
        case .triple(let subject, let predicate, let object):
            let loweredSubject: Expression
            switch try await lowered(subject) {
            case .value(let value): loweredSubject = value
            case .expressionError(let error): return .expressionError(error)
            }
            let loweredPredicate: Expression
            switch try await lowered(predicate) {
            case .value(let value): loweredPredicate = value
            case .expressionError(let error): return .expressionError(error)
            }
            let loweredObject: Expression
            switch try await lowered(object) {
            case .value(let value): loweredObject = value
            case .expressionError(let error): return .expressionError(error)
            }
            return .value(
                .triple(
                    subject: loweredSubject,
                    predicate: loweredPredicate,
                    object: loweredObject
                )
            )
        case .literal, .column, .variable, .parameter, .bound,
             .aggregate, .subquery, .exists, .inSubquery,
             .and, .or, .not, .caseWhen, .coalesce, .inList, .notInList:
            return .expressionError(
                .runtimeInvariant(
                    "dataset expression lowering reached an invalid outer node"
                )
            )
        }
    }

    private static func literalExpression(
        _ value: FieldValue
    ) -> SPARQLExpressionEvaluationOutcome<Expression> {
        switch literal(value) {
        case .value(let literal): return .value(.literal(literal))
        case .expressionError(let error): return .expressionError(error)
        }
    }

    private static func literal(
        _ value: FieldValue
    ) -> SPARQLExpressionEvaluationOutcome<Literal> {
        switch value {
        case .null: return .value(.null)
        case .string(let value): return .value(.string(value))
        case .int8(let value): return .value(.int(Int64(value)))
        case .int16(let value): return .value(.int(Int64(value)))
        case .int32(let value): return .value(.int(Int64(value)))
        case .int64(let value): return .value(.int(value))
        case .uint8(let value): return .value(.uint(UInt64(value)))
        case .uint16(let value): return .value(.uint(UInt64(value)))
        case .uint32(let value): return .value(.uint(UInt64(value)))
        case .uint64(let value): return .value(.uint(value))
        case .float32(let value): return .value(.double(Double(value)))
        case .float64(let value): return .value(.double(value))
        case .decimal(let value): return .value(.decimal(value))
        case .bool(let value): return .value(.bool(value))
        case .bytes(let value): return .value(.binary(value))
        case .date(let value): return .value(.date(value))
        case .timestamp(let value): return .value(.timestamp(value))
        case .uuid(let value): return .value(.uuid(value))
        case .array(let values):
            var literals: [Literal] = []
            literals.reserveCapacity(values.count)
            for element in values {
                switch literal(element) {
                case .value(let value): literals.append(value)
                case .expressionError(let error): return .expressionError(error)
                }
            }
            return .value(.array(literals))
        case .rdfTerm(let term): return .value(.rdfTerm(term))
        case .object: return unsupportedLiteral("object field value")
        case .reference: return unsupportedLiteral("reference field value")
        case .time: return unsupportedLiteral("time field value")
        case .dateTime: return unsupportedLiteral("civil date-time field value")
        case .timeSpan: return unsupportedLiteral("time-span field value")
        case .calendarPeriod: return unsupportedLiteral("calendar-period field value")
        case .geographicPoint: return unsupportedLiteral("geographic point")
        case .geographicPosition: return unsupportedLiteral("geographic position")
        case .vector: return unsupportedLiteral("vector field value")
        }
    }

    private static func unsupportedLiteral(
        _ name: String
    ) -> SPARQLExpressionEvaluationOutcome<Literal> {
        .expressionError(
            .unsupportedExpression("\(name) cannot be represented as a query literal")
        )
    }

    private static func canonicalBoolean(
        _ value: Bool,
        binding: VariableBinding
    ) -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        evaluateImmediate(.literal(.bool(value)), binding: binding)
    }

    private static func evaluateImmediate(
        _ expression: Expression,
        binding: VariableBinding
    ) -> SPARQLExpressionEvaluationOutcome<FieldValue> {
        do throws(SPARQLExpressionEvaluationError) {
            return .value(
                try ExpressionEvaluator.evaluate(expression, binding: binding)
            )
        } catch let error {
            return .expressionError(error)
        }
    }

    private static func effectiveBooleanValue(
        _ value: FieldValue
    ) -> SPARQLExpressionEvaluationOutcome<Bool> {
        do throws(SPARQLExpressionEvaluationError) {
            return .value(try ExpressionEvaluator.effectiveBooleanValue(value))
        } catch let error {
            return .expressionError(error)
        }
    }

    private static func fieldValuesEqual(
        _ left: FieldValue,
        _ right: FieldValue
    ) -> SPARQLExpressionEvaluationOutcome<Bool> {
        do throws(SPARQLExpressionEvaluationError) {
            return .value(try ExpressionEvaluator.equalFieldValues(left, right))
        } catch let error {
            return .expressionError(error)
        }
    }
}

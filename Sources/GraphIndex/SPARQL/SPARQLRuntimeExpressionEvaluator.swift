import Core
import DatabaseValue
import QueryIR

/// Dataset-dependent expression resolution supplied by the query executor.
/// The resolver must use the caller's transaction and active graph.
struct SPARQLRuntimeExpressionResolver: Sendable {
    let exists: @Sendable (SelectQuery, VariableBinding) async throws -> Bool
    let function: @Sendable (
        String,
        [FieldValue],
        VariableBinding
    ) async throws -> FieldValue
}

/// Evaluates the canonical QueryIR expression tree without erasing dataset
/// operations into synchronous closures.
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
        do {
            return try ExpressionEvaluator.effectiveBooleanValue(
                await evaluate(plan, binding: binding, resolver: resolver)
            )
        } catch let error as SPARQLExpressionEvaluationError
            where error.isSPARQLEvaluationError {
            return false
        }
    }

    static func evaluate(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        guard plan.requiresDataset
                || plan.usesExtensionFunction
                || plan.volatility != .immutable else {
            return try ExpressionEvaluator.evaluate(
                plan.expression,
                binding: binding
            )
        }
        return try await evaluateDatasetExpression(
            plan.expression,
            binding: binding,
            resolver: resolver
        )
    }

    private static func evaluateDatasetExpression(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        switch expression {
        case .exists(let query):
            let result = try await resolver.exists(query, binding)
            return try ExpressionEvaluator.evaluate(
                .literal(.bool(result)),
                binding: binding
            )

        case .and(let lhs, let rhs):
            return try await evaluateLogicalAnd(
                lhs,
                rhs,
                binding: binding,
                resolver: resolver
            )

        case .or(let lhs, let rhs):
            return try await evaluateLogicalOr(
                lhs,
                rhs,
                binding: binding,
                resolver: resolver
            )

        case .not(let operand):
            let value = try await evaluateExpression(
                operand,
                binding: binding,
                resolver: resolver
            )
            return try ExpressionEvaluator.evaluate(
                .literal(
                    .bool(!ExpressionEvaluator.effectiveBooleanValue(value))
                ),
                binding: binding
            )

        case .caseWhen(let pairs, let elseResult):
            for pair in pairs {
                let condition = try await evaluateExpression(
                    pair.condition,
                    binding: binding,
                    resolver: resolver
                )
                if try ExpressionEvaluator.effectiveBooleanValue(condition) {
                    return try await evaluateExpression(
                        pair.result,
                        binding: binding,
                        resolver: resolver
                    )
                }
            }
            if let elseResult {
                return try await evaluateExpression(
                    elseResult,
                    binding: binding,
                    resolver: resolver
                )
            }
            return .null

        case .coalesce(let expressions):
            return try await evaluateCoalesce(
                expressions,
                binding: binding,
                resolver: resolver
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
            where DatabaseText.isEqualIgnoringASCIICase(call.name, "IF"):
            guard call.arguments.count == 3 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(
                    call.name
                )
            }
            let condition = try await evaluateExpression(
                call.arguments[0],
                binding: binding,
                resolver: resolver
            )
            let branch = try ExpressionEvaluator.effectiveBooleanValue(condition)
                ? call.arguments[1]
                : call.arguments[2]
            return try await evaluateExpression(
                branch,
                binding: binding,
                resolver: resolver
            )

        case .function(let call)
            where DatabaseText.isEqualIgnoringASCIICase(call.name, "COALESCE"):
            return try await evaluateCoalesce(
                call.arguments,
                binding: binding,
                resolver: resolver
            )

        case .function(let call):
            let identifier: SPARQLFunctionIdentifier
            do {
                identifier = try SPARQLFunctionIdentifier.resolve(call.name)
            } catch {
                throw SPARQLExpressionEvaluationError.unsupportedExpression(
                    "function \(call.name)"
                )
            }
            switch identifier {
            case .extensionFunction,
                 .builtIn(.now), .builtIn(.rand), .builtIn(.uuid),
                 .builtIn(.strUUID), .builtIn(.blankNode):
                var arguments: [FieldValue] = []
                arguments.reserveCapacity(call.arguments.count)
                for argument in call.arguments {
                    arguments.append(
                        try await evaluateExpression(
                            argument,
                            binding: binding,
                            resolver: resolver
                        )
                    )
                }
                return try await resolver.function(
                    call.name,
                    arguments,
                    binding
                )
            case .builtIn, .datatypeConstructor:
                let lowered = try await lowerImmediateOperands(
                    expression,
                    binding: binding,
                    resolver: resolver
                )
                return try ExpressionEvaluator.evaluate(
                    lowered,
                    binding: binding
                )
            }

        case .subquery, .inSubquery:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                String(describing: expression)
            )

        case .literal, .variable, .bound:
            return try ExpressionEvaluator.evaluate(
                expression,
                binding: binding
            )

        default:
            let lowered = try await lowerImmediateOperands(
                expression,
                binding: binding,
                resolver: resolver
            )
            return try ExpressionEvaluator.evaluate(lowered, binding: binding)
        }
    }

    private static func evaluateExpression(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        try await evaluateDatasetExpression(
            expression,
            binding: binding,
            resolver: resolver
        )
    }

    private static func evaluateLogicalAnd(
        _ lhs: Expression,
        _ rhs: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        let left = try await booleanOutcome(
            lhs,
            binding: binding,
            resolver: resolver
        )
        if case .value(false) = left {
            return try canonicalBoolean(false, binding: binding)
        }

        let right = try await booleanOutcome(
            rhs,
            binding: binding,
            resolver: resolver
        )
        switch (left, right) {
        case (.value(true), .value(let value)):
            return try canonicalBoolean(value, binding: binding)
        case (.value(true), .expressionError(let error)):
            throw error
        case (.expressionError, .value(false)):
            return try canonicalBoolean(false, binding: binding)
        case (.expressionError(let error), .value(true)),
             (.expressionError(let error), .expressionError):
            throw error
        case (.value(false), _):
            return try canonicalBoolean(false, binding: binding)
        }
    }

    private static func evaluateLogicalOr(
        _ lhs: Expression,
        _ rhs: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        let left = try await booleanOutcome(
            lhs,
            binding: binding,
            resolver: resolver
        )
        if case .value(true) = left {
            return try canonicalBoolean(true, binding: binding)
        }

        let right = try await booleanOutcome(
            rhs,
            binding: binding,
            resolver: resolver
        )
        switch (left, right) {
        case (.value(false), .value(let value)):
            return try canonicalBoolean(value, binding: binding)
        case (.value(false), .expressionError(let error)):
            throw error
        case (.expressionError, .value(true)):
            return try canonicalBoolean(true, binding: binding)
        case (.expressionError(let error), .value(false)),
             (.expressionError(let error), .expressionError):
            throw error
        case (.value(true), _):
            return try canonicalBoolean(true, binding: binding)
        }
    }

    private static func booleanOutcome(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> BooleanOutcome {
        do {
            let value = try await evaluateExpression(
                expression,
                binding: binding,
                resolver: resolver
            )
            return try .value(ExpressionEvaluator.effectiveBooleanValue(value))
        } catch let error as SPARQLExpressionEvaluationError
            where error.isSPARQLEvaluationError {
            return .expressionError(error)
        }
    }

    private static func evaluateCoalesce(
        _ expressions: [Expression],
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        for expression in expressions {
            do {
                let value = try await evaluateExpression(
                    expression,
                    binding: binding,
                    resolver: resolver
                )
                if value != .null { return value }
            } catch let error as SPARQLExpressionEvaluationError
                where error.isSPARQLEvaluationError {
                continue
            }
        }
        throw SPARQLExpressionEvaluationError.typeError(
            "COALESCE has no expression without an error"
        )
    }

    private static func evaluateInList(
        _ operand: Expression,
        values: [Expression],
        negated: Bool,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> FieldValue {
        let left = try await evaluateExpression(
            operand,
            binding: binding,
            resolver: resolver
        )
        var firstError: SPARQLExpressionEvaluationError?
        for candidate in values {
            do {
                let right = try await evaluateExpression(
                    candidate,
                    binding: binding,
                    resolver: resolver
                )
                if try ExpressionEvaluator.equalFieldValues(left, right) {
                    return try canonicalBoolean(!negated, binding: binding)
                }
            } catch let error as SPARQLExpressionEvaluationError
                where error.isSPARQLEvaluationError {
                if firstError == nil { firstError = error }
            }
        }
        if let firstError { throw firstError }
        return try canonicalBoolean(negated, binding: binding)
    }

    /// Lowers only the immediate operands after each operand has been evaluated.
    /// This preserves lazy control-flow at the outer node and keeps RDF terms
    /// intact through QueryIR.Literal.rdfTerm.
    private static func lowerImmediateOperands(
        _ expression: Expression,
        binding: VariableBinding,
        resolver: SPARQLRuntimeExpressionResolver
    ) async throws -> Expression {
        func lowered(_ child: Expression) async throws -> Expression {
            let value = try await evaluateExpression(
                child,
                binding: binding,
                resolver: resolver
            )
            return try literalExpression(value)
        }

        switch expression {
        case .add(let lhs, let rhs):
            return try await .add(lowered(lhs), lowered(rhs))
        case .subtract(let lhs, let rhs):
            return try await .subtract(lowered(lhs), lowered(rhs))
        case .multiply(let lhs, let rhs):
            return try await .multiply(lowered(lhs), lowered(rhs))
        case .divide(let lhs, let rhs):
            return try await .divide(lowered(lhs), lowered(rhs))
        case .modulo(let lhs, let rhs):
            return try await .modulo(lowered(lhs), lowered(rhs))
        case .equal(let lhs, let rhs):
            return try await .equal(lowered(lhs), lowered(rhs))
        case .notEqual(let lhs, let rhs):
            return try await .notEqual(lowered(lhs), lowered(rhs))
        case .lessThan(let lhs, let rhs):
            return try await .lessThan(lowered(lhs), lowered(rhs))
        case .lessThanOrEqual(let lhs, let rhs):
            return try await .lessThanOrEqual(lowered(lhs), lowered(rhs))
        case .greaterThan(let lhs, let rhs):
            return try await .greaterThan(lowered(lhs), lowered(rhs))
        case .greaterThanOrEqual(let lhs, let rhs):
            return try await .greaterThanOrEqual(lowered(lhs), lowered(rhs))
        case .nullIf(let lhs, let rhs):
            return try await .nullIf(lowered(lhs), lowered(rhs))

        case .negate(let value):
            return try await .negate(lowered(value))
        case .isNull(let value):
            return try await .isNull(lowered(value))
        case .isNotNull(let value):
            return try await .isNotNull(lowered(value))
        case .like(let value, let pattern):
            return try await .like(lowered(value), pattern: pattern)
        case .regex(let value, let pattern, let flags):
            return try await .regex(lowered(value), pattern: pattern, flags: flags)
        case .cast(let value, let targetType):
            return try await .cast(lowered(value), targetType: targetType)
        case .isTriple(let value):
            return try await .isTriple(lowered(value))
        case .subject(let value):
            return try await .subject(lowered(value))
        case .predicate(let value):
            return try await .predicate(lowered(value))
        case .object(let value):
            return try await .object(lowered(value))

        case .between(let value, let low, let high):
            return try await .between(
                lowered(value),
                low: lowered(low),
                high: lowered(high)
            )

        case .function(let call):
            var arguments: [Expression] = []
            arguments.reserveCapacity(call.arguments.count)
            for argument in call.arguments {
                arguments.append(try await lowered(argument))
            }
            return .function(
                FunctionCall(
                    name: call.name,
                    arguments: arguments,
                    distinct: call.distinct
                )
            )

        case .triple(let subject, let predicate, let object):
            return try await .triple(
                subject: lowered(subject),
                predicate: lowered(predicate),
                object: lowered(object)
            )

        case .literal, .column, .variable, .parameter, .bound,
             .aggregate, .subquery, .exists, .inSubquery,
             .and, .or, .not, .caseWhen, .coalesce, .inList, .notInList:
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "dataset expression lowering reached an invalid outer node"
            )
        }
    }

    private static func literalExpression(_ value: FieldValue) throws -> Expression {
        .literal(try literal(value))
    }

    private static func literal(_ value: FieldValue) throws -> Literal {
        switch value {
        case .null:
            return .null
        case .string(let value):
            return .string(value)
        case .int64(let value):
            return .int(value)
        case .uint64(let value):
            return .uint(value)
        case .double(let value):
            return .double(value)
        case .bool(let value):
            return .bool(value)
        case .data(let value):
            return .binary(value)
        case .array(let values):
            var literals: [Literal] = []
            literals.reserveCapacity(values.count)
            for element in values {
                literals.append(try literal(element))
            }
            return .array(literals)
        case .rdfTerm(let term):
            return .rdfTerm(term)
        }
    }

    private static func canonicalBoolean(
        _ value: Bool,
        binding: VariableBinding
    ) throws -> FieldValue {
        try ExpressionEvaluator.evaluate(.literal(.bool(value)), binding: binding)
    }

}

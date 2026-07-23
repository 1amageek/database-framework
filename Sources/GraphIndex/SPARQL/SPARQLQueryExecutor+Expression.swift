#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import Graph
import DatabaseEngine
import QueryIR
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateCanonicalExpression(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph
    ) async throws -> FieldValue {
        guard let expressionContext else {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "query-scoped expression context is unavailable"
            )
        }
        let scopedBinding = try expressionContext.bindingWithExpressionScope(
            binding
        )
        let resolver = SPARQLRuntimeExpressionResolver(
            exists: { query, correlatedBinding in
                guard let pattern = plan.compiledExistsPattern(
                    for: query
                ) else {
                    throw SPARQLExpressionEvaluationError.runtimeInvariant(
                        "EXISTS pattern was not compiled with its expression plan"
                    )
                }
                return try await self.evaluateExists(
                    pattern,
                    binding: correlatedBinding,
                    transaction: transaction,
                    activeGraph: activeGraph
                )
            },
            function: { name, arguments, solution in
                try expressionContext.evaluateFunction(
                    name: name,
                    arguments: arguments,
                    binding: solution
                )
            }
        )
        return try await SPARQLRuntimeExpressionEvaluator.evaluate(
            plan,
            binding: scopedBinding,
            resolver: resolver
        )
    }

    func evaluateFilterExpression(
        _ expression: FilterExpression,
        binding: VariableBinding,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph
    ) async throws -> Bool {
        guard let expressionContext else {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "query-scoped expression context is unavailable"
            )
        }
        let binding = try expressionContext.bindingWithExpressionScope(binding)
        switch expression {
        case .and(let lhs, let rhs):
            guard try await evaluateFilterExpression(
                lhs,
                binding: binding,
                transaction: transaction,
                activeGraph: activeGraph
            ) else {
                return false
            }
            return try await evaluateFilterExpression(
                rhs,
                binding: binding,
                transaction: transaction,
                activeGraph: activeGraph
            )
        case .or(let lhs, let rhs):
            if try await evaluateFilterExpression(
                lhs,
                binding: binding,
                transaction: transaction,
                activeGraph: activeGraph
            ) {
                return true
            }
            return try await evaluateFilterExpression(
                rhs,
                binding: binding,
                transaction: transaction,
                activeGraph: activeGraph
            )
        case .not(let operand):
            return try await !evaluateFilterExpression(
                operand,
                binding: binding,
                transaction: transaction,
                activeGraph: activeGraph
            )
        case .query(let plan):
            let resolver = SPARQLRuntimeExpressionResolver(
                exists: { query, correlatedBinding in
                    guard let pattern = plan.compiledExistsPattern(
                        for: query
                    ) else {
                        throw SPARQLExpressionEvaluationError.runtimeInvariant(
                            "EXISTS pattern was not compiled with its expression plan"
                        )
                    }
                    return try await self.evaluateExists(
                        pattern,
                        binding: correlatedBinding,
                        transaction: transaction,
                        activeGraph: activeGraph
                    )
                },
                function: { name, arguments, solution in
                    try expressionContext.evaluateFunction(
                        name: name,
                        arguments: arguments,
                        binding: solution
                    )
                }
            )
            return try await SPARQLRuntimeExpressionEvaluator.evaluateAsBoolean(
                plan,
                binding: binding,
                resolver: resolver
            )
        default:
            return try expression.evaluate(binding)
        }
    }

    func evaluateExists(
        _ pattern: ExecutionPattern,
        binding: VariableBinding,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph
    ) async throws -> Bool {
        let result = try await evaluate(
            pattern: pattern,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: binding,
            resultLimit: 1
        )
        let exists = !result.bindings.isEmpty
        nestedExpressionStatistics?.record(result.stats)
        return exists
    }

}

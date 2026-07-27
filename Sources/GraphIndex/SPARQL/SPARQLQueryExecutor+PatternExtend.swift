#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateExtendPattern(
        innerPattern: ExecutionPattern,
        variable: String,
        expression: SPARQLExpressionPlan,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        let innerResult = try await evaluate(
            pattern: innerPattern,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed,
            resultLimit: resultLimit
        )
        let expectedCount = min(
            resultLimit ?? innerResult.bindings.count,
            innerResult.bindings.count
        )
        var extended = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .projection,
            expectedCount: expectedCount
        )
        for index in 0..<innerResult.bindings.count {
            try requiredWorkMeter().consume(at: .projection)
            try await innerResult.bindings.withElement(
                at: index
            ) { unscopedBinding in
                guard let expressionContext else {
                    throw SPARQLExpressionEvaluationError
                        .runtimeInvariant(
                            "query-scoped expression context is unavailable"
                        )
                }
                let binding = try expressionContext
                    .bindingWithExpressionScope(unscopedBinding)
                guard !binding.isBound(variable) else {
                    throw SPARQLExpressionEvaluationError
                        .runtimeInvariant(
                            "BIND target is already bound: \(variable)"
                        )
                }
                do {
                    let value = try await evaluateCanonicalExpression(
                        expression,
                        binding: binding,
                        transaction: transaction,
                        activeGraph: activeGraph
                    )
                    if value == .null {
                        try extended.append(
                            binding,
                            at: .projection
                        )
                    } else {
                        switch try extended.prepareAppend(
                            extending: binding,
                            variable: variable,
                            value: value,
                            at: .projection
                        ) {
                        case .incompatible:
                            throw SPARQLExpressionEvaluationError
                                .runtimeInvariant(
                                    "BIND preflight rejected an unbound target"
                                )
                        case .admitted(let admission):
                            let output = binding.binding(
                                variable,
                                to: value
                            )
                            extended.append(
                                output,
                                using: admission
                            )
                        }
                    }
                } catch let error as SPARQLExpressionEvaluationError
                    where error.isSPARQLEvaluationError {
                    try extended.append(
                        binding,
                        at: .projection
                    )
                }
            }
            if let resultLimit, extended.count >= resultLimit {
                break
            }
        }
        return EvaluationResult(
            bindings: extended.finish(),
            stats: stats
        )
            .mergedStats(with: innerResult.stats)
    }

}

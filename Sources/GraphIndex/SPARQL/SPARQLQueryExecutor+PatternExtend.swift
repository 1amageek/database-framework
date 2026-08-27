import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateExtendPattern(
        innerPattern: ExecutionPattern,
        variable: String,
        expression: SPARQLExpressionPlan,
        transaction: any TransactionReadAccess,
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
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: try requiredWorkMeter(),
            stage: .projection
        )
        defer { footprintMeter.shutdown() }
        for index in 0..<innerResult.bindings.count {
            try requiredWorkMeter().consume(at: .projection)
            try await innerResult.bindings.withElement(
                at: index
            ) { unscopedBinding in
                let sourceFootprint = try footprintMeter.footprint(
                    of: unscopedBinding
                )
                let candidateReservation = try requiredWorkMeter()
                    .reserveIntermediate(
                        rows: sourceFootprint.rows,
                        bytes: sourceFootprint.bytes,
                        at: .projection
                    )
                defer { candidateReservation.release() }
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
                let resultOwnership = try expression.resultOwnership(
                    binding: binding,
                    workMeter: try requiredWorkMeter(),
                    stage: .projection,
                    maximumExtensionResultByteCount: {
                        try expressionContext
                            .maximumExtensionFunctionResultByteCount(
                                identifier: $0
                            )
                    }
                )
                let evaluatedValue: DatabaseQueryScopedFieldValue?
                switch resultOwnership {
                case .borrowed:
                    evaluatedValue = try await evaluateExtendValue(
                        expression,
                        binding: binding,
                        transaction: transaction,
                        activeGraph: activeGraph
                    ).map(DatabaseQueryScopedFieldValue.borrowing)
                case .produced(let maximumFootprint):
                    evaluatedValue = try await DatabaseQueryScopedFieldValue
                        .producingOptional(
                            maximumFootprint: maximumFootprint,
                            workMeter: try requiredWorkMeter(),
                            stage: .projection
                        ) {
                            try await evaluateExtendValue(
                                expression,
                                binding: binding,
                                transaction: transaction,
                                activeGraph: activeGraph
                            )
                        }
                }
                let outputFootprint: DatabaseIntermediateFootprint
                if let evaluatedValue {
                    outputFootprint = try evaluatedValue.withValue { value in
                        switch try footprintMeter.footprint(
                            extending: binding,
                            variable: variable,
                            value: value
                        ) {
                        case .incompatible:
                            throw SPARQLExpressionEvaluationError
                                .runtimeInvariant(
                                    "BIND preflight rejected an unbound target"
                                )
                        case .compatible(let footprint):
                            return footprint
                        }
                    }
                } else {
                    outputFootprint = try footprintMeter.footprint(of: binding)
                }
                let admission = try extended.prepareAppend(
                    footprint: outputFootprint,
                    at: .projection
                )
                var output = binding
                if let evaluatedValue {
                    try evaluatedValue.withValue { boundValue in
                        guard output.merge(
                            variable: variable,
                            value: copy boundValue
                        ) else {
                            throw SPARQLExpressionEvaluationError
                                .runtimeInvariant(
                                    "BIND target changed after prospective admission"
                                )
                        }
                    }
                }
                extended.append(output, using: admission)
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

    private func evaluateExtendValue(
        _ expression: SPARQLExpressionPlan,
        binding: VariableBinding,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph
    ) async throws -> FieldValue? {
        switch try await evaluateCanonicalExpression(
            expression,
            binding: binding,
            transaction: transaction,
            activeGraph: activeGraph
        ) {
        case .value(let value):
            return value == .null ? nil : value
        case .expressionError(let evaluationError):
            if evaluationError.isSPARQLEvaluationError {
                return nil
            }
            throw evaluationError
        }
    }

}

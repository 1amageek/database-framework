import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateOrderedSolutionPlan(
        _ plan: SPARQLOrderedSolutionPlan,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding
    ) async throws -> EvaluationResult {
        let evaluated = try await evaluate(
            pattern: plan.algebra,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )
        let evaluationStats = evaluated.stats
        var bindings = consume evaluated.bindings

        if !plan.orderKeys.isEmpty {
            guard let expressionContext else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "query-scoped expression context is unavailable"
                )
            }
            bindings = try await BindingSorter.sort(
                consume bindings,
                by: plan.orderKeys,
                workMeter: try requiredWorkMeter(),
                maximumExtensionResultByteCount: {
                    try expressionContext
                        .maximumExtensionFunctionResultByteCount(
                            identifier: $0
                        )
                },
                evaluate: { expression, binding in
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
            )
        }

        return EvaluationResult(
            bindings: consume bindings,
            stats: evaluationStats
        )
    }

    func evaluateSelectPlan(
        _ plan: SPARQLSelectExecutionPlan,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding
    ) async throws -> EvaluationResult {
        let evaluated = try await evaluateOrderedSolutionPlan(
            plan.ordered,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )
        let evaluationStats = evaluated.stats
        let bindings = try applyProjectionDuplicateAndSlice(
            consume evaluated.bindings,
            projectionVariables: plan.projectionVariables,
            projectionIsIdentity: plan.projectionIsIdentity,
            duplicatePolicy: plan.duplicatePolicy,
            slice: plan.slice
        )

        return EvaluationResult(
            bindings: consume bindings,
            stats: evaluationStats
        )
    }

    func applyProjectionDuplicateAndSlice(
        _ source: consuming SPARQLRetainedBindings,
        projectionVariables: [String],
        projectionIsIdentity: Bool,
        duplicatePolicy: SPARQLDuplicatePolicy,
        slice: SPARQLSlice
    ) throws -> SPARQLRetainedBindings {
        var bindings = consume source

        if !projectionIsIdentity {
            var projectionBuilder = try SPARQLRetainedBindingBuilder.make(
                workMeter: try requiredWorkMeter(),
                stage: .projection,
                expectedCount: bindings.count
            )
            for index in 0..<bindings.count {
                try requiredWorkMeter().consume(at: .projection)
                try bindings.withElement(at: index) { binding in
                    let admission = try projectionBuilder.prepareAppend(
                        projecting: binding,
                        variables: projectionVariables,
                        at: .projection
                    )
                    let projected = binding.project(projectionVariables)
                    projectionBuilder.append(projected, using: admission)
                }
            }
            bindings = projectionBuilder.finish()
        }

        if duplicatePolicy == .distinct {
            var seen = try SPARQLRetainedBindingSet.make(
                workMeter: try requiredWorkMeter(),
                stage: .deduplication,
                expectedCount: 0
            )
            if bindings.isUnique {
                let footprintMeter = try SPARQLBindingFootprintMeter.make(
                    workMeter: try requiredWorkMeter(),
                    stage: .deduplication
                )
                var releasedFootprint = DatabaseIntermediateFootprint()
                bindings = try (consume bindings).stableCompactingUnique {
                    binding in
                    try requiredWorkMeter().consume(at: .deduplication)
                    guard try seen.insert(binding) else {
                        releasedFootprint = try releasedFootprint.adding(
                            try footprintMeter.footprint(of: binding)
                        )
                        return false
                    }
                    return true
                }
                footprintMeter.shutdown()
                bindings = try (consume bindings)
                    .releasingRetainedFootprint(releasedFootprint)
            } else {
                var distinctBuilder = try SPARQLRetainedBindingBuilder.make(
                    workMeter: try requiredWorkMeter(),
                    stage: .deduplication,
                    expectedCount: 0
                )
                for index in 0..<bindings.count {
                    try requiredWorkMeter().consume(at: .deduplication)
                    try bindings.withElement(at: index) { binding in
                        guard try seen.insert(binding) else { return }
                        try distinctBuilder.appendBorrowed(
                            binding,
                            at: .deduplication
                        )
                    }
                }
                bindings = distinctBuilder.finish()
            }
        }

        return try applyRetainedSlice(
            consume bindings,
            slice: slice
        )
    }

}

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
            bindings = try await BindingSorter.sort(
                consume bindings,
                by: plan.orderKeys,
                workMeter: try requiredWorkMeter(),
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
        var bindings = consume evaluated.bindings

        if !plan.projectionIsIdentity {
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
                        variables: plan.projectionVariables,
                        at: .projection
                    )
                    let projected = binding.project(plan.projectionVariables)
                    projectionBuilder.append(projected, using: admission)
                }
            }
            bindings = projectionBuilder.finish()
        }

        if plan.duplicatePolicy == .distinct {
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

        bindings = try applyRetainedSlice(
            consume bindings,
            slice: plan.slice
        )
        return EvaluationResult(
            bindings: consume bindings,
            stats: evaluationStats
        )
    }

}

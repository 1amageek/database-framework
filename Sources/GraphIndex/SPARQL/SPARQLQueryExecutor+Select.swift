#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import DatabaseKit
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateSubquery(
        _ plan: SPARQLSubqueryExecutionPlan,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        outerSeed: VariableBinding,
        filter: FilterExpression?
    ) async throws -> EvaluationResult {
        let unfiltered: EvaluationResult
        switch plan.inputPolicy {
        case .isolated:
            guard let subqueryCache else {
                throw SPARQLQueryError.executionFailed(
                    "SPARQL execution requires a transaction-attempt-scoped subquery cache"
                )
            }
            let key = SPARQLSubqueryCacheKey(
                occurrenceIdentifier: plan.occurrenceIdentifier,
                graphScope: activeGraph.scanScope
            )
            if let cached = subqueryCache.value(for: key) {
                unfiltered = EvaluationResult(
                    bindings: cached,
                    stats: ExecutionStatistics()
                )
            } else {
                let evaluated = try await evaluateSelectPlan(
                    plan.select,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    seed: VariableBinding()
                )
                let evaluatedStats = evaluated.stats
                let cachedBindings = try subqueryCache.store(
                    consume evaluated.bindings,
                    for: key
                )
                unfiltered = EvaluationResult(
                    bindings: consume cachedBindings,
                    stats: evaluatedStats
                )
            }

        case .lateral:
            unfiltered = try await evaluateSelectPlan(
                plan.select,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: outerSeed
            )
        }

        guard let filter else { return consume unfiltered }
        let unfilteredStats = unfiltered.stats
        let filtered = try await filterBindings(
            unfiltered.bindings,
            expression: filter,
            transaction: transaction,
            activeGraph: activeGraph,
            resultLimit: nil
        )
        return EvaluationResult(
            bindings: consume filtered,
            stats: unfilteredStats
        )
    }

    /// Applies the complete SPARQL Select modifier pipeline locally to a
    /// SubSelect. ORDER BY must observe non-projected variables, while DISTINCT
    /// must observe only the projected solution sequence.
}

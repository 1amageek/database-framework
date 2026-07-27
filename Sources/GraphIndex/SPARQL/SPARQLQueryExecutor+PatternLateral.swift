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
    func evaluateLateralPattern(
        left: ExecutionPattern,
        right: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        // SPARQL 1.2 LATERAL: correlated join
        // For each solution mu1 from the left, substitute mu1's bindings into
        // the right pattern, then evaluate the substituted right pattern.
        // This allows the RHS to reference LHS variables as bound values.
        let leftResult = try await evaluate(
            pattern: left,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )
        var allBindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        var mergedStats = stats.merged(with: leftResult.stats)
        for leftIndex in 0..<leftResult.bindings.count {
            try requiredWorkMeter().consume(at: .bindingCandidate)
            try await leftResult.bindings.withElement(
                at: leftIndex
            ) { mu1 in
                let substitutedRight = try substitutePattern(
                    right,
                    with: mu1
                )
                let rightResult = try await evaluate(
                    pattern: substitutedRight,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    seed: mu1
                )
                mergedStats = mergedStats.merged(with: rightResult.stats)
                for rightIndex in 0..<rightResult.bindings.count {
                    if let resultLimit,
                       allBindings.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try rightResult.bindings.withElement(
                        at: rightIndex
                    ) { mu2 in
                        _ = try allBindings.appendMerged(
                            mu1,
                            with: mu2,
                            at: .joinCandidate
                        )
                    }
                }
            }
            if let resultLimit, allBindings.count >= resultLimit {
                break
            }
        }
        return EvaluationResult(
            bindings: allBindings.finish(),
            stats: mergedStats
        )
    }

    /// Evaluates one Select algebra boundary without exposing its internal
    /// bindings to the enclosing graph pattern. An isolated SubSelect is a
    /// relation that can be reused by each outer join candidate. A lateral
    /// SubSelect receives the current outer solution and is evaluated once for
    /// each distinct lateral invocation.
}

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
    func evaluateJoin(
        leftBindings: borrowing SPARQLRetainedBindings,
        rightPattern: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        resultLimit: Int?
    ) async throws -> EvaluationResult {
        var results = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        var combinedStats = ExecutionStatistics()

        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .bindingCandidate)
            try await leftBindings.withElement(
                at: leftIndex
            ) { leftBinding in
                let substitutedPattern = try substitutePattern(
                    rightPattern,
                    with: leftBinding
                )
                let remainingLimit = resultLimit.map {
                    max(0, $0 - results.count)
                }

                let rightResult = try await evaluate(
                    pattern: substitutedPattern,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    seed: leftBinding,
                    resultLimit: remainingLimit
                )
                combinedStats = combinedStats.merged(
                    with: rightResult.stats
                )

                for rightIndex in 0..<rightResult.bindings.count {
                    if let resultLimit, results.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try rightResult.bindings.withElement(
                        at: rightIndex
                    ) { rightBinding in
                        _ = try results.appendMerged(
                            leftBinding,
                            with: rightBinding,
                            at: .joinCandidate
                        )
                    }
                }
            }
            if let resultLimit, results.count >= resultLimit { break }
        }

        return EvaluationResult(
            bindings: results.finish(),
            stats: combinedStats
        )
    }

    /// Evaluate OPTIONAL (left outer join)
    ///
    /// For each left binding, try to match the right pattern.
    /// If right pattern doesn't match, keep the left binding as-is.
}

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
    func evaluateOptional(
        leftBindings: borrowing SPARQLRetainedBindings,
        rightPattern: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        resultLimit: Int?
    ) async throws -> EvaluationResult {
        if case .basic(let patterns) = rightPattern, patterns.count == 1, let rightTriple = patterns.first {
            return try await evaluateOptionalBatchedSingleTriple(
                leftBindings: leftBindings,
                rightTriple: rightTriple,
                transaction: transaction,
                activeGraph: activeGraph,
                resultLimit: resultLimit
            )
        }

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

                if rightResult.bindings.isEmpty {
                    try results.appendBorrowed(
                        leftBinding,
                        at: .joinCandidate
                    )
                    combinedStats.optionalMisses += 1
                } else {
                    var anyMerged = false
                    for rightIndex in 0..<rightResult.bindings.count {
                        if let resultLimit,
                           results.count >= resultLimit {
                            break
                        }
                        try requiredWorkMeter().consume(at: .joinCandidate)
                        try rightResult.bindings.withElement(
                            at: rightIndex
                        ) { rightBinding in
                            if try results.appendMerged(
                                leftBinding,
                                with: rightBinding,
                                at: .joinCandidate
                            ) {
                                anyMerged = true
                            }
                        }
                    }
                    if !anyMerged {
                        try results.appendBorrowed(
                            leftBinding,
                            at: .joinCandidate
                        )
                        combinedStats.optionalMisses += 1
                    }
                }
            }
            if let resultLimit, results.count >= resultLimit {
                break
            }
        }

        return EvaluationResult(
            bindings: results.finish(),
            stats: combinedStats
        )
    }

    /// Evaluate OPTIONAL for a single-triple RHS using scan signature batching.
    ///
    /// Preserves SPARQL left-join semantics:
    /// - Emit merged rows when compatible right matches exist
    /// - Emit each left row exactly once when no compatible right exists
}

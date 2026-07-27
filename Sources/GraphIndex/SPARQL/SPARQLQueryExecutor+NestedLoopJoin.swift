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
    func evaluateNestedLoopJoinStep(
        pattern: ExecutionTriple,
        leftBindings: borrowing SPARQLRetainedBindings,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression?,
        resultLimit: Int?
    ) async throws -> EvaluationResult {
        var results = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        var stats = ExecutionStatistics()
        stats.joinStrategies.append(.nestedLoop)

        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .bindingCandidate)
            try await leftBindings.withElement(at: leftIndex) { binding in
                let substituted = pattern.substitute(binding)
                let matches = try await executePattern(
                    substituted,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    filter: nil,
                    resultLimit: resultLimit.map {
                        max(0, $0 - results.count)
                    }
                )
                stats.indexScans += matches.stats.indexScans

                for matchIndex in 0..<matches.bindings.count {
                    if let resultLimit, results.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try await matches.bindings.withElement(
                        at: matchIndex
                    ) { match in
                        let preparation = try results.prepareAppend(
                            merging: binding,
                            with: match,
                            at: .joinCandidate
                        )
                        switch consume preparation {
                        case .incompatible:
                            return
                        case .admitted(let admission):
                            guard let merged = binding.merged(with: match) else {
                                throw SPARQLQueryError.executionFailed(
                                    "join preflight disagrees with row construction"
                                )
                            }
                            if let filter {
                                try requiredWorkMeter().consume(
                                    at: .filterEvaluation
                                )
                                guard try await evaluateFilterExpression(
                                    filter,
                                    binding: merged,
                                    transaction: transaction,
                                    activeGraph: activeGraph
                                ) else {
                                    return
                                }
                            }
                            results.append(merged, using: admission)
                        }
                    }
                }
            }
            if let resultLimit, results.count >= resultLimit { break }
        }

        return EvaluationResult(
            bindings: results.finish(),
            stats: stats
        )
    }

}

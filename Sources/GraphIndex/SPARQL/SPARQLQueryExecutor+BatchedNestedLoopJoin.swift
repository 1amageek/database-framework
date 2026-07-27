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
    func evaluateBatchedNestedLoopJoinStep(
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
        stats.joinStrategies.append(.batchedNestedLoop)

        var scanCache: [
            ScanSignature: SPARQLSharedBindingSnapshot
        ] = [:]

        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .bindingCandidate)
            try await leftBindings.withElement(at: leftIndex) { binding in
                let substituted = pattern.substitute(binding)
                let signature = makeScanSignature(
                    for: substituted,
                    graphScope: activeGraph.scanScope
                )

                let matches: SPARQLRetainedBindings
                if let cachedMatches = scanCache[signature] {
                    matches = cachedMatches.retainedBindings()
                } else {
                    let scannedMatches = try await executePattern(
                        substituted,
                        transaction: transaction,
                        activeGraph: activeGraph,
                        filter: nil,
                        resultLimit: nil
                    )
                    stats.indexScans += scannedMatches.stats.indexScans
                    let sharedMatches = try (
                        consume scannedMatches.bindings
                    ).sharing(at: .joinCandidate)
                    scanCache[signature] = try SPARQLSharedBindingSnapshot(
                        shared: sharedMatches
                    )
                    matches = consume sharedMatches
                }

                for matchIndex in 0..<matches.count {
                    if let resultLimit, results.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try await matches.withElement(at: matchIndex) { match in
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

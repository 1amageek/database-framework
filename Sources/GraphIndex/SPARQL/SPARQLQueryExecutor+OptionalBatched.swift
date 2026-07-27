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
    func evaluateOptionalBatchedSingleTriple(
        leftBindings: borrowing SPARQLRetainedBindings,
        rightTriple: ExecutionTriple,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        resultLimit: Int?
    ) async throws -> EvaluationResult {
        guard !leftBindings.isEmpty else {
            return .empty()
        }

        var combinedStats = ExecutionStatistics()
        var results = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        var scanCache: [
            ScanSignature: SPARQLSharedBindingSnapshot
        ] = [:]

        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .deduplication)
            try await leftBindings.withElement(
                at: leftIndex
            ) { leftBinding in
                let substitutedTriple = rightTriple.substitute(leftBinding)
                let signature = makeScanSignature(
                    for: substitutedTriple,
                    graphScope: activeGraph.scanScope
                )
                let rightBindings: SPARQLRetainedBindings
                if let cached = scanCache[signature] {
                    rightBindings = cached.retainedBindings()
                } else {
                    let rightResult = try await evaluate(
                        pattern: .basic([substitutedTriple]),
                        transaction: transaction,
                        activeGraph: activeGraph
                    )
                    combinedStats = combinedStats.merged(
                        with: rightResult.stats
                    )
                    let shared = try (
                        consume rightResult.bindings
                    ).sharing(at: .joinCandidate)
                    scanCache[signature] = try SPARQLSharedBindingSnapshot(
                        shared: shared
                    )
                    rightBindings = consume shared
                }

                var anyMerged = false
                for rightIndex in 0..<rightBindings.count {
                    if let resultLimit, results.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try rightBindings.withElement(
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
            if let resultLimit, results.count >= resultLimit { break }
        }

        return EvaluationResult(
            bindings: results.finish(),
            stats: combinedStats
        )
    }

}

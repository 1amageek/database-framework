import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateOptionalBatchedSingleTriple(
        leftBindings: borrowing SPARQLRetainedBindings,
        rightTriple: ExecutionTriple,
        transaction: any TransactionReadAccess,
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
        let workMeter = try requiredWorkMeter()
        var scanCache = try SPARQLScanResultCache.make(
            workMeter: workMeter
        )

        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .deduplication)
            try await leftBindings.withElement(
                at: leftIndex
            ) { leftBinding in
                let substitutedTriple = rightTriple.substitute(leftBinding)
                let signature = makeScanSignature(
                    for: substitutedTriple,
                    graphTarget: activeGraph.graphTarget
                )
                let rightBindings: SPARQLRetainedBindings
                if let cached = scanCache.value(for: signature) {
                    rightBindings = cached
                } else {
                    let rightResult = try await evaluate(
                        pattern: .basic([substitutedTriple]),
                        transaction: transaction,
                        activeGraph: activeGraph
                    )
                    combinedStats = combinedStats.merged(
                        with: rightResult.stats
                    )
                    let sharedOwnership = try (
                        consume rightResult.bindings
                    ).sharingForFanOut(
                        at: .joinCandidate
                    )
                    try scanCache.insert(
                        sharedOwnership.snapshot,
                        for: signature,
                        sourceWorkMeter: workMeter
                    )
                    rightBindings = consume sharedOwnership.retained
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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import Graph
import DatabaseEngine
import QueryIR
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateMinusPattern(
        left: ExecutionPattern,
        right: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        // W3C SPARQL 1.1, Section 18.5: MINUS
        // Keep left bindings that have no compatible solution in right.
        let leftResult = try await evaluate(
            pattern: left,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )
        let rightResult = try await evaluate(
            pattern: right,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )

        let meter = try requiredWorkMeter()
        var filtered = try SPARQLRetainedBindingBuilder.make(
            workMeter: meter,
            stage: .joinCandidate,
            expectedCount: 0
        )
        for leftIndex in 0..<leftResult.bindings.count {
            var excluded = false
            try leftResult.bindings.withElement(
                at: leftIndex
            ) { mu1 in
                for rightIndex in 0..<rightResult.bindings.count {
                    try meter.consume(at: .joinCandidate)
                    rightResult.bindings.withElement(
                        at: rightIndex
                    ) { mu2 in
                        if mu1.isMinusCompatible(with: mu2) {
                            excluded = true
                        }
                    }
                    if excluded { break }
                }
                if !excluded {
                    try filtered.appendBorrowed(
                        mu1,
                        at: .joinCandidate
                    )
                }
            }
            if let resultLimit,
               filtered.count >= resultLimit {
                break
            }
        }
        return EvaluationResult(
            bindings: filtered.finish(),
            stats: stats
        )
            .mergedStats(with: leftResult.stats)
            .mergedStats(with: rightResult.stats)
    }

}

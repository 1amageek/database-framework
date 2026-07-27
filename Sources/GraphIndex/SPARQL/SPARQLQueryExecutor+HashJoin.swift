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
    func evaluateHashJoinWithFallback(
        pattern: ExecutionTriple,
        leftBindings: borrowing SPARQLRetainedBindings,
        joinVariables: Set<String>,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression?,
        resultLimit: Int?
    ) async throws -> HashJoinEvaluation {
        let sortedJoinVars = joinVariables.sorted()

        var hashTable: [JoinKey: [Int]] = [:]
        for leftIndex in 0..<leftBindings.count {
            try requiredWorkMeter().consume(at: .joinCandidate)
            leftBindings.withElement(at: leftIndex) { binding in
                let key = JoinKey(
                    binding: binding,
                    variables: sortedJoinVars
                )
                hashTable[key, default: []].append(leftIndex)
            }
        }

        let rightMatches = try await executePattern(
            pattern,
            transaction: transaction,
            activeGraph: activeGraph,
            filter: nil,
            resultLimit: resultLimit.map {
                min(Self.hashJoinRightSideScanCap + 1, $0)
            } ?? (Self.hashJoinRightSideScanCap + 1)
        )

        if rightMatches.bindings.count > Self.hashJoinRightSideScanCap {
            var precheckStats = ExecutionStatistics()
            precheckStats.indexScans += rightMatches.stats.indexScans
            precheckStats.joinFallbackReasons.append(.hashJoinRightSideExceededCap)
            return .fallback(reason: .hashJoinRightSideExceededCap, precheckStats: precheckStats)
        }

        var results = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        for matchIndex in 0..<rightMatches.bindings.count {
            try requiredWorkMeter().consume(at: .joinCandidate)
            try await rightMatches.bindings.withElement(
                at: matchIndex
            ) { match in
                let probeKey = JoinKey(
                    binding: match,
                    variables: sortedJoinVars
                )
                guard let leftGroup = hashTable[probeKey] else {
                    return
                }

                for leftIndex in leftGroup {
                    if let resultLimit, results.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try await leftBindings.withElement(
                        at: leftIndex
                    ) { leftBinding in
                        let preparation = try results.prepareAppend(
                            merging: leftBinding,
                            with: match,
                            at: .joinCandidate
                        )
                        switch consume preparation {
                        case .incompatible:
                            return
                        case .admitted(let admission):
                            guard let merged = leftBinding.merged(
                                with: match
                            ) else {
                                throw SPARQLQueryError.executionFailed(
                                    "hash join preflight disagrees with row construction"
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

        var hashStats = ExecutionStatistics()
        hashStats.indexScans += rightMatches.stats.indexScans
        hashStats.joinStrategies.append(.hashJoin)
        return .executed(
            results: results.finish(),
            stats: hashStats
        )
    }

    /// Evaluate a join of left bindings with right pattern
}

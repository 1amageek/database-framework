import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateHashJoinWithFallback(
        pattern: ExecutionTriple,
        leftBindings: borrowing SPARQLRetainedBindings,
        joinVariables: Set<String>,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression?,
        resultLimit: Int?
    ) async throws -> HashJoinEvaluation {
        let workMeter = try requiredWorkMeter()
        if let originatingWorkMeter = leftBindings.originatingWorkMeter,
           originatingWorkMeter !== workMeter {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }

        let orderedVariableFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: joinVariables.count,
                element: String.self
            )
        let orderedVariableReservation = try workMeter.reserveIntermediate(
            bytes: orderedVariableFootprint.bytes,
            at: .joinCandidate
        )
        defer { orderedVariableReservation.release() }
        var sortedJoinVars: [String] = []
        sortedJoinVars.reserveCapacity(joinVariables.count)
        for variable in joinVariables {
            sortedJoinVars.append(variable)
        }
        sortedJoinVars.sort()

        var hashIndex = try SPARQLHashJoinIndex.make(
            workMeter: workMeter
        )
        for leftIndex in 0..<leftBindings.count {
            try workMeter.consume(at: .joinCandidate)
            try leftBindings.withElement(at: leftIndex) { binding in
                try hashIndex.insert(
                    index: leftIndex,
                    for: binding,
                    variables: sortedJoinVars
                )
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
            workMeter: workMeter,
            stage: .joinCandidate,
            expectedCount: 0
        )
        for matchIndex in 0..<rightMatches.bindings.count {
            try workMeter.consume(at: .joinCandidate)
            try await rightMatches.bindings.withElement(
                at: matchIndex
            ) { match in
                try await hashIndex.withIndices(
                    for: match,
                    variables: sortedJoinVars
                ) { leftGroup in
                    for leftIndex in leftGroup {
                        if let resultLimit, results.count >= resultLimit {
                            break
                        }
                        try workMeter.consume(at: .joinCandidate)
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
                                    try workMeter.consume(
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

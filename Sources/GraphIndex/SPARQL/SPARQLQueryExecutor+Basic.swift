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
    func evaluateBasic(
        patterns: [ExecutionTriple],
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression? = nil,
        seed: VariableBinding,
        resultLimit: Int?
    ) async throws -> EvaluationResult {
        var stats = ExecutionStatistics()

        guard !patterns.isEmpty else {
            return EvaluationResult(
                bindings: try retainSingleBinding(
                    seed,
                    at: .bindingCandidate
                ),
                stats: stats
            )
        }

        // Optimize join order using greedy algorithm
        let orderedPatterns = optimizeJoinOrder(patterns)

        var currentBindings = try retainSingleBinding(
            seed,
            at: .bindingCandidate
        )

        // Determine which variables the filter needs
        let filterVariables = filter?.variables ?? []

        // Execute patterns in optimized order
        for (index, pattern) in orderedPatterns.enumerated() {
            stats.joinOperations += 1

            // Determine which variables will be bound after this pattern
            var boundVariablesAfterPattern = seed.boundVariables
            for i in 0...index {
                boundVariablesAfterPattern.formUnion(orderedPatterns[i].variables)
            }

            // Only apply filter to this pattern if all filter variables are bound
            let canApplyFilter = filterVariables.isSubset(of: boundVariablesAfterPattern)
            let patternFilter = canApplyFilter ? filter : nil
            let outputLimit = index == orderedPatterns.index(before: orderedPatterns.endIndex)
                    && (filter == nil || canApplyFilter)
                ? resultLimit
                : nil

            var boundVariablesBeforePattern = seed.boundVariables
            if index > 0 {
                for i in 0..<index {
                    boundVariablesBeforePattern.formUnion(orderedPatterns[i].variables)
                }
            }
            let joinVars = pattern.variables.intersection(boundVariablesBeforePattern)

            let stepResult: EvaluationResult
            if currentBindings.count <= Self.nestedLoopThreshold || joinVars.isEmpty {
                stepResult = try await evaluateNestedLoopJoinStep(
                    pattern: pattern,
                    leftBindings: currentBindings,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    filter: patternFilter,
                    resultLimit: outputLimit
                )
            } else if Self.hashJoinEnabled && pattern.boundCount >= Self.hashJoinMinStaticBound {
                let hashEvaluation = try await evaluateHashJoinWithFallback(
                    pattern: pattern,
                    leftBindings: currentBindings,
                    joinVariables: joinVars,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    filter: patternFilter,
                    resultLimit: outputLimit
                )

                switch consume hashEvaluation {
                case .executed(let results, let hashStats):
                    stepResult = EvaluationResult(
                        bindings: results,
                        stats: hashStats
                    )
                case .fallback(_, let precheckStats):
                    stats = stats.merged(with: precheckStats)
                    stepResult = try await evaluateBatchedNestedLoopJoinStep(
                        pattern: pattern,
                        leftBindings: currentBindings,
                        transaction: transaction,
                        activeGraph: activeGraph,
                        filter: patternFilter,
                        resultLimit: outputLimit
                    )
                }
            } else {
                stepResult = try await evaluateBatchedNestedLoopJoinStep(
                    pattern: pattern,
                    leftBindings: currentBindings,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    filter: patternFilter,
                    resultLimit: outputLimit
                )
            }

            stats = stats.merged(with: stepResult.stats)
            currentBindings = consume stepResult.bindings
            stats.intermediateResults += currentBindings.count

            // Early termination if no results
            if currentBindings.isEmpty {
                break
            }
        }

        // Apply filter one final time if not yet applied (safety)
        if let filter = filter, !currentBindings.isEmpty {
            var finalVars = seed.boundVariables
            for pattern in orderedPatterns {
                finalVars.formUnion(pattern.variables)
            }
            if !filterVariables.isSubset(of: finalVars) {
                let filtered = try await filterBindings(
                    currentBindings,
                    expression: filter,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    resultLimit: resultLimit
                )
                currentBindings = consume filtered
            }
        }

        return EvaluationResult(
            bindings: consume currentBindings,
            stats: stats
        )
    }

}

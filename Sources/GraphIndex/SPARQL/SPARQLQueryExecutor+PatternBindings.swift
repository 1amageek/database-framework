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
    func evaluateUnionPattern(
        left: ExecutionPattern,
        right: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression?,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        // Flatten UNION iteratively so a text query cannot create a deep
        // executor call stack. The first unique branch buffer is reopened
        // for admitted appends; only a shared cache branch requires a new
        // unique header buffer.
        if resultLimit == 0 {
            return .empty(stats: stats)
        }
        var pending: [ExecutionPattern] = [right, left]
        var mergedStats = stats
        let firstBranch: ExecutionPattern
        while true {
            guard let branch = pending.popLast() else {
                return .empty(stats: mergedStats)
            }
            if case .union(let nestedLeft, let nestedRight) = branch {
                pending.append(nestedRight)
                pending.append(nestedLeft)
                mergedStats.patternsEvaluated += 1
                continue
            }
            firstBranch = branch
            break
        }

        let firstResult = try await evaluate(
            pattern: firstBranch,
            transaction: transaction,
            activeGraph: activeGraph,
            filter: filter,
            seed: seed,
            resultLimit: resultLimit
        )
        let firstCount = firstResult.bindings.count
        try requiredWorkMeter().consume(
            UInt64(firstCount),
            at: .bindingCandidate
        )
        mergedStats = mergedStats.merged(with: firstResult.stats)
        var bindings = try SPARQLRetainedBindingBuilder.resuming(
            consume firstResult.bindings,
            workMeter: try requiredWorkMeter(),
            stage: .bindingCandidate
        )

        while let branch = pending.popLast() {
            if case .union(let nestedLeft, let nestedRight) = branch {
                pending.append(nestedRight)
                pending.append(nestedLeft)
                mergedStats.patternsEvaluated += 1
                continue
            }

            let remainingLimit = resultLimit.map {
                max(0, $0 - bindings.count)
            }
            if remainingLimit == 0 { break }
            let branchResult = try await evaluate(
                pattern: branch,
                transaction: transaction,
                activeGraph: activeGraph,
                filter: filter,
                seed: seed,
                resultLimit: remainingLimit
            )
            let branchCount = branchResult.bindings.count
            try requiredWorkMeter().consume(
                UInt64(branchCount),
                at: .bindingCandidate
            )
            mergedStats = mergedStats.merged(with: branchResult.stats)
            try bindings.appendBorrowed(
                contentsOf: branchResult.bindings,
                limit: remainingLimit,
                at: .bindingCandidate
            )
            if let resultLimit, bindings.count >= resultLimit {
                break
            }
        }
        return EvaluationResult(
            bindings: bindings.finish(),
            stats: mergedStats
        )
    }

}

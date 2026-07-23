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
    func applyOffsetLimit(
        _ evalResult: consuming EvaluationResult,
        offset: Int,
        limit: Int?
    ) throws -> ([VariableBinding], ExecutionStatistics) {
        guard offset >= 0, limit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let ownedResult = consume evalResult
        let stats = ownedResult.stats
        let visibleCount: Int
        if offset < ownedResult.bindings.count {
            let availableCount = ownedResult.bindings.count - offset
            visibleCount = min(limit ?? availableCount, availableCount)
        } else {
            visibleCount = 0
        }
        try requiredWorkMeter().consume(
            UInt64(visibleCount),
            at: .projection
        )
        let bindings = (consume ownedResult.bindings).promoteToOutput(
            offset: offset,
            limit: limit
        )
        return (bindings, stats)
    }

    func evaluationResultLimit(
        offset: Int,
        limit: Int?
    ) throws -> Int? {
        guard offset >= 0, limit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        guard let limit else { return nil }
        guard limit > 0 else { return 0 }
        let (result, overflow) = offset.addingReportingOverflow(limit)
        guard !overflow else {
            throw SPARQLQueryError.invalidPagination
        }
        return result
    }

    func retainSingleBinding(
        _ binding: consuming VariableBinding,
        at stage: DatabaseWorkStage
    ) throws -> SPARQLRetainedBindings {
        var builder = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: stage,
            expectedCount: 1
        )
        try builder.append(binding, at: stage)
        return builder.finish()
    }

    func filterBindings(
        _ source: borrowing SPARQLRetainedBindings,
        expression: FilterExpression,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        resultLimit: Int?
    ) async throws -> SPARQLRetainedBindings {
        var builder = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .filterEvaluation,
            expectedCount: 0
        )
        for index in 0..<source.count {
            try requiredWorkMeter().consume(at: .filterEvaluation)
            try await source.withElement(at: index) { binding in
                if try await evaluateFilterExpression(
                    expression,
                    binding: binding,
                    transaction: transaction,
                    activeGraph: activeGraph
                ) {
                    try builder.appendBorrowed(
                        binding,
                        at: .filterEvaluation
                    )
                }
            }
            if let resultLimit, builder.count >= resultLimit {
                break
            }
        }
        return builder.finish()
    }
}

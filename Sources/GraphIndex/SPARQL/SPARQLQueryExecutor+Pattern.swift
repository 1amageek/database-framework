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
    // MARK: - Pattern Evaluation

    /// Evaluate a graph pattern recursively
    func evaluate(
        pattern: ExecutionPattern,
        transaction: any TransactionAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression? = nil,
        seed: VariableBinding = VariableBinding(),
        resultLimit: Int? = nil
    ) async throws -> EvaluationResult {
        var stats = ExecutionStatistics()
        stats.patternsEvaluated = 1

        switch pattern {
        case .basic(let patterns):
            let basicResult = try await evaluateBasic(
                patterns: patterns,
                transaction: transaction,
                activeGraph: activeGraph,
                filter: filter,
                seed: seed,
                resultLimit: resultLimit
            )
            stats.indexScans = basicResult.stats.indexScans
            stats.joinOperations = basicResult.stats.joinOperations
            stats.intermediateResults = basicResult.stats.intermediateResults
            stats.optionalMisses = basicResult.stats.optionalMisses
            stats.joinStrategies = basicResult.stats.joinStrategies
            stats.joinFallbackReasons = basicResult.stats
                .joinFallbackReasons
            return EvaluationResult(
                bindings: consume basicResult.bindings,
                stats: stats
            )

        case .join(let left, let right):
            stats.joinOperations = 1
            let leftResult = try await evaluate(
                pattern: left,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed
            )
            let joinResult = try await evaluateJoin(
                leftBindings: leftResult.bindings,
                rightPattern: right,
                transaction: transaction,
                activeGraph: activeGraph,
                resultLimit: resultLimit
            )
            return EvaluationResult(
                bindings: consume joinResult.bindings,
                stats: stats
            )
                .mergedStats(with: leftResult.stats)
                .mergedStats(with: joinResult.stats)

        case .optional(let left, let right):
            let leftResult = try await evaluate(
                pattern: left,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed,
                resultLimit: resultLimit
            )
            let optionalResult = try await evaluateOptional(
                leftBindings: leftResult.bindings,
                rightPattern: right,
                transaction: transaction,
                activeGraph: activeGraph,
                resultLimit: resultLimit
            )
            let bindings: SPARQLRetainedBindings
            if let filter = filter {
                bindings = try await filterBindings(
                    optionalResult.bindings,
                    expression: filter,
                    transaction: transaction,
                    activeGraph: activeGraph,
                    resultLimit: resultLimit
                )
            } else {
                bindings = consume optionalResult.bindings
            }
            return EvaluationResult(bindings: bindings, stats: stats)
                .mergedStats(with: leftResult.stats)
                .mergedStats(with: optionalResult.stats)

        case .union(let left, let right):
            return try await evaluateUnionPattern(
                left: left,
                right: right,
                transaction: transaction,
                activeGraph: activeGraph,
                filter: filter,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .filter(let innerPattern, let expression):
            let combinedFilter: FilterExpression
            if let existingFilter = filter {
                combinedFilter = .and(existingFilter, expression)
            } else {
                combinedFilter = expression
            }
            let innerResult = try await evaluate(
                pattern: innerPattern,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed
            )
            let filteredBindings = try await filterBindings(
                innerResult.bindings,
                expression: combinedFilter,
                transaction: transaction,
                activeGraph: activeGraph,
                resultLimit: resultLimit
            )
            return EvaluationResult(bindings: filteredBindings, stats: stats)
                .mergedStats(with: innerResult.stats)

        case .extend(let innerPattern, let variable, let expression):
            return try await evaluateExtendPattern(
                innerPattern: innerPattern,
                variable: variable,
                expression: expression,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .values(let table):
            return try await evaluateValuesPattern(
                table,
                transaction: transaction,
                activeGraph: activeGraph,
                filter: filter,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .minus(let left, let right):
            return try await evaluateMinusPattern(
                left: left,
                right: right,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .groupBy(let sourcePattern, let grouping, let aggs, let havingExpr):
            return try await evaluateGroupPattern(
                sourcePattern: sourcePattern,
                grouping: grouping,
                aggregates: aggs,
                having: havingExpr,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .graph(let selector, let innerPattern):
            return try await evaluateGraphPattern(
                selector: selector,
                innerPattern: innerPattern,
                transaction: transaction,
                filter: filter,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .propertyPath(let subject, let path, let object):
            return try await evaluatePropertyPathPattern(
                subject: subject,
                path: path,
                object: object,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: consume seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .lateral(let left, let right):
            return try await evaluateLateralPattern(
                left: left,
                right: right,
                transaction: transaction,
                activeGraph: activeGraph,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        case .subquery(let plan):
            let result = try await evaluateSubquery(
                plan,
                transaction: transaction,
                activeGraph: activeGraph,
                outerSeed: seed,
                filter: filter
            )
            return EvaluationResult(
                bindings: consume result.bindings,
                stats: stats
            )
                .mergedStats(with: result.stats)
        }
    }

}

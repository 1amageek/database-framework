import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluatePropertyPathPattern(
        subject: ExecutionTerm,
        path: ExecutionPropertyPath,
        object: ExecutionTerm,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        seed: consuming VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        let pathResult = try await evaluateExecutionPropertyPath(
            subject: subject,
            path: path,
            object: object,
            seed: consume seed,
            resultLimit: resultLimit,
            transaction: transaction,
            activeGraph: activeGraph,
            config: propertyPathConfiguration
        )
        return EvaluationResult(
            bindings: consume pathResult.bindings,
            stats: stats
        )
            .mergedStats(with: pathResult.stats)
    }

}

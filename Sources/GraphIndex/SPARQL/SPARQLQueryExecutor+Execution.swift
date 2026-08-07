import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    // MARK: - Execution

    /// Execute a graph pattern and return bindings
    ///
    /// - Parameters:
    ///   - pattern: The graph pattern to evaluate
    ///   - limit: Maximum number of results (nil for unlimited)
    ///   - offset: Number of results to skip
    /// - Returns: Tuple of bindings and execution statistics
    public func execute(
        pattern: ExecutionPattern,
        limit: Int?,
        offset: Int,
        workMeter: DatabaseWorkMeter
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let resultLimit = try evaluationResultLimit(
            offset: offset,
            limit: limit
        )
        let executor = try requestScoped(by: workMeter)
        return try await StorageTransactionExecutor(engine: database)
            .withTransaction(
                configuration: .default,
                clock: monotonicClock
            ) { transaction in
            let attemptExecutor = try executor.transactionAttemptScoped()
            let evaluated = try await attemptExecutor.evaluate(
                pattern: pattern,
                transaction: transaction,
                activeGraph: attemptExecutor.initialActiveGraph,
                resultLimit: resultLimit
            )
            let evalResult = attemptExecutor
                .includingNestedExpressionStatistics(consume evaluated)
            return try attemptExecutor.applyOffsetLimit(
                consume evalResult,
                offset: offset,
                limit: limit
            )
        }
    }

    /// Evaluate a pattern inside a caller-supplied transaction.
    ///
    /// Used by federated queries so every source sees the same read version and
    /// snapshot. `offset`/`limit` behave identically to `execute(pattern:limit:offset:)`.
    public func executeInTransaction(
        pattern: ExecutionPattern,
        transaction: any TransactionAccess,
        limit: Int?,
        offset: Int,
        workMeter: DatabaseWorkMeter
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let resultLimit = try evaluationResultLimit(
            offset: offset,
            limit: limit
        )
        let executor = try requestScoped(by: workMeter)
            .transactionAttemptScoped()
        let evaluated = try await executor.evaluate(
            pattern: pattern,
            transaction: transaction,
            activeGraph: executor.initialActiveGraph,
            resultLimit: resultLimit
        )
        let evalResult = executor.includingNestedExpressionStatistics(
            consume evaluated
        )

        return try executor.applyOffsetLimit(
            consume evalResult,
            offset: offset,
            limit: limit
        )
    }

    func evaluateOrderedInTransaction(
        plan: SPARQLOrderedSolutionPlan,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> EvaluationResult {
        let executor = try scoped(to: plan.dataset)
            .requestScoped(by: workMeter)
            .transactionAttemptScoped()
        let evaluated = try await executor.evaluateOrderedSolutionPlan(
            plan,
            transaction: transaction,
            activeGraph: executor.initialActiveGraph,
            seed: VariableBinding()
        )
        return executor.includingNestedExpressionStatistics(
            consume evaluated
        )
    }

    func evaluateSlicedSolutionFormInTransaction(
        plan: SPARQLSolutionFormExecutionPlan,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> EvaluationResult {
        let executor = try scoped(to: plan.ordered.dataset)
            .requestScoped(by: workMeter)
            .transactionAttemptScoped()
        let evaluated = try await executor.evaluateOrderedSolutionPlan(
            plan.ordered,
            transaction: transaction,
            activeGraph: executor.initialActiveGraph,
            seed: VariableBinding()
        )
        let result = executor.includingNestedExpressionStatistics(
            consume evaluated
        )
        let stats = result.stats
        let bindings = try executor.applyRetainedSlice(
            consume result.bindings,
            slice: plan.slice
        )
        return EvaluationResult(
            bindings: consume bindings,
            stats: stats
        )
    }

    package func execute(
        selectPlan: SPARQLSelectExecutionPlan,
        workMeter: DatabaseWorkMeter
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let executor = try scoped(to: selectPlan.ordered.dataset)
            .requestScoped(by: workMeter)
        return try await StorageTransactionExecutor(engine: database)
            .withTransaction(
                configuration: .default,
                clock: monotonicClock
            ) { transaction in
            let attemptExecutor = try executor.transactionAttemptScoped()
            let evaluated = try await attemptExecutor.evaluateSelectPlan(
                selectPlan,
                transaction: transaction,
                activeGraph: attemptExecutor.initialActiveGraph,
                seed: VariableBinding()
            )
            let result = attemptExecutor.includingNestedExpressionStatistics(
                consume evaluated
            )
            let stats = result.stats
            let bindings = (consume result.bindings).promoteToOutput()
            return (bindings, stats)
        }
    }

    package func executeInTransaction(
        selectPlan: SPARQLSelectExecutionPlan,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let executor = try scoped(to: selectPlan.ordered.dataset)
            .requestScoped(by: workMeter)
            .transactionAttemptScoped()
        let evaluated = try await executor.evaluateSelectPlan(
            selectPlan,
            transaction: transaction,
            activeGraph: executor.initialActiveGraph,
            seed: VariableBinding()
        )
        let result = executor.includingNestedExpressionStatistics(
            consume evaluated
        )
        let stats = result.stats
        let bindings = (consume result.bindings).promoteToOutput()
        return (bindings, stats)
    }

}

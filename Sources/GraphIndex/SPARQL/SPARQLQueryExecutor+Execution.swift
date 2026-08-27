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
        guard let database else {
            throw SPARQLQueryError.executionFailed(
                "A caller-owned transaction is required for this executor"
            )
        }
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
        transaction: any TransactionReadAccess,
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

    /// Executes the complete SELECT modifier pipeline and keeps its result
    /// owned by the request meter across the executor boundary.
    package func executeRetainedProjectedInTransaction(
        pattern: ExecutionPattern,
        transaction: any TransactionReadAccess,
        orderBy: [BindingSortKey],
        projectionVariables: [String],
        projectionIsIdentity: Bool,
        duplicatePolicy: SPARQLDuplicatePolicy,
        offset: Int,
        limit: Int?,
        workMeter: DatabaseWorkMeter
    ) async throws -> SPARQLRetainedResult {
        guard offset >= 0, limit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let slice = try SPARQLSlice(
            offset: UInt64(offset),
            limit: limit.map { UInt64($0) }
        )
        let resultLimit = orderBy.isEmpty && duplicatePolicy == .preserve
            ? try evaluationResultLimit(offset: offset, limit: limit)
            : nil
        let executor = try requestScoped(by: workMeter)
            .transactionAttemptScoped()
        let evaluated = try await executor.evaluate(
            pattern: pattern,
            transaction: transaction,
            activeGraph: executor.initialActiveGraph,
            resultLimit: resultLimit
        )
        let result = executor.includingNestedExpressionStatistics(
            consume evaluated
        )
        let statistics = result.stats
        let ordered = try BindingSorter.sort(
            consume result.bindings,
            by: orderBy,
            workMeter: workMeter
        )
        let selected = try executor.applyProjectionDuplicateAndSlice(
            consume ordered,
            projectionVariables: projectionVariables,
            projectionIsIdentity: projectionIsIdentity,
            duplicatePolicy: duplicatePolicy,
            slice: slice
        )
        let bindings = try (consume selected).moveToSharedSnapshot(
            at: .resultMaterialization
        )
        let reachedLimit = limit.map { bindings.count >= $0 } ?? false
        return SPARQLRetainedResult(
            bindings: bindings,
            workMeter: workMeter,
            projectedVariables: projectionVariables,
            isComplete: !reachedLimit,
            limitReason: reachedLimit ? .explicitLimit : nil,
            statistics: statistics
        )
    }

    func evaluateOrderedInTransaction(
        plan: SPARQLOrderedSolutionPlan,
        transaction: any TransactionReadAccess,
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
        transaction: any TransactionReadAccess,
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
    ) async throws -> SPARQLResult {
        let executor = try scoped(to: selectPlan.ordered.dataset)
            .requestScoped(by: workMeter)
        guard let database else {
            throw SPARQLQueryError.executionFailed(
                "A caller-owned transaction is required for this executor"
            )
        }
        return try await StorageTransactionExecutor(engine: database)
            .withTransaction(
                configuration: .default,
                clock: monotonicClock
            ) { transaction in
            let retained = try await executor.executeRetainedInTransaction(
                selectPlan: selectPlan,
                transaction: transaction,
                workMeter: workMeter
            )
            return retained.promoteToResult()
        }
    }

    /// Executes a SELECT plan for an intermediate database operator without
    /// promoting its binding buffer outside request accounting.
    package func executeRetainedInTransaction(
        selectPlan: SPARQLSelectExecutionPlan,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> SPARQLRetainedResult {
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
        let statistics = result.stats
        let bindings = try (consume result.bindings).moveToSharedSnapshot(
            at: .resultMaterialization
        )
        let reachedLimit = selectPlan.slice.limit.map {
            bindings.count >= $0
        } ?? false
        return SPARQLRetainedResult(
            bindings: bindings,
            workMeter: workMeter,
            projectedVariables: selectPlan.projectionVariables,
            isComplete: !reachedLimit,
            limitReason: reachedLimit ? .explicitLimit : nil,
            statistics: statistics
        )
    }

}

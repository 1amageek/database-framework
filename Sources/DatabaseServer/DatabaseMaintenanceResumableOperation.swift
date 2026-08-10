import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public struct DatabaseMaintenanceResumableOperation: DatabaseResumableOperation {
    private struct DurableSlice: Sendable {
        let checkpoint: DatabaseMaintenanceJobCheckpoint
        let completedWorkUnits: UInt64
    }

    public static func job()
        throws(DatabaseWireError)
        -> JobOperation<
            MaintenanceExecuteOperation.Request,
            MaintenanceExecuteOperation.Response
        > {
        JobOperations.maintenance
    }

    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.runtimeLimits = runtimeLimits
    }

    public func compile(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<
        DatabaseMaintenanceJobPlan,
        DatabaseMaintenanceJobState
    > {
        try validateRequest(request)
        let plan: DatabaseMaintenanceJobPlan
        let state: DatabaseMaintenanceJobState

        switch request.invocation {
        case .runMigrations(let requestedTarget):
            let executor = try context.operationContext.requireBaseExecutor()
            let targetVersion = requestedTarget
                ?? executor.schema.version
            let status = try await executor.migrationStatus(
                targetVersion: targetVersion,
                transaction: context.transaction.storageAccess
            )
            let maximumStagesPerSlice = min(
                context.maximumSliceWorkUnits,
                request.budget.maximumWorkUnits,
                runtimeLimits.maximumWorkUnits
            )
            guard maximumStagesPerSlice > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                    "Migration has no executable work budget"
                )
            }
            plan = DatabaseMaintenanceJobPlan(
                invocation: .migrations(
                    targetVersion: targetVersion,
                    totalStageCount: UInt64(
                        status.pendingMigrationIdentifiers.count
                    ),
                    maximumStagesPerSlice: maximumStagesPerSlice
                )
            )
            state = DatabaseMaintenanceJobState(value: .migrations)
        case .rebuildIndex(
            let entity,
            let index,
            let partitions,
            let batchSize
        ):
            let executor = try context.operationContext.requireBaseExecutor()
            guard batchSize > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidBatchSize(batchSize)
            }
            let runtime = executor.makeIndexMaintenanceRuntime()
            let canonicalPartitions = try runtime.canonicalPartitions(
                entity: entity,
                index: index,
                partitions: partitions
            )
            let effectiveWorkUnits = min(
                context.maximumSliceWorkUnits,
                request.budget.maximumWorkUnits,
                UInt64(batchSize),
                runtimeLimits.maximumWorkUnits,
                DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
            )
            guard effectiveWorkUnits > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidBatchSize(batchSize)
            }
            plan = DatabaseMaintenanceJobPlan(
                invocation: .indexRebuild(
                    entity: entity,
                    index: index,
                    partitions: canonicalPartitions,
                    schemaVersion: context.operationContext.executor.schema.version,
                    maximumWorkUnits: effectiveWorkUnits
                )
            )
            state = DatabaseMaintenanceJobState(
                value: .indexRebuild(started: false)
            )
        case .compact:
            _ = try context.operationContext.requireBaseExecutor()
            guard let compaction = context.transaction.storageAccess.compaction
            else {
                throw DatabaseMaintenanceRuntimeError.compactionUnavailable
            }
            let compactionLimits = compaction.limits
            let effectiveWorkUnits = min(
                context.maximumSliceWorkUnits,
                request.budget.maximumWorkUnits,
                runtimeLimits.maximumWorkUnits,
                compactionLimits.maximumWorkUnitsPerSlice
            )
            guard effectiveWorkUnits > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                    "Compaction has no executable work budget"
                )
            }
            plan = DatabaseMaintenanceJobPlan(
                invocation: .compaction(
                    maximumWorkUnits: effectiveWorkUnits
                )
            )
            state = DatabaseMaintenanceJobState(
                value: .compaction(continuation: nil)
            )
        case .migrationStatus, .indexStatus:
            throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                "The maintenance invocation is not resumable"
            )
        }
        return DatabasePreparedResumableJob(
            plan: plan,
            initialState: state,
            sliceTimeoutMilliseconds: request.budget.timeoutMilliseconds
        )
    }

    public func commitModel(
        for plan: DatabaseMaintenanceJobPlan
    ) -> DatabaseResumableOperationCommitModel {
        _ = plan
        return .operationCheckpointed
    }

    public func runCheckpointedSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        switch (plan.invocation, state.value) {
        case let (
            .migrations(
            targetVersion,
            totalStageCount,
            maximumStagesPerSlice
            ),
            .migrations
        ):
            return try await runMigrationSlice(
                targetVersion: targetVersion,
                totalStageCount: totalStageCount,
                maximumStagesPerSlice: maximumStagesPerSlice,
                maximumWorkUnits: maximumWorkUnits,
                context: context
            )
        case (.indexRebuild, .indexRebuild),
             (.compaction, .compaction):
            return try await runDurablyCheckpointedMaintenanceSlice(
                plan: plan,
                state: state,
                maximumWorkUnits: maximumWorkUnits,
                context: context
            )
        case (.migrations, .indexRebuild),
             (.migrations, .compaction),
             (.indexRebuild, .migrations),
             (.indexRebuild, .compaction),
             (.compaction, .migrations),
             (.compaction, .indexRebuild):
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }

    private func runMigrationSlice(
        targetVersion: Schema.Version,
        totalStageCount: UInt64,
        maximumStagesPerSlice: UInt64,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        guard context.operationContext.executor.schema.version
                == targetVersion else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
        let effectiveWorkUnits = min(
            maximumWorkUnits,
            maximumStagesPerSlice,
            runtimeLimits.maximumWorkUnits
        )
        guard effectiveWorkUnits > 0 else {
            throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                "Migration has no executable work budget"
            )
        }
        let executor = try context.operationContext.requireBaseExecutor()
        let result = try await executor.runMigrations(
            targetVersion: targetVersion,
            maximumStageCount: effectiveWorkUnits
        )
        guard result.completedStageCount <= effectiveWorkUnits else {
            throw DatabaseJobRuntimeError.sliceExceededBudget(
                actual: result.completedStageCount,
                maximum: effectiveWorkUnits
            )
        }
        let status = try await executor.migrationStatus(
            targetVersion: targetVersion
        )
        let remaining = UInt64(status.pendingMigrationIdentifiers.count)
        guard remaining <= totalStageCount else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
        if result.isComplete {
            return .complete(
                completedWorkUnits: result.completedStageCount,
                result: .execution(
                    MaintenanceExecuteOperation.ExecutionResult(
                        kind: .migrations,
                        completedWorkUnits: totalStageCount - remaining,
                        isComplete: true
                    )
                )
            )
        }
        return .incomplete(
            completedWorkUnits: result.completedStageCount,
            state: DatabaseMaintenanceJobState(value: .migrations)
        )
    }

    private func runDurablyCheckpointedMaintenanceSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        let executor = try context.operationContext.requireBaseExecutor()
        let durable = try await executor.withActiveDataTransaction(
            requiredAccess: .administer,
            configuration: .batch
        ) { transaction in
            let stored = try await executor.maintenanceCheckpoint(
                for: context.jobID,
                transaction: transaction.storageAccess
            )
            let checkpoint: DatabaseMaintenanceJobCheckpoint?
            if let stored {
                do {
                    checkpoint = try PersistentJobPayloadStorage.decode(
                        DatabaseMaintenanceJobCheckpoint.self,
                        from: stored,
                        limits: context.operationContext.wireLimits
                    )
                } catch {
                    throw DatabaseJobRuntimeError.corruptedState
                }
            } else {
                checkpoint = nil
            }

            let controlWorkUnits = context.completedWorkUnitsBeforeSlice
            if let checkpoint {
                guard checkpoint.cumulativeWorkUnits >= controlWorkUnits else {
                    throw DatabaseJobRuntimeError.corruptedState
                }
                let replayedWorkUnits = checkpoint.cumulativeWorkUnits
                    - controlWorkUnits
                guard replayedWorkUnits <= maximumWorkUnits else {
                    throw DatabaseJobRuntimeError.corruptedState
                }
                if replayedWorkUnits > 0 || checkpoint.isComplete {
                    return DurableSlice(
                        checkpoint: checkpoint,
                        completedWorkUnits: replayedWorkUnits
                    )
                }
                guard checkpoint.state == state else {
                    throw DatabaseJobRuntimeError.corruptedState
                }
            } else if controlWorkUnits != 0 {
                throw DatabaseJobRuntimeError.corruptedState
            }

            let currentState = checkpoint?.state ?? state
            let currentWorkUnits = checkpoint?.cumulativeWorkUnits
                ?? controlWorkUnits
            let next = try await executeMaintenanceSlice(
                plan: plan,
                state: currentState,
                maximumWorkUnits: maximumWorkUnits,
                jobID: context.jobID,
                cumulativeWorkUnitsBeforeSlice: currentWorkUnits,
                transaction: transaction.storageAccess,
                operationContext: context.operationContext
            )
            let encoded = try PersistentJobPayloadStorage.encode(
                next.checkpoint,
                limits: context.operationContext.wireLimits
            )
            try executor.storeMaintenanceCheckpoint(
                encoded,
                for: context.jobID,
                transaction: transaction.storageAccess
            )
            return next
        }

        if durable.checkpoint.isComplete {
            return .complete(
                completedWorkUnits: durable.completedWorkUnits,
                result: .execution(
                    MaintenanceExecuteOperation.ExecutionResult(
                        kind: resultKind(for: plan),
                        completedWorkUnits:
                            durable.checkpoint.cumulativeWorkUnits,
                        isComplete: true
                    )
                )
            )
        }
        return .incomplete(
            completedWorkUnits: durable.completedWorkUnits,
            state: durable.checkpoint.state
        )
    }

    private func executeMaintenanceSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        jobID: DatabaseTypes.UUID,
        cumulativeWorkUnitsBeforeSlice: UInt64,
        transaction: any TransactionAccess,
        operationContext: DatabaseOperationContext
    ) async throws -> DurableSlice {
        switch (plan.invocation, state.value) {
        case let (
            .indexRebuild(
                entity,
                index,
                partitions,
                schemaVersion,
                planWorkUnits
            ),
            .indexRebuild(started)
        ):
            let executor = try operationContext.requireBaseExecutor()
            guard operationContext.executor.schema.version == schemaVersion else {
                throw DatabaseIndexRebuildError.corruptedRebuildState
            }
            let effectiveWorkUnits = min(
                maximumWorkUnits,
                planWorkUnits,
                runtimeLimits.maximumWorkUnits,
                DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
            )
            guard effectiveWorkUnits > 0 else {
                throw DatabaseIndexRebuildError.invalidWorkLimit(
                    effectiveWorkUnits
                )
            }
            let slice = try await executor.makeIndexMaintenanceRuntime()
                .runRebuildSlice(
                    entity: entity,
                    index: index,
                    partitions: partitions,
                    generation: jobID,
                    mode: started ? .resume : .start,
                    maximumWorkUnits: effectiveWorkUnits,
                    transaction: transaction
                )
            guard slice.completedWorkUnits <= effectiveWorkUnits else {
                throw DatabaseJobRuntimeError.sliceExceededBudget(
                    actual: slice.completedWorkUnits,
                    maximum: effectiveWorkUnits
                )
            }
            let cumulative = try cumulativeWorkUnits(
                before: cumulativeWorkUnitsBeforeSlice,
                completed: slice.completedWorkUnits
            )
            let checkpoint = DatabaseMaintenanceJobCheckpoint(
                state: DatabaseMaintenanceJobState(
                    value: .indexRebuild(started: true)
                ),
                cumulativeWorkUnits: cumulative,
                isComplete: slice.isComplete
            )
            return DurableSlice(
                checkpoint: checkpoint,
                completedWorkUnits: slice.completedWorkUnits
            )

        case let (
            .compaction(planWorkUnits),
            .compaction(backendContinuation)
        ):
            guard let compaction = transaction.compaction else {
                throw DatabaseMaintenanceRuntimeError.compactionUnavailable
            }
            let effectiveWorkUnits = min(
                maximumWorkUnits,
                planWorkUnits,
                runtimeLimits.maximumWorkUnits,
                compaction.limits.maximumWorkUnitsPerSlice
            )
            guard effectiveWorkUnits > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                    "Compaction has no executable work budget"
                )
            }
            let result = try await compaction.stageSlice(
                maximumWorkUnits: effectiveWorkUnits,
                continuation: backendContinuation.map {
                    StorageCompactionContinuation(bytes: $0)
                }
            )
            try validateCompactionResult(
                result,
                maximumWorkUnits: effectiveWorkUnits
            )
            let cumulative = try cumulativeWorkUnits(
                before: cumulativeWorkUnitsBeforeSlice,
                completed: result.workUnitsConsumed
            )
            let checkpoint = DatabaseMaintenanceJobCheckpoint(
                state: DatabaseMaintenanceJobState(
                    value: .compaction(
                        continuation: result.continuation?.bytes
                    )
                ),
                cumulativeWorkUnits: cumulative,
                isComplete: result.isComplete
            )
            return DurableSlice(
                checkpoint: checkpoint,
                completedWorkUnits: result.workUnitsConsumed
            )

        case (.migrations, .migrations):
            throw DatabaseJobRuntimeError.commitModelMismatch
        case (.migrations, .indexRebuild),
             (.migrations, .compaction),
             (.indexRebuild, .migrations),
             (.indexRebuild, .compaction),
             (.compaction, .migrations),
             (.compaction, .indexRebuild):
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }

    private func resultKind(
        for plan: DatabaseMaintenanceJobPlan
    ) -> MaintenanceExecuteOperation.ExecutionKind {
        switch plan.invocation {
        case .migrations:
            return .migrations
        case .indexRebuild:
            return .indexRebuild
        case .compaction:
            return .compaction
        }
    }

    public func runSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }

    public func prepareUnsuccessfulOutcomeCommit(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws {
        switch (plan.invocation, state.value) {
        case (.migrations, .migrations),
             (.compaction, .compaction):
            return
        case let (
            .indexRebuild(entity, index, partitions, _, _),
            .indexRebuild(started)
        ):
            let executor = try context.operationContext.requireBaseExecutor()
            let detail: String
            switch outcome {
            case .failed(let error):
                detail = "\(error.code): \(error.message)"
            case .cancelled:
                detail = "cancelled"
            }
            try await executor.withActiveDataTransaction(
                requiredAccess: .administer,
                configuration: .batch
            ) { transaction in
                let stored = try await executor.maintenanceCheckpoint(
                    for: context.jobID,
                    transaction: transaction.storageAccess
                )
                let checkpoint: DatabaseMaintenanceJobCheckpoint?
                if let stored {
                    do {
                        checkpoint = try PersistentJobPayloadStorage.decode(
                            DatabaseMaintenanceJobCheckpoint.self,
                            from: stored,
                            limits: context.operationContext.wireLimits
                        )
                    } catch {
                        throw DatabaseJobRuntimeError.corruptedState
                    }
                } else {
                    checkpoint = nil
                }
                let rebuildStarted: Bool
                if let checkpoint {
                    guard case .indexRebuild(let checkpointStarted) =
                            checkpoint.state.value,
                          !checkpoint.isComplete else {
                        throw DatabaseJobRuntimeError.corruptedState
                    }
                    rebuildStarted = checkpointStarted
                } else {
                    rebuildStarted = started
                }
                guard rebuildStarted else { return }
                try await executor.makeIndexMaintenanceRuntime().markFailed(
                    entity: entity,
                    index: index,
                    partitions: partitions,
                    generation: context.jobID,
                    detail: detail,
                    transaction: transaction.storageAccess
                )
            }
        case (.migrations, .indexRebuild),
             (.migrations, .compaction),
             (.indexRebuild, .migrations),
             (.indexRebuild, .compaction),
             (.compaction, .migrations),
             (.compaction, .indexRebuild):
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }

    public func applyUnsuccessfulOutcome(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        switch (plan.invocation, state.value) {
        case (.migrations, .migrations):
            return
        case (
            .indexRebuild(_, _, _, _, _),
            .indexRebuild(started: _)
        ):
            _ = outcome
            _ = context
            return
        case (.compaction, .compaction):
            return
        case (.migrations, .indexRebuild),
             (.migrations, .compaction),
             (.indexRebuild, .migrations),
             (.indexRebuild, .compaction),
             (.compaction, .migrations),
             (.compaction, .indexRebuild):
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }

    private func validateRequest(
        _ request: MaintenanceExecuteOperation.Request
    ) throws {
        try runtimeLimits.validate(request.budget)
        guard request.continuation == nil else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }

    private func cumulativeWorkUnits(
        before: UInt64,
        completed: UInt64
    ) throws -> UInt64 {
        let (value, overflow) = before.addingReportingOverflow(completed)
        guard !overflow else {
            throw DatabaseJobRuntimeError.workUnitOverflow
        }
        return value
    }

    private func validateCompactionResult(
        _ result: StorageCompactionResult,
        maximumWorkUnits: UInt64
    ) throws {
        guard result.workUnitsConsumed <= maximumWorkUnits else {
            throw DatabaseJobRuntimeError.sliceExceededBudget(
                actual: result.workUnitsConsumed,
                maximum: maximumWorkUnits
            )
        }
        guard (result.remainingWorkUnits == 0)
                == (result.continuation == nil) else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
    }
}

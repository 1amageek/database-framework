import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

public struct DatabaseMaintenanceResumableOperation: DatabaseResumableOperation {
    public typealias Job = DatabaseMaintenanceJobDescriptor
    public typealias Plan = DatabaseMaintenanceJobPlan
    public typealias State = DatabaseMaintenanceJobState

    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits

    public init(
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
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
            let targetVersion = requestedTarget
                ?? context.operationContext.container.schema.version
            let status = try await context.operationContext.container
                .migrationStatus(
                    targetVersion: targetVersion,
                    transaction: context.databaseTransaction.storageAccess
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
            guard batchSize > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidBatchSize(batchSize)
            }
            let canonicalPartitions = try await DatabaseIndexMaintenanceRuntime(
                container: context.operationContext.container,
                wireLimits: wireLimits
            ).prepareResources(
                entity: entity,
                index: index,
                partitions: partitions,
                transaction: context.databaseTransaction.storageAccess
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
                    schemaVersion: context.operationContext.container.schema.version,
                    maximumWorkUnits: effectiveWorkUnits
                )
            )
            state = DatabaseMaintenanceJobState(
                value: .indexRebuild(started: false)
            )
        case .compact:
            guard let compaction = context.databaseTransaction.storageAccess
                as? any DatabaseStorageCompactionTransaction else {
                throw DatabaseMaintenanceRuntimeError.compactionUnavailable
            }
            let effectiveWorkUnits = min(
                context.maximumSliceWorkUnits,
                request.budget.maximumWorkUnits,
                runtimeLimits.maximumWorkUnits,
                compaction.compactionLimits.maximumWorkUnitsPerSlice
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
        switch plan.invocation {
        case .migrations:
            return .operationCheckpointed
        case .indexRebuild, .compaction:
            return .atomicWithJobState
        }
    }

    public func runCheckpointedSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        guard case let .migrations(
            targetVersion,
            totalStageCount,
            maximumStagesPerSlice
        ) = plan.invocation,
        case .migrations = state.value else {
            throw DatabaseJobRuntimeError.commitModelMismatch
        }
        guard context.operationContext.container.schema.version
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
        let result = try await context.operationContext.container.runMigrations(
            targetVersion: targetVersion,
            maximumStageCount: effectiveWorkUnits
        )
        guard result.completedStageCount <= effectiveWorkUnits else {
            throw DatabaseJobRuntimeError.sliceExceededBudget(
                actual: result.completedStageCount,
                maximum: effectiveWorkUnits
            )
        }
        let status = try await context.operationContext.container
            .migrationStatus(targetVersion: targetVersion)
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

    public func runSlice(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<
        DatabaseMaintenanceJobState,
        MaintenanceExecuteOperation.Response
    > {
        switch (plan.invocation, state.value) {
        case (.migrations, .migrations):
            throw DatabaseJobRuntimeError.commitModelMismatch

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
            guard context.operationContext.container.schema.version
                    == schemaVersion else {
                throw DatabaseIndexRebuildError.corruptedRecord
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
            let slice = try await DatabaseIndexMaintenanceRuntime(
                container: context.operationContext.container,
                wireLimits: wireLimits
            ).runRebuildSlice(
                entity: entity,
                index: index,
                partitions: partitions,
                generation: context.jobID,
                mode: started ? .resume : .start,
                maximumWorkUnits: effectiveWorkUnits,
                transaction: context.databaseTransaction.storageAccess
            )
            guard slice.completedWorkUnits <= effectiveWorkUnits else {
                throw DatabaseJobRuntimeError.sliceExceededBudget(
                    actual: slice.completedWorkUnits,
                    maximum: effectiveWorkUnits
                )
            }
            let cumulativeWorkUnits = try cumulativeWorkUnits(
                before: context.completedWorkUnitsBeforeSlice,
                completed: slice.completedWorkUnits
            )
            if slice.isComplete {
                return .complete(
                    completedWorkUnits: slice.completedWorkUnits,
                    result: .execution(
                        MaintenanceExecuteOperation.ExecutionResult(
                            kind: .indexRebuild,
                            completedWorkUnits: cumulativeWorkUnits,
                            isComplete: true
                        )
                    )
                )
            }
            return .incomplete(
                completedWorkUnits: slice.completedWorkUnits,
                state: DatabaseMaintenanceJobState(
                    value: .indexRebuild(started: true)
                )
            )

        case let (
            .compaction(planWorkUnits),
            .compaction(backendContinuation)
        ):
            guard let compaction = context.databaseTransaction.storageAccess
                as? any DatabaseStorageCompactionTransaction else {
                throw DatabaseMaintenanceRuntimeError.compactionUnavailable
            }
            let effectiveWorkUnits = min(
                maximumWorkUnits,
                planWorkUnits,
                runtimeLimits.maximumWorkUnits,
                compaction.compactionLimits.maximumWorkUnitsPerSlice
            )
            guard effectiveWorkUnits > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                    "Compaction has no executable work budget"
                )
            }
            let result = try await compaction.stageCompactionSlice(
                maximumWorkUnits: effectiveWorkUnits,
                continuation: backendContinuation.map {
                    DatabaseStorageCompactionContinuation(
                        bytes: Bytes(retaining: $0)
                    )
                }
            )
            try validateCompactionResult(
                result,
                maximumWorkUnits: effectiveWorkUnits
            )
            let cumulativeWorkUnits = try cumulativeWorkUnits(
                before: context.completedWorkUnitsBeforeSlice,
                completed: result.workUnitsConsumed
            )
            if result.isComplete {
                return .complete(
                    completedWorkUnits: result.workUnitsConsumed,
                    result: .execution(
                        MaintenanceExecuteOperation.ExecutionResult(
                            kind: .compaction,
                            completedWorkUnits: cumulativeWorkUnits,
                            isComplete: true
                        )
                    )
                )
            }
            guard let backend = result.continuation else {
                throw DatabaseMaintenanceRuntimeError.invalidContinuation
            }
            return .incomplete(
                completedWorkUnits: result.workUnitsConsumed,
                state: DatabaseMaintenanceJobState(
                    value: .compaction(
                        continuation: DatabaseBytes(retaining: backend.bytes)
                    )
                )
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

    public func applyUnsuccessfulOutcome(
        plan: DatabaseMaintenanceJobPlan,
        state: DatabaseMaintenanceJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        switch (plan.invocation, state.value) {
        case (.migrations, .migrations):
            return
        case let (
            .indexRebuild(entity, index, partitions, _, _),
            .indexRebuild(started)
        ):
            guard started else { return }
            let detail: String
            switch outcome {
            case .failed(let error):
                detail = "\(error.code): \(error.message)"
            case .cancelled:
                detail = "cancelled"
            }
            try await DatabaseIndexMaintenanceRuntime(
                container: context.operationContext.container,
                wireLimits: wireLimits
            ).markFailed(
                entity: entity,
                index: index,
                partitions: partitions,
                generation: context.jobID,
                detail: detail,
                transaction: context.databaseTransaction.storageAccess
            )
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
        _ result: DatabaseStorageCompactionResult,
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

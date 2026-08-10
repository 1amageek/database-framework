import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public struct DatabaseSchemaApplyResumableOperation:
    DatabaseResumableOperation
{
    private static let jobKind = "database.schema-apply"

    private let runtimeFactory: AnyDatabaseSchemaRuntimeFactory
    private let runtimeLimits: DatabaseRuntimeLimits

    public var baseAdmission: DatabaseBaseAdmissionKind { .administration }

    public init(
        runtimeFactory: AnyDatabaseSchemaRuntimeFactory,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.runtimeFactory = runtimeFactory
        self.runtimeLimits = runtimeLimits
    }

    public static func job()
        throws(DatabaseWireError)
        -> JobOperation<
            SchemaExecuteOperation.Request,
            SchemaExecuteOperation.Response
        > {
        try DatabaseOperations.schemaExecute.resumableJob(kind: jobKind)
    }

    public func compile(
        _ request: SchemaExecuteOperation.Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<
        DatabaseSchemaApplyJobPlan,
        DatabaseSchemaApplyJobState
    > {
        guard case let .apply(
            manifest,
            expectedFingerprint,
            idempotencyKey
        ) = request.invocation else {
            throw DatabaseSchemaApplyJobError.invalidInvocation
        }
        let executor = try context.operationContext.requireControlExecutor()
        let currentSchema = executor.schema
        let currentFingerprint = executor.schemaFingerprint.detached()
        guard expectedFingerprint == currentFingerprint else {
            throw DatabaseSchemaPublicationError.fingerprintConflict(
                expected: expectedFingerprint,
                actual: currentFingerprint
            )
        }
        let analysis = DatabaseSchemaChangeAnalysis.analyze(
            current: currentSchema,
            target: manifest.schema
        )
        guard analysis.compatibility != .requiresMigration else {
            throw DatabaseSchemaExecutionError.migrationRequired(
                analysis.issues
            )
        }
        let maximumWorkUnits = min(
            context.maximumSliceWorkUnits,
            runtimeLimits.maximumWorkUnits,
            DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
        )
        guard maximumWorkUnits > 0 else {
            throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
        }
        let baseRecords = try await executor.loadBases(
            transaction: context.transaction
        )
        var bases: [DatabaseSchemaApplyJobPlan.BaseTarget] = []
        bases.reserveCapacity(baseRecords.count)
        for record in baseRecords {
            switch record.lifecycle {
            case .active, .retired:
                bases.append(
                    DatabaseSchemaApplyJobPlan.BaseTarget(
                        id: record.id,
                        placementGeneration: record.placementGeneration
                    )
                )
            case .tombstone:
                continue
            case .provisioning, .retiring, .moving, .deleting:
                throw DatabaseSchemaApplyJobError
                    .baseLifecycleTransitionInProgress(
                        record.id,
                        record.lifecycle.name
                    )
            }
        }
        let indexes = manifest.schema.entities.flatMap { entity in
            entity.indexDescriptors.map { descriptor in
                DatabaseSchemaApplyJobPlan.IndexTarget(
                    entity: entity.name,
                    index: descriptor.name,
                    usesDynamicDirectory: entity.hasDynamicDirectory
                )
            }
        }
        return DatabasePreparedResumableJob(
            plan: try DatabaseSchemaApplyJobPlan(
                previousFingerprint: currentFingerprint,
                targetFingerprint: manifest.fingerprint(),
                manifest: manifest,
                idempotencyKey: idempotencyKey,
                bases: bases,
                indexes: indexes,
                maximumWorkUnitsPerSlice: maximumWorkUnits
            ),
            initialState: DatabaseSchemaApplyJobState(),
            sliceTimeoutMilliseconds:
                runtimeLimits.maximumTimeoutMilliseconds
        )
    }

    public func commitModel(
        for plan: DatabaseSchemaApplyJobPlan
    ) -> DatabaseResumableOperationCommitModel {
        _ = plan
        return .operationCheckpointed
    }

    public func runSlice(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseSchemaApplyJobState,
        SchemaExecuteOperation.Response
    > {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }

    public func runCheckpointedSlice(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseSchemaApplyJobState,
        SchemaExecuteOperation.Response
    > {
        let executor = try context.operationContext.requireControlExecutor()
        let transition = executor.makeSchemaTransitionExecutor(
            runtimeFactory: runtimeFactory
        )
        let manifest = try plan.manifest
        let sliceLimit = min(
            maximumWorkUnits,
            plan.maximumWorkUnitsPerSlice,
            runtimeLimits.maximumWorkUnits,
            DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
        )
        guard sliceLimit > 0 else {
            throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
        }

        switch state.phase {
        case .staging:
            guard executor.schemaFingerprint == plan.previousFingerprint else {
                throw DatabaseSchemaApplyJobError.publishedSchemaMismatch
            }
            guard let offset = Int(exactly: state.baseOffset),
                  offset <= plan.bases.count else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            if offset < plan.bases.count {
                try await transition.stage(
                    plan.bases[offset],
                    previousSchema: executor.schema,
                    targetSchema: manifest.schema
                )
                return .incomplete(
                    completedWorkUnits: 1,
                    state: DatabaseSchemaApplyJobState(
                        phase: offset + 1 == plan.bases.count
                            ? .publishing
                            : .staging,
                        baseOffset: try increment(state.baseOffset)
                    )
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseSchemaApplyJobState(phase: .publishing)
            )

        case .publishing:
            let publication = try await transition.publish(
                manifest: manifest,
                expectedFingerprint: plan.previousFingerprint,
                targetFingerprint: plan.targetFingerprint,
                idempotencyKey: plan.idempotencyKey
            )
            guard publication.fingerprint == plan.targetFingerprint,
                  publication.schemaVersion == plan.schemaVersion else {
                throw DatabaseSchemaApplyJobError.publishedSchemaMismatch
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseSchemaApplyJobState(phase: .installing)
            )

        case .installing:
            guard executor.schemaFingerprint == plan.targetFingerprint,
                  executor.schema.version == plan.schemaVersion else {
                throw DatabaseSchemaApplyJobError.publishedSchemaMismatch
            }
            guard let offset = Int(exactly: state.baseOffset),
                  offset <= plan.bases.count else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            if offset < plan.bases.count {
                try await transition.installSnapshot(
                    plan.bases[offset],
                    schema: manifest.schema
                )
                return .incomplete(
                    completedWorkUnits: 1,
                    state: DatabaseSchemaApplyJobState(
                        phase: offset + 1 == plan.bases.count
                            ? .building
                            : .installing,
                        baseOffset: offset + 1 == plan.bases.count
                            ? 0
                            : try increment(state.baseOffset)
                    )
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseSchemaApplyJobState(phase: .building)
            )

        case .building:
            guard executor.schemaFingerprint == plan.targetFingerprint,
                  executor.schema.version == plan.schemaVersion else {
                throw DatabaseSchemaApplyJobError.publishedSchemaMismatch
            }
            let progress = try await buildSlice(
                plan: plan,
                state: state,
                maximumWorkUnits: sliceLimit,
                transition: transition,
                jobID: context.jobID
            )
            return .incomplete(
                completedWorkUnits: progress.completedWorkUnits,
                state: progress.state
            )

        case .finishing:
            let job = JobIdentity(
                jobID: context.jobID,
                operation: try Self.job().identifier,
                target: .database
            )
            try await transition.finish(job: job)
            return .complete(
                completedWorkUnits: 1,
                result: .applied(
                    SchemaExecuteOperation.Applied(
                        previousFingerprint: plan.previousFingerprint,
                        fingerprint: plan.targetFingerprint,
                        schemaVersion: plan.schemaVersion,
                        generation: executor.schemaGeneration
                    )
                )
            )
        }
    }

    public func prepareUnsuccessfulOutcomeCommit(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws {
        guard state.phase == .building,
              state.activeBuildStarted,
              let baseOffset = Int(exactly: state.baseOffset),
              plan.bases.indices.contains(baseOffset),
              let indexOffset = Int(exactly: state.indexOffset),
              plan.indexes.indices.contains(indexOffset),
              let partitions = state.activePartitions else {
            return
        }
        let detail: String
        switch outcome {
        case .failed(let error):
            detail = "\(error.code): \(error.message)"
        case .cancelled:
            detail = "cancelled"
        }
        let control = try context.operationContext.requireControlExecutor()
        let transition = control.makeSchemaTransitionExecutor(
            runtimeFactory: runtimeFactory
        )
        let target = plan.indexes[indexOffset]
        try await transition.withBase(plan.bases[baseOffset]) { executor in
            try await executor.withActiveDataTransaction(
                requiredAccess: .administer,
                configuration: .batch
            ) { transaction in
                try await executor.makeIndexMaintenanceRuntime().markFailed(
                    entity: target.entity,
                    index: target.index,
                    partitions: partitions,
                    generation: context.jobID,
                    detail: detail,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    public func applyUnsuccessfulOutcome(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = outcome
        let executor = try context.operationContext.requireControlExecutor()
        try await executor.finishSchemaApplication(
            job: JobIdentity(
                jobID: context.jobID,
                operation: try Self.job().identifier,
                target: .database
            ),
            transaction: context.transaction
        )
    }

    private func buildSlice(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        maximumWorkUnits: UInt64,
        transition: DatabaseSchemaTransitionExecutor,
        jobID: DatabaseTypes.UUID
    ) async throws -> BuildProgress {
        guard let baseOffset = Int(exactly: state.baseOffset),
              baseOffset <= plan.bases.count,
              let indexOffset = Int(exactly: state.indexOffset),
              indexOffset <= plan.indexes.count else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        guard baseOffset < plan.bases.count else {
            return BuildProgress(
                state: DatabaseSchemaApplyJobState(phase: .finishing),
                completedWorkUnits: 1
            )
        }
        guard indexOffset < plan.indexes.count else {
            let nextBase = try increment(state.baseOffset)
            return BuildProgress(
                state: DatabaseSchemaApplyJobState(
                    phase: nextBase == UInt64(plan.bases.count)
                        ? .finishing
                        : .building,
                    baseOffset: nextBase
                ),
                completedWorkUnits: 1
            )
        }

        return try await transition.withBase(plan.bases[baseOffset]) {
            executor in
            try await executor.withActiveDataTransaction(
                requiredAccess: .administer,
                configuration: .batch
            ) { transaction in
                try await buildIndexSlice(
                    plan: plan,
                    state: state,
                    maximumWorkUnits: maximumWorkUnits,
                    executor: executor,
                    jobID: jobID,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    private func buildIndexSlice(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        maximumWorkUnits: UInt64,
        executor: BaseOperationExecutor,
        jobID: DatabaseTypes.UUID,
        transaction: any TransactionAccess
    ) async throws -> BuildProgress {
        guard let offset = Int(exactly: state.indexOffset),
              plan.indexes.indices.contains(offset) else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        let target = plan.indexes[offset]
        var nextContinuation = state.nextPartitionContinuation
        var activePartitions = state.activePartitions
        var activeIsLast = state.activePartitionIsLast
        let activeStarted = state.activeBuildStarted

        if activePartitions == nil {
            if target.usesDynamicDirectory {
                let page = try await executor.partitionCatalogPage(
                    entity: target.entity,
                    continuation: nextContinuation,
                    limit: 1,
                    transaction: transaction
                )
                guard let entry = page.entries.first else {
                    try executor.completeSchemaIndexBuild(
                        entity: target.entity,
                        index: target.index,
                        transaction: transaction
                    )
                    return advancedIndexState(plan: plan, state: state)
                }
                activePartitions = entry.partitions
                nextContinuation = page.continuation
                activeIsLast = page.continuation == nil
            } else {
                activePartitions = FieldObject()
                nextContinuation = nil
                activeIsLast = true
            }
        }

        guard let partitions = activePartitions else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        let maintenance = executor.makeIndexMaintenanceRuntime()
        if !activeStarted {
            let status = try await maintenance.status(
                entity: target.entity,
                index: target.index,
                partitions: partitions,
                transaction: transaction
            )
            if status.indexState == .readable {
                return try finishedPartitionState(
                    plan: plan,
                    state: state,
                    target: target,
                    isLast: activeIsLast,
                    nextContinuation: nextContinuation,
                    executor: executor,
                    transaction: transaction
                )
            }
        }

        let slice = try await maintenance.runRebuildSlice(
            entity: target.entity,
            index: target.index,
            partitions: partitions,
            generation: jobID,
            mode: activeStarted ? .resume : .start,
            maximumWorkUnits: maximumWorkUnits,
            transaction: transaction
        )
        guard slice.completedWorkUnits <= maximumWorkUnits else {
            throw DatabaseJobRuntimeError.sliceExceededBudget(
                actual: slice.completedWorkUnits,
                maximum: maximumWorkUnits
            )
        }
        let chargedWork = max(UInt64(1), slice.completedWorkUnits)
        if !slice.isComplete {
            guard slice.completedWorkUnits > 0 else {
                throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
            }
            return BuildProgress(
                state: DatabaseSchemaApplyJobState(
                    phase: .building,
                    baseOffset: state.baseOffset,
                    indexOffset: state.indexOffset,
                    nextPartitionContinuation: nextContinuation,
                    activePartitions: partitions,
                    activePartitionIsLast: activeIsLast,
                    activeBuildStarted: true
                ),
                completedWorkUnits: chargedWork
            )
        }
        let progress = try finishedPartitionState(
            plan: plan,
            state: state,
            target: target,
            isLast: activeIsLast,
            nextContinuation: nextContinuation,
            executor: executor,
            transaction: transaction
        )
        return BuildProgress(
            state: progress.state,
            completedWorkUnits: chargedWork
        )
    }

    private func finishedPartitionState(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        target: DatabaseSchemaApplyJobPlan.IndexTarget,
        isLast: Bool,
        nextContinuation: ByteString?,
        executor: BaseOperationExecutor,
        transaction: any TransactionAccess
    ) throws -> BuildProgress {
        if isLast {
            try executor.completeSchemaIndexBuild(
                entity: target.entity,
                index: target.index,
                transaction: transaction
            )
            return advancedIndexState(plan: plan, state: state)
        }
        return BuildProgress(
            state: DatabaseSchemaApplyJobState(
                phase: .building,
                baseOffset: state.baseOffset,
                indexOffset: state.indexOffset,
                nextPartitionContinuation: nextContinuation
            ),
            completedWorkUnits: 1
        )
    }

    private func advancedIndexState(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState
    ) -> BuildProgress {
        let nextIndex = state.indexOffset + 1
        if nextIndex == UInt64(plan.indexes.count) {
            let nextBase = state.baseOffset + 1
            return BuildProgress(
                state: DatabaseSchemaApplyJobState(
                    phase: nextBase == UInt64(plan.bases.count)
                        ? .finishing
                        : .building,
                    baseOffset: nextBase
                ),
                completedWorkUnits: 1
            )
        }
        return BuildProgress(
            state: DatabaseSchemaApplyJobState(
                phase: .building,
                baseOffset: state.baseOffset,
                indexOffset: nextIndex
            ),
            completedWorkUnits: 1
        )
    }

    private func increment(_ value: UInt64) throws -> UInt64 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw DatabaseJobRuntimeError.stateRevisionOverflow
        }
        return result.partialValue
    }
}

private struct BuildProgress: Sendable {
    let state: DatabaseSchemaApplyJobState
    let completedWorkUnits: UInt64
}

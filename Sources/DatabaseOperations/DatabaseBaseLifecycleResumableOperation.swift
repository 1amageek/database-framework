import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_MULTIPLE_BASES
import DatabaseAdministrationOperations
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseBaseLifecycleResumableOperation:
    DatabaseResumableOperation {
    private static let jobKind = "database.base-lifecycle"

    private let timeoutMilliseconds: UInt32

    public init(runtimeLimits: DatabaseOperationLimits = .default) {
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public static func job()
        throws(DatabaseWireError)
        -> JobOperation<
            BaseExecuteOperation.Request,
            BaseExecuteOperation.Response
        > {
        try DatabaseOperationCatalog.baseExecute.resumableJob(kind: jobKind)
    }

    public var baseAdmission: DatabaseBaseAdmissionKind { .lifecycleJob }

    public func compile(
        _ request: BaseExecuteOperation.Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<
        DatabaseBaseLifecycleJobPlan,
        DatabaseBaseLifecycleJobState
    > {
        let plan: DatabaseBaseLifecycleJobPlan
        switch request.invocation {
        case .create(
            let baseID,
            let placementID,
            let initialGrants,
            let expectedRevision,
            _
        ):
            guard context.operationContext.target == .database else {
                throw DatabaseAdministrationError.targetMismatch(
                    context.operationContext.target
                )
            }
            plan = DatabaseBaseLifecycleJobPlan(
                action: .create,
                baseID: baseID,
                placementID: placementID,
                initialGrants: initialGrants,
                expectedRevision: expectedRevision
            )
        case .retire(let expectedRevision, _):
            plan = try makePlan(
                action: .retire,
                expectedRevision: expectedRevision,
                context: context
            )
        case .activate(let expectedRevision, _):
            plan = try makePlan(
                action: .activate,
                expectedRevision: expectedRevision,
                context: context
            )
        case .delete(let expectedRevision, _):
            plan = try makePlan(
                action: .delete,
                expectedRevision: expectedRevision,
                context: context
            )
        case .placementApply(
            let destination,
            let expectedRevision,
            _
        ):
            plan = try makePlan(
                action: .move,
                placementID: destination,
                expectedRevision: expectedRevision,
                context: context
            )
        case .placements, .list, .describe, .placementPlan,
             .legacyMigrationPlan, .legacyMigrationApply:
            throw DatabaseAdministrationError.unsupportedLifecycleAction
        }
        let initialPhase: DatabaseBaseLifecycleJobState.Phase
        switch plan.action {
        case .move:
            initialPhase = .movePrepare
        case .delete:
            initialPhase = .deletePrepare
        case .create, .retire, .activate:
            initialPhase = .simple
        }
        return DatabasePreparedResumableJob(
            plan: plan,
            initialState: DatabaseBaseLifecycleJobState(
                phase: initialPhase
            ),
            sliceTimeoutMilliseconds: timeoutMilliseconds
        )
    }

    public func commitModel(
        for plan: DatabaseBaseLifecycleJobPlan
    ) -> DatabaseResumableOperationCommitModel {
        _ = plan
        return .operationCheckpointed
    }

    public func runSlice(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseBaseLifecycleJobState,
        BaseExecuteOperation.Response
    > {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }

    public func runCheckpointedSlice(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseBaseLifecycleJobState,
        BaseExecuteOperation.Response
    > {
        guard maximumWorkUnits > 0 else {
            throw DatabaseJobRuntimeError.sliceMadeNoProgress
        }
        if plan.action == .move {
            return try await runPlacementMoveSlice(
                plan: plan,
                state: state,
                context: context
            )
        }
        if plan.action == .delete {
            return try await runDeletionSlice(
                plan: plan,
                state: state,
                context: context
            )
        }
        guard state.phase == .simple else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        let record: DatabaseBaseRecord
        switch plan.action {
        case .create:
            guard let placementID = plan.placementID else {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            record = try await context.operationContext
                .requireControlExecutor().provisionBase(
                plan.baseID,
                placementID: placementID,
                initialGrants: plan.initialGrants,
                expectedRevision: plan.expectedRevision
            )
        case .retire:
            record = try await context.operationContext
                .requireBaseExecutor().retire(
                expectedRevision: plan.expectedRevision
            )
        case .activate:
            record = try await context.operationContext
                .requireBaseExecutor().activate(
                expectedRevision: plan.expectedRevision
            )
        case .delete:
            throw DatabaseJobRuntimeError.corruptedPlan
        case .move:
            throw DatabaseJobRuntimeError.corruptedPlan
        }
        return .complete(
            completedWorkUnits: 1,
            result: .base(Self.description(record))
        )
    }

    public func prepareUnsuccessfulOutcomeCommit(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws {
        _ = outcome
        switch plan.action {
        case .create, .retire, .activate:
            return
        case .delete:
            switch state.phase {
            case .deletePrepare:
                return
            case .deleteClear, .deleteFinish:
                _ = try await context.operationContext.requireBaseExecutor()
                    .prepareUnsuccessfulDeletionRecovery(
                        owner: ByteString(context.jobID.bytes)
                    )
            case .simple, .movePrepare, .moveCopy, .moveVerifySource,
                 .moveVerifyDestination, .moveCutover, .moveCleanup:
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .move:
            switch state.phase {
            case .simple, .movePrepare:
                return
            case .moveCopy, .moveVerifySource, .moveVerifyDestination,
                 .moveCutover, .moveCleanup:
                let descriptor = try Self.requireDescriptor(state)
                _ = try await context.operationContext.requireBaseExecutor()
                    .prepareUnsuccessfulPlacementMoveRecovery(
                        descriptor,
                        owner: ByteString(context.jobID.bytes)
                    )
            case .deletePrepare, .deleteClear, .deleteFinish:
                throw DatabaseJobRuntimeError.corruptedState
            }
        }
    }

    public func applySuccessfulOutcome(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        context: DatabaseResumableOperationContext
    ) async throws {
        let owner = ByteString(context.jobID.bytes)
        switch plan.action {
        case .create, .retire, .activate:
            guard state.phase == .simple else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .delete:
            guard state.phase == .deleteFinish else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await context.operationContext.requireBaseExecutor()
                .finalizeSuccessfulDeletion(
                    owner: owner,
                    controlTransaction: context.databaseTransaction.storageAccess
                )
        case .move:
            guard state.phase == .moveCleanup else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await context.operationContext.requireBaseExecutor()
                .finalizeSuccessfulPlacementMove(
                    try Self.requireDescriptor(state),
                    owner: owner,
                    controlTransaction: context.databaseTransaction.storageAccess
                )
        }
    }

    public func applyUnsuccessfulOutcome(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = outcome
        switch plan.action {
        case .create, .retire, .activate:
            return
        case .delete:
            switch state.phase {
            case .deletePrepare:
                return
            case .deleteClear, .deleteFinish:
                try await context.operationContext.requireBaseExecutor()
                    .finalizeUnsuccessfulDeletion(
                        owner: ByteString(context.jobID.bytes),
                        controlTransaction:
                            context.databaseTransaction.storageAccess
                    )
            case .simple, .movePrepare, .moveCopy, .moveVerifySource,
                 .moveVerifyDestination, .moveCutover, .moveCleanup:
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .move:
            switch state.phase {
            case .simple, .movePrepare:
                return
            case .moveCopy, .moveVerifySource, .moveVerifyDestination,
                 .moveCutover, .moveCleanup:
                let descriptor = try Self.requireDescriptor(state)
                try await context.operationContext.requireBaseExecutor()
                    .finalizeUnsuccessfulPlacementMove(
                        descriptor,
                        owner: ByteString(context.jobID.bytes),
                        controlTransaction:
                            context.databaseTransaction.storageAccess
                    )
            case .deletePrepare, .deleteClear, .deleteFinish:
                throw DatabaseJobRuntimeError.corruptedState
            }
        }
    }

    private func makePlan(
        action: DatabaseBaseLifecycleJobPlan.Action,
        placementID: Base.Placement.ID? = nil,
        expectedRevision: UInt64,
        context: DatabaseResumableOperationStartContext
    ) throws -> DatabaseBaseLifecycleJobPlan {
        guard case .base(let baseID) = context.operationContext.target else {
            throw DatabaseAdministrationError.targetMismatch(
                context.operationContext.target
            )
        }
        return DatabaseBaseLifecycleJobPlan(
            action: action,
            baseID: baseID,
            placementID: placementID,
            expectedRevision: expectedRevision
        )
    }

    private func runPlacementMoveSlice(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseBaseLifecycleJobState,
        BaseExecuteOperation.Response
    > {
        guard let destinationPlacementID = plan.placementID else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
        let executor = try context.operationContext.requireBaseExecutor()
        let owner = ByteString(context.jobID.bytes)
        switch state.phase {
        case .simple, .deletePrepare, .deleteClear, .deleteFinish:
            throw DatabaseJobRuntimeError.corruptedState

        case .movePrepare:
            let descriptor = try await executor.preparePlacementMove(
                destinationPlacementID: destinationPlacementID,
                expectedRevision: plan.expectedRevision,
                owner: owner
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(
                    phase: .moveCopy,
                    descriptor: descriptor
                )
            )

        case .moveCopy:
            let descriptor = try Self.requireDescriptor(state)
            let progress = try await executor.copyPlacementBatch(
                descriptor,
                continuation: state.continuation,
                digest: state.digest,
                keyCount: state.keyCount,
                byteCount: state.byteCount
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(
                    phase: progress.isComplete
                        ? .moveVerifySource
                        : .moveCopy,
                    descriptor: descriptor,
                    continuation: progress.isComplete
                        ? nil
                        : progress.continuation,
                    digest: progress.isComplete ? nil : progress.digest,
                    keyCount: progress.isComplete ? 0 : progress.keyCount,
                    byteCount: progress.isComplete ? 0 : progress.byteCount
                )
            )

        case .moveVerifySource:
            let descriptor = try Self.requireDescriptor(state)
            let progress = try await executor.verifyPlacementBatch(
                descriptor,
                destination: false,
                continuation: state.continuation,
                digest: state.digest,
                keyCount: state.keyCount,
                byteCount: state.byteCount
            )
            if progress.isComplete {
                return .incomplete(
                    completedWorkUnits: 1,
                    state: DatabaseBaseLifecycleJobState(
                        phase: .moveVerifyDestination,
                        descriptor: descriptor,
                        sourceDigest: progress.digest,
                        sourceKeyCount: progress.keyCount,
                        sourceByteCount: progress.byteCount
                    )
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(
                    phase: .moveVerifySource,
                    descriptor: descriptor,
                    continuation: progress.continuation,
                    digest: progress.digest,
                    keyCount: progress.keyCount,
                    byteCount: progress.byteCount
                )
            )

        case .moveVerifyDestination:
            let descriptor = try Self.requireDescriptor(state)
            guard let sourceDigest = state.sourceDigest else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            let progress = try await executor.verifyPlacementBatch(
                descriptor,
                destination: true,
                continuation: state.continuation,
                digest: state.digest,
                keyCount: state.keyCount,
                byteCount: state.byteCount
            )
            if progress.isComplete {
                guard progress.digest == sourceDigest,
                      progress.keyCount == state.sourceKeyCount,
                      progress.byteCount == state.sourceByteCount else {
                    throw DatabaseBaseCatalogError.placementDigestMismatch(
                        plan.baseID
                    )
                }
                return .incomplete(
                    completedWorkUnits: 1,
                    state: DatabaseBaseLifecycleJobState(
                        phase: .moveCutover,
                        descriptor: descriptor,
                        sourceDigest: sourceDigest,
                        sourceKeyCount: state.sourceKeyCount,
                        sourceByteCount: state.sourceByteCount
                    )
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(
                    phase: .moveVerifyDestination,
                    descriptor: descriptor,
                    continuation: progress.continuation,
                    digest: progress.digest,
                    keyCount: progress.keyCount,
                    byteCount: progress.byteCount,
                    sourceDigest: sourceDigest,
                    sourceKeyCount: state.sourceKeyCount,
                    sourceByteCount: state.sourceByteCount
                )
            )

        case .moveCutover:
            let descriptor = try Self.requireDescriptor(state)
            _ = try await executor.cutOverPlacementMove(descriptor)
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(
                    phase: .moveCleanup,
                    descriptor: descriptor
                )
            )

        case .moveCleanup:
            let descriptor = try Self.requireDescriptor(state)
            let record = try await executor.finishPlacementMove(
                descriptor,
                owner: owner
            )
            return .complete(
                completedWorkUnits: 1,
                result: .base(Self.description(record))
            )
        }
    }

    private func runDeletionSlice(
        plan: DatabaseBaseLifecycleJobPlan,
        state: DatabaseBaseLifecycleJobState,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseBaseLifecycleJobState,
        BaseExecuteOperation.Response
    > {
        let executor = try context.operationContext.requireBaseExecutor()
        let owner = ByteString(context.jobID.bytes)
        switch state.phase {
        case .deletePrepare:
            _ = try await executor.prepareDeletion(
                expectedRevision: plan.expectedRevision,
                owner: owner
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(phase: .deleteClear)
            )
        case .deleteClear:
            _ = try await executor.clearForDeletion(
                owner: owner
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseBaseLifecycleJobState(phase: .deleteFinish)
            )
        case .deleteFinish:
            let record = try await executor.finishDeletion(
                owner: owner
            )
            return .complete(
                completedWorkUnits: 1,
                result: .base(Self.description(record))
            )
        case .simple, .movePrepare, .moveCopy, .moveVerifySource,
             .moveVerifyDestination, .moveCutover, .moveCleanup:
            throw DatabaseJobRuntimeError.corruptedState
        }
    }

    private static func requireDescriptor(
        _ state: DatabaseBaseLifecycleJobState
    ) throws -> DatabaseBasePlacementMoveDescriptor {
        guard let descriptor = state.descriptor else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        return descriptor
    }

    private static func description(
        _ record: DatabaseBaseRecord
    ) -> BaseExecuteOperation.Description {
        let lifecycle: BaseExecuteOperation.LifecycleState
        switch record.lifecycle {
        case .provisioning: lifecycle = .provisioning
        case .active: lifecycle = .active
        case .retiring: lifecycle = .retiring
        case .retired: lifecycle = .retired
        case .moving: lifecycle = .moving
        case .deleting: lifecycle = .deleting
        case .tombstone: lifecycle = .tombstone
        }
        return BaseExecuteOperation.Description(
            id: record.id,
            placementID: record.placementID,
            placementGeneration: record.placementGeneration,
            revision: record.revision,
            lifecycle: lifecycle
        )
    }
}

#endif

#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

public struct DatabaseLegacyLayoutMigrationResumableOperation:
    DatabaseResumableOperation
{
    private static let jobKind = "database.legacy-layout-migration"
    private let timeoutMilliseconds: UInt32

    public init(runtimeLimits: DatabaseRuntimeLimits = .default) {
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public static func job()
        throws(DatabaseWireError)
        -> JobOperation<
            BaseExecuteOperation.Request,
            BaseExecuteOperation.Response
        > {
        try DatabaseOperations.baseExecute.resumableJob(kind: jobKind)
    }

    public func compile(
        _ request: BaseExecuteOperation.Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<
        DatabaseLegacyLayoutMigrationJobPlan,
        DatabaseLegacyLayoutMigrationJobState
    > {
        guard context.operationContext.target == .database else {
            throw DatabaseAdministrationError.targetMismatch(
                context.operationContext.target
            )
        }
        guard case .legacyMigrationApply(
            let baseID,
            let placementID,
            let initialGrants,
            let expectedFingerprint,
            let expectedRevision,
            _
        ) = request.invocation else {
            throw DatabaseAdministrationError.unsupportedLifecycleAction
        }
        return DatabasePreparedResumableJob(
            plan: DatabaseLegacyLayoutMigrationJobPlan(
                baseID: baseID,
                placementID: placementID,
                initialGrants: initialGrants,
                expectedLayoutFingerprint: expectedFingerprint,
                expectedRevision: expectedRevision
            ),
            initialState: DatabaseLegacyLayoutMigrationJobState(
                phase: .verifySource
            ),
            sliceTimeoutMilliseconds: timeoutMilliseconds
        )
    }

    public func commitModel(
        for plan: DatabaseLegacyLayoutMigrationJobPlan
    ) -> DatabaseResumableOperationCommitModel {
        _ = plan
        return .operationCheckpointed
    }

    public func runSlice(
        plan: DatabaseLegacyLayoutMigrationJobPlan,
        state: DatabaseLegacyLayoutMigrationJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseLegacyLayoutMigrationJobState,
        BaseExecuteOperation.Response
    > {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }

    public func runCheckpointedSlice(
        plan: DatabaseLegacyLayoutMigrationJobPlan,
        state: DatabaseLegacyLayoutMigrationJobState,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<
        DatabaseLegacyLayoutMigrationJobState,
        BaseExecuteOperation.Response
    > {
        guard maximumWorkUnits > 0 else {
            throw DatabaseJobRuntimeError.sliceMadeNoProgress
        }
        let executor = try context.operationContext.requireControlExecutor()
        let inventory = try await executor.legacyLayoutInventory(
            allowCurrentLayout: state.phase == .cleanup
        )
        switch state.phase {
        case .verifySource:
            let progress = try await scan(
                executor: executor,
                inventory: inventory,
                state: state,
                mode: .source,
                destination: nil
            )
            guard progress.isComplete else {
                return .incomplete(
                    completedWorkUnits: 1,
                    state: state.advancing(with: progress)
                )
            }
            guard progress.digest == plan.expectedLayoutFingerprint.bytes else {
                throw DatabaseLegacyLayoutMigrationError.fingerprintMismatch
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: DatabaseLegacyLayoutMigrationJobState(
                    phase: .prepareDestination,
                    sourceDigest: progress.digest,
                    sourceKeyCount: progress.keyCount,
                    sourceByteCount: progress.byteCount
                )
            )

        case .prepareDestination:
            let destination = try await executor.prepareLegacyMigrationBase(
                plan.baseID,
                placementID: plan.placementID,
                initialGrants: plan.initialGrants,
                expectedRevision: plan.expectedRevision
            )
            try await executor.validateLegacyMigrationDestination(
                destination.root,
                inventory: inventory
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: state.resetting(to: .copy)
            )

        case .copy:
            let destination = try await executor.legacyMigrationBase(
                plan.baseID
            )
            let progress = try await scan(
                executor: executor,
                inventory: inventory,
                state: state,
                mode: .copy,
                destination: destination
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: progress.isComplete
                    ? state.resetting(to: .reverifySource)
                    : state.advancing(with: progress)
            )

        case .reverifySource:
            let progress = try await scan(
                executor: executor,
                inventory: inventory,
                state: state,
                mode: .source,
                destination: nil
            )
            guard progress.isComplete else {
                return .incomplete(
                    completedWorkUnits: 1,
                    state: state.advancing(with: progress)
                )
            }
            guard progress.digest == state.sourceDigest,
                  progress.keyCount == state.sourceKeyCount,
                  progress.byteCount == state.sourceByteCount else {
                throw DatabaseLegacyLayoutMigrationError
                    .sourceChangedDuringMigration
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: state.resetting(to: .verifyDestination)
            )

        case .verifyDestination:
            let destination = try await executor.legacyMigrationBase(
                plan.baseID
            )
            let progress = try await scan(
                executor: executor,
                inventory: inventory,
                state: state,
                mode: .destination,
                destination: destination
            )
            guard progress.isComplete else {
                return .incomplete(
                    completedWorkUnits: 1,
                    state: state.advancing(with: progress)
                )
            }
            guard progress.digest == state.sourceDigest,
                  progress.keyCount == state.sourceKeyCount,
                  progress.byteCount == state.sourceByteCount else {
                throw DatabaseLegacyLayoutMigrationError
                    .destinationDigestMismatch
            }
            return .incomplete(
                completedWorkUnits: 1,
                state: state.resetting(to: .rebuildAndCutOver)
            )

        case .rebuildAndCutOver:
            let destination = try await executor.legacyMigrationBase(
                plan.baseID
            )
            _ = try await executor.rebuildAndCutOverLegacyMigration(
                record: destination.record,
                root: destination.root
            )
            return .incomplete(
                completedWorkUnits: 1,
                state: state.resetting(to: .cleanup)
            )

        case .cleanup:
            try await executor.cleanupLegacyLayout(inventory: inventory)
            let destination = try await executor.legacyMigrationBase(
                plan.baseID
            )
            return .complete(
                completedWorkUnits: 1,
                result: .base(Self.description(destination.record))
            )
        }
    }

    public func prepareUnsuccessfulOutcomeCommit(
        plan: DatabaseLegacyLayoutMigrationJobPlan,
        state: DatabaseLegacyLayoutMigrationJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws {
        _ = state
        _ = outcome
        let executor = try context.operationContext.requireControlExecutor()
        if executor.layoutStatus == .current {
            let inventory = try await executor.legacyLayoutInventory(
                allowCurrentLayout: true
            )
            try await executor.cleanupLegacyLayout(inventory: inventory)
        } else {
            try await executor.abortLegacyMigrationBase(plan.baseID)
        }
    }

    public func applyUnsuccessfulOutcome(
        plan: DatabaseLegacyLayoutMigrationJobPlan,
        state: DatabaseLegacyLayoutMigrationJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = outcome
        _ = context
        // Operation-owned cleanup is idempotently completed before this
        // control-domain transaction publishes the terminal job outcome.
    }

    private func scan(
        executor: DatabaseControlExecutor,
        inventory: DatabaseLegacyLayoutInventory,
        state: DatabaseLegacyLayoutMigrationJobState,
        mode: DBContainer.LegacyLayoutTransferMode,
        destination: (record: DatabaseBaseRecord, root: Subspace)?
    ) async throws -> DatabaseLegacyLayoutTransferProgress {
        guard let entryIndex = Int(exactly: state.entryIndex) else {
            throw DatabaseLegacyLayoutMigrationError.invalidTransferState
        }
        return try await executor.scanLegacyLayoutBatch(
            inventory: inventory,
            destinationBaseRoot: destination?.root,
            destinationDomainID: destination?.record.domainID,
            mode: mode,
            progress: DatabaseLegacyLayoutTransferProgress(
                entryIndex: entryIndex,
                continuation: state.continuation,
                digest: state.digest,
                keyCount: state.keyCount,
                byteCount: state.byteCount,
                isComplete: false
            )
        )
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

private extension DatabaseLegacyLayoutMigrationJobState {
    func advancing(
        with progress: DatabaseLegacyLayoutTransferProgress
    ) -> Self {
        Self(
            phase: phase,
            entryIndex: UInt64(progress.entryIndex),
            continuation: progress.continuation,
            digest: progress.digest,
            keyCount: progress.keyCount,
            byteCount: progress.byteCount,
            sourceDigest: sourceDigest,
            sourceKeyCount: sourceKeyCount,
            sourceByteCount: sourceByteCount
        )
    }
}

#endif

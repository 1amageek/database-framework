import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public struct DatabaseSchemaApplyResumableOperation:
    DatabaseResumableOperation {
    private static let jobKind = "database.schema-apply"

    private let runtimeLimits: DatabaseRuntimeLimits

    public init(runtimeLimits: DatabaseRuntimeLimits = .default) {
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
            _
        ) = request.invocation else {
            throw DatabaseSchemaApplyJobError.invalidInvocation
        }
        let currentSchema = context.operationContext.container.schema
        let currentFingerprint = context.operationContext.container
            .schemaFingerprint.detached()
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
        let pending = try await context.operationContext.container
            .pendingSchemaIndexBuilds(
                in: manifest.schema,
                transaction: context.transaction.storageAccess
            )
        let indexBuilds = DatabaseSchemaChangeAnalysis.mergedIndexBuilds(
            analyzed: analysis.indexBuilds,
            pending: pending,
            schema: manifest.schema
        )
        guard !indexBuilds.isEmpty else {
            throw DatabaseSchemaApplyJobError.noIndexBuildTargets
        }
        let maximumWorkUnits = min(
            context.maximumSliceWorkUnits,
            runtimeLimits.maximumWorkUnits,
            DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
        )
        guard maximumWorkUnits > 0 else {
            throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
        }
        let targets = indexBuilds.map {
            DatabaseSchemaApplyJobPlan.Target(
                entity: $0.entity,
                index: $0.index,
                usesDynamicDirectory: $0.usesDynamicDirectory
            )
        }
        return DatabasePreparedResumableJob(
            plan: DatabaseSchemaApplyJobPlan(
                previousFingerprint: currentFingerprint,
                targetFingerprint: try manifest.fingerprint(),
                schemaVersion: manifest.schema.version,
                targets: targets,
                maximumWorkUnitsPerSlice: maximumWorkUnits
            ),
            initialState: DatabaseSchemaApplyJobState(),
            sliceTimeoutMilliseconds:
                runtimeLimits.maximumTimeoutMilliseconds
        )
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
        let container = context.operationContext.container
        guard container.schemaFingerprint == plan.targetFingerprint,
              container.schema.version == plan.schemaVersion else {
            throw DatabaseSchemaApplyJobError.publishedSchemaMismatch
        }
        let sliceLimit = min(
            maximumWorkUnits,
            plan.maximumWorkUnitsPerSlice,
            runtimeLimits.maximumWorkUnits,
            DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
        )
        guard sliceLimit > 0 else {
            throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
        }

        var targetOffset = state.targetOffset
        var nextContinuation = state.nextPartitionContinuation
        var activePartitions = state.activePartitions
        var activeIsLast = state.activePartitionIsLast
        var activeStarted = state.activeBuildStarted
        var completedWorkUnits: UInt64 = 0
        let maintenance = DatabaseIndexMaintenanceRuntime(
            container: container
        )
        let transaction = context.databaseTransaction.storageAccess

        while let offset = Int(exactly: targetOffset),
              offset < plan.targets.count {
            let target = plan.targets[offset]
            if activePartitions == nil {
                if target.usesDynamicDirectory {
                    let page = try await container.partitionCatalogPage(
                        entity: target.entity,
                        continuation: nextContinuation,
                        limit: 1,
                        transaction: transaction
                    )
                    guard let entry = page.entries.first else {
                        try container.completeSchemaIndexBuild(
                            entity: target.entity,
                            index: target.index,
                            transaction: transaction
                        )
                        targetOffset += 1
                        nextContinuation = nil
                        activeIsLast = false
                        activeStarted = false
                        continue
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
            if !activeStarted {
                let status = try await maintenance.status(
                    entity: target.entity,
                    index: target.index,
                    partitions: partitions,
                    transaction: transaction
                )
                if status.indexState == .readable {
                    try finishPartition(
                        target: target,
                        isLast: activeIsLast,
                        container: container,
                        transaction: transaction,
                        targetOffset: &targetOffset,
                        nextContinuation: &nextContinuation,
                        activePartitions: &activePartitions,
                        activeIsLast: &activeIsLast,
                        activeStarted: &activeStarted
                    )
                    continue
                }
            }

            let remaining = sliceLimit - completedWorkUnits
            guard remaining > 0 else {
                return .incomplete(
                    completedWorkUnits: completedWorkUnits,
                    state: DatabaseSchemaApplyJobState(
                        targetOffset: targetOffset,
                        nextPartitionContinuation: nextContinuation,
                        activePartitions: activePartitions,
                        activePartitionIsLast: activeIsLast,
                        activeBuildStarted: activeStarted
                    )
                )
            }
            let slice = try await maintenance.runRebuildSlice(
                entity: target.entity,
                index: target.index,
                partitions: partitions,
                generation: context.jobID,
                mode: activeStarted ? .resume : .start,
                maximumWorkUnits: remaining,
                transaction: transaction
            )
            guard slice.completedWorkUnits <= remaining else {
                throw DatabaseJobRuntimeError.sliceExceededBudget(
                    actual: slice.completedWorkUnits,
                    maximum: remaining
                )
            }
            completedWorkUnits += slice.completedWorkUnits
            if !slice.isComplete {
                guard slice.completedWorkUnits > 0 else {
                    throw DatabaseSchemaApplyJobError.sliceMadeNoProgress
                }
                return .incomplete(
                    completedWorkUnits: completedWorkUnits,
                    state: DatabaseSchemaApplyJobState(
                        targetOffset: targetOffset,
                        nextPartitionContinuation: nextContinuation,
                        activePartitions: activePartitions,
                        activePartitionIsLast: activeIsLast,
                        activeBuildStarted: true
                    )
                )
            }
            try finishPartition(
                target: target,
                isLast: activeIsLast,
                container: container,
                transaction: transaction,
                targetOffset: &targetOffset,
                nextContinuation: &nextContinuation,
                activePartitions: &activePartitions,
                activeIsLast: &activeIsLast,
                activeStarted: &activeStarted
            )
            if completedWorkUnits == sliceLimit,
               Int(exactly: targetOffset) != plan.targets.count {
                return .incomplete(
                    completedWorkUnits: completedWorkUnits,
                    state: DatabaseSchemaApplyJobState(
                        targetOffset: targetOffset,
                        nextPartitionContinuation: nextContinuation,
                        activePartitions: activePartitions,
                        activePartitionIsLast: activeIsLast,
                        activeBuildStarted: activeStarted
                    )
                )
            }
        }

        guard Int(exactly: targetOffset) == plan.targets.count else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        let job = JobIdentity(
            jobID: context.jobID,
            operation: try Self.job().identifier
        )
        return .complete(
            completedWorkUnits: completedWorkUnits,
            result: .applied(
                SchemaExecuteOperation.Applied(
                    previousFingerprint: plan.previousFingerprint,
                    fingerprint: plan.targetFingerprint,
                    schemaVersion: plan.schemaVersion,
                    generation: container.schemaGeneration,
                    job: job
                )
            )
        )
    }

    public func applyUnsuccessfulOutcome(
        plan: DatabaseSchemaApplyJobPlan,
        state: DatabaseSchemaApplyJobState,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        guard state.activeBuildStarted,
              let offset = Int(exactly: state.targetOffset),
              plan.targets.indices.contains(offset),
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
        let target = plan.targets[offset]
        try await DatabaseIndexMaintenanceRuntime(
            container: context.operationContext.container
        ).markFailed(
            entity: target.entity,
            index: target.index,
            partitions: partitions,
            generation: context.jobID,
            detail: detail,
            transaction: context.databaseTransaction.storageAccess
        )
    }

    private func finishPartition(
        target: DatabaseSchemaApplyJobPlan.Target,
        isLast: Bool,
        container: DBContainer,
        transaction: any TransactionAccess,
        targetOffset: inout UInt64,
        nextContinuation: inout ByteString?,
        activePartitions: inout FieldObject?,
        activeIsLast: inout Bool,
        activeStarted: inout Bool
    ) throws {
        activePartitions = nil
        activeStarted = false
        if isLast {
            try container.completeSchemaIndexBuild(
                entity: target.entity,
                index: target.index,
                transaction: transaction
            )
            let incremented = targetOffset.addingReportingOverflow(1)
            guard !incremented.overflow else {
                throw DatabaseJobRuntimeError.stateRevisionOverflow
            }
            targetOffset = incremented.partialValue
            nextContinuation = nil
        }
        activeIsLast = false
    }
}

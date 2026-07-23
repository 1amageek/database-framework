import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

public struct DatabaseMaintenanceOperationService: DatabaseMaintenanceService {
    private let context: DatabaseServerServiceContext
    private let identifierGenerator: AnyDatabaseUUIDGenerator

    init<IdentifierGenerator: DatabaseUUIDGenerator>(
        context: DatabaseServerServiceContext,
        identifierGenerator: IdentifierGenerator
    ) {
        self.context = context
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
    }

    public func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context operationContext: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<
        MaintenanceExecuteOperation
    > {
        try context.runtimeLimits.validate(request.budget)
        switch request.invocation {
        case .migrationStatus:
            let status = try await context.container.migrationStatus()
            return .encoding(
                .migrationStatus(
                    MaintenanceExecuteOperation.MigrationStatus(
                        currentVersion: status.currentVersion,
                        targetVersion: status.targetVersion,
                        pendingMigrationIdentifiers:
                            status.pendingMigrationIdentifiers
                    )
                )
            )
        case .runMigrations(let requestedTarget):
            let targetVersion = requestedTarget
                ?? context.container.schema.version
            let requestFingerprint = try maintenanceRequestFingerprint(request)
            let completedBefore: UInt64
            if let continuation = request.continuation {
                let decoded: DatabaseMigrationContinuation
                do {
                    decoded = try DatabaseEnvelopeCodec.decode(
                        DatabaseMigrationContinuation.self,
                        from: continuation,
                        limits: context.wireLimits
                    )
                } catch {
                    throw DatabaseMaintenanceRuntimeError.invalidContinuation
                }
                guard decoded.targetVersion == targetVersion,
                      decoded.requestFingerprint == requestFingerprint else {
                    throw DatabaseMaintenanceRuntimeError.invalidContinuation
                }
                completedBefore = decoded.completedWorkUnits
            } else {
                completedBefore = 0
            }
            let result = try await context.container.runMigrations(
                targetVersion: targetVersion,
                maximumStageCount: request.budget.maximumWorkUnits
            )
            let completed = completedBefore.addingReportingOverflow(
                result.completedStageCount
            )
            guard !completed.overflow else {
                throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                    "Migration work count overflowed"
                )
            }
            let continuation = result.isComplete
                ? nil
                : try DatabaseEnvelopeCodec.encode(
                    DatabaseMigrationContinuation(
                        targetVersion: targetVersion,
                        requestFingerprint: requestFingerprint,
                        completedWorkUnits: completed.partialValue
                    ),
                    limits: context.wireLimits
                )
            return .encoding(
                .execution(
                    MaintenanceExecuteOperation.ExecutionResult(
                        kind: .migrations,
                        completedWorkUnits: completed.partialValue,
                        isComplete: result.isComplete,
                        continuation: continuation
                    )
                )
            )
        case .indexStatus(let entity, let index, let partitions):
            let targetPage = try await DatabaseIndexStatusPager(
                container: context.container,
                wireLimits: context.wireLimits
            ).page(
                entity: entity,
                index: index,
                partitions: partitions,
                continuation: request.continuation,
                budget: request.budget
            )
            let runtime = DatabaseIndexMaintenanceRuntime(
                container: context.container,
                wireLimits: context.wireLimits
            )
            let statuses = try await context.container.newContext().withTransaction {
                transaction in
                var values: [DatabaseIndexMaintenanceStatus] = []
                values.reserveCapacity(targetPage.targets.count)
                for target in targetPage.targets {
                    values.append(
                        try await runtime.status(
                            entity: target.entity,
                            index: target.index,
                            partitions: target.partitions,
                            transaction: transaction.storageAccess
                        )
                    )
                }
                return values
            }
            return .encoding(
                .indexStatus(
                    MaintenanceExecuteOperation.IndexStatusPage(
                        indexes: statuses.map(wireStatus),
                        continuation: targetPage.continuation
                    )
                )
            )
        case .rebuildIndex(
            let entity,
            let index,
            let partitions,
            let batchSize
        ):
            guard batchSize > 0 else {
                throw DatabaseMaintenanceRuntimeError.invalidBatchSize(batchSize)
            }
            let requestFingerprint = try maintenanceRequestFingerprint(request)
            let generation: DatabaseUUID
            let mode: DatabaseIndexRebuildSliceMode
            if let continuation = request.continuation {
                do {
                    let decoded = try DatabaseEnvelopeCodec.decode(
                        DatabaseIndexRebuildContinuation.self,
                        from: continuation,
                        limits: context.wireLimits
                    )
                    guard decoded.requestFingerprint == requestFingerprint else {
                        throw DatabaseMaintenanceRuntimeError.invalidContinuation
                    }
                    generation = decoded.generation
                    mode = .resume
                } catch let error as DatabaseMaintenanceRuntimeError {
                    throw error
                } catch {
                    throw DatabaseMaintenanceRuntimeError.invalidContinuation
                }
            } else {
                generation = identifierGenerator.generate()
                mode = .start
            }
            let runtime = DatabaseIndexMaintenanceRuntime(
                container: context.container,
                wireLimits: context.wireLimits
            )
            return try await context.coordinator.execute(
                MaintenanceExecuteOperation.self,
                requestPayload: operationContext.requestPayload,
                context: operationContext,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                if case .start = mode {
                    _ = try await runtime.prepareResources(
                        entity: entity,
                        index: index,
                        partitions: partitions,
                        transaction: transactionContext.storageAccess
                    )
                }
                return try await runtime.runRebuildSlice(
                    entity: entity,
                    index: index,
                    partitions: partitions,
                    generation: generation,
                    mode: mode,
                    maximumWorkUnits: min(
                        request.budget.maximumWorkUnits,
                        UInt64(batchSize),
                        DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
                    ),
                    transaction: transactionContext.storageAccess
                )
            } makeResponse: { slice, _ in
                let continuation = slice.isComplete
                    ? nil
                    : try DatabaseEnvelopeCodec.encode(
                        DatabaseIndexRebuildContinuation(
                            generation: generation,
                            requestFingerprint: requestFingerprint
                        ),
                        limits: context.wireLimits
                    )
                return .execution(
                    MaintenanceExecuteOperation.ExecutionResult(
                        kind: .indexRebuild,
                        completedWorkUnits: slice.indexedRecordCount,
                        isComplete: slice.isComplete,
                        continuation: continuation
                    )
                )
            }
        case .compact:
            throw DatabaseMaintenanceRuntimeError.compactionRequiresJob
        }
    }

    private func maintenanceRequestFingerprint(
        _ request: MaintenanceExecuteOperation.Request
    ) throws -> DatabaseBytes {
        let canonical = MaintenanceExecuteOperation.Request(
            invocation: request.invocation,
            continuation: nil,
            budget: request.budget
        )
        return DatabaseRequestDigest.compute(
            operation: .maintenanceExecute,
            payload: try DatabaseEnvelopeCodec.encode(
                canonical,
                limits: context.wireLimits
            )
        )
    }

    private func wireStatus(
        _ status: DatabaseIndexMaintenanceStatus
    ) -> MaintenanceExecuteOperation.IndexStatus {
        let wireState: MaintenanceExecuteOperation.IndexState
        switch status.indexState {
        case .readable:
            wireState = .ready
        case .writeOnly:
            wireState = status.rebuildRecord?.phase == .failed
                ? .failed
                : .building
        case .disabled:
            wireState = .stale
        }
        return MaintenanceExecuteOperation.IndexStatus(
            entity: status.entity,
            index: status.index,
            partitions: status.partitions,
            state: wireState,
            indexedRecordCount: status.rebuildRecord?.indexedRecordCount ?? 0,
            detail: status.rebuildRecord?.detail
        )
    }
}

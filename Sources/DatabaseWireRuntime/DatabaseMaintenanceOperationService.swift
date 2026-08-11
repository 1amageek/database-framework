import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

public struct DatabaseMaintenanceOperationService: DatabaseMaintenanceService {
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits
    private let identifierGenerator: AnyDatabaseUUIDGenerator

    init<IdentifierGenerator: DatabaseUUIDGenerator>(
        context: DatabaseOperationServiceContext,
        identifierGenerator: IdentifierGenerator
    ) {
        self.coordinator = context.coordinator
        self.runtimeLimits = context.runtimeLimits
        self.wireLimits = context.wireLimits
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
    }

    public func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context operationContext: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult {
        try runtimeLimits.validate(request.budget)
        let executor = try operationContext.requireDataExecutor()
        switch request.invocation {
        case .migrationStatus:
            let status = try await executor.migrationStatus()
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
                ?? executor.schema.version
            let requestFingerprint = try maintenanceRequestFingerprint(request)
            let completedBefore: UInt64
            if let continuation = request.continuation {
                let decoded: DatabaseMigrationContinuation
                do {
                    decoded = try DatabaseRuntimePayloadDecoder.decode(
                        DatabaseMigrationContinuation.self,
                        from: continuation,
                        limits: wireLimits
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
            let result = try await executor.runMigrations(
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
                : try DatabaseRuntimePayloadEncoder.encode(
                    DatabaseMigrationContinuation(
                        targetVersion: targetVersion,
                        requestFingerprint: requestFingerprint,
                        completedWorkUnits: completed.partialValue
                    ),
                    limits: wireLimits
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
            let runtime = executor.makeIndexMaintenanceRuntime()
            let (targetPage, statuses) = try await executor
                .withStorageTransaction(requiredAccess: .administer) {
                    transaction in
                let targetPage = try await executor.indexStatusPage(
                    entity: entity,
                    index: index,
                    partitions: partitions,
                    continuation: request.continuation,
                    budget: request.budget,
                    wireLimits: wireLimits,
                    transaction: transaction
                )
                var values: [DatabaseIndexMaintenanceStatus] = []
                values.reserveCapacity(targetPage.targets.count)
                for target in targetPage.targets {
                    values.append(
                        try await runtime.status(
                            entity: target.entity,
                            index: target.index,
                            partitions: target.partitions,
                            transaction: transaction
                        )
                    )
                }
                return (targetPage, values)
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
            let generation: DatabaseTypes.UUID
            let mode: DatabaseIndexRebuildSliceMode
            if let continuation = request.continuation {
                do {
                    let decoded = try DatabaseRuntimePayloadDecoder.decode(
                        DatabaseIndexRebuildContinuation.self,
                        from: continuation,
                        limits: wireLimits
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
            let runtime = executor.makeIndexMaintenanceRuntime()
            let coordinated = try await coordinator.execute(
                operation: MaintenanceExecuteOperation.identifier,
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
            } makeResponse: {
                (
                    slice: DatabaseIndexRebuildSlice,
                    _: UInt64
                ) in
                let continuation = slice.isComplete
                    ? nil
                    : try DatabaseRuntimePayloadEncoder.encode(
                        DatabaseIndexRebuildContinuation(
                            generation: generation,
                            requestFingerprint: requestFingerprint
                        ),
                            limits: wireLimits
                    )
                return DatabaseOperationResponseEncoder(
                    MaintenanceExecuteOperation.self,
                    response: .execution(
                        MaintenanceExecuteOperation.ExecutionResult(
                            kind: .indexRebuild,
                            completedWorkUnits: slice.indexedEntityCount,
                            isComplete: slice.isComplete,
                            continuation: continuation
                        )
                    )
                )
            }
            return try MaintenanceExecutionResult(
                coordinated: coordinated,
                limits: wireLimits
            )
        case .compact:
            throw DatabaseMaintenanceRuntimeError.compactionRequiresJob
        }
    }

    private func maintenanceRequestFingerprint(
        _ request: MaintenanceExecuteOperation.Request
    ) throws -> ByteString {
        let canonical = MaintenanceExecuteOperation.Request(
            invocation: request.invocation,
            continuation: nil,
            budget: request.budget
        )
        return DatabaseRequestDigest.compute(
            operation: .maintenanceExecute,
            payload: try DatabaseWireEncoder(
                limits: wireLimits
            ).encodeRequestPayload(
                DatabaseOperations.maintenanceExecute,
                request: canonical
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
            wireState = status.rebuildState?.phase == .failed
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
            indexedEntityCount: status.rebuildState?.indexedEntityCount ?? 0,
            detail: status.rebuildState?.detail
        )
    }
}

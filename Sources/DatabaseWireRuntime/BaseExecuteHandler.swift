#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

/// Executes Base catalog reads and schedules durable lifecycle operations.
public struct BaseExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = BaseExecuteOperation

    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let jobService: AnyDatabaseJobService
    private let timeoutMilliseconds: UInt32

    public init(
        coordinator: DatabaseTransactionalOperationCoordinator,
        jobService: AnyDatabaseJobService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.coordinator = coordinator
        self.jobService = jobService
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public func requirement(
        for request: BaseExecuteOperation.Request
    ) throws -> DatabaseOperationRequirement {
        switch request.invocation {
        case .placements, .list:
            return DatabaseOperationRequirement(
                acceptedTargets: .database,
                access: .read,
                transaction: .read
            )
        case .describe:
            return DatabaseOperationRequirement(
                acceptedTargets: .base,
                access: .read,
                transaction: .read,
                baseAdmission: .administration
            )
        case .create, .legacyMigrationPlan, .legacyMigrationApply:
            return DatabaseOperationRequirement(
                acceptedTargets: .database,
                access: .administer,
                transaction: .write,
                permitsMigrationRequiredLayout: {
                    switch request.invocation {
                    case .legacyMigrationPlan, .legacyMigrationApply:
                        return true
                    default:
                        return false
                    }
                }()
            )
        case .retire, .activate, .delete, .placementPlan, .placementApply:
            return DatabaseOperationRequirement(
                acceptedTargets: .base,
                access: .administer,
                transaction: .write,
                baseAdmission: .administration
            )
        }
    }

    public func invoke(
        request: BaseExecuteOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        switch request.invocation {
        case .placements:
            let executor = try context.requireControlExecutor()
            let placements = try await executor.withTransaction(
                requiredAccess: .read,
                configuration: .readOnly
            ) { _ in
                executor.placementIDs().map {
                        BaseExecuteOperation.PlacementDescription(
                            id: $0,
                            isDefault: $0 == executor.defaultPlacementID
                        )
                    }
            }
            return DatabaseOperationResult(
                BaseExecuteOperation.self,
                response: .placements(placements)
            )

        case .list:
            let executor = try context.requireControlExecutor()
            let records = try await executor.loadBases()
            return DatabaseOperationResult(
                BaseExecuteOperation.self,
                response: .bases(records.map(Self.description))
            )

        case .describe:
            let id = try baseID(context)
            try await authorizeBase(context, access: .read)
            let record = try await load(id, context: context)
            return DatabaseOperationResult(
                BaseExecuteOperation.self,
                response: .base(Self.description(record))
            )

        case .placementPlan(let destination, let expectedRevision):
            let id = try baseID(context)
            try await authorizeBase(context, access: .administer)
            let executor = try context.requireBaseExecutor()
            try executor.requirePlacement(destination)
            let plan = try await {
                let record = try await executor.loadRecord()
                guard record.lifecycle == .retired else {
                    throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                        baseID: id,
                        from: record.lifecycle.rawValue,
                        to: DatabaseBaseLifecycleState.moving.rawValue
                    )
                }
                guard record.revision == expectedRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: expectedRevision,
                        actual: record.revision
                    )
                }
                let (resultingRevision, overflow) = record.revision
                    .addingReportingOverflow(2)
                guard !overflow else {
                    throw DatabaseBaseCatalogError.corruptedRecord(id)
                }
                return try BaseExecuteOperation.Plan(
                    action: .move,
                    currentRevision: record.revision,
                    resultingRevision: resultingRevision,
                    destinationPlacementID: destination,
                    requiresJob: true
                )
            }()
            return DatabaseOperationResult(
                BaseExecuteOperation.self,
                response: .plan(plan)
            )

        case .legacyMigrationPlan(
            let baseID,
            let placementID,
            _
        ):
            let executor = try context.requireControlExecutor()
            try await executor.withTransaction(
                requiredAccess: .administer,
                configuration: .readOnly
            ) { _ in () }
            guard executor.layoutStatus == .migrationRequired else {
                throw DatabaseLegacyLayoutMigrationError.layoutIsCurrent
            }
            try executor.requirePlacement(placementID)
            let existing = try await executor.loadBase(baseID)
            if let existing {
                guard existing.lifecycle == .provisioning,
                      existing.placementID == placementID,
                      existing.revision == 1 else {
                    throw DatabaseLegacyLayoutMigrationError
                        .destinationBaseExists(baseID)
                }
            }
            let inventory = try await executor.legacyLayoutInventory()
            let actualFingerprint = try await executor
                .legacyLayoutFingerprint(inventory: inventory)
            return DatabaseOperationResult(
                BaseExecuteOperation.self,
                response: .plan(try BaseExecuteOperation.Plan(
                    action: .migrateLegacyLayout,
                    currentRevision: 0,
                    resultingRevision: 2,
                    layoutFingerprint: DatabaseLayoutFingerprint(
                        actualFingerprint
                    ),
                    requiresJob: true
                ))
            )

        case .placementApply(_, _, let key):
            try requireIdempotencyKey(key, context: context)
            return try await startLifecycleJob(
                request,
                job: try DatabaseBaseLifecycleResumableOperation.job(),
                context: context,
                limits: limits
            )

        case .legacyMigrationApply(_, _, _, let fingerprint, _, let key):
            try requireIdempotencyKey(key, context: context)
            let executor = try context.requireControlExecutor()
            let inventory = try await executor.legacyLayoutInventory()
            guard try await executor.legacyLayoutFingerprint(
                inventory: inventory
            ) == fingerprint.bytes else {
                throw DatabaseLegacyLayoutMigrationError.fingerprintMismatch
            }
            return try await startLifecycleJob(
                request,
                job: try DatabaseLegacyLayoutMigrationResumableOperation.job(),
                context: context,
                limits: limits
            )

        case .create(_, _, _, _, let key),
             .retire(_, let key),
             .activate(_, let key),
             .delete(_, let key):
            try requireIdempotencyKey(key, context: context)
            return try await startLifecycleJob(
                request,
                job: try DatabaseBaseLifecycleResumableOperation.job(),
                context: context,
                limits: limits
            )
        }
    }

    private func startLifecycleJob(
        _ request: BaseExecuteOperation.Request,
        job: JobOperation<
            BaseExecuteOperation.Request,
            BaseExecuteOperation.Response
        >,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        _ = limits
        let startRequest = JobStartOperation.Request(
            target: context.target,
            operation: job.identifier,
            requestPayload: context.requestPayload,
            maximumSliceWorkUnits: 1
        )
        let coordinated: DatabaseCoordinatedOperationResponse
        switch context.target {
        case .database:
            coordinated = try await coordinator.executeControlMetadata(
                operation: .baseExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await jobService.createPersistentJob(
                    startRequest,
                    context: context,
                    transaction: transaction
                )
            } makeResponse: { job, _ in
                DatabaseOperationResponseEncoder(
                    BaseExecuteOperation.self,
                    response: .job(job)
                )
            }
        case .base:
            try await authorizeBase(context, access: .administer)
            let executor = try context.requireBaseExecutor()
            coordinated = try await coordinator
                .executeControlMetadataAfterTargetAuthorizationStaged(
                    operation: .baseExecute,
                    requestPayload: context.requestPayload,
                    context: context,
                    timeoutMilliseconds: timeoutMilliseconds
                ) {
                    try await executor.withAdministrationTransaction(
                        requiredAccess: .administer,
                        configuration: .batch
                    ) { transaction in
                        try await jobService.preparePersistentJob(
                            startRequest,
                            context: context,
                            transaction: transaction
                        )
                    }
                } body: { prepared, transaction in
                    try await jobService.storePreparedPersistentJob(
                        prepared,
                        transaction: transaction
                    )
                } makeResponse: { job, _ in
                    DatabaseOperationResponseEncoder(
                        BaseExecuteOperation.self,
                        response: .job(job)
                    )
                }
        case .composition:
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
        try await jobService.recoverPersistentJobSchedule()
        return coordinated.result
    }

    private func authorizeBase(
        _ context: DatabaseOperationContext,
        access: Security.Access
    ) async throws {
        try await context.requireBaseExecutor().authorize(access)
    }

    private func load(
        _ id: Base.ID,
        context: DatabaseOperationContext
    ) async throws -> DatabaseBaseRecord {
        let executor = try context.requireBaseExecutor()
        guard executor.baseID == id else {
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
        return try await executor.loadRecord()
    }

    private func baseID(
        _ context: DatabaseOperationContext
    ) throws -> Base.ID {
        guard case .base(let id) = context.target else {
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
        return id
    }

    private func requireIdempotencyKey(
        _ key: String,
        context: DatabaseOperationContext
    ) throws {
        guard context.metadata.idempotencyKey == key else {
            throw DatabaseAdministrationError.idempotencyKeyMismatch
        }
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

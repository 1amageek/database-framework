import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

/// Immutable result of validating and compiling a persistent job in the
/// request target's transaction domain. It is later stored in the control
/// domain without reopening or retaining the target transaction.
package struct DatabasePreparedPersistentJob: Sendable {
    let identity: JobIdentity
    let specification: DatabasePersistentJobStore.PreparedSpecification
    let plan: DatabasePersistentJobPlan
    let state: DatabasePersistentJobState
}

public final class DatabasePersistentJobService:
    DatabaseJobService,
    DatabasePersistentJobCreating,
    Sendable {
    public var jobOperations: [JobOperationIdentifier] {
        registry.identifiers
    }

    public func baseAdmission(
        for operation: JobOperationIdentifier
    ) throws -> DatabaseBaseAdmissionKind {
        try registry.resolve(operation).baseAdmission
    }

    private let store: DatabasePersistentJobStore
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let registry: DatabaseResumableOperationRegistry
    private let runner: DatabasePersistentJobRunner
    private let clock: AnyDatabaseWallClock
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let configuration: DatabaseJobRuntimeConfiguration
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits
    private let storageLimits: DatabasePersistentJobStorageLimits

    init(
        store: DatabasePersistentJobStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        registry: DatabaseResumableOperationRegistry,
        runner: DatabasePersistentJobRunner,
        clock: AnyDatabaseWallClock,
        identifierGenerator: AnyDatabaseUUIDGenerator,
        configuration: DatabaseJobRuntimeConfiguration,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) {
        self.store = store
        self.coordinator = coordinator
        self.registry = registry
        self.runner = runner
        self.clock = clock
        self.identifierGenerator = identifierGenerator
        self.configuration = configuration
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.storageLimits = storageLimits
    }

    public func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStartExecutionResult {
        try validate(request)
        let runtimeLimits = self.runtimeLimits
        let wireLimits = self.wireLimits
        let requestPayload = context.requestPayload
        let service = self
        let coordinated: DatabaseCoordinatedOperationResponse
        switch context.target {
        case .database:
            coordinated = try await coordinator.executeControlMetadata(
                operation: JobStartOperation.identifier,
                requestPayload: requestPayload,
                context: context,
                timeoutMilliseconds: runtimeLimits.maximumTimeoutMilliseconds
            ) { transactionContext in
                try await service.createPersistentJob(
                    request,
                    context: context,
                    transaction: transactionContext
                )
            } makeResponse: { job, _ in
                DatabaseOperationResponseEncoder(
                    JobStartOperation.self,
                    response: JobStartOperation.Response(job: job)
                )
            }
        case .base:
            #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
            let executor = try context.requireBaseExecutor()
            let baseAdmission = try baseAdmission(for: request.operation)
            guard context.requirement.baseAdmission == baseAdmission else {
                throw DatabaseJobRuntimeError.invalidTarget
            }
            coordinated = try await coordinator
                .executeControlMetadataAfterTargetAuthorizationStaged(
                    operation: JobStartOperation.identifier,
                    requestPayload: requestPayload,
                    context: context,
                    timeoutMilliseconds:
                        runtimeLimits.maximumTimeoutMilliseconds
                ) {
                    switch baseAdmission {
                    case .activeData:
                        return try await executor.withActiveDataTransaction(
                            requiredAccess: .administer,
                            configuration: .batch
                        ) { transaction in
                            try await service.preparePersistentJob(
                                request,
                                context: context,
                                transaction: transaction
                            )
                        }
                    case .administration, .lifecycleJob:
                        return try await executor.withAdministrationTransaction(
                            requiredAccess: .administer,
                            configuration: .batch
                        ) { transaction in
                            try await service.preparePersistentJob(
                                request,
                                context: context,
                                transaction: transaction
                            )
                        }
                    }
                } body: { prepared, transactionContext in
                    try await service.storePreparedPersistentJob(
                        prepared,
                        transaction: transactionContext
                    )
                } makeResponse: { job, _ in
                    DatabaseOperationResponseEncoder(
                        JobStartOperation.self,
                        response: JobStartOperation.Response(job: job)
                    )
                }
            #else
            throw DatabaseJobRuntimeError.invalidTarget
            #endif
        case .composition:
            throw DatabaseJobRuntimeError.invalidTarget
        }
        try await runner.recoverSchedule()
        return try JobStartExecutionResult(
            coordinated: coordinated,
            limits: wireLimits
        )
    }

    package func createPersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity {
        let prepared = try await preparePersistentJob(
            request,
            context: context,
            transaction: transaction
        )
        return try await storePreparedPersistentJob(
            prepared,
            transaction: transaction
        )
    }

    package func preparePersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> DatabasePreparedPersistentJob {
        try validate(request)
        guard request.target == context.target else {
            throw DatabaseJobRuntimeError.invalidTarget
        }
        let operation = try registry.resolve(request.operation)
        let jobID = identifierGenerator.generate()
        let createdAt = clock.now
        let compiled = try await operation.compile(
            requestPayload: request.requestPayload,
            context: DatabaseResumableOperationStartContext(
                jobID: jobID,
                maximumSliceWorkUnits: request.maximumSliceWorkUnits,
                transaction: transaction,
                operationContext: context
            ),
            limits: wireLimits,
            storageLimits: storageLimits
        )
        guard compiled.sliceTimeoutMilliseconds
                <= runtimeLimits.maximumTimeoutMilliseconds else {
            throw DatabaseRuntimeLimitError.invalidTimeout(
                requested: compiled.sliceTimeoutMilliseconds,
                maximum: runtimeLimits.maximumTimeoutMilliseconds
            )
        }
        try configuration.validate(
            sliceTimeoutMilliseconds: compiled.sliceTimeoutMilliseconds
        )

        let targetDigestPrefix = try DatabaseWireWriter.encode {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try request.target.encode(into: &writer)
        }
        let requestDigest = DatabaseRequestDigest.compute(
            jobOperation: request.operation,
            prefix: targetDigestPrefix,
            payload: request.requestPayload
        )
        let planDigest = DatabasePersistentJobDigest.plan(
            operation: request.operation,
            payload: compiled.planPayload
        )
        guard let principalIdentifier = context.authorization.principal?
                .identifier else {
            throw DatabaseJobAuthorizationError.referenceRequired
        }
        guard let authorizationReference =
                context.jobAuthorizationReference else {
            throw DatabaseJobAuthorizationError.referenceRequired
        }
        let specification = DatabasePersistentJobSpecification(
            jobID: jobID,
            operation: request.operation,
            target: request.target,
            requestDigest: requestDigest,
            requestID: context.requestID,
            traceID: context.metadata.traceID,
            principalIdentifier: principalIdentifier,
            authorizationReference: authorizationReference,
            maximumSliceWorkUnits: request.maximumSliceWorkUnits,
            sliceTimeoutMilliseconds: compiled.sliceTimeoutMilliseconds,
            retryPolicy: request.retryPolicy,
            planDigest: planDigest,
            createdAt: createdAt
        )
        try specification.validate()
        let preparedSpecification = try store.prepareSpecification(
            specification
        )
        let specificationDigest = preparedSpecification.digest
        let plan = DatabasePersistentJobPlan(
            jobID: jobID,
            operation: request.operation,
            specificationDigest: specificationDigest,
            payload: compiled.planPayload
        )
        let state = DatabasePersistentJobState(
            jobID: jobID,
            specificationDigest: specificationDigest,
            revision: 0,
            status: .pending,
            operationStatePayload: compiled.initialStatePayload,
            completedWorkUnits: 0,
            totalWorkUnits: nil,
            executionCount: 0,
            currentSliceAttempt: 0,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: createdAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: createdAt
        )
        let identity = JobIdentity(
            jobID: jobID,
            operation: request.operation,
            target: request.target
        )
        return DatabasePreparedPersistentJob(
            identity: identity,
            specification: preparedSpecification,
            plan: plan,
            state: state
        )
    }

    package func storePreparedPersistentJob(
        _ prepared: DatabasePreparedPersistentJob,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity {
        try await store.create(
            specification: prepared.specification,
            plan: prepared.plan,
            state: prepared.state,
            transaction: transaction.storageAccess
        )
        return prepared.identity
    }

    package func recoverPersistentJobSchedule() async throws {
        try await runner.recoverSchedule()
    }

    public func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        let snapshot = try await requiredSnapshot(request.job)
        try await authorizeTarget(context, snapshot: snapshot)
        let state = snapshot.state
        if state.status == .succeeded {
            _ = try await store.loadResultManifest(for: snapshot)
        }
        return try JobStatusOperation.Response(
            state: state.status,
            job: request.job,
            completedWorkUnits: state.completedWorkUnits,
            totalWorkUnits: state.totalWorkUnits,
            executionCount: state.executionCount,
            currentSliceAttempt: state.currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt:
                state.unsuccessfulOutcomeCommitAttempt,
            lastUnsuccessfulOutcomeCommitError:
                state.lastUnsuccessfulOutcomeCommitError,
            cancellationRequested: state.cancellationRequested,
            nextAttemptAt: state.nextAttemptAt,
            updatedAt: state.updatedAt
        )
    }

    public func result(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response {
        let snapshot = try await requiredSnapshot(request.job)
        try await authorizeTarget(context, snapshot: snapshot)
        switch snapshot.state.status {
        case .succeeded:
            let manifest = try await store.loadResultManifest(for: snapshot)
            let chunkIndex = try resultChunkIndex(
                request.continuation,
                job: request.job,
                manifest: manifest
            )
            let payload: ByteString
            if manifest.chunkCount == 0 {
                payload = []
            } else {
                payload = try await store.loadResultChunk(
                    manifest: manifest,
                    index: chunkIndex
                )
            }
            let nextIndex = chunkIndex.addingReportingOverflow(1)
            let continuation: JobResultOperation.Continuation?
            if !nextIndex.overflow,
               nextIndex.partialValue < manifest.chunkCount {
                continuation = try JobResultOperation.Continuation(
                    job: request.job,
                    responseDigest: manifest.responseDigest,
                    nextChunkIndex: nextIndex.partialValue
                )
            } else {
                continuation = nil
            }
            return .succeeded(
                job: request.job,
                responsePayloadPage: payload,
                totalResponseBytes: manifest.totalBytes,
                responseDigest: manifest.responseDigest,
                continuation: continuation
            )
        case .failed:
            guard let failure = snapshot.state.failure else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            return .failed(
                job: request.job,
                error: failure
            )
        case .cancelled:
            return .cancelled(job: request.job)
        case .pending, .running, .committingUnsuccessfulOutcome:
            throw DatabaseJobRuntimeError.resultNotReady(request.jobID)
        }
    }

    public func cancel(
        _ request: JobCancelOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobCancellationExecutionResult {
        let store = self.store
        let clock = self.clock
        let requestPayload = context.requestPayload
        let requestedSnapshot = try await requiredSnapshot(request.job)
        try await authorizeTarget(context, snapshot: requestedSnapshot)
        let mutation: @Sendable (DatabaseTransaction) async throws
            -> JobCancelOperation.Response = { transactionContext in
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                request.jobID,
                transaction: transaction
            ) else {
                throw DatabaseJobRuntimeError.jobNotFound(request.jobID)
            }
            guard snapshot.specification.operation == request.operation else {
                throw DatabaseJobRuntimeError.jobOperationMismatch(
                    expected: request.operation,
                    actual: snapshot.specification.operation
                )
            }
            guard snapshot.specification.target == request.target else {
                throw DatabaseJobRuntimeError.invalidTarget
            }
            let cancellationRequestedAt = max(
                clock.now,
                snapshot.state.updatedAt
            )
            let updated: DatabasePersistentJobState
            switch snapshot.state.status {
            case .pending:
                updated = try snapshot.state.schedulingUnsuccessfulOutcomeCommit(
                    .cancelled,
                    nextAttemptAt: cancellationRequestedAt,
                    updatedAt: cancellationRequestedAt
                )
            case .running:
                guard !snapshot.state.cancellationRequested else {
                    return try JobCancelOperation.Response(
                        job: request.job,
                        state: snapshot.state.status,
                        accepted: false
                    )
                }
                updated = try snapshot.state.requestingCancellation(
                    updatedAt: cancellationRequestedAt
                )
            case .committingUnsuccessfulOutcome, .succeeded, .failed, .cancelled:
                return try JobCancelOperation.Response(
                    job: request.job,
                    state: snapshot.state.status,
                    accepted: false
                )
            }
            try store.storeState(
                updated,
                replacing: snapshot.state,
                transaction: transaction
            )
            return try JobCancelOperation.Response(
                job: request.job,
                state: updated.status,
                accepted: true
            )
        }
        let coordinated: DatabaseCoordinatedOperationResponse
        switch context.target {
        case .database:
            coordinated = try await coordinator.executeControlMetadata(
                operation: JobCancelOperation.identifier,
                requestPayload: requestPayload,
                context: context,
                timeoutMilliseconds: runtimeLimits.maximumTimeoutMilliseconds,
                body: mutation
            ) { response, _ in
                DatabaseOperationResponseEncoder(
                    JobCancelOperation.self,
                    response: response
                )
            }
        case .base:
            #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
            coordinated = try await coordinator
                .executeControlMetadataAfterTargetAuthorization(
                    operation: JobCancelOperation.identifier,
                    requestPayload: requestPayload,
                    context: context,
                    timeoutMilliseconds:
                        runtimeLimits.maximumTimeoutMilliseconds,
                    body: mutation
                ) { response, _ in
                    DatabaseOperationResponseEncoder(
                        JobCancelOperation.self,
                        response: response
                    )
                }
            #else
            throw DatabaseJobRuntimeError.invalidTarget
            #endif
        case .composition:
            throw DatabaseJobRuntimeError.invalidTarget
        }
        try await runner.recoverSchedule()
        return try JobCancellationExecutionResult(
            coordinated: coordinated,
            limits: wireLimits
        )
    }

    public func runScheduledWork() async throws {
        try await runner.runScheduledWork()
    }

    private func requiredSnapshot(
        _ job: JobIdentity
    ) async throws -> DatabasePersistentJobSnapshot {
        guard let snapshot = try await store.load(job.jobID) else {
            throw DatabaseJobRuntimeError.jobNotFound(job.jobID)
        }
        guard snapshot.specification.operation == job.operation else {
            throw DatabaseJobRuntimeError.jobOperationMismatch(
                expected: job.operation,
                actual: snapshot.specification.operation
            )
        }
        guard snapshot.specification.target == job.target else {
            throw DatabaseJobRuntimeError.invalidTarget
        }
        return snapshot
    }

    private func authorizeTarget(
        _ context: DatabaseOperationContext,
        snapshot: DatabasePersistentJobSnapshot
    ) async throws {
        switch context.target {
        case .database:
            try await context.requireControlExecutor().withTransaction(
                requiredAccess: .administer,
                configuration: .readOnly
            ) { _ in () }
        case .base(let baseID):
            #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
            let executor = try context.requireBaseExecutor()
            guard executor.baseID == baseID else {
                throw DatabaseJobRuntimeError.invalidTarget
            }
            do {
                try await executor.authorize(.administer)
            } catch {
                guard Self.sameAuthenticatedPrincipal(
                    context.authorization,
                    snapshot.specification.principalIdentifier
                ), try await executor.permitsDeletionFinalization(
                        owner: ByteString(snapshot.specification.jobID.bytes)
                    ) else {
                    throw error
                }
            }
            #else
            _ = baseID
            throw DatabaseJobRuntimeError.invalidTarget
            #endif
        case .composition:
            throw DatabaseJobRuntimeError.invalidTarget
        }
    }

    private static func sameAuthenticatedPrincipal(
        _ lhs: AuthorizationContext,
        _ rhsIdentifier: String
    ) -> Bool {
        guard let lhsIdentifier = lhs.principal?.identifier else {
            return false
        }
        return lhsIdentifier == rhsIdentifier
    }

    private func resultChunkIndex(
        _ continuation: JobResultOperation.Continuation?,
        job: JobIdentity,
        manifest: DatabasePersistentJobResultManifest
    ) throws -> UInt32 {
        guard let continuation else {
            return 0
        }
        guard continuation.job == job,
              continuation.responseDigest == manifest.responseDigest,
              continuation.nextChunkIndex < manifest.chunkCount else {
            throw DatabaseJobRuntimeError.invalidResultContinuation
        }
        return continuation.nextChunkIndex
    }

    private func validate(_ request: JobStartOperation.Request) throws {
        guard request.maximumSliceWorkUnits > 0,
              request.maximumSliceWorkUnits <= runtimeLimits.maximumWorkUnits else {
            throw DatabaseRuntimeLimitError.invalidMaximumWorkUnits(
                requested: request.maximumSliceWorkUnits,
                maximum: runtimeLimits.maximumWorkUnits
            )
        }
        guard request.requestPayload.count <= wireLimits.maximumByteStringBytes else {
            throw DatabaseJobRuntimeError.requestPayloadTooLarge(
                actual: request.requestPayload.count,
                maximum: wireLimits.maximumByteStringBytes
            )
        }
        guard request.retryPolicy.maximumAttempts > 0,
              request.retryPolicy.maximumAttempts
                <= configuration.maximumSliceAttempts,
              request.retryPolicy.initialBackoffMilliseconds
                <= request.retryPolicy.maximumBackoffMilliseconds,
              request.retryPolicy.maximumBackoffMilliseconds
                <= configuration.maximumSliceRetryBackoffMilliseconds else {
            throw DatabaseJobRuntimeError.invalidRetryPolicy
        }
    }

    static func operationContext(
        for snapshot: DatabasePersistentJobSnapshot,
        container: DBContainer,
        baseContext: DatabaseContext?,
        baseAdmission: DatabaseBaseAdmissionKind,
        authorization: AuthorizationContext,
        wireLimits: DatabaseWireLimits
    ) -> DatabaseOperationContext {
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        return DatabaseOperationContext(
            container: container,
            target: snapshot.specification.target,
            baseContext: baseContext,
            composition: nil,
            requirement: DatabaseOperationRequirement(
                acceptedTargets: [.database, .base],
                access: .administer,
                transaction: .write,
                baseAdmission: baseAdmission
            ),
            requestID: snapshot.specification.requestID,
            metadata: OperationRequestMetadata(
                traceID: snapshot.specification.traceID
            ),
            authorization: authorization,
            requestPayload: [],
            requestDigest: snapshot.specification.requestDigest,
            wireLimits: wireLimits
        )
        #else
        _ = baseContext
        _ = baseAdmission
        return DatabaseOperationContext(
            container: container,
            target: snapshot.specification.target,
            requirement: DatabaseOperationRequirement(
                acceptedTargets: .database,
                access: .administer,
                transaction: .write
            ),
            requestID: snapshot.specification.requestID,
            metadata: OperationRequestMetadata(
                traceID: snapshot.specification.traceID
            ),
            authorization: authorization,
            requestPayload: [],
            requestDigest: snapshot.specification.requestDigest,
            wireLimits: wireLimits
        )
        #endif
    }
}

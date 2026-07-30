import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public final class DatabasePersistentJobService: DatabaseJobService, Sendable {
    public var jobOperations: [JobOperationIdentifier] {
        registry.identifiers
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
        let store = self.store
        let registry = self.registry
        let identifierGenerator = self.identifierGenerator
        let clock = self.clock
        let configuration = self.configuration
        let runtimeLimits = self.runtimeLimits
        let wireLimits = self.wireLimits
        let storageLimits = self.storageLimits
        let requestPayload = context.requestPayload
        let coordinated = try await coordinator.execute(
            operation: JobStartOperation.identifier,
            requestPayload: requestPayload,
            context: context,
            timeoutMilliseconds: runtimeLimits.maximumTimeoutMilliseconds
        ) { transactionContext in
            let operation = try registry.resolve(request.operation)
            let jobID = identifierGenerator.generate()
            let createdAt = clock.now
            let compiled = try await operation.compile(
                requestPayload: request.requestPayload,
                context: DatabaseResumableOperationStartContext(
                    jobID: jobID,
                    maximumSliceWorkUnits: request.maximumSliceWorkUnits,
                    transaction: transactionContext,
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

            let requestDigest = DatabaseRequestDigest.compute(
                jobOperation: request.operation,
                payload: request.requestPayload
            )
            let planDigest = DatabasePersistentJobDigest.plan(
                operation: request.operation,
                payload: compiled.planPayload
            )
            let specification = DatabasePersistentJobSpecification(
                jobID: jobID,
                operation: request.operation,
                requestDigest: requestDigest,
                requestID: context.requestID,
                traceID: context.metadata.traceID,
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
            try await store.create(
                specification: preparedSpecification,
                plan: plan,
                state: state,
                transaction: transactionContext.storageAccess
            )
            return JobIdentity(
                jobID: jobID,
                operation: request.operation
            )
        } makeResponse: { job, _ in
            return DatabaseOperationResponseEncoder(
                JobStartOperation.self,
                response: JobStartOperation.Response(job: job)
            )
        }
        try await runner.recoverSchedule()
        return try JobStartExecutionResult(
            coordinated: coordinated,
            limits: wireLimits
        )
    }

    public func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        _ = context
        let snapshot = try await requiredSnapshot(request.job)
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
        _ = context
        let snapshot = try await requiredSnapshot(request.job)
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
        let coordinated = try await coordinator.execute(
            operation: JobCancelOperation.identifier,
            requestPayload: requestPayload,
            context: context,
            timeoutMilliseconds: runtimeLimits.maximumTimeoutMilliseconds
        ) { transactionContext in
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
        } makeResponse: { response, _ in
            DatabaseOperationResponseEncoder(
                JobCancelOperation.self,
                response: response
            )
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
        return snapshot
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
        container: DBContainer
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: snapshot.specification.requestID,
            metadata: OperationRequestMetadata(
                traceID: snapshot.specification.traceID
            ),
            requestPayload: [],
            requestDigest: snapshot.specification.requestDigest
        )
    }
}

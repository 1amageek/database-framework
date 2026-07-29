import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public actor DatabasePersistentJobRunner {
    private struct LeasedJob: Sendable {
        enum Action: Sendable {
            case execute
            case commitUnsuccessfulOutcome
        }

        let snapshot: DatabasePersistentJobSnapshot
        let action: Action
    }

    private let container: DBContainer
    private let store: DatabasePersistentJobStore
    private let registry: DatabaseResumableOperationRegistry
    private let scheduler: AnyDatabaseJobScheduler
    private let clock: AnyDatabaseWallClock
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let errorMapper: AnyDatabaseErrorMapper
    private let configuration: DatabaseJobRuntimeConfiguration
    private let wireLimits: DatabaseWireLimits
    private let storageLimits: DatabasePersistentJobStorageLimits
    private let failureStoragePolicy: DatabasePersistentJobFailureStoragePolicy
    private let runnerID: DatabaseTypes.UUID

    init(
        container: DBContainer,
        store: DatabasePersistentJobStore,
        registry: DatabaseResumableOperationRegistry,
        scheduler: AnyDatabaseJobScheduler,
        clock: AnyDatabaseWallClock,
        identifierGenerator: AnyDatabaseUUIDGenerator,
        errorMapper: AnyDatabaseErrorMapper,
        configuration: DatabaseJobRuntimeConfiguration,
        wireLimits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits,
        failureStoragePolicy: DatabasePersistentJobFailureStoragePolicy,
        runnerID: DatabaseTypes.UUID
    ) {
        self.container = container
        self.store = store
        self.registry = registry
        self.scheduler = scheduler
        self.clock = clock
        self.identifierGenerator = identifierGenerator
        self.errorMapper = errorMapper
        self.configuration = configuration
        self.wireLimits = wireLimits
        self.storageLimits = storageLimits
        self.failureStoragePolicy = failureStoragePolicy
        self.runnerID = runnerID
    }

    public func recoverSchedule() async throws {
        try await scheduleNext()
    }

    public func notifyWorkAvailable(at timestamp: Timestamp) async throws {
        try await scheduler.ensureWakeUp(noLaterThan: timestamp)
    }

    public func runScheduledWork() async throws {
        let dueJobs = try await store.dueJobs(
            through: clock.now(),
            limit: configuration.maximumJobsPerRun
        )
        var firstProcessingError: (any Error)?
        for dueJob in dueJobs {
            do {
                try await run(dueJob)
            } catch {
                if firstProcessingError == nil {
                    firstProcessingError = error
                }
            }
        }
        do {
            try await scheduleNext()
        } catch {
            if let firstProcessingError {
                throw DatabaseJobSchedulingRecoveryError(
                    processingError: firstProcessingError,
                    schedulingError: error
                )
            }
            throw error
        }
        if let firstProcessingError {
            throw firstProcessingError
        }
    }

    private func run(_ dueJob: DatabasePersistentJobDueEntry) async throws {
        guard let leasedJob = try await acquireLease(for: dueJob) else {
            return
        }
        let leasedSnapshot = leasedJob.snapshot
        let operationContext = DatabasePersistentJobService.operationContext(
            for: leasedSnapshot,
            container: container
        )
        switch leasedJob.action {
        case .execute:
            let operation: AnyDatabaseResumableOperation
            do {
                operation = try registry.resolve(
                    leasedSnapshot.specification.operation
                )
            } catch {
                try await handleFailure(
                    error,
                    snapshot: leasedSnapshot,
                    operationContext: operationContext
                )
                return
            }
            do {
                switch try operation.commitModel(
                    planPayload: leasedSnapshot.plan.payload,
                    limits: wireLimits,
                    storageLimits: storageLimits
                ) {
                case .atomicWithJobState:
                    try await execute(
                        operation,
                        snapshot: leasedSnapshot,
                        operationContext: operationContext
                    )
                case .operationCheckpointed:
                    try await executeCheckpointed(
                        operation,
                        snapshot: leasedSnapshot,
                        operationContext: operationContext
                    )
                }
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                try await handleFailure(
                    error,
                    snapshot: leasedSnapshot,
                    operationContext: operationContext
                )
            }
        case .commitUnsuccessfulOutcome:
            guard let outcome = leasedSnapshot.state.pendingUnsuccessfulOutcome else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            let operation: AnyDatabaseResumableOperation
            do {
                operation = try registry.resolve(
                    leasedSnapshot.specification.operation
                )
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                let commitError = DatabaseJobUnsuccessfulOutcomeCommitError(
                    jobID: leasedSnapshot.specification.jobID,
                    outcome: outcome,
                    underlyingError: error
                )
                try await recordUnsuccessfulOutcomeCommitFailure(
                    commitError,
                    snapshot: leasedSnapshot
                )
                throw commitError
            }
            do {
                try await commitUnsuccessfulOutcome(
                    outcome,
                    operation: operation,
                    snapshot: leasedSnapshot,
                    operationContext: operationContext
                )
            } catch is CancellationError {
                throw CancellationError()
            } catch let error as DatabaseJobUnsuccessfulOutcomeCommitError {
                try await recordUnsuccessfulOutcomeCommitFailure(
                    error,
                    snapshot: leasedSnapshot
                )
                throw error
            }
        }
    }

    private func acquireLease(
        for dueJob: DatabasePersistentJobDueEntry
    ) async throws -> LeasedJob? {
        let leaseToken = identifierGenerator.generate()
        let store = self.store
        let runnerID = self.runnerID
        let maximumSliceAttempts = configuration.maximumSliceAttempts
        let leaseDurationMilliseconds = configuration.leaseDurationMilliseconds
        let clock = self.clock
        let container = self.container

        return try await container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let observedNow = clock.now()
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                dueJob.jobID,
                transaction: transaction
            ) else {
                throw DatabaseJobRuntimeError.jobNotFound(dueJob.jobID)
            }
            let state = snapshot.state
            guard state.revision == dueJob.stateRevision else {
                return nil
            }
            guard let scheduledAt = state.scheduledAt,
                  scheduledAt <= observedNow else {
                return nil
            }
            let transitionAt = max(observedNow, state.updatedAt)
            let leaseExpiresAt = try Self.adding(
                milliseconds: leaseDurationMilliseconds,
                to: transitionAt
            )
            let allowedAttempts = min(
                snapshot.specification.retryPolicy.maximumAttempts,
                maximumSliceAttempts
            )
            let action: LeasedJob.Action
            if state.status == .committingUnsuccessfulOutcome {
                action = .commitUnsuccessfulOutcome
            } else {
                if state.cancellationRequested {
                    let committing = try state.schedulingUnsuccessfulOutcomeCommit(
                        .cancelled,
                        nextAttemptAt: transitionAt,
                        updatedAt: transitionAt
                    )
                    try store.storeState(
                        committing,
                        replacing: state,
                        transaction: transaction
                    )
                    return nil
                }
                if state.currentSliceAttempt >= allowedAttempts {
                    let committing = try state.schedulingUnsuccessfulOutcomeCommit(
                        .failed(RemoteOperationError(
                            category: .internalFailure,
                            code: "JOB_ATTEMPTS_EXHAUSTED",
                            message: "Job exhausted its configured attempts",
                            retryability: .never
                        )),
                        nextAttemptAt: transitionAt,
                        updatedAt: transitionAt
                    )
                    try store.storeState(
                        committing,
                        replacing: state,
                        transaction: transaction
                    )
                    return nil
                }
                action = .execute
            }
            let leased = try state.acquiringLease(
                owner: runnerID,
                token: leaseToken,
                expiresAt: leaseExpiresAt,
                updatedAt: transitionAt
            )
            try store.storeState(
                leased,
                replacing: state,
                transaction: transaction
            )
            return LeasedJob(
                snapshot: DatabasePersistentJobSnapshot(
                    specification: snapshot.specification,
                    specificationDigest: snapshot.specificationDigest,
                    plan: snapshot.plan,
                    state: leased
                ),
                action: action
            )
        }
    }

    private func execute(
        _ operation: AnyDatabaseResumableOperation,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let wireLimits = self.wireLimits
        let storageLimits = self.storageLimits
        let clock = self.clock
        let leasedState = snapshot.state
        let transactionConfiguration = Self.batchConfiguration(
            timeoutMilliseconds: snapshot.specification.sliceTimeoutMilliseconds
        )

        try await container.newContext().withTransaction(
            configuration: transactionConfiguration
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateOperationLease(
                currentState,
                expected: leasedState,
                runnerID: runnerID,
                now: clock.now()
            )
            let current = DatabasePersistentJobSnapshot(
                specification: snapshot.specification,
                specificationDigest: snapshot.specificationDigest,
                plan: snapshot.plan,
                state: currentState
            )
            if currentState.cancellationRequested {
                let cancellationRequestedAt = max(
                    clock.now(),
                    current.state.updatedAt
                )
                let committing = try current.state.schedulingUnsuccessfulOutcomeCommit(
                    .cancelled,
                    nextAttemptAt: cancellationRequestedAt,
                    updatedAt: cancellationRequestedAt
                )
                try store.storeState(
                    committing,
                    replacing: current.state,
                    transaction: transaction
                )
                return
            }

            let slice = try await operation.runSlice(
                planPayload: current.plan.payload,
                statePayload: current.state.operationStatePayload,
                maximumWorkUnits: current.specification.maximumSliceWorkUnits,
                context: DatabaseResumableOperationContext(
                    jobID: current.specification.jobID,
                    completedWorkUnitsBeforeSlice: current.state.completedWorkUnits,
                    transaction: transactionContext,
                    operationContext: operationContext
                ),
                limits: wireLimits,
                storageLimits: storageLimits
            )
            try Self.validate(
                slice,
                maximumWorkUnits: current.specification.maximumSliceWorkUnits
            )
            let (cumulativeWorkUnits, overflow) = current.state
                .completedWorkUnits
                .addingReportingOverflow(slice.completedWorkUnits)
            guard !overflow else {
                throw DatabaseJobRuntimeError.workUnitOverflow
            }
            try Self.validateTotalWorkUnits(
                existing: current.state.totalWorkUnits,
                reported: slice.totalWorkUnits,
                completed: cumulativeWorkUnits
            )
            let reportedTotalWorkUnits = slice.totalWorkUnits
            let completedAt = max(clock.now(), current.state.updatedAt)
            let updated: DatabasePersistentJobState
            switch slice.outcome {
            case .complete(let responsePayload):
                let responseDigest = try await store.storeResult(
                    responsePayload,
                    snapshot: current,
                    completedAt: completedAt,
                    transaction: transaction
                )
                updated = try current.state.succeeding(
                    cumulativeWorkUnits: cumulativeWorkUnits,
                    totalWorkUnits: reportedTotalWorkUnits ?? cumulativeWorkUnits,
                    resultDigest: responseDigest,
                    updatedAt: completedAt
                )
            case .incomplete(let operationStatePayload):
                updated = try current.state.continuing(
                    operationStatePayload: operationStatePayload,
                    cumulativeWorkUnits: cumulativeWorkUnits,
                    totalWorkUnits: reportedTotalWorkUnits
                        ?? current.state.totalWorkUnits,
                    nextAttemptAt: completedAt,
                    updatedAt: completedAt
                )
            }
            try store.storeState(
                updated,
                replacing: current.state,
                transaction: transaction
            )
        }
    }

    private func executeCheckpointed(
        _ operation: AnyDatabaseResumableOperation,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let wireLimits = self.wireLimits
        let storageLimits = self.storageLimits
        let clock = self.clock
        let leasedState = snapshot.state

        let slice = try await operation.runCheckpointedSlice(
            planPayload: snapshot.plan.payload,
            statePayload: leasedState.operationStatePayload,
            maximumWorkUnits: snapshot.specification.maximumSliceWorkUnits,
            context: DatabaseCheckpointedResumableOperationContext(
                jobID: snapshot.specification.jobID,
                completedWorkUnitsBeforeSlice: leasedState.completedWorkUnits,
                operationContext: operationContext
            ),
            limits: wireLimits,
            storageLimits: storageLimits
        )
        try Self.validate(
            slice,
            maximumWorkUnits: snapshot.specification.maximumSliceWorkUnits
        )

        try await container.newContext().withTransaction(
            configuration: Self.batchConfiguration(
                timeoutMilliseconds: snapshot.specification
                    .sliceTimeoutMilliseconds
            )
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateOperationLease(
                currentState,
                expected: leasedState,
                runnerID: runnerID,
                now: clock.now()
            )
            let current = DatabasePersistentJobSnapshot(
                specification: snapshot.specification,
                specificationDigest: snapshot.specificationDigest,
                plan: snapshot.plan,
                state: currentState
            )
            let (cumulativeWorkUnits, overflow) = current.state
                .completedWorkUnits
                .addingReportingOverflow(slice.completedWorkUnits)
            guard !overflow else {
                throw DatabaseJobRuntimeError.workUnitOverflow
            }
            try Self.validateTotalWorkUnits(
                existing: current.state.totalWorkUnits,
                reported: slice.totalWorkUnits,
                completed: cumulativeWorkUnits
            )
            let reportedTotalWorkUnits = slice.totalWorkUnits
            let completedAt = max(clock.now(), current.state.updatedAt)
            let updated: DatabasePersistentJobState
            switch slice.outcome {
            case .complete(let responsePayload):
                let responseDigest = try await store.storeResult(
                    responsePayload,
                    snapshot: current,
                    completedAt: completedAt,
                    transaction: transaction
                )
                updated = try current.state.succeeding(
                    cumulativeWorkUnits: cumulativeWorkUnits,
                    totalWorkUnits: reportedTotalWorkUnits ?? cumulativeWorkUnits,
                    resultDigest: responseDigest,
                    updatedAt: completedAt
                )
            case .incomplete(let operationStatePayload):
                let totalWorkUnits = reportedTotalWorkUnits
                    ?? current.state.totalWorkUnits
                if current.state.cancellationRequested {
                    updated = try current.state.schedulingCancellationOutcomeCommitAfterCheckpoint(
                        operationStatePayload: operationStatePayload,
                        cumulativeWorkUnits: cumulativeWorkUnits,
                        totalWorkUnits: totalWorkUnits,
                        updatedAt: completedAt
                    )
                } else {
                    updated = try current.state.continuing(
                        operationStatePayload: operationStatePayload,
                        cumulativeWorkUnits: cumulativeWorkUnits,
                        totalWorkUnits: totalWorkUnits,
                        nextAttemptAt: completedAt,
                        updatedAt: completedAt
                    )
                }
            }
            try store.storeState(
                updated,
                replacing: current.state,
                transaction: transaction
            )
        }
    }

    private func handleFailure(
        _ error: any Error,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let observedFailureAt = clock.now()
        let remoteError = errorMapper.remoteError(
            for: error,
            context: operationContext,
            limits: wireLimits
        )
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let configuration = self.configuration
        let clock = self.clock
        let leasedState = snapshot.state
        let failureStoragePolicy = self.failureStoragePolicy

        try await container.newContext().withTransaction(
            configuration: Self.batchConfiguration(
                timeoutMilliseconds: snapshot.specification
                    .sliceTimeoutMilliseconds
            )
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateOperationLease(
                currentState,
                expected: leasedState,
                runnerID: runnerID,
                now: clock.now()
            )
            let updated: DatabasePersistentJobState
            let failedAt = max(observedFailureAt, currentState.updatedAt)
            if currentState.cancellationRequested {
                updated = try currentState.schedulingUnsuccessfulOutcomeCommit(
                    .cancelled,
                    nextAttemptAt: failedAt,
                    updatedAt: failedAt
                )
            } else {
                let persistentFailure = try failureStoragePolicy.storableFailure(
                    for: remoteError
                )
                if persistentFailure.retryability != .never,
                   currentState.currentSliceAttempt
                    < snapshot.specification.retryPolicy.maximumAttempts,
                   currentState.currentSliceAttempt
                    < configuration.maximumSliceAttempts {
                    let backoff = Self.backoffMilliseconds(
                        retryability: persistentFailure.retryability,
                        policy: snapshot.specification.retryPolicy,
                        currentSliceAttempt: currentState.currentSliceAttempt,
                        maximum:
                            configuration.maximumSliceRetryBackoffMilliseconds
                    )
                    updated = try currentState.retrying(
                        at: try Self.adding(
                            milliseconds: backoff,
                            to: failedAt
                        ),
                        updatedAt: failedAt
                    )
                } else {
                    updated = try currentState.schedulingUnsuccessfulOutcomeCommit(
                        .failed(persistentFailure),
                        nextAttemptAt: failedAt,
                        updatedAt: failedAt
                    )
                }
            }
            try store.storeState(
                updated,
                replacing: currentState,
                transaction: transaction
            )
        }
    }

    private func commitUnsuccessfulOutcome(
        _ outcome: DatabaseJobUnsuccessfulOutcome,
        operation: AnyDatabaseResumableOperation,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let wireLimits = self.wireLimits
        let storageLimits = self.storageLimits
        let clock = self.clock
        let leasedState = snapshot.state
        do {
            try await container.newContext().withTransaction(
                configuration: Self.batchConfiguration(
                    timeoutMilliseconds: snapshot.specification
                        .sliceTimeoutMilliseconds
                )
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                let currentState = try await store.loadState(
                    snapshot.specification.jobID,
                    specificationDigest: snapshot.specificationDigest,
                    transaction: transaction
                )
                try Self.validateUnsuccessfulOutcomeLease(
                    currentState,
                    expected: leasedState,
                    runnerID: runnerID,
                    now: clock.now()
                )
                guard currentState.pendingUnsuccessfulOutcome == outcome else {
                    throw DatabaseJobRuntimeError.invalidStateTransition
                }
                try await operation.applyUnsuccessfulOutcome(
                    planPayload: snapshot.plan.payload,
                    statePayload: currentState.operationStatePayload,
                    outcome: outcome,
                    context: DatabaseResumableOperationContext(
                        jobID: snapshot.specification.jobID,
                        completedWorkUnitsBeforeSlice:
                            currentState.completedWorkUnits,
                        transaction: transactionContext,
                        operationContext: operationContext
                    ),
                    limits: wireLimits,
                    storageLimits: storageLimits
                )
                try store.storeState(
                    try currentState.completingUnsuccessfulOutcomeCommit(
                        updatedAt: max(clock.now(), currentState.updatedAt)
                    ),
                    replacing: currentState,
                    transaction: transaction
                )
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseJobUnsuccessfulOutcomeCommitError(
                jobID: snapshot.specification.jobID,
                outcome: outcome,
                underlyingError: error
            )
        }
    }

    private func recordUnsuccessfulOutcomeCommitFailure(
        _ error: DatabaseJobUnsuccessfulOutcomeCommitError,
        snapshot: DatabasePersistentJobSnapshot
    ) async throws {
        let diagnostic = RemoteOperationError(
            category: .internalFailure,
            code: "JOB_UNSUCCESSFUL_OUTCOME_COMMIT_FAILED",
            message: "The operation unsuccessful outcome could not be committed",
            retryability: .backoff
        )
        let observedFailureAt = clock.now()
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let leasedState = snapshot.state
        let initialBackoffMilliseconds =
            configuration.unsuccessfulOutcomeCommitInitialBackoffMilliseconds
        let maximumBackoffMilliseconds =
            configuration.unsuccessfulOutcomeCommitMaximumBackoffMilliseconds
        try await container.newContext().withTransaction(
            configuration: Self.batchConfiguration(
                timeoutMilliseconds: snapshot.specification
                    .sliceTimeoutMilliseconds
            )
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateUnsuccessfulOutcomeLeaseIdentity(
                currentState,
                expected: leasedState,
                runnerID: runnerID
            )
            guard currentState.pendingUnsuccessfulOutcome == error.outcome else {
                throw DatabaseJobRuntimeError.invalidStateTransition
            }
            let failedAt = max(observedFailureAt, currentState.updatedAt)
            let backoff = Self.unsuccessfulOutcomeCommitBackoffMilliseconds(
                initial: initialBackoffMilliseconds,
                commitAttempt: currentState.unsuccessfulOutcomeCommitAttempt,
                maximum: maximumBackoffMilliseconds
            )
            try store.storeState(
                try currentState.schedulingUnsuccessfulOutcomeCommitRetry(
                    after: diagnostic,
                    at: try Self.adding(milliseconds: backoff, to: failedAt),
                    updatedAt: failedAt
                ),
                replacing: currentState,
                transaction: transaction
            )
        }
    }

    private func scheduleNext() async throws {
        guard let next = try await store.earliestScheduledAt() else {
            return
        }
        try await scheduler.ensureWakeUp(noLaterThan: next)
    }

    private static func validateOperationLease(
        _ current: DatabasePersistentJobState,
        expected: DatabasePersistentJobState,
        runnerID: DatabaseTypes.UUID,
        now: Timestamp
    ) throws {
        guard current.status == .running,
              expected.status == .running,
              current.leaseOwner == runnerID,
              current.leaseToken == expected.leaseToken,
              current.leaseExpiresAt == expected.leaseExpiresAt,
              current.revision == expected.revision
                || current.isCancellationRequest(after: expected),
              current.leaseExpiresAt.map({ $0 > now }) == true else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func validateUnsuccessfulOutcomeLease(
        _ current: DatabasePersistentJobState,
        expected: DatabasePersistentJobState,
        runnerID: DatabaseTypes.UUID,
        now: Timestamp
    ) throws {
        try validateUnsuccessfulOutcomeLeaseIdentity(
            current,
            expected: expected,
            runnerID: runnerID
        )
        guard current.leaseExpiresAt == expected.leaseExpiresAt,
              current.leaseExpiresAt.map({ $0 > now }) == true else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func validateUnsuccessfulOutcomeLeaseIdentity(
        _ current: DatabasePersistentJobState,
        expected: DatabasePersistentJobState,
        runnerID: DatabaseTypes.UUID
    ) throws {
        guard current.status == expected.status,
              current.status == .committingUnsuccessfulOutcome,
              current.revision == expected.revision,
              current.leaseOwner == runnerID,
              current.leaseToken == expected.leaseToken else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func validate(
        _ slice: AnyDatabaseResumableOperation.Slice,
        maximumWorkUnits: UInt64
    ) throws {
        guard slice.completedWorkUnits <= maximumWorkUnits else {
            throw DatabaseJobRuntimeError.sliceExceededBudget(
                actual: slice.completedWorkUnits,
                maximum: maximumWorkUnits
            )
        }
        if case .incomplete = slice.outcome,
           slice.completedWorkUnits == 0 {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func validateTotalWorkUnits(
        existing: UInt64?,
        reported: UInt64?,
        completed: UInt64
    ) throws {
        if let existing, let reported, existing != reported {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        if let total = reported ?? existing, total < completed {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func backoffMilliseconds(
        retryability: OperationRetryability,
        policy: JobStartOperation.RetryPolicy,
        currentSliceAttempt: UInt32,
        maximum: UInt32
    ) -> UInt32 {
        guard retryability == .backoff else { return 0 }
        let configuredMaximum = min(
            policy.maximumBackoffMilliseconds,
            maximum
        )
        var value = UInt64(policy.initialBackoffMilliseconds)
        guard value > 0 else { return 0 }
        var remainingDoublings = currentSliceAttempt > 0
            ? currentSliceAttempt - 1
            : 0
        while remainingDoublings > 0,
              value < UInt64(configuredMaximum) {
            value = min(value * 2, UInt64(configuredMaximum))
            remainingDoublings -= 1
        }
        return UInt32(value)
    }

    private static func unsuccessfulOutcomeCommitBackoffMilliseconds(
        initial: UInt32,
        commitAttempt: UInt64,
        maximum: UInt32
    ) -> UInt32 {
        var value = UInt64(initial)
        var remainingDoublings = commitAttempt > 0
            ? commitAttempt - 1
            : 0
        while remainingDoublings > 0,
              value < UInt64(maximum) {
            value = min(value * 2, UInt64(maximum))
            remainingDoublings -= 1
        }
        return UInt32(value)
    }

    private static func adding(
        milliseconds: UInt32,
        to timestamp: Timestamp
    ) throws -> Timestamp {
        let additionalSeconds = Int64(milliseconds / 1_000)
        let additionalNanoseconds = UInt64(milliseconds % 1_000) * 1_000_000
        let nanoseconds = UInt64(timestamp.nanoseconds) + additionalNanoseconds
        let carry = Int64(nanoseconds / 1_000_000_000)
        let (secondsWithDuration, firstOverflow) = timestamp.secondsSinceUnixEpoch
            .addingReportingOverflow(additionalSeconds)
        let (seconds, secondOverflow) = secondsWithDuration
            .addingReportingOverflow(carry)
        guard !firstOverflow, !secondOverflow else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "timestamp overflow"
            )
        }
        return try Timestamp(
            secondsSinceUnixEpoch: seconds,
            nanoseconds: UInt32(nanoseconds % 1_000_000_000)
        )
    }

    private static func batchConfiguration(
        timeoutMilliseconds: UInt32
    ) -> TransactionConfiguration {
        let batch = TransactionConfiguration.batch
        return TransactionConfiguration(
            timeout: Int(timeoutMilliseconds),
            maximumAttempts: batch.maximumAttempts,
            maxRetryDelay: batch.maxRetryDelay,
            initialRetryDelay: batch.initialRetryDelay,
            priority: batch.priority,
            readPriority: batch.readPriority,
            disableReadCache: batch.disableReadCache,
            cachePolicy: batch.cachePolicy,
            tracing: batch.tracing
        )
    }
}

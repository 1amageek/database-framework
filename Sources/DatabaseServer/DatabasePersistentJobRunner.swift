import DatabaseEngine
import DatabaseValue
import DatabaseWire

public actor DatabasePersistentJobRunner {
    private struct LeasedJob: Sendable {
        enum Action: Sendable {
            case execute
            case fail(DatabaseRemoteError)
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
    private let runnerID: DatabaseUUID

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
        runnerID: DatabaseUUID
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
        self.runnerID = runnerID
    }

    public func recoverSchedule() async throws {
        try await scheduleNext()
    }

    public func notifyWorkAvailable(at timestamp: DatabaseTimestamp) async throws {
        try await scheduler.schedule(at: timestamp)
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
        let operation: AnyDatabaseResumableOperation
        do {
            operation = try registry.resolve(leasedSnapshot.specification.operation)
        } catch {
            try await handleFailure(
                error,
                operation: nil,
                snapshot: leasedSnapshot,
                operationContext: operationContext
            )
            return
        }
        switch leasedJob.action {
        case .execute:
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
            } catch let error as DatabaseJobTerminalHookExecutionError {
                try await recordTerminalHookFailure(
                    error,
                    snapshot: leasedSnapshot,
                    operationContext: operationContext
                )
                throw error
            } catch {
                try await handleFailure(
                    error,
                    operation: operation,
                    snapshot: leasedSnapshot,
                    operationContext: operationContext
                )
            }
        case .fail(let failure):
            try await handleFailure(
                failure,
                operation: operation,
                snapshot: leasedSnapshot,
                operationContext: operationContext
            )
        }
    }

    private func acquireLease(
        for dueJob: DatabasePersistentJobDueEntry
    ) async throws -> LeasedJob? {
        let leaseToken = identifierGenerator.generate()
        let store = self.store
        let runnerID = self.runnerID
        let maximumAttempts = configuration.maximumAttempts
        let leaseDurationMilliseconds = configuration.leaseDurationMilliseconds
        let clock = self.clock
        let container = self.container

        return try await container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let now = clock.now()
            let leaseExpiresAt = try Self.adding(
                milliseconds: leaseDurationMilliseconds,
                to: now
            )
            let transaction = transactionContext.rawTransaction
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
                  scheduledAt <= now else {
                return nil
            }
            let allowedAttempts = min(
                snapshot.specification.retryPolicy.maximumAttempts,
                maximumAttempts
            )
            let action: LeasedJob.Action
            if !state.cancellationRequested,
               state.currentSliceAttempt >= allowedAttempts {
                action = .fail(DatabaseRemoteError(
                    category: .internalFailure,
                    code: "JOB_ATTEMPTS_EXHAUSTED",
                    message: "Job exhausted its configured attempts",
                    retryability: .never
                ))
            } else {
                action = .execute
            }
            let leased = try state.acquiringLease(
                owner: runnerID,
                token: leaseToken,
                expiresAt: leaseExpiresAt,
                updatedAt: now
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
            let transaction = transactionContext.rawTransaction
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateLease(
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
                do {
                    try await operation.handleTerminalState(
                        planPayload: current.plan.payload,
                        statePayload: current.state.operationStatePayload,
                        state: .cancelled,
                        context: DatabaseResumableOperationContext(
                            jobID: current.specification.jobID,
                            completedWorkUnitsBeforeSlice: current.state.completedWorkUnits,
                            transaction: transactionContext,
                            operationContext: operationContext
                        ),
                        limits: wireLimits,
                        storageLimits: storageLimits
                    )
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    throw DatabaseJobTerminalHookExecutionError(
                        underlyingError: error
                    )
                }
                let cancelled = try current.state.cancelling(
                    updatedAt: clock.now()
                )
                try store.storeState(
                    cancelled,
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
            let completedAt = clock.now()
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
                    totalWorkUnits: slice.totalWorkUnits ?? cumulativeWorkUnits,
                    resultDigest: responseDigest,
                    updatedAt: completedAt
                )
            case .incomplete(let operationStatePayload):
                updated = try current.state.continuing(
                    operationStatePayload: operationStatePayload,
                    cumulativeWorkUnits: cumulativeWorkUnits,
                    totalWorkUnits: slice.totalWorkUnits
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

        if leasedState.cancellationRequested {
            try await container.newContext().withTransaction(
                configuration: .batch
            ) { transactionContext in
                let transaction = transactionContext.rawTransaction
                let currentState = try await store.loadState(
                    snapshot.specification.jobID,
                    specificationDigest: snapshot.specificationDigest,
                    transaction: transaction
                )
                try Self.validateLease(
                    currentState,
                    expected: leasedState,
                    runnerID: runnerID,
                    now: clock.now()
                )
                try await operation.handleTerminalState(
                    planPayload: snapshot.plan.payload,
                    statePayload: currentState.operationStatePayload,
                    state: .cancelled,
                    context: DatabaseResumableOperationContext(
                        jobID: snapshot.specification.jobID,
                        completedWorkUnitsBeforeSlice: currentState.completedWorkUnits,
                        transaction: transactionContext,
                        operationContext: operationContext
                    ),
                    limits: wireLimits,
                    storageLimits: storageLimits
                )
                try store.storeState(
                    try currentState.cancelling(updatedAt: clock.now()),
                    replacing: currentState,
                    transaction: transaction
                )
            }
            return
        }

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
            let transaction = transactionContext.rawTransaction
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateLease(
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
                try await operation.handleTerminalState(
                    planPayload: current.plan.payload,
                    statePayload: current.state.operationStatePayload,
                    state: .cancelled,
                    context: DatabaseResumableOperationContext(
                        jobID: current.specification.jobID,
                        completedWorkUnitsBeforeSlice: current.state.completedWorkUnits,
                        transaction: transactionContext,
                        operationContext: operationContext
                    ),
                    limits: wireLimits,
                    storageLimits: storageLimits
                )
                try store.storeState(
                    try current.state.cancelling(updatedAt: clock.now()),
                    replacing: current.state,
                    transaction: transaction
                )
                return
            }

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
            let completedAt = clock.now()
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
                    totalWorkUnits: slice.totalWorkUnits ?? cumulativeWorkUnits,
                    resultDigest: responseDigest,
                    updatedAt: completedAt
                )
            case .incomplete(let operationStatePayload):
                updated = try current.state.continuing(
                    operationStatePayload: operationStatePayload,
                    cumulativeWorkUnits: cumulativeWorkUnits,
                    totalWorkUnits: slice.totalWorkUnits
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

    private func handleFailure(
        _ error: any Error,
        operation: AnyDatabaseResumableOperation?,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let failedAt = clock.now()
        let remoteError = errorMapper.remoteError(
            for: error,
            context: operationContext,
            limits: wireLimits
        )
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let configuration = self.configuration
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
                let transaction = transactionContext.rawTransaction
                let currentState = try await store.loadState(
                    snapshot.specification.jobID,
                    specificationDigest: snapshot.specificationDigest,
                    transaction: transaction
                )
                try Self.validateLease(
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
                let updated: DatabasePersistentJobState
                let terminalState: DatabaseResumableOperationTerminalState?
                if current.state.cancellationRequested {
                    updated = try current.state.cancelling(updatedAt: failedAt)
                    terminalState = .cancelled
                } else if remoteError.retryability != .never,
                          current.state.currentSliceAttempt
                            < current.specification.retryPolicy.maximumAttempts,
                          current.state.currentSliceAttempt
                            < configuration.maximumAttempts {
                    let backoff = Self.backoffMilliseconds(
                        retryability: remoteError.retryability,
                        policy: current.specification.retryPolicy,
                        currentSliceAttempt: current.state.currentSliceAttempt,
                        maximum: configuration.maximumBackoffMilliseconds
                    )
                    updated = try current.state.retrying(
                        at: try Self.adding(milliseconds: backoff, to: failedAt),
                        updatedAt: failedAt
                    )
                    terminalState = nil
                } else {
                    updated = try current.state.failing(
                        remoteError,
                        updatedAt: failedAt
                    )
                    terminalState = .failed(remoteError)
                }
                if let operation, let terminalState {
                    do {
                        try await operation.handleTerminalState(
                            planPayload: current.plan.payload,
                            statePayload: current.state.operationStatePayload,
                            state: terminalState,
                            context: DatabaseResumableOperationContext(
                                jobID: current.specification.jobID,
                                completedWorkUnitsBeforeSlice: current.state.completedWorkUnits,
                                transaction: transactionContext,
                                operationContext: operationContext
                            ),
                            limits: wireLimits,
                            storageLimits: storageLimits
                        )
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        throw DatabaseJobTerminalHookExecutionError(
                            underlyingError: error
                        )
                    }
                }
                try store.storeState(
                    updated,
                    replacing: current.state,
                    transaction: transaction
                )
            }
        } catch let error as DatabaseJobTerminalHookExecutionError {
            try await recordTerminalHookFailure(
                error,
                snapshot: snapshot,
                operationContext: operationContext
            )
            throw error
        }
    }

    private func recordTerminalHookFailure(
        _ error: DatabaseJobTerminalHookExecutionError,
        snapshot: DatabasePersistentJobSnapshot,
        operationContext: DatabaseOperationContext
    ) async throws {
        let mapped = errorMapper.remoteError(
            for: error.underlyingError,
            context: operationContext,
            limits: wireLimits
        )
        let failure = DatabaseRemoteError(
            category: .internalFailure,
            code: "JOB_TERMINAL_HOOK_FAILED",
            message: "Terminal hook failed with \(mapped.code): \(mapped.message)",
            retryability: .never
        )
        let failedAt = clock.now()
        let store = self.store
        let container = self.container
        let runnerID = self.runnerID
        let leasedState = snapshot.state
        try await container.newContext().withTransaction(
            configuration: Self.batchConfiguration(
                timeoutMilliseconds: snapshot.specification
                    .sliceTimeoutMilliseconds
            )
        ) { transactionContext in
            let transaction = transactionContext.rawTransaction
            let currentState = try await store.loadState(
                snapshot.specification.jobID,
                specificationDigest: snapshot.specificationDigest,
                transaction: transaction
            )
            try Self.validateLeaseIdentity(
                currentState,
                expected: leasedState,
                runnerID: runnerID
            )
            try store.storeState(
                try currentState.failing(failure, updatedAt: failedAt),
                replacing: currentState,
                transaction: transaction
            )
        }
    }

    private func scheduleNext() async throws {
        guard let next = try await store.earliestScheduledAt() else {
            return
        }
        try await scheduler.schedule(at: next)
    }

    private static func validateLease(
        _ current: DatabasePersistentJobState,
        expected: DatabasePersistentJobState,
        runnerID: DatabaseUUID,
        now: DatabaseTimestamp
    ) throws {
        try validateLeaseIdentity(
            current,
            expected: expected,
            runnerID: runnerID
        )
        guard current.leaseExpiresAt == expected.leaseExpiresAt,
              current.leaseExpiresAt.map({ $0 > now }) == true else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    private static func validateLeaseIdentity(
        _ current: DatabasePersistentJobState,
        expected: DatabasePersistentJobState,
        runnerID: DatabaseUUID
    ) throws {
        guard current.status == .running,
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
        retryability: DatabaseRetryability,
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

    private static func adding(
        milliseconds: UInt32,
        to timestamp: DatabaseTimestamp
    ) throws -> DatabaseTimestamp {
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
        return DatabaseTimestamp(
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

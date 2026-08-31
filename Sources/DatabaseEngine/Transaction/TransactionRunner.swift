// TransactionRunner.swift
// DatabaseEngine - Transaction execution with retry logic
//
// Reference: FoundationDB transaction retry pattern
// https://apple.github.io/foundationdb/developer-guide.html#transactions

import StorageKit
import Synchronization

private enum TransactionDeadlineRaceResult<Value: Sendable>: Sendable {
    case value(Value)
    case timedOut
    case cleanupFailed(StorageTransactionCleanupError)
    case lostRace
}

private struct TransactionAttemptResult<Value: Sendable>: Sendable {
    let value: Value
    let readVersionForCache: Int64?
}

private struct EffectiveTransactionDeadline: Sendable {
    let instant: StorageInstant
    let timeoutMilliseconds: UInt64
    let source: TransactionExecutionDeadlineExceeded.Source
}

private enum TransactionCancellationOutcome: Sendable {
    case succeeded
    case failed(any Error)
}

private final class TransactionDeadlineRaceState: Sendable {
    private enum Winner: Sendable {
        case undecided
        case operation
        case timeout
    }

    private let winner = Mutex(Winner.undecided)

    func selectOperation() -> Bool {
        winner.withLock { winner in
            guard winner == .undecided else { return false }
            winner = .operation
            return true
        }
    }

    func selectTimeout() -> Bool {
        winner.withLock { winner in
            guard winner == .undecided else { return false }
            winner = .timeout
            return true
        }
    }
}

private final class TransactionCancellationGate: Sendable {
    private let transaction: any Transaction
    private let onCancel: (@Sendable (_ transaction: any Transaction) -> Void)?
    private let task = Mutex<Task<TransactionCancellationOutcome, Never>?>(nil)

    init(
        transaction: any Transaction,
        onCancel: (@Sendable (_ transaction: any Transaction) -> Void)?
    ) {
        self.transaction = transaction
        self.onCancel = onCancel
    }

    var hasStarted: Bool {
        task.withLock { $0 != nil }
    }

    var storageFailure: StorageError? {
        transaction.storageFailure
    }

    func cancel(
        preserving operationError: any Error
    ) async -> StorageTransactionCleanupError? {
        let cancellationTask = task.withLock {
            task -> Task<TransactionCancellationOutcome, Never> in
            if let task { return task }
            let transaction = self.transaction
            let onCancel = self.onCancel
            let created = Task {
                let outcome: TransactionCancellationOutcome
                do {
                    try await transaction.cancel()
                    outcome = .succeeded
                } catch {
                    outcome = .failed(error)
                }
                onCancel?(transaction)
                return outcome
            }
            task = created
            return created
        }

        switch await cancellationTask.value {
        case .succeeded:
            return nil
        case .failed(let cancellationError):
            return StorageTransactionCleanupError(
                operationError: operationError,
                cancellationError: cancellationError
            )
        }
    }
}

// MARK: - TransactionRunner

/// Executes transactions with configurable retry logic and exponential backoff
///
/// TransactionRunner handles:
/// - Creating new transactions for each retry attempt
/// - Applying TransactionConfiguration options
/// - Applying cached read versions (based on CachePolicy)
/// - Retrying on retryable FDB errors with exponential backoff
/// - Respecting retry limits from configuration
///
/// **Cache Policy**:
/// When `TransactionConfiguration.cachePolicy` is `.cached` or `.stale(N)` and a
/// `ReadVersionCache` is provided, the runner will attempt to use a cached read
/// version on the first attempt. This reduces `getReadVersion()` network round-trips.
/// - Only applied on first attempt (retry uses fresh version)
/// - Cache is updated after successful commit
///
/// **Backoff Algorithm**:
/// - Initial delay: Supplied by `TransactionConfiguration`
/// - Exponential growth: delay doubles each attempt (10ms → 20ms → 40ms)
/// - Capped by `maxRetryDelay` from configuration (default: 250ms)
/// - Jitter: 0-50% added to prevent thundering herd
///
/// **Environment Variable**: `DATABASE_TRANSACTION_INITIAL_DELAY` to configure initial delay
///
/// **Reference**: FDB client retry loop pattern, AWS exponential backoff
internal struct TransactionRunner: Sendable {
    // MARK: - Properties

    /// Storage transaction execution is internally thread-safe.
    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock

    private let logger: DatabaseLogger
    private let retryCounter: DatabaseMetricCounter
    private let retryExhaustedCounter: DatabaseMetricCounter
    private let cacheUpdateFailureCounter: DatabaseMetricCounter
    private let unsupportedConfigurationHintCounter: DatabaseMetricCounter
    private let unsupportedCachedReadVersionCounter: DatabaseMetricCounter

    // MARK: - Initialization

    init(
        transactionExecutor: StorageTransactionExecutor,
        clock: any StorageMonotonicClock,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.transactionExecutor = transactionExecutor
        self.clock = clock
        self.logger = logging.logger(
            label: "com.database.framework.transaction-runner"
        )
        self.retryCounter = metrics.counter(
            label: "database_transaction_retries_total"
        )
        self.retryExhaustedCounter = metrics.counter(
            label: "database_transaction_retry_exhausted_total"
        )
        self.cacheUpdateFailureCounter = metrics.counter(
            label: "database_transaction_read_version_cache_update_failures_total"
        )
        self.unsupportedConfigurationHintCounter = metrics.counter(
            label: "database_transaction_unsupported_configuration_hints_total"
        )
        self.unsupportedCachedReadVersionCounter = metrics.counter(
            label: "database_transaction_unsupported_cached_read_versions_total"
        )
    }

    // MARK: - Partition authority

    /// Leases the operation's Partition, or nothing when the caller addresses
    /// no single Partition.
    ///
    /// Returning the lease keeps it owned by the attempt scope, so an error
    /// thrown anywhere in the attempt releases it through its own lifetime
    /// rather than through a cleanup path that could be skipped.
    private static func leaseIfRequired(
        _ authority: DatabasePartitionAuthority?,
        transaction: any TransactionReadAccess
    ) async throws -> PartitionLease? {
        guard let authority else { return nil }
        return try await authority.lease(in: transaction)
    }

    // MARK: - Execution

    /// Execute a transaction with the given configuration
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority, weak read semantics)
    ///   - readVersionCache: Optional cache for weak read semantics
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: The operation or storage error that remains after retries
    func run<T: Sendable>(
        configuration: TransactionConfiguration,
        executionDeadline: TransactionExecutionDeadline? = nil,
        readVersionCache: ReadVersionCache? = nil,
        operationDescription: String = "transaction",
        onRetry: (@Sendable (_ attempt: Int, _ error: StorageError) -> Void)? = nil,
        onCancel: (@Sendable (_ transaction: any Transaction) -> Void)? = nil,
        onCommitOutcomeUnknown: (@Sendable () -> Void)? = nil,
        onCommitSuccess: (@Sendable (_ transaction: any Transaction, _ commitNanos: UInt64) -> Void)? = nil,
        partitionAuthority: DatabasePartitionAuthority? = nil,
        restoringReadPosition: DatabaseReadPosition? = nil,
        operation: @escaping @Sendable (any TransactionAccess) async throws -> T
    ) async throws -> T {
        try configuration.validate()
        guard !ActiveTransactionContext.isActive else {
            throw StorageError.invalidOperation(
                "Nested transaction runners are not supported; pass the active transaction to the nested operation"
            )
        }
        let maxAttempts = configuration.maximumAttempts
        let maxDelayMs = configuration.maxRetryDelay
        var lastRetryableError: StorageError?
        let timeoutMilliseconds = configuration.timeout.flatMap {
            $0 == 0 ? nil : $0
        }
        let startedAt = clock.now
        let configuredDeadline = timeoutMilliseconds.map {
            EffectiveTransactionDeadline(
                instant: startedAt.advanced(by: .milliseconds($0)),
                timeoutMilliseconds: UInt64($0),
                source: .transactionConfiguration
            )
        }
        let inheritedDeadline = executionDeadline.map {
            EffectiveTransactionDeadline(
                instant: $0.instant,
                timeoutMilliseconds: UInt64($0.timeoutMilliseconds),
                source: .inheritedOperation
            )
        }
        let effectiveDeadline = Self.earliestDeadline(
            configuredDeadline,
            inheritedDeadline
        )

        for attempt in 0..<maxAttempts {
            try ensureDatabaseTaskIsActive()
            try ensureBeforeDeadline(effectiveDeadline)
            var cancellationGate: TransactionCancellationGate?
            var commitDispatched = false

            do {
                // 1. Create NEW transaction for each attempt
                let newTransaction = try transactionExecutor
                    .createOwnedTransaction()
                let newCancellationGate = TransactionCancellationGate(
                    transaction: newTransaction,
                    onCancel: onCancel
                )
                cancellationGate = newCancellationGate

                // 2. Apply configuration options
                let attemptConfiguration = try configurationForAttempt(
                    configuration,
                    deadline: effectiveDeadline
                )
                let resolution = try attemptConfiguration.apply(
                    to: newTransaction
                )
                recordConfigurationResolution(
                    resolution,
                    operationDescription: operationDescription
                )
                // 3. Select this attempt's read version before any read.
                //    The Partition lease below reads, and a backend that
                //    supports historical reads refuses a read version set
                //    after a transaction has taken one, so selection cannot
                //    move into the operation.
                //    A restored position names the exact version the caller
                //    asked for, so every attempt restores it. A cached version
                //    is applied only on the first attempt, because a retry
                //    wants a fresh one rather than repeating a
                //    transaction_too_old failure.
                if let restoringReadPosition {
                    try ReadAuthorizedTransactionAccess.restoreReadPosition(
                        restoringReadPosition,
                        on: newTransaction
                    )
                } else if attempt == 0 {
                    try applyCachedReadVersion(
                        to: newTransaction,
                        configuration: configuration,
                        cache: readVersionCache
                    )
                }

                // 4. Acquire Partition authority for this attempt.
                //    The lease is taken in the attempt's own transaction so a
                //    stale Partition is rejected here, and it is held across
                //    the operation and the commit so a concurrent move or
                //    removal of this Partition is refused for that whole
                //    window. A data-plane caller supplies the authority; the
                //    executor-level system paths address no single Partition
                //    and pass none.
                let partitionLease = try await Self.leaseIfRequired(
                    partitionAuthority,
                    transaction: newTransaction
                )

                // 5. Execute operation (set TaskLocal for nested transaction detection)
                let attemptResult = try await executeOperationWithinDeadline(
                    transaction: newTransaction,
                    deadline: effectiveDeadline,
                    cancellationGate: newCancellationGate,
                    operation: { [self] transaction in
                        let value = try await operation(transaction)
                        let readVersion = await captureReadVersionForCache(
                            transaction: transaction,
                            cache: readVersionCache
                        )
                        return TransactionAttemptResult(
                            value: value,
                            readVersionForCache: readVersion
                        )
                    }
                )

                // 6. Commit (throws on failure)
                try ensureDatabaseTaskIsActive()
                try ensureBeforeDeadline(effectiveDeadline)
                let commitStart = clock.now
                // Once commit dispatch begins, cancellation or the portable
                // deadline cannot determine whether the backend committed. The
                // unstructured task is immediately awaited so the backend's
                // authoritative commit result always wins and no work escapes.
                commitDispatched = true
                let commitTask = Task {
                    try await newTransaction.commit()
                }
                try await commitTask.value
                let commitNanos = DatabaseMonotonicMeasurement.nanoseconds(
                    from: commitStart,
                    to: clock.now
                )

                // 7. Update cache after successful commit
                updateCacheAfterCommit(
                    transaction: newTransaction,
                    capturedReadVersion: attemptResult.readVersionForCache,
                    cache: readVersionCache
                )
                onCommitSuccess?(newTransaction, commitNanos)

                // 8. The transaction closed authoritatively, so the Partition
                //    is no longer in use by this attempt. A failing attempt
                //    releases the same way when the lease leaves scope.
                if let partitionLease {
                    partitionLease.release()
                }

                if attempt > 0 {
                    logger.info(
                        "Transaction retry succeeded",
                        metadata: [
                            "operation": "\(operationDescription)",
                            "attempts": "\(attempt + 1)"
                        ]
                    )
                }
                return attemptResult.value

            } catch {
                let operationError = error
                // Storage backends record failures on the owned transaction,
                // while application operations and transaction creation can
                // throw a StorageError before backend state records it. Both
                // paths carry the same domain contract and must participate in
                // the runner's single retry policy.
                let storageError = cancellationGate?.storageFailure
                    ?? (operationError as? StorageError)

                if Task.isCancelled {
                    if commitDispatched {
                        onCommitOutcomeUnknown?()
                        throw Self.commitUnknownError()
                    }
                    let cancellationError = CancellationError()
                    if let cancellationGate, !cancellationGate.hasStarted {
                        if let cleanupError = await cancellationGate.cancel(
                            preserving: cancellationError
                        ) {
                            throw cleanupError
                        }
                    }
                    throw cancellationError
                }

                if let storageError,
                   storageError.code == .commitUnknownResult {
                    onCommitOutcomeUnknown?()
                    throw storageError
                }
                if commitDispatched, storageError == nil {
                    onCommitOutcomeUnknown?()
                    throw Self.commitUnknownError()
                }
                if let cancellationGate, !cancellationGate.hasStarted {
                    if let cleanupError = await cancellationGate.cancel(
                        preserving: operationError
                    ) {
                        throw cleanupError
                    }
                }

                if let storageError, storageError.isRetryable {
                    lastRetryableError = storageError
                    if attempt < maxAttempts - 1 {
                        logger.debug(
                            "Transaction retry scheduled",
                            metadata: [
                                "operation": "\(operationDescription)",
                                "attempt": "\(attempt + 1)",
                                "maxAttempts": "\(maxAttempts)",
                                "classification": "retryable storage error"
                            ]
                        )
                        retryCounter.increment()
                        onRetry?(attempt + 1, storageError)
                        try ensureDatabaseTaskIsActive()
                        try await applyBackoff(
                            attempt: attempt,
                            initialDelayMs: configuration.initialRetryDelay,
                            maxDelayMs: maxDelayMs,
                            deadline: effectiveDeadline
                        )
                        continue
                    }
                    retryExhaustedCounter.increment()
                    logger.error(
                        "Transaction retry exhausted",
                        metadata: [
                            "operation": "\(operationDescription)",
                            "attempts": "\(maxAttempts)",
                            "classification": "retryable storage error"
                        ]
                    )
                    throw storageError
                }

                throw operationError
            }
        }

        // Should not reach here, but safety fallback
        if let lastRetryableError {
            throw lastRetryableError
        }
        throw StorageError.invalidOperation(
            "Transaction runner terminated without producing an attempt result"
        )
    }

    private func executeOperationWithinDeadline<T: Sendable>(
        transaction: any Transaction,
        deadline: EffectiveTransactionDeadline?,
        cancellationGate: TransactionCancellationGate,
        operation: @escaping @Sendable (any TransactionAccess) async throws -> T
    ) async throws -> T {
        guard let deadline else {
            return try await ActiveTransactionContext.withActiveTransaction(
                transaction
            ) { access in
                try await operation(access)
            }
        }

        try ensureBeforeDeadline(deadline)
        let raceState = TransactionDeadlineRaceState()
        return try await withThrowingTaskGroup(
            of: TransactionDeadlineRaceResult<T>.self,
            returning: T.self
        ) { group in
            group.addTask {
                do {
                    let value = try await ActiveTransactionContext
                        .withActiveTransaction(transaction) { access in
                            try await operation(access)
                    }
                    guard raceState.selectOperation() else { return .lostRace }
                    return .value(value)
                } catch {
                    guard raceState.selectOperation() else { return .lostRace }
                    throw error
                }
            }
            group.addTask {
                try await sleepUntil(deadline.instant)
                guard raceState.selectTimeout() else { return .lostRace }
                let timeoutError = Self.timeoutError(deadline)
                if let cleanupError = await cancellationGate.cancel(
                    preserving: timeoutError
                ) {
                    return .cleanupFailed(cleanupError)
                }
                return .timedOut
            }

            while let next = try await group.next() {
                switch next {
                case .value(let value):
                    group.cancelAll()
                    return value
                case .timedOut:
                    group.cancelAll()
                    throw Self.timeoutError(deadline)
                case .cleanupFailed(let error):
                    group.cancelAll()
                    throw error
                case .lostRace:
                    continue
                }
            }
            throw Self.timeoutError(deadline)
        }
    }

    private func ensureBeforeDeadline(
        _ deadline: EffectiveTransactionDeadline?
    ) throws {
        guard let deadline,
              clock.now >= deadline.instant else {
            return
        }
        throw Self.timeoutError(deadline)
    }

    private func configurationForAttempt(
        _ configuration: TransactionConfiguration,
        deadline: EffectiveTransactionDeadline?
    ) throws -> TransactionConfiguration {
        guard let deadline else { return configuration }
        let remaining = try remainingMilliseconds(until: deadline)
        return configuration.replacing(timeout: remaining)
    }

    private func remainingMilliseconds(
        until deadline: EffectiveTransactionDeadline
    ) throws -> Int {
        let remaining = clock.now.duration(
            to: deadline.instant
        )
        guard remaining > .zero else {
            throw Self.timeoutError(deadline)
        }

        let components = remaining.components
        let maximumWholeSeconds = Int64(Int.max / 1_000)
        guard components.seconds <= maximumWholeSeconds else {
            return Int.max
        }
        let wholeMilliseconds = components.seconds * 1_000
        let fractionalMilliseconds = (
            components.attoseconds + 999_999_999_999_999
        ) / 1_000_000_000_000_000
        let (total, overflow) = wholeMilliseconds.addingReportingOverflow(
            fractionalMilliseconds
        )
        guard !overflow, total > 0 else {
            throw Self.timeoutError(deadline)
        }
        guard total <= Int64(Int.max) else { return Int.max }
        return Int(total)
    }

    private static func earliestDeadline(
        _ first: EffectiveTransactionDeadline?,
        _ second: EffectiveTransactionDeadline?
    ) -> EffectiveTransactionDeadline? {
        switch (first, second) {
        case (.none, .none):
            return nil
        case (.some(let deadline), .none), (.none, .some(let deadline)):
            return deadline
        case (.some(let first), .some(let second)):
            return first.instant <= second.instant ? first : second
        }
    }

    private static func timeoutError(
        _ deadline: EffectiveTransactionDeadline
    ) -> TransactionExecutionDeadlineExceeded {
        TransactionExecutionDeadlineExceeded(
            timeoutMilliseconds: deadline.timeoutMilliseconds,
            source: deadline.source
        )
    }

    private static func commitUnknownError() -> StorageError {
        StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            message: "Commit dispatch ended without an authoritative backend outcome",
            underlyingDescription: "backend commit outcome was unavailable"
        )
    }

    // MARK: - Cache Policy

    /// Apply cached read version to transaction if available and valid
    ///
    /// Only called on first attempt. On retry, we use fresh version to avoid
    /// repeating `transaction_too_old` errors from stale cached versions.
    private func applyCachedReadVersion(
        to transaction: any TransactionAccess,
        configuration: TransactionConfiguration,
        cache: ReadVersionCache?
    ) throws {
        guard let cache = cache,
              let cachedVersion = cache.getCachedVersion(policy: configuration.cachePolicy) else {
            return
        }

        guard transaction.capabilities.historicalReadVersion else {
            cache.clear()
            unsupportedCachedReadVersionCounter.increment()
            logger.debug(
                "Cached read version omitted because the backend has no historical reads",
                metadata: ["cachedVersion": "\(cachedVersion)"]
            )
            return
        }

        try transaction.setReadVersion(cachedVersion)
    }

    private func captureReadVersionForCache(
        transaction: any TransactionAccess,
        cache: ReadVersionCache?
    ) async -> Int64? {
        guard let cache, transaction.capabilities.readVersion else { return nil }
        do {
            return try await transaction.getReadVersion()
        } catch {
            cache.clear()
            cacheUpdateFailureCounter.increment()
            logger.warning(
                "Read version capture failed before commit; cache cleared",
                metadata: ["outcome": "read-version capture failed"]
            )
            return nil
        }
    }

    /// Publish a version captured while the transaction was open, but only
    /// after commit has completed successfully.
    private func updateCacheAfterCommit(
        transaction: any Transaction,
        capturedReadVersion: Int64?,
        cache: ReadVersionCache?
    ) {
        guard let cache = cache else { return }

        let capabilities = transaction.capabilities
        guard capabilities.committedVersion || capabilities.readVersion else {
            cache.clear()
            return
        }

        do {
            if capabilities.committedVersion {
                let committedVersion = try transaction.getCommittedVersion()
                if committedVersion >= 0 {
                    cache.updateFromCommit(version: committedVersion)
                    return
                }
            }
            if let capturedReadVersion {
                cache.updateFromRead(version: capturedReadVersion)
                return
            }
            cache.clear()
        } catch {
            cache.clear()
            cacheUpdateFailureCounter.increment()
            logger.warning(
                "Read version cache update failed after commit; cache cleared",
                metadata: [
                    "outcome": "read-version cache update failed"
                ]
            )
        }
    }

    private func recordConfigurationResolution(
        _ resolution: TransactionConfigurationResolution,
        operationDescription: String
    ) {
        for hint in resolution.unsupportedHints {
            unsupportedConfigurationHintCounter.increment()
            logger.debug(
                "Transaction configuration hint omitted for backend",
                metadata: [
                    "operation": "\(operationDescription)",
                    "hint": "\(hint.rawValue)"
                ]
            )
        }
    }

    // MARK: - Backoff

    /// Apply exponential backoff with jitter
    ///
    /// - Parameters:
    ///   - attempt: Current attempt number (0-based)
    ///   - maxDelayMs: Maximum delay in milliseconds
    private func applyBackoff(
        attempt: Int,
        initialDelayMs: Int,
        maxDelayMs: Int,
        deadline: EffectiveTransactionDeadline?
    ) async throws {
        let delayMs = Self.calculateBackoff(
            attempt: attempt,
            initialDelayMs: initialDelayMs,
            maxDelayMs: maxDelayMs
        )
        let wake = clock.now.advanced(by: .milliseconds(delayMs))
        if let deadline, wake >= deadline.instant {
            try await sleepUntil(deadline.instant)
            throw Self.timeoutError(deadline)
        }
        try await sleepUntil(wake)
    }

    private func sleepUntil(_ deadline: StorageInstant) async throws {
        do {
            try await clock.sleep(until: deadline)
        } catch .cancelled {
            throw CancellationError()
        } catch {
            throw error
        }
    }

    /// Calculate backoff delay with exponential growth and jitter
    ///
    /// Formula: min(initialDelay * 2^attempt, maxDelay) + jitter
    ///
    /// **Reference**: AWS recommended exponential backoff algorithm
    /// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    ///
    /// - Parameters:
    ///   - attempt: Current attempt number (0-based)
    ///   - maxDelayMs: Maximum delay in milliseconds
    /// - Returns: Delay in milliseconds
    static func calculateBackoff(
        attempt: Int,
        initialDelayMs: Int = 10,
        maxDelayMs: Int
    ) -> Int {
        precondition(initialDelayMs >= 0, "Initial retry delay must be non-negative")
        precondition(maxDelayMs >= 0, "Maximum retry delay must be non-negative")

        // Cap exponent at 10 to keep exponential growth bounded.
        let exponent = min(attempt, 10)
        let multiplier = 1 << exponent
        let multiplication = initialDelayMs.multipliedReportingOverflow(by: multiplier)
        let exponentialDelay = multiplication.overflow
            ? maxDelayMs
            : multiplication.partialValue
        let cappedDelay = min(exponentialDelay, maxDelayMs)

        // Jitter never exceeds the configured maximum delay.
        let remaining = maxDelayMs - cappedDelay
        let maximumJitter = min(cappedDelay / 2, remaining)
        let jitter = Int.random(in: 0...maximumJitter)
        return cappedDelay + jitter
    }
}

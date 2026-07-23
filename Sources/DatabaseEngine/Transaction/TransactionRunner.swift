// TransactionRunner.swift
// DatabaseEngine - Transaction execution with retry logic
//
// Reference: FoundationDB transaction retry pattern
// https://apple.github.io/foundationdb/developer-guide.html#transactions

import Logging
import Metrics
import StorageKit
import Synchronization

private enum TransactionDeadlineRaceResult<Value: Sendable>: Sendable {
    case value(Value)
    case timedOut
    case lostRace
}

private struct TransactionAttemptResult<Value: Sendable>: Sendable {
    let value: Value
    let readVersionForCache: Int64?
}

private struct EffectiveTransactionDeadline: Sendable {
    let instant: ContinuousClock.Instant
    let timeoutMilliseconds: UInt64
    let source: TransactionExecutionDeadlineExceeded.Source
}

private enum TransactionCancellationOutcome: Sendable {
    case succeeded
    case failed(any Error)
}

private struct TransactionCancellationAlreadyHandled: Error, Sendable {
    let cleanupError: StorageTransactionCleanupError
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

    func cancel(
        preserving operationError: any Error
    ) async throws(StorageTransactionCleanupError) {
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
            return
        case .failed(let cancellationError):
            if let cleanupError = operationError
                as? StorageTransactionCleanupError {
                throw cleanupError.addingCancellationError(
                    cancellationError
                )
            }
            throw StorageTransactionCleanupError(
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

    /// StorageEngine is internally thread-safe (manages backend connections).
    private let database: any StorageEngine

    private let logger = Logger(label: "com.database.transaction.runner")

    private static let retryCounter = Counter(label: "database_transaction_retries_total")
    private static let retryExhaustedCounter = Counter(label: "database_transaction_retry_exhausted_total")
    private static let cacheUpdateFailureCounter = Counter(
        label: "database_transaction_read_version_cache_update_failures_total"
    )
    private static let unsupportedConfigurationHintCounter = Counter(
        label: "database_transaction_unsupported_configuration_hints_total"
    )
    private static let unsupportedCachedReadVersionCounter = Counter(
        label: "database_transaction_unsupported_cached_read_versions_total"
    )

    // MARK: - Initialization

    init(database: any StorageEngine) {
        self.database = database
    }

    // MARK: - Execution

    /// Execute a transaction with the given configuration
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority, weak read semantics)
    ///   - readVersionCache: Optional cache for weak read semantics
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: FDBError if the transaction cannot be committed after retries
    func run<T: Sendable>(
        configuration: TransactionConfiguration,
        executionDeadline: TransactionExecutionDeadline? = nil,
        readVersionCache: ReadVersionCache? = nil,
        operationDescription: String = "transaction",
        onRetry: (@Sendable (_ attempt: Int, _ error: StorageError) -> Void)? = nil,
        onCancel: (@Sendable (_ transaction: any Transaction) -> Void)? = nil,
        onCommitSuccess: (@Sendable (_ transaction: any Transaction, _ commitNanos: UInt64) -> Void)? = nil,
        operation: @escaping @Sendable (any Transaction) async throws -> T
    ) async throws -> T {
        try configuration.validate()
        guard ActiveTransactionScope.current == nil else {
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
        let startedAt = database.monotonicClock.now
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
            try Task.checkCancellation()
            try ensureBeforeDeadline(effectiveDeadline)
            var cancellationGate: TransactionCancellationGate?
            var commitDispatched = false

            do {
                // 1. Create NEW transaction for each attempt
                let newTransaction = try database.createTransaction()
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
                // 3. Apply cached read version (only on first attempt)
                //    On retry, we want a fresh version to avoid repeating transaction_too_old errors
                if attempt == 0 {
                    try applyCachedReadVersion(
                        to: newTransaction,
                        configuration: configuration,
                        cache: readVersionCache
                    )
                }

                // 4. Execute operation (set TaskLocal for nested transaction detection)
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

                // 5. Commit (throws on failure)
                try Task.checkCancellation()
                try ensureBeforeDeadline(effectiveDeadline)
                let commitStart = MonotonicClock.now().uptimeNanoseconds
                // Once commit dispatch begins, cancellation or the portable
                // deadline cannot determine whether the backend committed. The
                // unstructured task is immediately awaited so the backend's
                // authoritative commit result always wins and no work escapes.
                commitDispatched = true
                let commitTask = Task {
                    try await newTransaction.commit()
                }
                try await commitTask.value
                let commitNanos = MonotonicClock.now().uptimeNanoseconds - commitStart

                // 6. Update cache after successful commit
                updateCacheAfterCommit(
                    transaction: newTransaction,
                    capturedReadVersion: attemptResult.readVersionForCache,
                    cache: readVersionCache
                )
                onCommitSuccess?(newTransaction, commitNanos)

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

            } catch let handled as TransactionCancellationAlreadyHandled {
                throw handled.cleanupError
            } catch is CancellationError {
                if commitDispatched {
                    throw Self.commitUnknownError(
                        underlying: CancellationError()
                    )
                }
                let operationError = CancellationError()
                if let cancellationGate {
                    try await cancellationGate.cancel(preserving: operationError)
                }
                throw operationError
            } catch {
                let operationError = error
                if let storageError = operationError as? StorageError,
                   storageError.code == .commitUnknownResult {
                    throw storageError
                }
                if commitDispatched, !(operationError is StorageError) {
                    throw Self.commitUnknownError(underlying: operationError)
                }
                // Cancel transaction before retry or rethrow
                // Reference: FDB best practice - cancel uncommitted transactions
                if let cancellationGate {
                    try await cancellationGate.cancel(preserving: operationError)
                }

                // 6. Check if retryable
                if let storageError = operationError as? StorageError,
                   storageError.isRetryable {
                    lastRetryableError = storageError
                    if attempt < maxAttempts - 1 {
                        logger.debug(
                            "Transaction retry scheduled",
                            metadata: [
                                "operation": "\(operationDescription)",
                                "attempt": "\(attempt + 1)",
                                "maxAttempts": "\(maxAttempts)",
                                "error": .string(
                                    String(describing: storageError)
                                )
                            ]
                        )
                        Self.retryCounter.increment()
                        onRetry?(attempt + 1, storageError)
                        try Task.checkCancellation()
                        // Apply exponential backoff before retry
                        try await applyBackoff(
                            attempt: attempt,
                            initialDelayMs: configuration.initialRetryDelay,
                            maxDelayMs: maxDelayMs,
                            deadline: effectiveDeadline
                        )
                        continue
                    }
                    Self.retryExhaustedCounter.increment()
                    logger.error(
                        "Transaction retry exhausted",
                        metadata: [
                            "operation": "\(operationDescription)",
                            "attempts": "\(maxAttempts)",
                            "error": .string(
                                String(describing: storageError)
                            )
                        ]
                    )
                    throw storageError
                }

                // Non-retryable error or max retries exceeded
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
        operation: @escaping @Sendable (any Transaction) async throws -> T
    ) async throws -> T {
        guard let deadline else {
            return try await ActiveTransactionScope.$current.withValue(transaction) {
                try await operation(transaction)
            }
        }

        try ensureBeforeDeadline(deadline)
        let clock = database.monotonicClock
        let raceState = TransactionDeadlineRaceState()
        return try await withThrowingTaskGroup(
            of: TransactionDeadlineRaceResult<T>.self,
            returning: T.self
        ) { group in
            group.addTask {
                do {
                    let value = try await ActiveTransactionScope.$current.withValue(transaction) {
                        try await operation(transaction)
                    }
                    guard raceState.selectOperation() else { return .lostRace }
                    return .value(value)
                } catch {
                    guard raceState.selectOperation() else { return .lostRace }
                    throw error
                }
            }
            group.addTask {
                try await clock.sleep(until: deadline.instant)
                guard raceState.selectTimeout() else { return .lostRace }
                let timeoutError = Self.timeoutError(deadline)
                do {
                    try await cancellationGate.cancel(
                        preserving: timeoutError
                    )
                } catch let cleanupError as StorageTransactionCleanupError {
                    throw TransactionCancellationAlreadyHandled(
                        cleanupError: cleanupError
                    )
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
              database.monotonicClock.now >= deadline.instant else {
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
        let remaining = database.monotonicClock.now.duration(
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

    private static func commitUnknownError(
        underlying: any Error
    ) -> StorageError {
        StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            message: "Commit dispatch ended without an authoritative backend outcome",
            underlyingDescription: String(describing: underlying)
        )
    }

    // MARK: - Cache Policy

    /// Apply cached read version to transaction if available and valid
    ///
    /// Only called on first attempt. On retry, we use fresh version to avoid
    /// repeating `transaction_too_old` errors from stale cached versions.
    private func applyCachedReadVersion(
        to transaction: any Transaction,
        configuration: TransactionConfiguration,
        cache: ReadVersionCache?
    ) throws {
        guard let cache = cache,
              let cachedVersion = cache.getCachedVersion(policy: configuration.cachePolicy) else {
            return
        }

        guard transaction.capabilities.historicalReadVersion else {
            cache.clear()
            Self.unsupportedCachedReadVersionCounter.increment()
            logger.debug(
                "Cached read version omitted because the backend has no historical reads",
                metadata: ["cachedVersion": "\(cachedVersion)"]
            )
            return
        }

        try transaction.setReadVersion(cachedVersion)
    }

    private func captureReadVersionForCache(
        transaction: any Transaction,
        cache: ReadVersionCache?
    ) async -> Int64? {
        guard let cache, transaction.capabilities.readVersion else { return nil }
        do {
            return try await transaction.getReadVersion()
        } catch {
            cache.clear()
            Self.cacheUpdateFailureCounter.increment()
            logger.warning(
                "Read version capture failed before commit; cache cleared",
                metadata: ["error": "\(String(describing: error))"]
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
            Self.cacheUpdateFailureCounter.increment()
            logger.warning(
                "Read version cache update failed after commit; cache cleared",
                metadata: [
                    "error": "\(String(describing: error))"
                ]
            )
        }
    }

    private func recordConfigurationResolution(
        _ resolution: TransactionConfigurationResolution,
        operationDescription: String
    ) {
        for hint in resolution.unsupportedHints {
            Self.unsupportedConfigurationHintCounter.increment()
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
        let clock = database.monotonicClock
        let wake = clock.now.advanced(by: .milliseconds(delayMs))
        if let deadline, wake >= deadline.instant {
            try await clock.sleep(until: deadline.instant)
            throw Self.timeoutError(deadline)
        }
        try await clock.sleep(until: wake)
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

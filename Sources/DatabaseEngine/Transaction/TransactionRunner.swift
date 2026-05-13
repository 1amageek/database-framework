// TransactionRunner.swift
// DatabaseEngine - Transaction execution with retry logic
//
// Reference: FoundationDB transaction retry pattern
// https://apple.github.io/foundationdb/developer-guide.html#transactions

import Foundation
import Logging
import Metrics
import StorageKit

// MARK: - TransactionRetryExhaustedError

public struct TransactionRetryExhaustedError: Error, Sendable, LocalizedError, CustomStringConvertible {
    public let attempts: Int
    public let operationDescription: String
    public let lastStorageError: StorageError?
    public let lastErrorDescription: String

    public init(
        attempts: Int,
        operationDescription: String,
        lastError: any Error
    ) {
        self.attempts = attempts
        self.operationDescription = operationDescription
        self.lastStorageError = lastError as? StorageError
        self.lastErrorDescription = lastError.localizedDescription
    }

    public var errorDescription: String? {
        description
    }

    public var description: String {
        "Transaction retry exhausted after \(attempts) attempt(s) for \(operationDescription): \(lastErrorDescription)"
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
/// - Initial delay: Configurable via `DatabaseConfiguration.shared.transactionInitialDelay` (default: 10ms)
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

    /// Initial backoff delay in milliseconds
    ///
    /// Sourced from `DatabaseConfiguration.shared.transactionInitialDelay`
    /// Can be configured via `DATABASE_TRANSACTION_INITIAL_DELAY` environment variable.
    private static var initialDelayMs: Int {
        DatabaseConfiguration.shared.transactionInitialDelay
    }

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
        readVersionCache: ReadVersionCache? = nil,
        operationDescription: String = "transaction",
        onRetry: (@Sendable (_ attempt: Int, _ error: StorageError) -> Void)? = nil,
        onCancel: (@Sendable (_ transaction: any Transaction) -> Void)? = nil,
        onCommitSuccess: (@Sendable (_ transaction: any Transaction, _ commitNanos: UInt64) -> Void)? = nil,
        operation: @Sendable (any Transaction) async throws -> T
    ) async throws -> T {
        let maxAttempts = max(1, configuration.retryLimit)
        let maxDelayMs = configuration.maxRetryDelay
        var lastRetryableError: StorageError?

        for attempt in 0..<maxAttempts {
            try Task.checkCancellation()
            var transaction: (any Transaction)?

            do {
                // 1. Create NEW transaction for each attempt
                let newTransaction = try database.createTransaction()
                transaction = newTransaction

                // 2. Apply configuration options
                try configuration.apply(to: newTransaction)

                // 3. Apply cached read version (only on first attempt)
                //    On retry, we want a fresh version to avoid repeating transaction_too_old errors
                if attempt == 0 {
                    applyCachedReadVersion(
                        to: newTransaction,
                        configuration: configuration,
                        cache: readVersionCache
                    )
                }

                // 4. Execute operation (set TaskLocal for nested transaction detection)
                let result = try await ActiveTransactionScope.$current.withValue(newTransaction) {
                    try await operation(newTransaction)
                }

                // 5. Commit (throws on failure)
                let commitStart = DispatchTime.now().uptimeNanoseconds
                try await newTransaction.commit()
                let commitNanos = DispatchTime.now().uptimeNanoseconds - commitStart

                // 6. Update cache after successful commit
                await updateCacheAfterCommit(
                    transaction: newTransaction,
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
                return result

            } catch is CancellationError {
                if let transaction {
                    transaction.cancel()
                    onCancel?(transaction)
                }
                throw CancellationError()
            } catch {
                // Cancel transaction before retry or rethrow
                // Reference: FDB best practice - cancel uncommitted transactions
                if let transaction {
                    transaction.cancel()
                    onCancel?(transaction)
                }

                // 6. Check if retryable
                if let storageError = error as? StorageError, storageError.isRetryable {
                    lastRetryableError = storageError
                    if attempt < maxAttempts - 1 {
                        logger.debug(
                            "Transaction retry scheduled",
                            metadata: [
                                "operation": "\(operationDescription)",
                                "attempt": "\(attempt + 1)",
                                "maxAttempts": "\(maxAttempts)",
                                "error": "\(storageError.localizedDescription)"
                            ]
                        )
                        Self.retryCounter.increment()
                        onRetry?(attempt + 1, storageError)
                        try Task.checkCancellation()
                        // Apply exponential backoff before retry
                        try await applyBackoff(attempt: attempt, maxDelayMs: maxDelayMs)
                        continue
                    }
                    Self.retryExhaustedCounter.increment()
                    logger.error(
                        "Transaction retry exhausted",
                        metadata: [
                            "operation": "\(operationDescription)",
                            "attempts": "\(maxAttempts)",
                            "error": "\(storageError.localizedDescription)"
                        ]
                    )
                    throw TransactionRetryExhaustedError(
                        attempts: maxAttempts,
                        operationDescription: operationDescription,
                        lastError: storageError
                    )
                }

                // Non-retryable error or max retries exceeded
                throw error
            }
        }

        // Should not reach here, but safety fallback
        throw TransactionRetryExhaustedError(
            attempts: maxAttempts,
            operationDescription: operationDescription,
            lastError: lastRetryableError ?? StorageError.transactionTooOld
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
    ) {
        guard let cache = cache,
              let cachedVersion = cache.getCachedVersion(policy: configuration.cachePolicy) else {
            return
        }

        transaction.setReadVersion(cachedVersion)
    }

    /// Update cache after successful commit
    ///
    /// For write transactions: use committed version (most accurate)
    /// For read-only transactions: use read version
    private func updateCacheAfterCommit(
        transaction: any Transaction,
        cache: ReadVersionCache?
    ) async {
        guard let cache = cache else { return }

        do {
            let committedVersion = try transaction.getCommittedVersion()
            if committedVersion > 0 {
                // Write transaction: use committed version
                cache.updateFromCommit(version: committedVersion)
            } else {
                // Read-only transaction: use read version
                let readVersion = try await transaction.getReadVersion()
                cache.updateFromRead(version: readVersion)
            }
        } catch {
            // Ignore cache update errors - they're not critical
        }
    }

    // MARK: - Backoff

    /// Apply exponential backoff with jitter
    ///
    /// - Parameters:
    ///   - attempt: Current attempt number (0-based)
    ///   - maxDelayMs: Maximum delay in milliseconds
    private func applyBackoff(attempt: Int, maxDelayMs: Int) async throws {
        let delayMs = Self.calculateBackoff(attempt: attempt, maxDelayMs: maxDelayMs)
        try await Task.sleep(nanoseconds: UInt64(delayMs) * 1_000_000)
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
    static func calculateBackoff(attempt: Int, maxDelayMs: Int) -> Int {
        // Cap exponent at 10 to avoid overflow (2^10 = 1024)
        let exponent = min(attempt, 10)
        let exponentialDelay = initialDelayMs * (1 << exponent)
        let cappedDelay = min(exponentialDelay, maxDelayMs)

        // Add jitter (0-50% of delay) to prevent thundering herd
        let jitter = Int.random(in: 0...(cappedDelay / 2))
        return cappedDelay + jitter
    }
}

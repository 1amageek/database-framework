// TransactionRunner.swift
// DatabaseEngine - Transaction execution with retry logic
//
// Reference: FoundationDB transaction retry pattern
// https://apple.github.io/foundationdb/developer-guide.html#transactions

import Foundation
import StorageKit

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
/// - Initial delay: Configurable via `DatabaseConfiguration.shared.transactionInitialDelay` (default: 300ms)
/// - Exponential growth: delay doubles each attempt (300ms → 600ms → 1000ms cap)
/// - Capped by `maxRetryDelay` from configuration (default: 1000ms)
/// - Jitter: 0-50% added to prevent thundering herd
///
/// **Environment Variable**: `DATABASE_TRANSACTION_INITIAL_DELAY` to configure initial delay
///
/// **Reference**: FDB client retry loop pattern, AWS exponential backoff
internal struct TransactionRunner: Sendable {
    // MARK: - Properties

    /// StorageEngine is internally thread-safe (manages FDB connections)
    nonisolated(unsafe) private let database: any StorageEngine

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
        operation: @Sendable (any Transaction) async throws -> T
    ) async throws -> T {
        let maxRetries = max(1, configuration.retryLimit)
        let maxDelayMs = configuration.maxRetryDelay

        for attempt in 0..<maxRetries {
            // 1. Create NEW transaction for each attempt
            let transaction = try database.createTransaction()

            // 2. Apply configuration options
            try configuration.apply(to: transaction)

            // 3. Apply cached read version (only on first attempt)
            //    On retry, we want a fresh version to avoid repeating transaction_too_old errors
            if attempt == 0 {
                applyCachedReadVersion(
                    to: transaction,
                    configuration: configuration,
                    cache: readVersionCache
                )
            }

            do {
                // 4. Execute operation (set TaskLocal for nested transaction detection)
                let result = try await ActiveTransactionScope.$current.withValue(transaction) {
                    try await operation(transaction)
                }

                // 5. Commit (throws on failure)
                try await transaction.commit()

                // 6. Update cache after successful commit
                await updateCacheAfterCommit(
                    transaction: transaction,
                    cache: readVersionCache
                )
                return result

            } catch {
                // Cancel transaction before retry or rethrow
                // Reference: FDB best practice - cancel uncommitted transactions
                transaction.cancel()

                // 6. Check if retryable
                if let storageError = error as? StorageError, storageError.isRetryable {
                    if attempt < maxRetries - 1 {
                        // Apply exponential backoff before retry
                        try await applyBackoff(attempt: attempt, maxDelayMs: maxDelayMs)
                        continue
                    }
                }

                // Non-retryable error or max retries exceeded
                throw error
            }
        }

        // Should not reach here, but safety fallback
        throw StorageError.transactionTooOld
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


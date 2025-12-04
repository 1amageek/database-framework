// TransactionRunner.swift
// DatabaseEngine - Transaction execution with retry logic
//
// Reference: FoundationDB transaction retry pattern
// https://apple.github.io/foundationdb/developer-guide.html#transactions

import Foundation
import FoundationDB

// MARK: - TransactionRunner

/// Executes transactions with configurable retry logic and exponential backoff
///
/// TransactionRunner handles:
/// - Creating new transactions for each retry attempt
/// - Applying TransactionConfiguration options
/// - Retrying on retryable FDB errors with exponential backoff
/// - Respecting retry limits from configuration
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

    /// DatabaseProtocol is internally thread-safe (manages FDB connections)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Initial backoff delay in milliseconds
    ///
    /// Sourced from `DatabaseConfiguration.shared.transactionInitialDelay`
    /// Can be configured via `DATABASE_TRANSACTION_INITIAL_DELAY` environment variable.
    private static var initialDelayMs: Int {
        DatabaseConfiguration.shared.transactionInitialDelay
    }

    // MARK: - Initialization

    init(database: any DatabaseProtocol) {
        self.database = database
    }

    // MARK: - Execution

    /// Execute a transaction with the given configuration
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority)
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: FDBError if the transaction cannot be committed after retries
    func run<T: Sendable>(
        configuration: TransactionConfiguration,
        operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let maxRetries = max(1, configuration.retryLimit)
        let maxDelayMs = configuration.maxRetryDelay

        for attempt in 0..<maxRetries {
            // 1. Create NEW transaction for each attempt
            let transaction = try database.createTransaction()

            // 2. Apply configuration options
            try configuration.apply(to: transaction)

            do {
                // 3. Execute operation
                let result = try await operation(transaction)

                // 4. Commit
                let committed = try await transaction.commit()
                if committed {
                    return result
                }

                // Commit returned false → apply backoff before retry
                if attempt < maxRetries - 1 {
                    try await applyBackoff(attempt: attempt, maxDelayMs: maxDelayMs)
                }
                continue

            } catch {
                // 5. Check if retryable
                if let fdbError = error as? FDBError {
                    if fdbError.isRetryable && attempt < maxRetries - 1 {
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
        throw FDBError(.transactionTooOld)
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

// MARK: - FDBError Extension

extension FDBError {
    /// Create an FDBError from an error code enum case
    init(_ code: FDBErrorCode) {
        self.init(code: Int(code.rawValue))
    }
}

/// Internal error codes matching FDB
internal enum FDBErrorCode: Int32 {
    case transactionTooOld = 1020
}

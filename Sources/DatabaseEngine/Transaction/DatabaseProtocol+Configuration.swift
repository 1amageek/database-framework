// DatabaseProtocol+Configuration.swift
// DatabaseEngine - Extension to add TransactionConfiguration support to DatabaseProtocol
//
// This extension bridges database-framework's TransactionConfiguration with
// fdb-swift-bindings' DatabaseProtocol, enabling configuration-aware transactions.

import Foundation
import FoundationDB

// MARK: - DatabaseProtocol Extension

extension DatabaseProtocol {
    /// Execute a transaction with configuration
    ///
    /// This method wraps the standard `withTransaction` to apply `TransactionConfiguration`
    /// options (priority, timeout, retry limit, etc.) before executing the operation.
    ///
    /// **Usage**:
    /// ```swift
    /// try await database.withTransaction(configuration: .batch) { transaction in
    ///     // Low-priority batch operation
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration to apply
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: `FDBError` if the transaction fails
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let maxRetries = configuration.retryLimit ?? 100

        for attempt in 0..<maxRetries {
            let transaction = try createTransaction()
            try transaction.apply(configuration)

            do {
                let result = try await operation(transaction)
                let committed = try await transaction.commit()

                if committed {
                    return result
                }
            } catch {
                transaction.cancel()

                if let fdbError = error as? FDBError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
                        // Exponential backoff with jitter
                        let maxDelay = configuration.maxRetryDelay ?? 1000
                        let baseDelay = min(maxDelay, 10 * (1 << min(attempt, 10)))
                        let jitter = Int.random(in: 0...(baseDelay / 4))
                        let delay = baseDelay + jitter
                        try await Task.sleep(nanoseconds: UInt64(delay) * 1_000_000)
                        continue
                    }
                }

                throw error
            }
        }

        throw FDBError(code: 1020)  // transaction_too_old
    }
}

// MARK: - Convenience Presets

extension DatabaseProtocol {
    /// Execute a batch-priority transaction
    ///
    /// Optimized for background processing with:
    /// - Low priority (won't interfere with interactive traffic)
    /// - Longer timeout (30 seconds)
    /// - More retries (20)
    public func withBatchTransaction<T: Sendable>(
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await withTransaction(configuration: .batch, operation)
    }

    /// Execute a system-priority transaction
    ///
    /// For critical system operations with:
    /// - Highest priority
    /// - Short timeout (2 seconds)
    /// - Few retries (5)
    public func withSystemTransaction<T: Sendable>(
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await withTransaction(configuration: .system, operation)
    }

    /// Execute a read-only transaction with GRV cache
    ///
    /// Optimized for read operations with:
    /// - GRV cache enabled (reduces latency)
    /// - Normal priority
    public func withReadOnlyTransaction<T: Sendable>(
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await withTransaction(configuration: .readOnly, operation)
    }

    /// Execute an interactive transaction
    ///
    /// For user-facing operations with:
    /// - Short timeout (1 second)
    /// - Few retries (3)
    public func withInteractiveTransaction<T: Sendable>(
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await withTransaction(configuration: .interactive, operation)
    }
}

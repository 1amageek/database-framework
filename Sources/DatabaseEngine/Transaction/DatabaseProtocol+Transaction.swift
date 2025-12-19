// DatabaseProtocol+Transaction.swift
// DatabaseEngine - Convenience extensions for DatabaseProtocol transaction execution
//
// Provides a higher-level API for running transactions with configuration.

import Foundation
import FoundationDB

// MARK: - DatabaseProtocol Extension

extension DatabaseProtocol {
    /// Execute a transaction with the specified configuration
    ///
    /// This method uses `TransactionRunner` to provide:
    /// - Exponential backoff with jitter (prevents thundering herd)
    /// - Configurable retry limits (respects `configuration.retryLimit`)
    /// - Configurable max delay (respects `configuration.maxRetryDelay`)
    /// - Weak read semantics support (when cache is provided)
    ///
    /// **Usage**:
    /// ```swift
    /// // Batch operation with proper priority and timeout
    /// try await database.withTransaction(configuration: .batch) { transaction in
    ///     // ... batch operations
    /// }
    ///
    /// // Interactive operation with short timeout
    /// try await database.withTransaction(configuration: .interactive) { transaction in
    ///     // ... user-facing operations
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration to apply
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: Error if transaction fails after all retry attempts
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let runner = TransactionRunner(database: self)
        return try await runner.run(configuration: configuration, operation: operation)
    }
}

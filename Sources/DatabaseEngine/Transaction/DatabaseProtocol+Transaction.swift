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
    /// This is the preferred way to run transactions with specific settings.
    /// Configuration is automatically applied, eliminating manual `apply(to:)` calls.
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
    /// - Throws: Error if transaction fails
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        try await self.withTransaction { transaction in
            try configuration.apply(to: transaction)
            return try await operation(transaction)
        }
    }
}

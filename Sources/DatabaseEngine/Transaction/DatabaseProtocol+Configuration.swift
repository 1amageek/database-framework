// DatabaseProtocol+Configuration.swift
// DatabaseEngine - Extension to add TransactionConfiguration support to DatabaseProtocol
//
// This extension bridges database-framework's TransactionConfiguration with
// fdb-swift-bindings' DatabaseProtocol, enabling configuration-aware transactions.

import Foundation
import FoundationDB
import Synchronization

// MARK: - Shared ReadVersionCache

/// Shared read version cache for GRV optimization
///
/// This cache is used by DatabaseProtocol extensions to share read versions
/// across transactions when useGrvCache is enabled.
///
/// **Usage**:
/// The cache is automatically used when `TransactionConfiguration.useGrvCache` is true.
/// Configure staleness with `SharedReadVersionCache.configure(staleness:)`.
public final class SharedReadVersionCache: Sendable {
    /// Shared singleton instance
    public static let shared = SharedReadVersionCache()

    /// Underlying cache
    private let cache: ReadVersionCache

    /// Default staleness for cached reads (seconds)
    private let staleness: Mutex<Double>

    private init() {
        self.cache = ReadVersionCache()
        self.staleness = Mutex(5.0)  // Default 5 second staleness
    }

    /// Configure the default staleness for cached reads
    ///
    /// - Parameter seconds: Maximum staleness in seconds
    public func configure(staleness seconds: Double) {
        staleness.withLock { $0 = seconds }
    }

    /// Get cached version if available
    public func getCachedVersion() -> Int64? {
        let stalenessSeconds = staleness.withLock { $0 }
        return cache.getCachedVersion(semantics: .bounded(seconds: stalenessSeconds))
    }

    /// Update the cached read version
    public func updateReadVersion(_ version: Int64) {
        cache.updateReadVersion(version, timestamp: Date())
    }

    /// Record a commit version
    public func recordCommitVersion(_ version: Int64) {
        cache.recordCommitVersion(version)
    }

    /// Invalidate the cache
    public func invalidate() {
        cache.invalidate()
    }

    /// Get cache statistics
    public var statistics: ReadVersionCacheStatistics {
        cache.statistics
    }
}

// MARK: - DatabaseProtocol Extension

extension DatabaseProtocol {
    /// Execute a transaction with configuration
    ///
    /// This method wraps the standard `withTransaction` to apply `TransactionConfiguration`
    /// options (priority, timeout, retry limit, etc.) before executing the operation.
    ///
    /// When `useGrvCache` is enabled, this method uses the shared ReadVersionCache
    /// to reduce GRV round-trips for improved latency.
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

            // Apply cached read version if GRV cache is enabled
            if configuration.useGrvCache {
                if let cachedVersion = SharedReadVersionCache.shared.getCachedVersion() {
                    transaction.setReadVersion(cachedVersion)
                }
            }

            do {
                let result = try await operation(transaction)
                let committed = try await transaction.commit()

                if committed {
                    // Update cache with commit version for future transactions
                    if configuration.useGrvCache {
                        if let commitVersion = try? transaction.getCommittedVersion() {
                            SharedReadVersionCache.shared.recordCommitVersion(commitVersion)
                        }
                    }
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

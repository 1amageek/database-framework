// TransactionConfiguration.swift
// DatabaseEngine - Configurable transaction settings
//
// Reference: FoundationDB transaction options
// https://apple.github.io/foundationdb/api-general.html#transaction-options

import Foundation
import FoundationDB

// MARK: - TransactionPriority

/// Transaction priority level
///
/// Controls how the transaction is scheduled relative to other transactions.
///
/// **Reference**: FDB transaction options `prioritySystemImmediate`, `priorityBatch`
public enum TransactionPriority: String, Sendable, Hashable, Codable {
    /// Default priority - normal transaction processing
    case `default`

    /// Batch priority - lower priority, useful for background work
    ///
    /// Transactions will be processed after default priority transactions.
    /// May be throttled or cut off during high load or machine failures.
    /// Use for batch jobs that can tolerate delays.
    case batch

    /// System immediate priority - highest priority
    ///
    /// Lower priority transactions will block behind this one.
    /// **Warning**: Use is discouraged outside of low-level tools.
    case system
}

// MARK: - ReadPriority

/// Read operation priority level
///
/// Controls the priority of read operations within a transaction.
///
/// **Reference**: FDB transaction options `readPriorityLow`, `readPriorityHigh`
public enum ReadPriority: String, Sendable, Hashable, Codable {
    /// Normal read priority (default)
    case normal

    /// Low read priority - for background scans
    ///
    /// Use for batch operations that should not interfere with
    /// latency-sensitive workloads.
    case low

    /// High read priority - for urgent reads
    ///
    /// Use sparingly for time-critical operations.
    case high
}

// MARK: - TransactionConfiguration

/// Configuration for transaction behavior
///
/// Provides type-safe configuration for FDB transaction options including
/// timeout, retry limits, and priority settings.
///
/// **Usage**:
/// ```swift
/// // Use preset configurations
/// try await context.withTransaction(configuration: .batch) { tx in
///     // Batch processing with appropriate timeouts and priorities
/// }
///
/// // Custom configuration
/// let config = TransactionConfiguration(
///     timeout: 5000,
///     retryLimit: 5,
///     priority: .default
/// )
///
/// // With tracing enabled
/// let tracedConfig = TransactionConfiguration(
///     timeout: 5000,
///     tracing: .init(transactionID: "user-request-12345")
/// )
/// ```
///
/// **Reference**: FDB transaction options, FDB Record Layer FDBRecordContextConfig
public struct TransactionConfiguration: Sendable, Hashable {
    // MARK: - Properties

    /// Timeout in milliseconds
    ///
    /// When elapsed, the transaction is automatically cancelled.
    /// - `nil`: Use FDB default (typically 5 seconds)
    /// - `0`: Disable timeout
    ///
    /// **Reference**: FDB `timeout` option (code 500)
    public let timeout: Int?

    /// Maximum number of retry attempts
    ///
    /// After this many retries, the transaction will fail with an error.
    /// Values ≤ 0 are treated as 1 (minimum one attempt).
    ///
    /// Default: `DatabaseConfiguration.shared.transactionRetryLimit` (5)
    ///
    /// **Note**: Unlimited retries are not supported to prevent runaway transactions.
    public let retryLimit: Int

    /// Maximum delay between retries in milliseconds
    ///
    /// Caps the exponential backoff delay.
    ///
    /// Default: `DatabaseConfiguration.shared.transactionMaxRetryDelay` (1000ms)
    public let maxRetryDelay: Int

    /// Transaction priority
    ///
    /// Controls scheduling relative to other transactions.
    public let priority: TransactionPriority

    /// Read operation priority
    ///
    /// Controls priority of read operations within the transaction.
    public let readPriority: ReadPriority

    /// Whether to disable server-side caching for reads
    ///
    /// Set to `true` for reads not expected to be repeated,
    /// to avoid polluting the cache.
    ///
    /// **Reference**: FDB `readServerSideCacheDisable` option (code 508)
    public let disableReadCache: Bool

    /// Cache policy for read operations
    ///
    /// Controls whether transactions reuse cached read versions,
    /// reducing `getReadVersion()` network round-trips.
    ///
    /// - `.server`: Strict consistency (always get fresh read version)
    /// - `.cached`: Use cached read version if available (no time limit)
    /// - `.stale(N)`: Use cached read version only if younger than N seconds
    public let cachePolicy: CachePolicy

    /// Tracing and logging configuration
    public let tracing: Tracing

    // MARK: - Initialization

    /// Create a custom transaction configuration
    ///
    /// Default values are sourced from `DatabaseConfiguration.shared`, which can be
    /// configured via environment variables:
    /// - `DATABASE_TRANSACTION_RETRY_LIMIT`
    /// - `DATABASE_TRANSACTION_MAX_RETRY_DELAY`
    /// - `DATABASE_TRANSACTION_TIMEOUT`
    ///
    /// - Parameters:
    ///   - timeout: Timeout in milliseconds (default: from DatabaseConfiguration)
    ///   - retryLimit: Max retry attempts (default: from DatabaseConfiguration)
    ///   - maxRetryDelay: Max delay between retries in ms (default: from DatabaseConfiguration)
    ///   - priority: Transaction priority (default: .default)
    ///   - readPriority: Read operation priority (default: .normal)
    ///   - disableReadCache: Whether to disable server-side read caching (default: false)
    ///   - cachePolicy: Cache policy for read operations (default: .server = strict consistency)
    ///   - tracing: Tracing and logging configuration (default: .disabled)
    public init(
        timeout: Int? = DatabaseConfiguration.shared.transactionTimeout,
        retryLimit: Int = DatabaseConfiguration.shared.transactionRetryLimit,
        maxRetryDelay: Int = DatabaseConfiguration.shared.transactionMaxRetryDelay,
        priority: TransactionPriority = .default,
        readPriority: ReadPriority = .normal,
        disableReadCache: Bool = false,
        cachePolicy: CachePolicy = .server,
        tracing: Tracing = .disabled
    ) {
        self.timeout = timeout
        self.retryLimit = retryLimit
        self.maxRetryDelay = maxRetryDelay
        self.priority = priority
        self.readPriority = readPriority
        self.disableReadCache = disableReadCache
        self.cachePolicy = cachePolicy
        self.tracing = tracing
    }

    // MARK: - Presets

    /// Default configuration
    ///
    /// Uses FDB defaults for all settings.
    public static let `default` = TransactionConfiguration()

    /// Batch processing configuration
    ///
    /// Optimized for background batch operations:
    /// - Longer timeout (30 seconds)
    /// - More retries (20)
    /// - Batch priority (lower than interactive)
    /// - Low read priority
    /// - Server-side cache disabled (to avoid cache pollution)
    /// - Strict consistency (no weak read semantics)
    ///
    /// **Note**: Weak read semantics is disabled for batch processing because
    /// operations like index building require the latest data to ensure consistency.
    /// Use `.longRunning` if you need relaxed consistency for read-heavy workloads.
    public static let batch = TransactionConfiguration(
        timeout: 30_000,
        retryLimit: 20,
        maxRetryDelay: 2000,
        priority: .batch,
        readPriority: .low,
        disableReadCache: true
        // No weakReadSemantics: batch operations need latest data for consistency
    )

    /// System/administrative configuration
    ///
    /// For critical system operations:
    /// - Short timeout (2 seconds)
    /// - Limited retries (5)
    /// - System priority (highest)
    /// - High read priority
    public static let system = TransactionConfiguration(
        timeout: 2_000,
        retryLimit: 5,
        priority: .system,
        readPriority: .high
    )

    /// Interactive/user-facing configuration
    ///
    /// For latency-sensitive operations:
    /// - Short timeout (1 second)
    /// - Limited retries (3)
    /// - Default priority
    public static let interactive = TransactionConfiguration(
        timeout: 1_000,
        retryLimit: 3
    )

    /// Long-running operation configuration
    ///
    /// For operations that may take extended time:
    /// - Extended timeout (60 seconds)
    /// - Many retries (50)
    /// - Batch priority
    /// - Cache policy: .stale(60) (up to 60 second staleness)
    public static let longRunning = TransactionConfiguration(
        timeout: 60_000,
        retryLimit: 50,
        maxRetryDelay: 5000,
        priority: .batch,
        readPriority: .low,
        cachePolicy: .stale(60)
    )

    /// Read-only configuration
    ///
    /// Optimized for read-only operations that can use cached versions:
    /// - Short timeout (2 seconds)
    /// - Limited retries (3)
    /// - Default priority
    /// - Cache policy: .cached (use cached version if available)
    ///
    /// **Use Case**: Dashboard queries, analytics reads, cached lookups
    /// where slightly stale data is acceptable.
    public static let readOnly = TransactionConfiguration(
        timeout: 2_000,
        retryLimit: 3,
        cachePolicy: .cached
    )
}

// MARK: - Tracing Configuration

extension TransactionConfiguration {
    /// Tracing and logging configuration for transactions
    ///
    /// Groups observability-related settings for cleaner API.
    ///
    /// **Usage**:
    /// ```swift
    /// // Simple tracing with just transaction ID
    /// let config = TransactionConfiguration(
    ///     tracing: .init(transactionID: "req-12345")
    /// )
    ///
    /// // Full debugging configuration
    /// let debugConfig = TransactionConfiguration(
    ///     tracing: .init(
    ///         transactionID: "debug-session",
    ///         logTransaction: true,
    ///         serverRequestTracing: true,
    ///         tags: ["debug", "performance"]
    ///     )
    /// )
    /// ```
    public struct Tracing: Sendable, Hashable {
        /// Transaction identifier for log correlation
        ///
        /// A unique identifier used to correlate logs across different systems.
        /// When set, this ID is:
        /// - Included in transaction lifecycle events
        /// - Passed to FDB client trace logs (if `logTransaction` is enabled)
        /// - Available for custom logging and debugging
        ///
        /// **Best Practice**: Use request IDs or trace IDs from your distributed tracing system.
        ///
        /// **Reference**: FDB Record Layer `FDBRecordContextConfig.transactionID`
        public let transactionID: String?

        /// Enable detailed transaction logging to FDB client trace logs
        ///
        /// When `true`, all read and written keys/values are logged to FDB client trace logs.
        /// This is useful for debugging but has high overhead.
        ///
        /// **Requirements**: `transactionID` must be set for this to work.
        ///
        /// **Warning**: This logs sensitive data. Use only in development/debugging.
        ///
        /// **Reference**: FDB Record Layer `FDBRecordContextConfig.logTransaction`
        public let logTransaction: Bool

        /// Enable server request tracing
        ///
        /// When `true`, additional logging and tracing for each FDB server operation
        /// associated with this transaction is enabled.
        ///
        /// **Warning**: High overhead. Use only for performance debugging.
        ///
        /// **Reference**: FDB Record Layer `FDBRecordContextConfig.serverRequestTracing`
        public let serverRequestTracing: Bool

        /// Tags for transaction categorization
        ///
        /// Tags can be used for:
        /// - Filtering logs
        /// - Metrics grouping
        /// - Debugging
        ///
        /// Example: `["user-request", "api-v2"]`
        public let tags: Set<String>

        /// Disabled tracing (default)
        public static let disabled = Tracing()

        /// Create a tracing configuration
        ///
        /// - Parameters:
        ///   - transactionID: Transaction ID for log correlation (default: nil)
        ///   - logTransaction: Enable detailed FDB logging (default: false)
        ///   - serverRequestTracing: Enable server tracing (default: false)
        ///   - tags: Tags for categorization (default: empty)
        public init(
            transactionID: String? = nil,
            logTransaction: Bool = false,
            serverRequestTracing: Bool = false,
            tags: Set<String> = []
        ) {
            self.transactionID = transactionID
            self.logTransaction = logTransaction
            self.serverRequestTracing = serverRequestTracing
            self.tags = tags
        }
    }
}

// MARK: - Convenience Accessors

extension TransactionConfiguration {
    /// Transaction ID for log correlation (convenience accessor)
    public var transactionID: String? { tracing.transactionID }

    /// Whether detailed transaction logging is enabled (convenience accessor)
    public var logTransaction: Bool { tracing.logTransaction }

    /// Whether server request tracing is enabled (convenience accessor)
    public var serverRequestTracing: Bool { tracing.serverRequestTracing }

    /// Tags for categorization (convenience accessor)
    public var tags: Set<String> { tracing.tags }
}

// MARK: - Apply to Transaction

extension TransactionConfiguration {
    /// Apply this configuration to a raw transaction
    ///
    /// **Preferred**: Use `database.withTransaction(configuration:)` instead, which
    /// automatically applies the configuration:
    /// ```swift
    /// try await database.withTransaction(configuration: .batch) { transaction in
    ///     // ... batch operations
    /// }
    /// ```
    ///
    /// **Direct Usage** (when you need manual control):
    /// ```swift
    /// try await database.withTransaction { transaction in
    ///     try TransactionConfiguration.batch.apply(to: transaction)
    ///     // ... operations
    /// }
    /// ```
    ///
    /// - Parameter transaction: The transaction to configure
    /// - Throws: FDBError if option setting fails
    ///
    /// **Note**: `retryLimit` is NOT applied to the FDB transaction here because
    /// TransactionRunner manages retries at a higher level. Applying retryLimit
    /// to both would cause double retry control and unexpected behavior.
    public func apply(to transaction: any TransactionProtocol) throws {
        // Transaction priority
        switch priority {
        case .batch:
            try transaction.setOption(forOption: .priorityBatch)
        case .system:
            try transaction.setOption(forOption: .prioritySystemImmediate)
        case .default:
            break
        }

        // Read priority
        switch readPriority {
        case .low:
            try transaction.setOption(forOption: .readPriorityLow)
        case .high:
            try transaction.setOption(forOption: .readPriorityHigh)
        case .normal:
            break
        }

        // Timeout
        if let timeout = timeout {
            try transaction.setOption(to: timeout, forOption: .timeout)
        }

        // Note: retryLimit and maxRetryDelay are NOT applied to FDB transaction.
        // TransactionRunner manages retries with its own exponential backoff.

        // Disable server-side read cache
        if disableReadCache {
            try transaction.setOption(forOption: .readServerSideCacheDisable)
        }
    }
}

// MARK: - CustomStringConvertible

extension TransactionConfiguration: CustomStringConvertible {
    public var description: String {
        var parts: [String] = []

        if let timeout = timeout {
            parts.append("timeout: \(timeout)ms")
        }
        if retryLimit != 5 {
            parts.append("retryLimit: \(retryLimit)")
        }
        if maxRetryDelay != 1000 {
            parts.append("maxRetryDelay: \(maxRetryDelay)ms")
        }
        if priority != .default {
            parts.append("priority: .\(priority)")
        }
        if readPriority != .normal {
            parts.append("readPriority: .\(readPriority)")
        }
        if disableReadCache {
            parts.append("disableReadCache: true")
        }
        if cachePolicy != .server {
            parts.append("cachePolicy: \(cachePolicy)")
        }
        if tracing != .disabled {
            parts.append("tracing: \(tracing)")
        }

        if parts.isEmpty {
            return "TransactionConfiguration.default"
        }

        return "TransactionConfiguration(\(parts.joined(separator: ", ")))"
    }
}

extension TransactionConfiguration.Tracing: CustomStringConvertible {
    public var description: String {
        var parts: [String] = []

        if let id = transactionID {
            parts.append("transactionID: \"\(id)\"")
        }
        if logTransaction {
            parts.append("logTransaction: true")
        }
        if serverRequestTracing {
            parts.append("serverRequestTracing: true")
        }
        if !tags.isEmpty {
            parts.append("tags: \(tags)")
        }

        return "Tracing(\(parts.joined(separator: ", ")))"
    }
}

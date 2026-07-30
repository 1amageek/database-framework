// TransactionConfiguration.swift
// DatabaseEngine - Configurable transaction settings
//
// Reference: FoundationDB transaction options
// https://apple.github.io/foundationdb/api-general.html#transaction-options

import StorageKit

// MARK: - TransactionPriority

/// Transaction priority level
///
/// Controls how the transaction is scheduled relative to other transactions.
///
/// **Reference**: FDB transaction options `prioritySystemImmediate`, `priorityBatch`
public enum TransactionPriority: String, Sendable, Hashable {
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
public enum ReadPriority: String, Sendable, Hashable {
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
///     maximumAttempts: 5,
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

    /// Maximum total number of transaction attempts, including the first.
    ///
    /// Values less than one are rejected before a transaction is created.
    ///
    /// Default: 5
    ///
    /// **Note**: Unlimited retries are not supported to prevent runaway transactions.
    public let maximumAttempts: Int

    /// Maximum delay between retries in milliseconds
    ///
    /// Caps the exponential backoff delay.
    ///
    /// Default: 250ms
    public let maxRetryDelay: Int

    /// Initial delay before the first retry, in milliseconds.
    public let initialRetryDelay: Int

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

    /// Maximum portable logical mutation bytes accepted in one transaction
    /// attempt. `nil` leaves accounting disabled for trusted internal work.
    public let maximumMutationAggregateBytes: Int?

    // MARK: - Initialization

    /// Create a custom transaction configuration
    ///
    /// - Parameters:
    ///   - timeout: Timeout in milliseconds
    ///   - maximumAttempts: Maximum total attempts, including the first
    ///   - maxRetryDelay: Maximum delay between retries in milliseconds
    ///   - initialRetryDelay: Initial retry delay in milliseconds
    ///   - priority: Transaction priority (default: .default)
    ///   - readPriority: Read operation priority (default: .normal)
    ///   - disableReadCache: Whether to disable server-side read caching (default: false)
    ///   - cachePolicy: Cache policy for read operations (default: .server = strict consistency)
    ///   - tracing: Tracing and logging configuration (default: .disabled)
    public init(
        timeout: Int? = nil,
        maximumAttempts: Int = 5,
        maxRetryDelay: Int = 250,
        initialRetryDelay: Int = 10,
        priority: TransactionPriority = .default,
        readPriority: ReadPriority = .normal,
        disableReadCache: Bool = false,
        cachePolicy: CachePolicy = .server,
        tracing: Tracing = .disabled,
        maximumMutationAggregateBytes: Int? = nil
    ) {
        self.timeout = timeout
        self.maximumAttempts = maximumAttempts
        self.maxRetryDelay = maxRetryDelay
        self.initialRetryDelay = initialRetryDelay
        self.priority = priority
        self.readPriority = readPriority
        self.disableReadCache = disableReadCache
        self.cachePolicy = cachePolicy
        self.tracing = tracing
        self.maximumMutationAggregateBytes = maximumMutationAggregateBytes
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
        maximumAttempts: 20,
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
        maximumAttempts: 5,
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
        maximumAttempts: 3
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
        maximumAttempts: 50,
        maxRetryDelay: 5000,
        priority: .batch,
        readPriority: .low,
        cachePolicy: .stale(.seconds(60))
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
        maximumAttempts: 3,
        cachePolicy: .cached
    )

    /// Returns the same transaction policy with a different total timeout.
    /// The timeout covers all retry attempts; once commit is dispatched the
    /// backend's authoritative commit outcome still takes precedence.
    public func replacing(timeout: Int?) -> TransactionConfiguration {
        TransactionConfiguration(
            timeout: timeout,
            maximumAttempts: maximumAttempts,
            maxRetryDelay: maxRetryDelay,
            initialRetryDelay: initialRetryDelay,
            priority: priority,
            readPriority: readPriority,
            disableReadCache: disableReadCache,
            cachePolicy: cachePolicy,
            tracing: tracing,
            maximumMutationAggregateBytes: maximumMutationAggregateBytes
        )
    }

    /// Returns the same transaction policy with a portable aggregate mutation
    /// payload limit. The runner creates a fresh meter for every retry attempt.
    public func limitingMutationAggregateBytes(
        to maximumBytes: Int
    ) -> TransactionConfiguration {
        TransactionConfiguration(
            timeout: timeout,
            maximumAttempts: maximumAttempts,
            maxRetryDelay: maxRetryDelay,
            initialRetryDelay: initialRetryDelay,
            priority: priority,
            readPriority: readPriority,
            disableReadCache: disableReadCache,
            cachePolicy: cachePolicy,
            tracing: tracing,
            maximumMutationAggregateBytes: maximumBytes
        )
    }
}

// MARK: - Validation

extension TransactionConfiguration {
    /// Validate the portable execution policy before creating a transaction.
    public func validate() throws {
        if let timeout, timeout < 0 {
            throw StorageError.invalidOperation(
                "Transaction timeout must be non-negative"
            )
        }
        guard maximumAttempts > 0 else {
            throw StorageError.invalidOperation(
                "Transaction maximumAttempts must be greater than zero"
            )
        }
        guard maxRetryDelay >= 0 else {
            throw StorageError.invalidOperation(
                "Transaction maxRetryDelay must be non-negative"
            )
        }
        guard initialRetryDelay >= 0 else {
            throw StorageError.invalidOperation(
                "Transaction initialRetryDelay must be non-negative"
            )
        }
        if let maximumMutationAggregateBytes,
           maximumMutationAggregateBytes <= 0 {
            throw StorageError.invalidOperation(
                "Transaction mutation aggregate byte limit must be positive"
            )
        }
    }
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

// MARK: - Transaction Runner Configuration

extension TransactionConfiguration {
    /// Applies this configuration before an owned transaction is exposed.
    ///
    /// `TransactionRunner` is the lifecycle owner. Database operations receive
    /// only `TransactionAccess` after this configuration is resolved.
    ///
    /// - Parameter transaction: The transaction to configure
    /// - Returns: The exact native and portable policy resolution.
    /// - Throws: A typed storage error when a requested native option fails.
    ///
    /// **Note**: `maximumAttempts` is NOT applied to the FDB transaction here because
    /// TransactionRunner manages retries at a higher level. Applying maximumAttempts
    /// to both would cause double retry control and unexpected behavior.
    package func apply(
        to transaction: any Transaction
    ) throws -> TransactionConfigurationResolution {
        try validate()

        let capabilities = transaction.capabilities
        var unsupportedHints: [TransactionConfigurationResolution.UnsupportedHint] = []

        // Transaction priority
        switch priority {
        case .batch:
            if capabilities.schedulingPriority {
                try transaction.setOption(forOption: .priorityBatch)
            } else {
                unsupportedHints.append(.schedulingPriority)
            }
        case .system:
            if capabilities.schedulingPriority {
                try transaction.setOption(forOption: .prioritySystemImmediate)
            } else {
                unsupportedHints.append(.schedulingPriority)
            }
        case .default:
            break
        }

        // Read priority
        switch readPriority {
        case .low:
            if capabilities.readPriority {
                try transaction.setOption(forOption: .readPriorityLow)
            } else {
                unsupportedHints.append(.readPriority)
            }
        case .high:
            if capabilities.readPriority {
                try transaction.setOption(forOption: .readPriorityHigh)
            } else {
                unsupportedHints.append(.readPriority)
            }
        case .normal:
            break
        }

        // Timeout
        let backendTimeoutApplied: Bool
        if let timeout, capabilities.transactionTimeout {
            try transaction.setOption(to: timeout, forOption: .timeout(milliseconds: timeout))
            backendTimeoutApplied = true
        } else {
            backendTimeoutApplied = false
        }

        // Note: maximumAttempts and maxRetryDelay are NOT applied to FDB transaction.
        // TransactionRunner manages retries with its own exponential backoff.

        // Disable server-side read cache
        if disableReadCache {
            if capabilities.readCacheControl {
                try transaction.setOption(forOption: .readServerSideCacheDisable)
            } else {
                unsupportedHints.append(.readCacheControl)
            }
        }

        try transaction.configureMutationByteLimit(
            maximumBytes: maximumMutationAggregateBytes
        )

        return TransactionConfigurationResolution(
            backendTimeoutApplied: backendTimeoutApplied,
            executionTimeoutMilliseconds: timeout.flatMap { $0 == 0 ? nil : $0 },
            unsupportedHints: unsupportedHints
        )
    }
}

// MARK: - CustomStringConvertible

extension TransactionConfiguration: CustomStringConvertible {
    public var description: String {
        var parts: [String] = []

        if let timeout = timeout {
            parts.append("timeout: \(timeout)ms")
        }
        if maximumAttempts != 5 {
            parts.append("maximumAttempts: \(maximumAttempts)")
        }
        if maxRetryDelay != 250 {
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

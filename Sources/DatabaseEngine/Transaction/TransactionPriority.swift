// TransactionPriority.swift
// DatabaseEngine - Transaction configuration using FDB native types
//
// Design: Use FDB types directly to avoid translation layers.
// Reference: FDB Record Layer FDBTransactionPriority.java

import Foundation
import FoundationDB
import Synchronization

// MARK: - TransactionConfiguration

/// Configuration for transaction behavior using FDB native types
///
/// This configuration directly uses `FDB.TransactionOption` values,
/// eliminating the need for translation layers and enabling full
/// access to FDB capabilities.
///
/// **Usage**:
/// ```swift
/// // Use preset configurations
/// try await container.withTransaction(configuration: .batch) { tx in
///     // batch priority transaction
/// }
///
/// // Custom configuration
/// let config = TransactionConfiguration(
///     priority: .priorityBatch,
///     timeout: 5000,
///     retryLimit: 3
/// )
/// ```
///
/// **Reference**:
/// - FoundationDB Transaction Options: https://apple.github.io/foundationdb/api-general.html
public struct TransactionConfiguration: Sendable, Equatable {
    // MARK: - Priority Options

    /// Transaction priority option
    ///
    /// - `.priorityBatch`: Low priority for background jobs
    /// - `.prioritySystemImmediate`: Highest priority for system operations
    /// - `nil`: Default priority
    public let priority: FDB.TransactionOption?

    /// Read priority option
    ///
    /// - `.readPriorityLow`: Low priority reads
    /// - `.readPriorityNormal`: Normal priority reads (default)
    /// - `.readPriorityHigh`: High priority reads
    /// - `nil`: Use default
    public let readPriority: FDB.TransactionOption?

    // MARK: - Timeout and Retry

    /// Transaction timeout in milliseconds
    ///
    /// Maps to `FDB.TransactionOption.timeout`.
    /// Set to `nil` for FDB default (typically 5 seconds).
    public let timeout: Int?

    /// Maximum number of retries
    ///
    /// Maps to `FDB.TransactionOption.retryLimit`.
    public let retryLimit: Int?

    /// Maximum retry delay in milliseconds
    ///
    /// Maps to `FDB.TransactionOption.maxRetryDelay`.
    public let maxRetryDelay: Int?

    // MARK: - Read Options

    /// Use cached GRV (Get Read Version)
    ///
    /// When true, sets `FDB.TransactionOption.useGrvCache`.
    /// Reduces latency by avoiding GRV round-trip.
    ///
    /// **Note**: Requires `disableClientBypass` network option.
    public let useGrvCache: Bool

    /// Disable snapshot read-your-writes
    ///
    /// When true, sets `FDB.TransactionOption.snapshotRywDisable`.
    public let snapshotRywDisable: Bool

    // MARK: - Debugging

    /// Debug transaction identifier
    ///
    /// Maps to `FDB.TransactionOption.debugTransactionIdentifier`.
    /// Used for tracing and profiling.
    public let debugTransactionIdentifier: String?

    /// Enable transaction logging
    ///
    /// When true, sets `FDB.TransactionOption.logTransaction`.
    /// Requires `debugTransactionIdentifier` to be set.
    public let logTransaction: Bool

    /// Transaction tags for throttling
    ///
    /// Each tag maps to `FDB.TransactionOption.tag`.
    /// At most 5 tags can be set per transaction.
    public let tags: [String]

    // MARK: - Presets

    /// Default configuration - no special options
    public static let `default` = TransactionConfiguration()

    /// Read-only query configuration
    ///
    /// Optimized for read operations:
    /// - Uses GRV cache to reduce latency
    /// - Normal read priority
    public static let readOnly = TransactionConfiguration(
        useGrvCache: true
    )

    /// Batch processing configuration
    ///
    /// Optimized for background batch jobs:
    /// - Low priority (won't interfere with interactive traffic)
    /// - Longer timeout (30 seconds)
    /// - More retries (20)
    /// - Higher max retry delay (5 seconds)
    public static let batch = TransactionConfiguration(
        priority: .priorityBatch,
        readPriority: .readPriorityLow,
        timeout: 30_000,
        retryLimit: 20,
        maxRetryDelay: 5_000
    )

    /// System operations configuration
    ///
    /// For critical system operations:
    /// - Highest priority
    /// - Short timeout (2 seconds) - fail fast
    /// - Few retries (5)
    public static let system = TransactionConfiguration(
        priority: .prioritySystemImmediate,
        readPriority: .readPriorityHigh,
        timeout: 2_000,
        retryLimit: 5,
        maxRetryDelay: 100
    )

    /// Interactive operations configuration
    ///
    /// For user-facing operations:
    /// - Default priority
    /// - Short timeout (1 second)
    /// - Few retries (3)
    public static let interactive = TransactionConfiguration(
        timeout: 1_000,
        retryLimit: 3,
        maxRetryDelay: 50
    )

    // MARK: - Initialization

    public init(
        priority: FDB.TransactionOption? = nil,
        readPriority: FDB.TransactionOption? = nil,
        timeout: Int? = nil,
        retryLimit: Int? = nil,
        maxRetryDelay: Int? = nil,
        useGrvCache: Bool = false,
        snapshotRywDisable: Bool = false,
        debugTransactionIdentifier: String? = nil,
        logTransaction: Bool = false,
        tags: [String] = []
    ) {
        self.priority = priority
        self.readPriority = readPriority
        self.timeout = timeout
        self.retryLimit = retryLimit
        self.maxRetryDelay = maxRetryDelay
        self.useGrvCache = useGrvCache
        self.snapshotRywDisable = snapshotRywDisable
        self.debugTransactionIdentifier = debugTransactionIdentifier
        self.logTransaction = logTransaction
        self.tags = tags
    }

    // MARK: - Builder Pattern

    /// Create a copy with modified timeout
    public func withTimeout(_ ms: Int) -> TransactionConfiguration {
        TransactionConfiguration(
            priority: priority,
            readPriority: readPriority,
            timeout: ms,
            retryLimit: retryLimit,
            maxRetryDelay: maxRetryDelay,
            useGrvCache: useGrvCache,
            snapshotRywDisable: snapshotRywDisable,
            debugTransactionIdentifier: debugTransactionIdentifier,
            logTransaction: logTransaction,
            tags: tags
        )
    }

    /// Create a copy with debug identifier
    public func withDebugIdentifier(_ id: String) -> TransactionConfiguration {
        TransactionConfiguration(
            priority: priority,
            readPriority: readPriority,
            timeout: timeout,
            retryLimit: retryLimit,
            maxRetryDelay: maxRetryDelay,
            useGrvCache: useGrvCache,
            snapshotRywDisable: snapshotRywDisable,
            debugTransactionIdentifier: id,
            logTransaction: true,
            tags: tags
        )
    }

    /// Create a copy with additional tags
    public func withTags(_ additionalTags: [String]) -> TransactionConfiguration {
        TransactionConfiguration(
            priority: priority,
            readPriority: readPriority,
            timeout: timeout,
            retryLimit: retryLimit,
            maxRetryDelay: maxRetryDelay,
            useGrvCache: useGrvCache,
            snapshotRywDisable: snapshotRywDisable,
            debugTransactionIdentifier: debugTransactionIdentifier,
            logTransaction: logTransaction,
            tags: tags + additionalTags
        )
    }

    /// Create a copy with retry limit
    public func withRetryLimit(_ limit: Int) -> TransactionConfiguration {
        TransactionConfiguration(
            priority: priority,
            readPriority: readPriority,
            timeout: timeout,
            retryLimit: limit,
            maxRetryDelay: maxRetryDelay,
            useGrvCache: useGrvCache,
            snapshotRywDisable: snapshotRywDisable,
            debugTransactionIdentifier: debugTransactionIdentifier,
            logTransaction: logTransaction,
            tags: tags
        )
    }
}

// MARK: - TransactionProtocol Extension

extension TransactionProtocol {
    /// Apply configuration options to this transaction
    ///
    /// This method sets FDB transaction options based on the configuration.
    /// Call this immediately after creating the transaction.
    ///
    /// - Parameter config: The configuration to apply
    /// - Throws: `FDBError` if an option cannot be set
    public func apply(_ config: TransactionConfiguration) throws {
        // Priority
        if let priority = config.priority {
            try setOption(forOption: priority)
        }

        // Read priority
        if let readPriority = config.readPriority {
            try setOption(forOption: readPriority)
        }

        // Timeout
        if let timeout = config.timeout {
            try setOption(to: timeout, forOption: .timeout)
        }

        // Retry limit
        if let retryLimit = config.retryLimit {
            try setOption(to: retryLimit, forOption: .retryLimit)
        }

        // Max retry delay
        if let maxRetryDelay = config.maxRetryDelay {
            try setOption(to: maxRetryDelay, forOption: .maxRetryDelay)
        }

        // GRV cache
        if config.useGrvCache {
            try setOption(forOption: .useGrvCache)
        }

        // Snapshot RYW disable
        if config.snapshotRywDisable {
            try setOption(forOption: .snapshotRywDisable)
        }

        // Debug identifier and logging
        if let debugId = config.debugTransactionIdentifier {
            try setOption(to: debugId, forOption: .debugTransactionIdentifier)
            if config.logTransaction {
                try setOption(forOption: .logTransaction)
            }
        }

        // Tags (max 5)
        for tag in config.tags.prefix(5) {
            try setOption(to: tag, forOption: .tag)
        }
    }
}

// MARK: - Transaction Timing

/// Timing information for a transaction
public struct TransactionTiming: Sendable {
    /// When the transaction started
    public let startTime: Date

    /// When the transaction completed
    public let endTime: Date

    /// Time spent getting read version
    public let getReadVersionDuration: TimeInterval?

    /// Time spent in user code
    public let userCodeDuration: TimeInterval?

    /// Time spent committing
    public let commitDuration: TimeInterval?

    /// Total transaction duration
    public var totalDuration: TimeInterval {
        endTime.timeIntervalSince(startTime)
    }

    /// Number of retries performed
    public let retryCount: Int

    /// Whether the transaction succeeded
    public let succeeded: Bool

    /// Error if transaction failed
    public let error: Error?

    public init(
        startTime: Date,
        endTime: Date,
        getReadVersionDuration: TimeInterval? = nil,
        userCodeDuration: TimeInterval? = nil,
        commitDuration: TimeInterval? = nil,
        retryCount: Int = 0,
        succeeded: Bool,
        error: Error? = nil
    ) {
        self.startTime = startTime
        self.endTime = endTime
        self.getReadVersionDuration = getReadVersionDuration
        self.userCodeDuration = userCodeDuration
        self.commitDuration = commitDuration
        self.retryCount = retryCount
        self.succeeded = succeeded
        self.error = error
    }
}

// MARK: - Transaction Statistics

/// Statistics about transaction execution
public struct TransactionStatistics: Sendable {
    /// Total transactions executed
    public var totalTransactions: Int = 0

    /// Successful transactions
    public var successfulTransactions: Int = 0

    /// Failed transactions
    public var failedTransactions: Int = 0

    /// Total retries across all transactions
    public var totalRetries: Int = 0

    /// Transactions by priority option
    public var transactionsByPriority: [FDB.TransactionOption: Int] = [:]

    /// Average transaction duration in seconds
    public var averageDurationSeconds: Double = 0

    /// Maximum transaction duration in seconds
    public var maxDurationSeconds: Double = 0

    /// Record a completed transaction
    public mutating func record(_ timing: TransactionTiming, priority: FDB.TransactionOption?) {
        totalTransactions += 1
        if timing.succeeded {
            successfulTransactions += 1
        } else {
            failedTransactions += 1
        }
        totalRetries += timing.retryCount

        if let priority = priority {
            transactionsByPriority[priority, default: 0] += 1
        }

        let duration = timing.totalDuration
        let n = Double(totalTransactions)
        averageDurationSeconds = averageDurationSeconds * ((n - 1) / n) + duration / n
        maxDurationSeconds = max(maxDurationSeconds, duration)
    }

    /// Success rate (0.0 - 1.0)
    public var successRate: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(successfulTransactions) / Double(totalTransactions)
    }

    /// Average retries per transaction
    public var averageRetries: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(totalRetries) / Double(totalTransactions)
    }
}

// MARK: - Priority-Based Rate Limiting

/// Rate limiter that respects transaction priority
///
/// Higher priority transactions are allowed through more readily
/// than lower priority transactions when the system is under load.
public final class PriorityRateLimiter: Sendable {
    /// Tokens available for each priority level
    private struct State: Sendable {
        var batchTokens: Int
        var defaultTokens: Int
        var systemTokens: Int
        var lastRefill: Date
    }

    private let state: Mutex<State>

    /// Maximum tokens for each priority level
    public let maxBatchTokens: Int
    public let maxDefaultTokens: Int
    public let maxSystemTokens: Int

    /// Token refill rate per second
    public let refillRate: Int

    public init(
        maxBatchTokens: Int = 10,
        maxDefaultTokens: Int = 100,
        maxSystemTokens: Int = 1000,
        refillRate: Int = 50
    ) {
        self.maxBatchTokens = maxBatchTokens
        self.maxDefaultTokens = maxDefaultTokens
        self.maxSystemTokens = maxSystemTokens
        self.refillRate = refillRate
        self.state = Mutex(State(
            batchTokens: maxBatchTokens,
            defaultTokens: maxDefaultTokens,
            systemTokens: maxSystemTokens,
            lastRefill: Date()
        ))
    }

    /// Try to acquire a token for the given priority
    ///
    /// - Parameter priority: The transaction priority option
    /// - Returns: Whether a token was acquired
    public func tryAcquire(priority: FDB.TransactionOption?) -> Bool {
        state.withLock { state in
            // Refill tokens based on elapsed time
            let now = Date()
            let elapsed = now.timeIntervalSince(state.lastRefill)
            let tokensToAdd = Int(elapsed * Double(refillRate))

            if tokensToAdd > 0 {
                state.batchTokens = min(maxBatchTokens, state.batchTokens + tokensToAdd)
                state.defaultTokens = min(maxDefaultTokens, state.defaultTokens + tokensToAdd)
                state.systemTokens = min(maxSystemTokens, state.systemTokens + tokensToAdd)
                state.lastRefill = now
            }

            // Try to acquire based on priority
            switch priority {
            case .priorityBatch:
                if state.batchTokens > 0 {
                    state.batchTokens -= 1
                    return true
                }
                return false

            case .prioritySystemImmediate:
                if state.systemTokens > 0 {
                    state.systemTokens -= 1
                    return true
                }
                return false

            default:
                // Default priority
                if state.defaultTokens > 0 {
                    state.defaultTokens -= 1
                    return true
                }
                return false
            }
        }
    }

    /// Current token counts
    public var tokenCounts: (batch: Int, default: Int, system: Int) {
        state.withLock { ($0.batchTokens, $0.defaultTokens, $0.systemTokens) }
    }
}

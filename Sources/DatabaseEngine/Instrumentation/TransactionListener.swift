// TransactionListener.swift
// DatabaseEngine - Transaction event listener protocol
//
// Reference: FDB Record Layer TransactionListener interface
// Allows observing transaction lifecycle events for logging, metrics, and debugging.

import Foundation
import FoundationDB
import Metrics
import Synchronization

// MARK: - TransactionListener

/// Protocol for observing transaction lifecycle events
///
/// Similar to FDB Record Layer's TransactionListener interface.
/// Implementations can use this for:
/// - Logging and debugging
/// - Metrics collection
/// - Performance analysis
/// - Audit trails
///
/// **Thread Safety**: Implementations must be thread-safe.
///
/// **Usage**:
/// ```swift
/// struct LoggingListener: TransactionListener {
///     func transactionStarted(context: TransactionContext) {
///         print("Transaction started: \(context.id)")
///     }
/// }
///
/// let container = FDBContainer(...)
/// container.addTransactionListener(LoggingListener())
/// ```
public protocol TransactionListener: Sendable {

    /// Called when a transaction is started
    ///
    /// - Parameter context: Context describing the transaction
    func transactionStarted(context: TransactionContext)

    /// Called when a transaction successfully commits
    ///
    /// - Parameters:
    ///   - context: Context describing the transaction
    ///   - timing: Timing information about the transaction
    func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo)

    /// Called when a transaction fails
    ///
    /// - Parameters:
    ///   - context: Context describing the transaction
    ///   - error: The error that caused the failure
    ///   - timing: Timing information about the transaction
    func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo)

    /// Called when a transaction is retried
    ///
    /// - Parameters:
    ///   - context: Context describing the transaction
    ///   - error: The error that caused the retry
    ///   - attempt: The retry attempt number (1-indexed)
    func transactionRetried(context: TransactionContext, error: Error, attempt: Int)

    /// Called when read version is obtained
    ///
    /// - Parameters:
    ///   - context: Context describing the transaction
    ///   - readVersion: The read version obtained
    ///   - cached: Whether the version came from cache
    ///   - duration: Time taken to get the version (nanoseconds)
    func readVersionObtained(context: TransactionContext, readVersion: Int64, cached: Bool, duration: UInt64)

    /// Called when a range scan completes
    ///
    /// - Parameters:
    ///   - context: Context describing the transaction
    ///   - keyCount: Number of keys returned
    ///   - byteCount: Total bytes read
    ///   - duration: Time taken (nanoseconds)
    func rangeScanCompleted(context: TransactionContext, keyCount: Int, byteCount: Int, duration: UInt64)
}

// MARK: - Default Implementations

extension TransactionListener {
    public func transactionStarted(context: TransactionContext) {}
    public func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo) {}
    public func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo) {}
    public func transactionRetried(context: TransactionContext, error: Error, attempt: Int) {}
    public func readVersionObtained(context: TransactionContext, readVersion: Int64, cached: Bool, duration: UInt64) {}
    public func rangeScanCompleted(context: TransactionContext, keyCount: Int, byteCount: Int, duration: UInt64) {}
}

// MARK: - TransactionContext

/// Context information about a transaction
public struct TransactionContext: Sendable {
    /// Unique identifier for this transaction
    public let id: UUID

    /// Optional user-provided identifier for debugging
    public let debugIdentifier: String?

    /// The operation being performed
    public let operation: TransactionOperation

    /// Tags associated with this transaction
    public let tags: [String]

    /// Timestamp when the transaction started
    public let startTime: Date

    /// Custom metadata
    public let metadata: [String: String]

    public init(
        id: UUID = UUID(),
        debugIdentifier: String? = nil,
        operation: TransactionOperation = .generic,
        tags: [String] = [],
        startTime: Date = Date(),
        metadata: [String: String] = [:]
    ) {
        self.id = id
        self.debugIdentifier = debugIdentifier
        self.operation = operation
        self.tags = tags
        self.startTime = startTime
        self.metadata = metadata
    }
}

// MARK: - TransactionOperation

/// Categories of transaction operations
public enum TransactionOperation: String, Sendable, CaseIterable {
    case generic = "generic"
    case save = "save"
    case fetch = "fetch"
    case delete = "delete"
    case query = "query"
    case indexScan = "index_scan"
    case indexUpdate = "index_update"
    case onlineIndex = "online_index"
    case migration = "migration"
    case maintenance = "maintenance"
}

// MARK: - TransactionTimingInfo

/// Timing information about a transaction
public struct TransactionTimingInfo: Sendable {
    /// Total duration in nanoseconds
    public let totalDurationNanos: UInt64

    /// Time to get read version (nanoseconds)
    public let getReadVersionNanos: UInt64?

    /// Time executing user code (nanoseconds)
    public let userCodeNanos: UInt64?

    /// Time to commit (nanoseconds)
    public let commitNanos: UInt64?

    /// Number of retries
    public let retryCount: Int

    /// Read version used
    public let readVersion: Int64?

    /// Commit version (if committed)
    public let commitVersion: Int64?

    /// Whether read version came from cache
    public let readVersionCached: Bool

    public init(
        totalDurationNanos: UInt64,
        getReadVersionNanos: UInt64? = nil,
        userCodeNanos: UInt64? = nil,
        commitNanos: UInt64? = nil,
        retryCount: Int = 0,
        readVersion: Int64? = nil,
        commitVersion: Int64? = nil,
        readVersionCached: Bool = false
    ) {
        self.totalDurationNanos = totalDurationNanos
        self.getReadVersionNanos = getReadVersionNanos
        self.userCodeNanos = userCodeNanos
        self.commitNanos = commitNanos
        self.retryCount = retryCount
        self.readVersion = readVersion
        self.commitVersion = commitVersion
        self.readVersionCached = readVersionCached
    }

    /// Total duration in milliseconds
    public var totalDurationMs: Double {
        Double(totalDurationNanos) / 1_000_000
    }
}

// MARK: - CompositeTransactionListener

/// Combines multiple listeners into one
public final class CompositeTransactionListener: TransactionListener, Sendable {
    private struct State: Sendable {
        var listeners: [any TransactionListener] = []
    }

    private let state: Mutex<State>

    public init() {
        self.state = Mutex(State())
    }

    public init(listeners: [any TransactionListener]) {
        self.state = Mutex(State(listeners: listeners))
    }

    /// Add a listener
    public func add(_ listener: any TransactionListener) {
        state.withLock { $0.listeners.append(listener) }
    }

    /// Remove all listeners
    public func removeAll() {
        state.withLock { $0.listeners.removeAll() }
    }

    /// Get current listener count
    public var listenerCount: Int {
        state.withLock { $0.listeners.count }
    }

    // MARK: - TransactionListener

    public func transactionStarted(context: TransactionContext) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.transactionStarted(context: context)
        }
    }

    public func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.transactionCommitted(context: context, timing: timing)
        }
    }

    public func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.transactionFailed(context: context, error: error, timing: timing)
        }
    }

    public func transactionRetried(context: TransactionContext, error: Error, attempt: Int) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.transactionRetried(context: context, error: error, attempt: attempt)
        }
    }

    public func readVersionObtained(context: TransactionContext, readVersion: Int64, cached: Bool, duration: UInt64) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.readVersionObtained(context: context, readVersion: readVersion, cached: cached, duration: duration)
        }
    }

    public func rangeScanCompleted(context: TransactionContext, keyCount: Int, byteCount: Int, duration: UInt64) {
        let current = state.withLock { $0.listeners }
        for listener in current {
            listener.rangeScanCompleted(context: context, keyCount: keyCount, byteCount: byteCount, duration: duration)
        }
    }
}

// MARK: - MetricsTransactionListener

/// Transaction listener that emits metrics using swift-metrics
public struct MetricsTransactionListener: TransactionListener {
    private let prefix: String

    // Pre-created metrics for efficiency
    private let transactionDurationTimer: Metrics.Timer
    private let transactionSuccessCounter: Counter
    private let transactionFailureCounter: Counter
    private let transactionRetryCounter: Counter
    private let readVersionDurationTimer: Metrics.Timer
    private let readVersionCacheHitCounter: Counter
    private let readVersionCacheMissCounter: Counter
    private let rangeScanDurationTimer: Metrics.Timer
    private let rangeScanKeysCounter: Counter
    private let rangeScanBytesCounter: Counter

    public init(prefix: String = "fdb") {
        self.prefix = prefix

        // Initialize metrics
        self.transactionDurationTimer = Metrics.Timer(label: "\(prefix)_transaction_duration_nanoseconds")
        self.transactionSuccessCounter = Counter(label: "\(prefix)_transactions_total", dimensions: [("status", "success")])
        self.transactionFailureCounter = Counter(label: "\(prefix)_transactions_total", dimensions: [("status", "failure")])
        self.transactionRetryCounter = Counter(label: "\(prefix)_transaction_retries_total")
        self.readVersionDurationTimer = Metrics.Timer(label: "\(prefix)_get_read_version_nanoseconds")
        self.readVersionCacheHitCounter = Counter(label: "\(prefix)_read_version_cache_total", dimensions: [("result", "hit")])
        self.readVersionCacheMissCounter = Counter(label: "\(prefix)_read_version_cache_total", dimensions: [("result", "miss")])
        self.rangeScanDurationTimer = Metrics.Timer(label: "\(prefix)_range_scan_nanoseconds")
        self.rangeScanKeysCounter = Counter(label: "\(prefix)_range_scan_keys_total")
        self.rangeScanBytesCounter = Counter(label: "\(prefix)_range_scan_bytes_total")
    }

    public func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo) {
        transactionDurationTimer.recordNanoseconds(Int64(timing.totalDurationNanos))
        transactionSuccessCounter.increment()

        // Record per-operation metrics
        Counter(
            label: "\(prefix)_transactions_by_operation_total",
            dimensions: [("operation", context.operation.rawValue), ("status", "success")]
        ).increment()

        if timing.retryCount > 0 {
            transactionRetryCounter.increment(by: timing.retryCount)
        }
    }

    public func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo) {
        transactionDurationTimer.recordNanoseconds(Int64(timing.totalDurationNanos))
        transactionFailureCounter.increment()

        // Record per-operation metrics
        Counter(
            label: "\(prefix)_transactions_by_operation_total",
            dimensions: [("operation", context.operation.rawValue), ("status", "failure")]
        ).increment()

        if timing.retryCount > 0 {
            transactionRetryCounter.increment(by: timing.retryCount)
        }
    }

    public func transactionRetried(context: TransactionContext, error: Error, attempt: Int) {
        transactionRetryCounter.increment()
    }

    public func readVersionObtained(context: TransactionContext, readVersion: Int64, cached: Bool, duration: UInt64) {
        readVersionDurationTimer.recordNanoseconds(Int64(duration))
        if cached {
            readVersionCacheHitCounter.increment()
        } else {
            readVersionCacheMissCounter.increment()
        }
    }

    public func rangeScanCompleted(context: TransactionContext, keyCount: Int, byteCount: Int, duration: UInt64) {
        rangeScanDurationTimer.recordNanoseconds(Int64(duration))
        rangeScanKeysCounter.increment(by: keyCount)
        rangeScanBytesCounter.increment(by: byteCount)
    }
}

// MARK: - LoggingTransactionListener

/// Transaction listener that logs events
public struct LoggingTransactionListener: TransactionListener {
    public enum LogLevel: Sendable {
        case debug
        case info
        case warning
        case error
    }

    private let minLevel: LogLevel
    private let logger: @Sendable (LogLevel, String) -> Void

    public init(
        minLevel: LogLevel = .info,
        logger: @escaping @Sendable (LogLevel, String) -> Void = { level, message in
            print("[\(level)] \(message)")
        }
    ) {
        self.minLevel = minLevel
        self.logger = logger
    }

    public func transactionStarted(context: TransactionContext) {
        log(.debug, "Transaction started: \(context.id) operation=\(context.operation)")
    }

    public func transactionCommitted(context: TransactionContext, timing: TransactionTimingInfo) {
        log(.debug, "Transaction committed: \(context.id) duration=\(timing.totalDurationMs)ms retries=\(timing.retryCount)")
    }

    public func transactionFailed(context: TransactionContext, error: Error, timing: TransactionTimingInfo) {
        log(.warning, "Transaction failed: \(context.id) error=\(error) duration=\(timing.totalDurationMs)ms")
    }

    public func transactionRetried(context: TransactionContext, error: Error, attempt: Int) {
        log(.info, "Transaction retry: \(context.id) attempt=\(attempt) error=\(error)")
    }

    private func log(_ level: LogLevel, _ message: String) {
        guard shouldLog(level) else { return }
        logger(level, message)
    }

    private func shouldLog(_ level: LogLevel) -> Bool {
        switch (minLevel, level) {
        case (.debug, _): return true
        case (.info, .debug): return false
        case (.info, _): return true
        case (.warning, .debug), (.warning, .info): return false
        case (.warning, _): return true
        case (.error, .error): return true
        case (.error, _): return false
        }
    }
}

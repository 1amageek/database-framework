// TransactionListener.swift
// DatabaseEngine - Transaction lifecycle event listener
//
// Reference: FDB Record Layer TransactionListener
// Provides hooks for transaction lifecycle events at the database level.

import StorageKit
import DatabaseTypes
import Synchronization

// MARK: - TransactionEvent

/// Events in a transaction's lifecycle
public enum TransactionEvent: Sendable, CustomStringConvertible {
    /// Transaction was created
    case created(id: String?, timestamp: Timestamp)

    /// Transaction is about to commit
    case committing(id: String?, timestamp: Timestamp)

    /// Transaction committed successfully
    case committed(id: String?, timestamp: Timestamp, duration: Duration, version: Int64?)

    /// Transaction failed
    case failed(id: String?, timestamp: Timestamp, duration: Duration, error: Error)

    /// Transaction was cancelled
    case cancelled(id: String?, timestamp: Timestamp, duration: Duration)

    /// Transaction was closed (cleanup)
    case closed(id: String?, timestamp: Timestamp, totalDuration: Duration)

    public var description: String {
        switch self {
        case .created(let id, let ts):
            return "Transaction[\(id ?? "unnamed")] created at \(ts.secondsSinceUnixEpoch)"
        case .committing(let id, let ts):
            return "Transaction[\(id ?? "unnamed")] committing at \(ts.secondsSinceUnixEpoch)"
        case .committed(let id, let ts, let duration, let version):
            let versionStr = version.map { ", version=\($0)" } ?? ""
            return "Transaction[\(id ?? "unnamed")] committed at \(ts.secondsSinceUnixEpoch) (duration=\(DatabaseMonotonicMeasurement.nanoseconds(duration))ns\(versionStr))"
        case .failed(let id, let ts, let duration, _):
            return "Transaction[\(id ?? "unnamed")] failed at \(ts.secondsSinceUnixEpoch) (duration=\(DatabaseMonotonicMeasurement.nanoseconds(duration))ns)"
        case .cancelled(let id, let ts, let duration):
            return "Transaction[\(id ?? "unnamed")] cancelled at \(ts.secondsSinceUnixEpoch) (duration=\(DatabaseMonotonicMeasurement.nanoseconds(duration))ns)"
        case .closed(let id, let ts, let totalDuration):
            return "Transaction[\(id ?? "unnamed")] closed at \(ts.secondsSinceUnixEpoch) (total=\(DatabaseMonotonicMeasurement.nanoseconds(totalDuration))ns)"
        }
    }

    /// Extract transaction ID from event
    public var transactionID: String? {
        switch self {
        case .created(let id, _),
             .committing(let id, _),
             .committed(let id, _, _, _),
             .failed(let id, _, _, _),
             .cancelled(let id, _, _),
             .closed(let id, _, _):
            return id
        }
    }

    /// Extract timestamp from event
    public var timestamp: Timestamp {
        switch self {
        case .created(_, let ts),
             .committing(_, let ts),
             .committed(_, let ts, _, _),
             .failed(_, let ts, _, _),
             .cancelled(_, let ts, _),
             .closed(_, let ts, _):
            return ts
        }
    }
}

// MARK: - TransactionListener Protocol

/// Protocol for receiving transaction lifecycle events
///
/// TransactionListeners are registered at the database level and receive
/// notifications about all transactions.
///
/// **Use Cases**:
/// - Centralized logging
/// - Metrics collection
/// - Debugging/tracing
/// - Audit trails
///
/// **Important**:
/// - Listeners should be lightweight and fast
/// - Avoid blocking operations in listener methods
/// - Listeners are called synchronously; slow listeners affect performance
///
/// **Usage**:
/// ```swift
/// class MetricsListener: TransactionListener {
///     func onEvent(_ event: TransactionEvent) {
///         switch event {
///         case .committed(_, _, let duration, _):
///             metrics.recordDuration(duration)
///         case .failed(_, _, _, let error):
///             metrics.recordError(error)
///         default:
///             break
///         }
///     }
/// }
///
/// container.addTransactionListener(MetricsListener())
/// ```
///
/// **Reference**: FDB Record Layer `TransactionListener`
public protocol TransactionListener: Sendable {
    /// Called when a transaction event occurs
    ///
    /// - Parameter event: The transaction event
    func onEvent(_ event: TransactionEvent)
}

// MARK: - Transaction Event Handlers

/// Delivers each transaction lifecycle event to an application handler.
public struct TransactionEventHandler: TransactionListener {
    private let handler: @Sendable (TransactionEvent) -> Void

    public init(_ handler: @escaping @Sendable (TransactionEvent) -> Void) {
        self.handler = handler
    }

    public func onEvent(_ event: TransactionEvent) {
        handler(event)
    }
}

/// Logging transaction listener
///
/// Logs transaction events using Swift's Logger.
public final class LoggingTransactionListener: TransactionListener {
    private let logger: any LoggerProtocol
    private let logLevel: LogLevel

    public enum LogLevel: Sendable {
        case debug
        case info
        case warning
        case error
    }

    public init(logger: any LoggerProtocol, level: LogLevel = .info) {
        self.logger = logger
        self.logLevel = level
    }

    public func onEvent(_ event: TransactionEvent) {
        let message = event.description

        switch logLevel {
        case .debug:
            logger.debug(message)
        case .info:
            logger.info(message)
        case .warning:
            logger.warning(message)
        case .error:
            logger.error(message)
        }
    }
}

/// Protocol for logger abstraction
public protocol LoggerProtocol: Sendable {
    func debug(_ message: String)
    func info(_ message: String)
    func warning(_ message: String)
    func error(_ message: String)
}

/// Simple print-based logger for testing
public struct PrintLogger: LoggerProtocol {
    public init() {}

    public func debug(_ message: String) {
        print("[DEBUG] \(message)")
    }

    public func info(_ message: String) {
        print("[INFO] \(message)")
    }

    public func warning(_ message: String) {
        print("[WARNING] \(message)")
    }

    public func error(_ message: String) {
        print("[ERROR] \(message)")
    }
}

/// Metrics-collecting transaction listener
///
/// Collects transaction metrics for monitoring.
public final class MetricsTransactionListener: TransactionListener {
    private let state: Mutex<State>

    private struct State: Sendable {
        var totalTransactions: Int = 0
        var committedTransactions: Int = 0
        var failedTransactions: Int = 0
        var cancelledTransactions: Int = 0
        var totalDurationNanos: UInt64 = 0
        var maxDurationNanos: UInt64 = 0
        var minDurationNanos: UInt64 = .max
    }

    public init() {
        self.state = Mutex(State())
    }

    public func onEvent(_ event: TransactionEvent) {
        state.withLock { state in
            switch event {
            case .created:
                state.totalTransactions += 1

            case .committed(_, _, let duration, _):
                state.committedTransactions += 1
                let nanos = DatabaseMonotonicMeasurement.nanoseconds(duration)
                state.totalDurationNanos += nanos
                state.maxDurationNanos = max(state.maxDurationNanos, nanos)
                state.minDurationNanos = min(state.minDurationNanos, nanos)

            case .failed:
                state.failedTransactions += 1

            case .cancelled:
                state.cancelledTransactions += 1

            default:
                break
            }
        }
    }

    /// Get current metrics snapshot
    public var metrics: TransactionListenerMetrics {
        state.withLock { state in
            TransactionListenerMetrics(
                totalTransactions: state.totalTransactions,
                committedTransactions: state.committedTransactions,
                failedTransactions: state.failedTransactions,
                cancelledTransactions: state.cancelledTransactions,
                avgDurationMs: state.committedTransactions > 0
                    ? Double(state.totalDurationNanos) / Double(state.committedTransactions) / 1_000_000
                    : 0,
                maxDurationMs: Double(state.maxDurationNanos) / 1_000_000,
                minDurationMs: state.minDurationNanos == .max ? 0 : Double(state.minDurationNanos) / 1_000_000
            )
        }
    }

    /// Reset metrics
    public func reset() {
        state.withLock { $0 = State() }
    }
}

/// Metrics from transaction listener
public struct TransactionListenerMetrics: Sendable, CustomStringConvertible {
    public let totalTransactions: Int
    public let committedTransactions: Int
    public let failedTransactions: Int
    public let cancelledTransactions: Int
    public let avgDurationMs: Double
    public let maxDurationMs: Double
    public let minDurationMs: Double

    public var successRate: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(committedTransactions) / Double(totalTransactions)
    }

    public var description: String {
        """
        TransactionMetrics:
          Total: \(totalTransactions) (committed: \(committedTransactions), failed: \(failedTransactions), cancelled: \(cancelledTransactions))
          Success rate: \(DatabaseTextFormatting.fixedDecimal(successRate * 100, fractionDigits: 1))%
          Duration: avg=\(DatabaseTextFormatting.fixedDecimal(avgDurationMs, fractionDigits: 2))ms, min=\(DatabaseTextFormatting.fixedDecimal(minDurationMs, fractionDigits: 2))ms, max=\(DatabaseTextFormatting.fixedDecimal(maxDurationMs, fractionDigits: 2))ms
        """
    }
}

/// Filtering transaction listener
///
/// Only forwards events that match a filter.
public struct FilteringTransactionListener: TransactionListener {
    private let inner: any TransactionListener
    private let filter: @Sendable (TransactionEvent) -> Bool

    public init(_ inner: any TransactionListener, filter: @escaping @Sendable (TransactionEvent) -> Bool) {
        self.inner = inner
        self.filter = filter
    }

    public func onEvent(_ event: TransactionEvent) {
        if filter(event) {
            inner.onEvent(event)
        }
    }
}

// MARK: - TransactionListenerRegistry

/// Registry for managing transaction listeners
///
/// Maintains a collection of listeners that receive transaction events.
public final class TransactionListenerRegistry: Sendable {
    private let listeners: Mutex<[any TransactionListener]>

    public init() {
        self.listeners = Mutex([])
    }

    /// Add a listener
    public func add(_ listener: any TransactionListener) {
        listeners.withLock { $0.append(listener) }
    }

    /// Add an application-defined transaction event handler.
    public func add(_ handler: @escaping @Sendable (TransactionEvent) -> Void) {
        add(TransactionEventHandler(handler))
    }

    /// Remove all listeners
    public func clear() {
        listeners.withLock { $0.removeAll() }
    }

    /// Notify all listeners of an event
    public func notify(_ event: TransactionEvent) {
        let currentListeners = listeners.withLock { $0 }
        for listener in currentListeners {
            listener.onEvent(event)
        }
    }

    /// Number of registered listeners
    public var count: Int {
        listeners.withLock { $0.count }
    }
}

// MARK: - Transaction Lifecycle Tracker

/// Tracks the lifecycle of a single transaction
///
/// Used internally to manage transaction state and emit events.
public final class TransactionLifecycleTracker: Sendable {
    private let id: String?
    private let startTime: StorageInstant
    private let monotonicClock: any StorageMonotonicClock
    private let wallClock: any WallClock
    private let registry: TransactionListenerRegistry?

    private struct State: Sendable {
        var isCommitted: Bool = false
        var isFailed: Bool = false
        var isCancelled: Bool = false
        var isClosed: Bool = false
    }
    private let state: Mutex<State>

    public init(
        id: String?,
        registry: TransactionListenerRegistry?,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock
    ) {
        self.id = id
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.startTime = monotonicClock.now
        self.registry = registry
        self.state = Mutex(State())

        // Emit created event
        registry?.notify(.created(id: id, timestamp: wallClock.now))
    }

    /// Mark transaction as committing
    public func markCommitting() {
        registry?.notify(.committing(id: id, timestamp: wallClock.now))
    }

    /// Mark transaction as committed
    public func markCommitted(version: Int64?) {
        let wasAlreadyDone = state.withLock { state in
            if state.isCommitted || state.isFailed || state.isCancelled {
                return true
            }
            state.isCommitted = true
            return false
        }

        if !wasAlreadyDone {
            let duration = startTime.duration(to: monotonicClock.now)
            registry?.notify(.committed(id: id, timestamp: wallClock.now, duration: duration, version: version))
        }
    }

    /// Mark transaction as failed
    public func markFailed(error: Error) {
        let wasAlreadyDone = state.withLock { state in
            if state.isCommitted || state.isFailed || state.isCancelled {
                return true
            }
            state.isFailed = true
            return false
        }

        if !wasAlreadyDone {
            let duration = startTime.duration(to: monotonicClock.now)
            registry?.notify(.failed(id: id, timestamp: wallClock.now, duration: duration, error: error))
        }
    }

    /// Mark transaction as cancelled
    public func markCancelled() {
        let wasAlreadyDone = state.withLock { state in
            if state.isCommitted || state.isFailed || state.isCancelled {
                return true
            }
            state.isCancelled = true
            return false
        }

        if !wasAlreadyDone {
            let duration = startTime.duration(to: monotonicClock.now)
            registry?.notify(.cancelled(id: id, timestamp: wallClock.now, duration: duration))
        }
    }

    /// Mark transaction as closed
    public func markClosed() {
        let wasAlreadyClosed = state.withLock { state in
            if state.isClosed {
                return true
            }
            state.isClosed = true
            return false
        }

        if !wasAlreadyClosed {
            let totalDuration = startTime.duration(to: monotonicClock.now)
            registry?.notify(.closed(id: id, timestamp: wallClock.now, totalDuration: totalDuration))
        }
    }

    /// Transaction ID
    public var transactionID: String? { id }

    /// Duration since transaction started
    public var elapsed: Duration {
        startTime.duration(to: monotonicClock.now)
    }
}

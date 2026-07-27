// PerformanceMonitor.swift
// DatabaseEngine - Performance monitoring and slow query logging
//
// Provides comprehensive performance monitoring including:
// - Slow query logging with configurable threshold
// - Latency percentile tracking (P50, P99)
// - QPS (queries per second) calculation
// - Transaction statistics

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import Synchronization

// MARK: - SlowQueryEntry

/// A recorded slow database operation.
///
/// Captures a transaction whose execution exceeded the configured threshold.
///
/// **Usage**:
/// ```swift
/// let slowQueries = await monitor.getSlowQueries(limit: 10)
/// for query in slowQueries {
///     print("Slow: \(query.queryDescription) - \(query.executionTime)s")
/// }
/// ```
public struct SlowQueryEntry: Sendable {
    /// Time at which the operation was recorded.
    public let timestamp: Date

    /// Query or transaction description.
    public let queryDescription: String

    /// Persistable type name, when known.
    public let typeName: String?

    /// Execution time in seconds.
    public let executionTime: TimeInterval

    /// Transaction identifier, when available.
    public let transactionID: String?

    public init(
        timestamp: Date = Date(),
        queryDescription: String,
        typeName: String? = nil,
        executionTime: TimeInterval,
        transactionID: String? = nil
    ) {
        self.timestamp = timestamp
        self.queryDescription = queryDescription
        self.typeName = typeName
        self.executionTime = executionTime
        self.transactionID = transactionID
    }
}

extension SlowQueryEntry: CustomStringConvertible {
    public var description: String {
        let typeStr = typeName.map { " [\($0)]" } ?? ""
        let idStr = transactionID.map { " (tx: \($0))" } ?? ""
        return "[\(timestamp)]\(typeStr) \(queryDescription) - \(DatabaseTextFormatting.fixedDecimal(executionTime * 1000, fractionDigits: 3))ms\(idStr)"
    }
}

// MARK: - DatabaseMetrics

/// Current database performance metrics.
///
/// Represents a point-in-time performance snapshot.
///
/// **Usage**:
/// ```swift
/// let metrics = await monitor.currentMetrics()
/// print("P50 latency: \(metrics.latencyP50Ms)ms")
/// print("QPS: \(metrics.queriesPerSecond)")
/// ```
public struct DatabaseMetrics: Sendable {
    /// Snapshot timestamp.
    public let timestamp: Date

    /// Number of active transactions.
    public let activeTransactions: Int

    /// P50 latency in seconds.
    public let latencyP50: TimeInterval

    /// P99 latency in seconds.
    public let latencyP99: TimeInterval

    /// Queries per second.
    public let queriesPerSecond: Double

    /// Total transaction count.
    public let totalTransactions: Int64

    /// Successful transaction count.
    public let successfulTransactions: Int64

    /// Success ratio from 0.0 through 1.0.
    public var successRate: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(successfulTransactions) / Double(totalTransactions)
    }

    /// P50 latency in milliseconds.
    public var latencyP50Ms: Double {
        latencyP50 * 1000
    }

    /// P99 latency in milliseconds.
    public var latencyP99Ms: Double {
        latencyP99 * 1000
    }

    public init(
        timestamp: Date = Date(),
        activeTransactions: Int,
        latencyP50: TimeInterval,
        latencyP99: TimeInterval,
        queriesPerSecond: Double,
        totalTransactions: Int64,
        successfulTransactions: Int64
    ) {
        self.timestamp = timestamp
        self.activeTransactions = activeTransactions
        self.latencyP50 = latencyP50
        self.latencyP99 = latencyP99
        self.queriesPerSecond = queriesPerSecond
        self.totalTransactions = totalTransactions
        self.successfulTransactions = successfulTransactions
    }
}

extension DatabaseMetrics: CustomStringConvertible {
    public var description: String {
        """
        DatabaseMetrics (\(timestamp)):
          Active transactions: \(activeTransactions)
          Latency: P50=\(DatabaseTextFormatting.fixedDecimal(latencyP50Ms, fractionDigits: 2))ms, P99=\(DatabaseTextFormatting.fixedDecimal(latencyP99Ms, fractionDigits: 2))ms
          QPS: \(DatabaseTextFormatting.fixedDecimal(queriesPerSecond, fractionDigits: 2))
          Transactions: \(totalTransactions) (success rate: \(DatabaseTextFormatting.fixedDecimal(successRate * 100, fractionDigits: 1))%)
        """
    }
}

// MARK: - PerformanceMonitorProtocol

/// Database performance monitoring contract.
///
/// Provides slow-operation logging and current metrics.
public protocol PerformanceMonitorProtocol: Sendable {
    /// Enables slow-operation logging.
    ///
    /// - Parameter threshold: Slow-operation threshold in seconds.
    func enableSlowQueryLog(threshold: TimeInterval)

    /// Disables slow-operation logging.
    func disableSlowQueryLog()

    /// Returns recorded slow operations.
    ///
    /// - Parameter limit: Maximum number of entries.
    /// - Returns: Entries ordered from newest to oldest.
    func getSlowQueries(limit: Int) -> [SlowQueryEntry]

    /// Clears recorded slow operations.
    func clearSlowQueries()

    /// Returns current metrics.
    func currentMetrics() -> DatabaseMetrics

    /// Resets all metrics.
    func reset()
}

// MARK: - PerformanceMonitor

/// Default database performance monitor.
///
/// Collects metrics from TransactionListener events.
///
/// **Usage**:
/// ```swift
/// let monitor = PerformanceMonitor()
/// container.addTransactionListener(monitor)
///
/// // Record operations taking at least 100 milliseconds.
/// monitor.enableSlowQueryLog(threshold: 0.1)
///
/// // Execute transactions.
///
/// let metrics = monitor.currentMetrics()
/// print("P50 latency: \(metrics.latencyP50Ms)ms")
///
/// let slowQueries = monitor.getSlowQueries(limit: 10)
/// for query in slowQueries {
///     print(query)
/// }
/// ```
public final class PerformanceMonitor: PerformanceMonitorProtocol, TransactionListener {

    // MARK: - Configuration

    /// Slow-operation threshold; nil disables logging.
    private let slowQueryThreshold: Mutex<TimeInterval?>

    /// Maximum number of retained slow operations.
    private let maxSlowQueries: Int

    /// Maximum number of retained latency samples.
    private let maxLatencySamples: Int

    /// Query-rate measurement window in seconds.
    private let qpsWindow: TimeInterval

    // MARK: - State

    /// Bounded slow-operation log.
    private let slowQueries: Mutex<[SlowQueryEntry]>

    /// Latency samples maintained through reservoir sampling.
    private let latencySamples: Mutex<LatencySampleState>

    /// Transaction counters.
    private let counters: Mutex<TransactionCounters>

    /// Query timestamps used to calculate query rate.
    private let queryTimestamps: Mutex<[Date]>

    // MARK: - Internal Types

    private struct LatencySampleState: Sendable {
        var samples: [TimeInterval] = []
        var totalCount: Int = 0
    }

    private struct TransactionCounters: Sendable {
        var total: Int64 = 0
        var successful: Int64 = 0
        var active: Int = 0
    }

    // MARK: - Initialization

    /// Creates a performance monitor.
    ///
    /// - Parameters:
    ///   - maxSlowQueries: Maximum slow-operation entries. Defaults to 100.
    ///   - maxLatencySamples: Maximum latency samples. Defaults to 1,000.
    ///   - qpsWindow: Query-rate window in seconds. Defaults to 60.
    public init(
        maxSlowQueries: Int = 100,
        maxLatencySamples: Int = 1000,
        qpsWindow: TimeInterval = 60
    ) {
        self.maxSlowQueries = maxSlowQueries
        self.maxLatencySamples = maxLatencySamples
        self.qpsWindow = qpsWindow

        self.slowQueryThreshold = Mutex(nil)
        self.slowQueries = Mutex([])
        self.latencySamples = Mutex(LatencySampleState())
        self.counters = Mutex(TransactionCounters())
        self.queryTimestamps = Mutex([])
    }

    // MARK: - TransactionListener

    public func onEvent(_ event: TransactionEvent) {
        switch event {
        case .created:
            counters.withLock { $0.active += 1 }

        case .committed(let id, _, let duration, _):
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
                counters.successful += 1
            }
            recordLatency(duration)
            recordQueryTimestamp()
            checkSlowQuery(duration: duration, transactionID: id)

        case .failed(let id, _, let duration, _):
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
            }
            recordLatency(duration)
            recordQueryTimestamp()
            checkSlowQuery(duration: duration, transactionID: id, failed: true)

        case .cancelled:
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
            }

        case .committing, .closed:
            break
        }
    }

    // MARK: - PerformanceMonitorProtocol

    public func enableSlowQueryLog(threshold: TimeInterval) {
        slowQueryThreshold.withLock { $0 = threshold }
    }

    public func disableSlowQueryLog() {
        slowQueryThreshold.withLock { $0 = nil }
    }

    public func getSlowQueries(limit: Int) -> [SlowQueryEntry] {
        slowQueries.withLock { queries in
            Array(queries.suffix(limit).reversed())
        }
    }

    public func clearSlowQueries() {
        slowQueries.withLock { $0.removeAll() }
    }

    public func currentMetrics() -> DatabaseMetrics {
        let currentCounters = counters.withLock { $0 }
        let (p50, p99) = calculatePercentiles()
        let qps = calculateQPS()

        return DatabaseMetrics(
            activeTransactions: currentCounters.active,
            latencyP50: p50,
            latencyP99: p99,
            queriesPerSecond: qps,
            totalTransactions: currentCounters.total,
            successfulTransactions: currentCounters.successful
        )
    }

    public func reset() {
        slowQueries.withLock { $0.removeAll() }
        latencySamples.withLock { $0 = LatencySampleState() }
        counters.withLock { $0 = TransactionCounters() }
        queryTimestamps.withLock { $0.removeAll() }
    }

    // MARK: - Private Methods

    /// Records latency using reservoir sampling.
    private func recordLatency(_ duration: TimeInterval) {
        latencySamples.withLock { state in
            state.totalCount += 1

            if state.samples.count < maxLatencySamples {
                // Fill the reservoir before replacing samples.
                state.samples.append(duration)
            } else {
                // Replace an existing sample with reservoir probability.
                let index = Int.random(in: 0..<state.totalCount)
                if index < maxLatencySamples {
                    state.samples[index] = duration
                }
            }
        }
    }

    /// Records a query timestamp for rate calculation.
    private func recordQueryTimestamp() {
        let now = Date()
        let cutoff = now.addingTimeInterval(-qpsWindow)

        queryTimestamps.withLock { timestamps in
            // Remove timestamps outside the measurement window.
            timestamps.removeAll { $0 < cutoff }
            timestamps.append(now)
        }
    }

    /// Records the operation when it exceeds the slow threshold.
    private func checkSlowQuery(duration: TimeInterval, transactionID: String?, failed: Bool = false) {
        guard let threshold = slowQueryThreshold.withLock({ $0 }) else { return }
        guard duration >= threshold else { return }

        let entry = SlowQueryEntry(
            queryDescription: failed ? "Failed transaction" : "Transaction",
            executionTime: duration,
            transactionID: transactionID
        )

        slowQueries.withLock { queries in
            queries.append(entry)
            // Discard the oldest entry when capacity is exceeded.
            if queries.count > maxSlowQueries {
                queries.removeFirst(queries.count - maxSlowQueries)
            }
        }
    }

    /// Calculates a percentile from retained samples.
    private func calculatePercentiles() -> (p50: TimeInterval, p99: TimeInterval) {
        let samples = latencySamples.withLock { $0.samples }

        guard !samples.isEmpty else {
            return (0, 0)
        }

        let sorted = samples.sorted()
        let count = sorted.count

        let p50Index = Int(Double(count) * 0.50)
        let p99Index = min(Int(Double(count) * 0.99), count - 1)

        return (sorted[p50Index], sorted[p99Index])
    }

    /// Calculates queries per second.
    private func calculateQPS() -> Double {
        let timestamps = queryTimestamps.withLock { $0 }
        guard !timestamps.isEmpty else { return 0 }

        let now = Date()
        let cutoff = now.addingTimeInterval(-qpsWindow)
        let recentCount = timestamps.filter { $0 >= cutoff }.count

        return Double(recentCount) / qpsWindow
    }
}

// MARK: - PerformanceMonitor Extensions

extension PerformanceMonitor {
    /// Records a slow operation explicitly.
    ///
    /// Use this when the caller measures an operation outside listener events.
    ///
    /// - Parameters:
    ///   - description: Operation description.
    ///   - typeName: Persistable type name, when known.
    ///   - executionTime: Execution time in seconds.
    public func recordSlowQuery(
        description: String,
        typeName: String? = nil,
        executionTime: TimeInterval
    ) {
        guard let threshold = slowQueryThreshold.withLock({ $0 }) else { return }
        guard executionTime >= threshold else { return }

        let entry = SlowQueryEntry(
            queryDescription: description,
            typeName: typeName,
            executionTime: executionTime
        )

        slowQueries.withLock { queries in
            queries.append(entry)
            if queries.count > maxSlowQueries {
                queries.removeFirst(queries.count - maxSlowQueries)
            }
        }
    }

    /// Current slow-operation threshold.
    public var currentThreshold: TimeInterval? {
        slowQueryThreshold.withLock { $0 }
    }

    /// Whether slow-operation logging is enabled.
    public var isSlowQueryLogEnabled: Bool {
        currentThreshold != nil
    }

    /// Number of retained slow operations.
    public var slowQueryCount: Int {
        slowQueries.withLock { $0.count }
    }

    /// Number of retained latency samples.
    public var sampleCount: Int {
        latencySamples.withLock { $0.samples.count }
    }
}

// InstrumentedTransaction.swift
// DatabaseEngine - Transaction wrapper with detailed metrics collection
//
// Reference: FDB Record Layer FDBRecordContext instrumentation
// Provides comprehensive metrics for transaction operations.

import Foundation
import FoundationDB
import Synchronization

// MARK: - Transaction Metrics

/// Metrics collected during a transaction
///
/// **Usage**:
/// ```swift
/// let (result, metrics) = try await database.withInstrumentedTransaction { tx in
///     // operations...
/// }
/// print("Reads: \(metrics.readCount), Bytes read: \(metrics.bytesRead)")
/// ```
public struct TransactionMetrics: Sendable, CustomStringConvertible {
    /// Number of read operations (getValue, getRange)
    public var readCount: Int = 0

    /// Number of write operations (setValue, clear)
    public var writeCount: Int = 0

    /// Total bytes read
    public var bytesRead: Int = 0

    /// Total bytes written
    public var bytesWritten: Int = 0

    /// Number of range scans
    public var rangeScanCount: Int = 0

    /// Number of empty scan results
    public var emptyScanCount: Int = 0

    /// Number of key-value pairs scanned
    public var scannedKeyValueCount: Int = 0

    /// Whether the transaction committed successfully
    public var committed: Bool = false

    /// Whether the transaction was rolled back
    public var rolledBack: Bool = false

    /// Number of retries
    public var retryCount: Int = 0

    /// Transaction start time
    public var startTime: Date = Date()

    /// Transaction end time
    public var endTime: Date?

    /// Time to get read version (nanoseconds)
    public var getReadVersionNanos: UInt64?

    /// Time to commit (nanoseconds)
    public var commitNanos: UInt64?

    /// Total duration in seconds
    public var duration: TimeInterval {
        (endTime ?? Date()).timeIntervalSince(startTime)
    }

    /// Duration in nanoseconds
    public var durationNanos: UInt64 {
        UInt64(duration * 1_000_000_000)
    }

    public var description: String {
        """
        TransactionMetrics:
          Reads: \(readCount), Writes: \(writeCount)
          Bytes read: \(bytesRead), Bytes written: \(bytesWritten)
          Range scans: \(rangeScanCount), Empty scans: \(emptyScanCount)
          Scanned KVs: \(scannedKeyValueCount)
          Committed: \(committed), Rolled back: \(rolledBack)
          Retries: \(retryCount)
          Duration: \(String(format: "%.3f", duration * 1000))ms
        """
    }

    /// Export metrics to StoreTimer
    public func export(to timer: StoreTimer) {
        timer.increment(.recordsLoaded, by: readCount)
        timer.increment(.recordsSaved, by: writeCount)
        timer.recordSize(.bytesDeserialized, bytes: bytesRead)
        timer.recordSize(.bytesSerialized, bytes: bytesWritten)
        timer.increment(.rangesScanned, by: rangeScanCount)
        timer.increment(.rangeKeyValues, by: scannedKeyValueCount)

        if committed {
            timer.record(.transactionDuration, duration: durationNanos)
        }
        if let commitNanos = commitNanos {
            timer.record(.commit, duration: commitNanos)
        }
        if let grvNanos = getReadVersionNanos {
            timer.record(.getReadVersion, duration: grvNanos)
        }
        timer.increment(.retries, by: retryCount)
    }
}

// MARK: - InstrumentedTransaction

/// Transaction wrapper that collects detailed metrics
///
/// Wraps a `TransactionProtocol` and intercepts all operations to track:
/// - Read/write counts and bytes
/// - Range scan statistics
/// - Commit/rollback status
/// - Timing information
///
/// **Important**: Metrics for writes are only finalized on successful commit.
/// This prevents counting writes that were rolled back.
///
/// **Reference**: FDB Record Layer FDBRecordContext instrumentation pattern
///
/// **Usage**:
/// ```swift
/// let instrumented = InstrumentedTransaction(wrapping: transaction)
/// // ... perform operations ...
/// let metrics = instrumented.metrics
/// metrics.export(to: storeTimer)
/// ```
public final class InstrumentedTransaction: @unchecked Sendable {
    // MARK: - Properties

    /// The underlying transaction
    private let transaction: any TransactionProtocol

    /// Collected metrics (thread-safe access)
    private let state: Mutex<TransactionMetrics>

    /// Pending write metrics (only committed on success)
    private struct PendingWrites: Sendable {
        var count: Int = 0
        var bytes: Int = 0
    }
    private let pendingWrites: Mutex<PendingWrites>

    /// Optional StoreTimer for automatic export
    private let timer: StoreTimer?

    // MARK: - Initialization

    /// Create an instrumented transaction wrapper
    ///
    /// - Parameters:
    ///   - transaction: The underlying transaction to wrap
    ///   - timer: Optional StoreTimer to export metrics on commit
    public init(wrapping transaction: any TransactionProtocol, timer: StoreTimer? = nil) {
        self.transaction = transaction
        self.timer = timer
        self.state = Mutex(TransactionMetrics())
        self.pendingWrites = Mutex(PendingWrites())
    }

    /// Current metrics snapshot
    public var metrics: TransactionMetrics {
        state.withLock { $0 }
    }

    // MARK: - Read Operations

    /// Get a value and record metrics
    public func getValue(for key: FDB.Bytes, snapshot: Bool = false) async throws -> FDB.Bytes? {
        let startTime = DispatchTime.now()
        let result = try await transaction.getValue(for: key, snapshot: snapshot)
        let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds

        state.withLock { state in
            state.readCount += 1
            if let bytes = result {
                state.bytesRead += bytes.count + key.count
            }
        }

        timer?.record(.loadRecord, duration: elapsed)
        return result
    }

    /// Get a range of values and record metrics
    ///
    /// Uses the underlying transaction's getRange method.
    /// Call `recordRangeScanResults` after consuming the sequence to record metrics.
    public func getRange(
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        snapshot: Bool = false
    ) -> FDB.AsyncKVSequence {
        state.withLock { state in
            state.rangeScanCount += 1
        }

        // Return wrapped sequence that tracks results
        return transaction.getRange(
            beginSelector: beginSelector,
            endSelector: endSelector,
            snapshot: snapshot
        )
    }

    /// Record range scan results
    public func recordRangeScanResults(count: Int, bytes: Int, isEmpty: Bool) {
        state.withLock { state in
            state.scannedKeyValueCount += count
            state.bytesRead += bytes
            if isEmpty {
                state.emptyScanCount += 1
            }
        }
    }

    // MARK: - Write Operations

    /// Set a value (metrics recorded as pending until commit)
    public func setValue(_ value: FDB.Bytes, for key: FDB.Bytes) {
        transaction.setValue(value, for: key)

        // Record as pending (only finalized on commit)
        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count + value.count
        }
    }

    /// Clear a key (metrics recorded as pending until commit)
    public func clear(key: FDB.Bytes) {
        transaction.clear(key: key)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count
        }
    }

    /// Clear a range (metrics recorded as pending until commit)
    public func clearRange(beginKey: FDB.Bytes, endKey: FDB.Bytes) {
        transaction.clearRange(beginKey: beginKey, endKey: endKey)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += beginKey.count + endKey.count
        }
    }

    /// Perform an atomic operation
    public func atomicOp(key: FDB.Bytes, param: FDB.Bytes, mutationType: FDB.MutationType) {
        transaction.atomicOp(key: key, param: param, mutationType: mutationType)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count + param.count
        }
    }

    // MARK: - Transaction Control

    /// Commit the transaction and finalize metrics
    @discardableResult
    public func commit() async throws -> Bool {
        let startTime = DispatchTime.now()

        do {
            let result = try await transaction.commit()
            let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds

            // Finalize metrics on successful commit
            let pending = pendingWrites.withLock { $0 }

            state.withLock { state in
                state.committed = result
                state.endTime = Date()
                state.commitNanos = elapsed

                // Only count writes on successful commit
                if result {
                    state.writeCount += pending.count
                    state.bytesWritten += pending.bytes
                }
            }

            // Export to timer
            if let timer = timer {
                metrics.export(to: timer)
            }

            return result
        } catch {
            state.withLock { state in
                state.rolledBack = true
                state.endTime = Date()
            }
            throw error
        }
    }

    /// Cancel the transaction
    public func cancel() {
        transaction.cancel()

        state.withLock { state in
            state.rolledBack = true
            state.endTime = Date()
        }
    }

    /// Record a retry
    public func recordRetry() {
        state.withLock { state in
            state.retryCount += 1
        }
        timer?.increment(.retries)
    }

    // MARK: - Read Version

    /// Get read version and record timing
    public func getReadVersion() async throws -> Int64 {
        let startTime = DispatchTime.now()
        let version = try await transaction.getReadVersion()
        let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds

        state.withLock { state in
            state.getReadVersionNanos = elapsed
        }

        timer?.record(.getReadVersion, duration: elapsed)
        return version
    }

    /// Set read version
    public func setReadVersion(_ version: Int64) {
        transaction.setReadVersion(version)
    }

    /// Get committed version
    public func getCommittedVersion() throws -> Int64 {
        try transaction.getCommittedVersion()
    }

    // MARK: - Options

    /// Set transaction option
    public func setOption(forOption option: FDB.TransactionOption) throws {
        try transaction.setOption(forOption: option)
    }

    /// Set transaction option with integer value
    public func setOption(to value: Int, forOption option: FDB.TransactionOption) throws {
        try transaction.setOption(to: value, forOption: option)
    }

    /// Set transaction option with string value
    public func setOption(to value: String, forOption option: FDB.TransactionOption) throws {
        try transaction.setOption(to: value, forOption: option)
    }

    // MARK: - Access to Underlying Transaction

    /// Get the underlying transaction for operations not yet wrapped
    public var underlying: any TransactionProtocol {
        transaction
    }
}

// MARK: - DatabaseProtocol Extension

extension DatabaseProtocol {
    /// Execute a transaction with instrumentation
    ///
    /// Returns both the operation result and collected metrics.
    ///
    /// **Usage**:
    /// ```swift
    /// let (result, metrics) = try await database.withInstrumentedTransaction { tx in
    ///     let value = try await tx.getValue(for: key)
    ///     tx.setValue(newValue, for: key)
    ///     return value
    /// }
    /// print("Transaction metrics: \(metrics)")
    /// ```
    ///
    /// - Parameters:
    ///   - configuration: Optional transaction configuration
    ///   - timer: Optional StoreTimer for automatic metric export
    ///   - operation: The operation to execute
    /// - Returns: Tuple of (operation result, transaction metrics)
    public func withInstrumentedTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        timer: StoreTimer? = nil,
        _ operation: @Sendable (InstrumentedTransaction) async throws -> T
    ) async throws -> (result: T, metrics: TransactionMetrics) {
        let maxRetries = configuration.retryLimit ?? 100

        for attempt in 0..<maxRetries {
            let transaction = try createTransaction()
            try transaction.apply(configuration)

            let instrumented = InstrumentedTransaction(wrapping: transaction, timer: timer)

            if attempt > 0 {
                instrumented.recordRetry()
            }

            do {
                let result = try await operation(instrumented)
                let committed = try await instrumented.commit()

                if committed {
                    return (result, instrumented.metrics)
                }
            } catch {
                instrumented.cancel()

                if let fdbError = error as? FDBError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
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

// MARK: - StoreTimerEvent Extensions

extension StoreTimerEvent {
    /// Number of reads in a transaction
    public static let transactionReads = StoreTimerEvent(name: "transaction_reads", isCount: true)

    /// Number of writes in a transaction
    public static let transactionWrites = StoreTimerEvent(name: "transaction_writes", isCount: true)

    /// Bytes read in a transaction
    public static let transactionBytesRead = StoreTimerEvent(name: "transaction_bytes_read", isSize: true)

    /// Bytes written in a transaction
    public static let transactionBytesWritten = StoreTimerEvent(name: "transaction_bytes_written", isSize: true)

    /// Number of empty scans (no results)
    public static let emptyScans = StoreTimerEvent(name: "empty_scans", isCount: true)

    /// Number of transaction commits
    public static let commits = StoreTimerEvent(name: "commits", isCount: true)

    /// Number of transaction rollbacks
    public static let rollbacks = StoreTimerEvent(name: "rollbacks", isCount: true)
}

// MARK: - MetricsAggregator

/// Aggregates metrics from multiple transactions
///
/// **Usage**:
/// ```swift
/// let aggregator = MetricsAggregator()
///
/// for _ in 0..<100 {
///     let (_, metrics) = try await db.withInstrumentedTransaction { tx in
///         // ...
///     }
///     aggregator.record(metrics)
/// }
///
/// print(aggregator.summary)
/// ```
public final class MetricsAggregator: Sendable {
    private struct State: Sendable {
        var totalTransactions: Int = 0
        var successfulCommits: Int = 0
        var totalRollbacks: Int = 0
        var totalRetries: Int = 0
        var totalReads: Int = 0
        var totalWrites: Int = 0
        var totalBytesRead: Int = 0
        var totalBytesWritten: Int = 0
        var totalRangeScans: Int = 0
        var totalEmptyScans: Int = 0
        var totalDurationNanos: UInt64 = 0
        var maxDurationNanos: UInt64 = 0
        var minDurationNanos: UInt64 = .max
    }

    private let state: Mutex<State>

    public init() {
        self.state = Mutex(State())
    }

    /// Record metrics from a completed transaction
    public func record(_ metrics: TransactionMetrics) {
        state.withLock { state in
            state.totalTransactions += 1
            if metrics.committed {
                state.successfulCommits += 1
            }
            if metrics.rolledBack {
                state.totalRollbacks += 1
            }
            state.totalRetries += metrics.retryCount
            state.totalReads += metrics.readCount
            state.totalWrites += metrics.writeCount
            state.totalBytesRead += metrics.bytesRead
            state.totalBytesWritten += metrics.bytesWritten
            state.totalRangeScans += metrics.rangeScanCount
            state.totalEmptyScans += metrics.emptyScanCount

            let durationNanos = metrics.durationNanos
            state.totalDurationNanos += durationNanos
            state.maxDurationNanos = max(state.maxDurationNanos, durationNanos)
            state.minDurationNanos = min(state.minDurationNanos, durationNanos)
        }
    }

    /// Summary of aggregated metrics
    public var summary: AggregatedMetricsSummary {
        state.withLock { state in
            AggregatedMetricsSummary(
                totalTransactions: state.totalTransactions,
                successfulCommits: state.successfulCommits,
                totalRollbacks: state.totalRollbacks,
                totalRetries: state.totalRetries,
                totalReads: state.totalReads,
                totalWrites: state.totalWrites,
                totalBytesRead: state.totalBytesRead,
                totalBytesWritten: state.totalBytesWritten,
                totalRangeScans: state.totalRangeScans,
                totalEmptyScans: state.totalEmptyScans,
                avgDurationMs: state.totalTransactions > 0
                    ? Double(state.totalDurationNanos) / Double(state.totalTransactions) / 1_000_000
                    : 0,
                maxDurationMs: Double(state.maxDurationNanos) / 1_000_000,
                minDurationMs: state.minDurationNanos == .max ? 0 : Double(state.minDurationNanos) / 1_000_000
            )
        }
    }

    /// Reset all aggregated metrics
    public func reset() {
        state.withLock { $0 = State() }
    }
}

/// Summary of aggregated metrics
public struct AggregatedMetricsSummary: Sendable, CustomStringConvertible {
    public let totalTransactions: Int
    public let successfulCommits: Int
    public let totalRollbacks: Int
    public let totalRetries: Int
    public let totalReads: Int
    public let totalWrites: Int
    public let totalBytesRead: Int
    public let totalBytesWritten: Int
    public let totalRangeScans: Int
    public let totalEmptyScans: Int
    public let avgDurationMs: Double
    public let maxDurationMs: Double
    public let minDurationMs: Double

    /// Success rate (0.0 - 1.0)
    public var successRate: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(successfulCommits) / Double(totalTransactions)
    }

    /// Average reads per transaction
    public var avgReadsPerTransaction: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(totalReads) / Double(totalTransactions)
    }

    /// Average writes per transaction
    public var avgWritesPerTransaction: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(totalWrites) / Double(totalTransactions)
    }

    public var description: String {
        """
        AggregatedMetrics:
          Transactions: \(totalTransactions) (success: \(successfulCommits), rollback: \(totalRollbacks))
          Success rate: \(String(format: "%.1f%%", successRate * 100))
          Total retries: \(totalRetries)
          Reads: \(totalReads) (avg: \(String(format: "%.1f", avgReadsPerTransaction))/tx)
          Writes: \(totalWrites) (avg: \(String(format: "%.1f", avgWritesPerTransaction))/tx)
          Bytes read: \(totalBytesRead), Bytes written: \(totalBytesWritten)
          Range scans: \(totalRangeScans), Empty scans: \(totalEmptyScans)
          Duration: avg=\(String(format: "%.2f", avgDurationMs))ms, min=\(String(format: "%.2f", minDurationMs))ms, max=\(String(format: "%.2f", maxDurationMs))ms
        """
    }
}

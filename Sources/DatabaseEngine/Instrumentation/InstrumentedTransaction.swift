// InstrumentedTransaction.swift
// DatabaseEngine - Transaction wrapper with detailed metrics collection
//
// Reference: FDB Record Layer FDBRecordContext instrumentation
// Provides comprehensive metrics for transaction operations.

import DatabaseTypes
import StorageKit
import Synchronization

// MARK: - Transaction Metrics

/// Metrics collected during a transaction
///
/// **Usage**:
/// ```swift
/// let (result, metrics) = try await database.withInstrumentedTransaction { tx in
///     // operations...
/// }
/// print("Reads: \(metrics.readCount), bytes read: \(metrics.bytesRead)")
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

    /// Monotonic instant at which observation began.
    public var startTime: StorageInstant = StorageInstant(
        durationSinceReference: .zero
    )

    /// Monotonic instant at which observation ended.
    public var endTime: StorageInstant?

    /// Time to get read version (nanoseconds)
    public var getReadVersionNanos: UInt64?

    /// Time to commit (nanoseconds)
    public var commitNanos: UInt64?

    /// Duration in nanoseconds
    public var durationNanos: UInt64 = 0

    public var description: String {
        """
        TransactionMetrics:
          Reads: \(readCount), Writes: \(writeCount)
          Bytes read: \(bytesRead), bytes written: \(bytesWritten)
          Range scans: \(rangeScanCount), Empty scans: \(emptyScanCount)
          Scanned KVs: \(scannedKeyValueCount)
          Committed: \(committed), Rolled back: \(rolledBack)
          Retries: \(retryCount)
          Duration: \(DatabaseTextFormatting.fixedDecimal(Double(durationNanos) / 1_000_000, fractionDigits: 3))ms
        """
    }

    /// Export metrics to StoreTimer
    public func export(to timer: StoreTimer) {
        timer.increment(.entitiesLoaded, by: readCount)
        timer.increment(.entitiesSaved, by: writeCount)
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

/// Transaction-access observer that collects detailed metrics.
///
/// Intercepts storage operations without receiving commit, cancellation, or
/// retry authority:
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
public final class InstrumentedTransaction: Sendable {
    // MARK: - Properties

    /// The observed storage access.
    private let transaction: any TransactionAccess

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

    /// Monotonic time source shared with the owning transaction runner.
    private let monotonicClock: any StorageMonotonicClock

    // MARK: - Initialization

    /// Creates instrumented storage access.
    ///
    /// - Parameters:
    ///   - transaction: The storage access to observe.
    ///   - timer: Optional StoreTimer to export metrics on commit
    public init(
        wrapping transaction: any TransactionAccess,
        monotonicClock: any StorageMonotonicClock,
        timer: StoreTimer? = nil
    ) {
        self.transaction = transaction
        self.monotonicClock = monotonicClock
        self.timer = timer
        var metrics = TransactionMetrics()
        metrics.startTime = monotonicClock.now
        self.state = Mutex(metrics)
        self.pendingWrites = Mutex(PendingWrites())
    }

    /// Current metrics snapshot
    public var metrics: TransactionMetrics {
        state.withLock { $0 }
    }

    // MARK: - Read Operations

    /// Get a value and record metrics
    public func getValue(for key: ByteString, snapshot: Bool = false) async throws -> ByteString? {
        let startTime = monotonicClock.now
        let result = try await transaction.getValue(for: key, snapshot: snapshot)
        let elapsed = DatabaseMonotonicMeasurement.nanoseconds(
            from: startTime,
            to: monotonicClock.now
        )

        state.withLock { state in
            state.readCount += 1
            if let bytes = result {
                state.bytesRead += bytes.count + key.count
            }
        }

        timer?.record(.loadEntity, duration: elapsed)
        return result
    }

    /// Collect a range of values and record metrics
    ///
    /// Uses the observed storage access's `collectRange` operation.
    public func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) async throws -> [(ByteString, ByteString)] {
        state.withLock { state in
            state.rangeScanCount += 1
        }

        let results = try await transaction.collectRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )

        // Record metrics
        let totalBytes = results.reduce(0) { $0 + $1.0.count + $1.1.count }
        state.withLock { state in
            state.scannedKeyValueCount += results.count
            state.bytesRead += totalBytes
            if results.isEmpty {
                state.emptyScanCount += 1
            }
        }

        return results
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
    public func setValue(_ value: ByteString, for key: ByteString) throws {
        try transaction.setValue(value, for: key)

        // Record as pending (only finalized on commit)
        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count + value.count
        }
    }

    /// Clear a key (metrics recorded as pending until commit)
    public func clear(key: ByteString) throws {
        try transaction.clear(key: key)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count
        }
    }

    /// Clear a range (metrics recorded as pending until commit)
    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try transaction.clearRange(beginKey: beginKey, endKey: endKey)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += beginKey.count + endKey.count
        }
    }

    /// Perform an atomic operation
    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try transaction.atomicOp(key: key, param: param, mutationType: mutationType)

        pendingWrites.withLock { pending in
            pending.count += 1
            pending.bytes += key.count + param.count
        }
    }

    /// Finalize metrics after an external transaction runner committed the
    /// wrapped transaction.
    ///
    /// This keeps instrumentation as an observer instead of letting it own
    /// commit/retry policy.
    func recordExternalCommit(commitNanos: UInt64) {
        let pending = pendingWrites.withLock { $0 }
        let endTime = monotonicClock.now

        state.withLock { state in
            state.committed = true
            state.endTime = endTime
            state.durationNanos = DatabaseMonotonicMeasurement.nanoseconds(
                from: state.startTime,
                to: endTime
            )
            state.commitNanos = commitNanos
            state.writeCount += pending.count
            state.bytesWritten += pending.bytes
        }

        if let timer = timer {
            metrics.export(to: timer)
        }
    }

    /// Record cancellation completed by the external transaction runner.
    func recordExternalCancellation() {
        let endTime = monotonicClock.now
        state.withLock { state in
            state.rolledBack = true
            state.endTime = endTime
            state.durationNanos = DatabaseMonotonicMeasurement.nanoseconds(
                from: state.startTime,
                to: endTime
            )
        }
    }

    /// Record a retry
    public func recordRetry() {
        state.withLock { state in
            state.retryCount += 1
        }
    }

    // MARK: - Read Version

    /// Get read version and record timing
    public func getReadVersion() async throws -> Int64 {
        let startTime = monotonicClock.now
        let version = try await transaction.getReadVersion()
        let elapsed = DatabaseMonotonicMeasurement.nanoseconds(
            from: startTime,
            to: monotonicClock.now
        )

        state.withLock { state in
            state.getReadVersionNanos = elapsed
        }

        timer?.record(.getReadVersion, duration: elapsed)
        return version
    }

    /// Set read version
    public func setReadVersion(_ version: Int64) throws {
        try transaction.setReadVersion(version)
    }

    // MARK: - Options

    /// Set transaction option
    public func setOption(forOption option: TransactionOption) throws {
        try transaction.setOption(forOption: option)
    }

    /// Set transaction option with integer value
    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        try transaction.setOption(to: value, forOption: option)
    }

    /// Set transaction option with string value
    public func setOption(to value: String, forOption option: TransactionOption) throws {
        try transaction.setOption(
            to: ByteString(utf8: value),
            forOption: option
        )
    }

    /// Storage capability for operations that do not require dedicated metrics.
    public var storageAccess: any TransactionAccess {
        transaction
    }
}

// MARK: - StorageEngine Extension

extension StorageTransactionExecutor {
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
    ///   - timer: Optional StoreTimer for automatic metric export
    ///   - operation: The operation to execute
    /// - Returns: Tuple of (operation result, transaction metrics)
    public func withInstrumentedTransaction<T: Sendable>(
        timer: StoreTimer? = nil,
        clock: any StorageMonotonicClock,
        _ operation: @escaping @Sendable (InstrumentedTransaction) async throws -> T
    ) async throws -> (result: T, metrics: TransactionMetrics) {
        let retryCount = Mutex<Int>(0)
        let current = Mutex<InstrumentedTransaction?>(nil)
        let runner = TransactionRunner(
            transactionExecutor: self,
            clock: clock
        )

        let result = try await runner.run(
            configuration: .default,
            operationDescription: "StorageEngine.withInstrumentedTransaction",
            onRetry: { _, _ in
                retryCount.withLock { $0 += 1 }
            },
            onCancel: { _ in
                current.withLock { $0 }?.recordExternalCancellation()
            },
            onCommitSuccess: { _, commitNanos in
                current.withLock { $0 }?.recordExternalCommit(commitNanos: commitNanos)
            }
        ) { transaction in
            let instrumented = InstrumentedTransaction(
                wrapping: transaction,
                monotonicClock: clock,
                timer: timer
            )
            let retries = retryCount.withLock { $0 }
            for _ in 0..<retries {
                instrumented.recordRetry()
            }
            current.withLock { $0 = instrumented }

            return try await operation(instrumented)
        }

        guard let metrics = current.withLock({ $0?.metrics }) else {
            throw StorageError.invalidOperation("Instrumented transaction did not run")
        }
        return (result, metrics)
    }
}

// MARK: - StoreTimerEvent Extensions

extension StoreTimerEvent {
    /// Number of reads in a transaction
    public static let transactionReads = StoreTimerEvent(name: "transaction_reads", isCount: true)

    /// Number of writes in a transaction
    public static let transactionWrites = StoreTimerEvent(name: "transaction_writes", isCount: true)

    /// Bytes read in a transaction.
    public static let transactionBytesRead = StoreTimerEvent(name: "transaction_bytes_read", isSize: true)

    /// Bytes written in a transaction.
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
          Success rate: \(DatabaseTextFormatting.fixedDecimal(successRate * 100, fractionDigits: 1))%
          Total retries: \(totalRetries)
          Reads: \(totalReads) (avg: \(DatabaseTextFormatting.fixedDecimal(avgReadsPerTransaction, fractionDigits: 1))/tx)
          Writes: \(totalWrites) (avg: \(DatabaseTextFormatting.fixedDecimal(avgWritesPerTransaction, fractionDigits: 1))/tx)
          Bytes read: \(totalBytesRead), bytes written: \(totalBytesWritten)
          Range scans: \(totalRangeScans), Empty scans: \(totalEmptyScans)
          Duration: avg=\(DatabaseTextFormatting.fixedDecimal(avgDurationMs, fractionDigits: 2))ms, min=\(DatabaseTextFormatting.fixedDecimal(minDurationMs, fractionDigits: 2))ms, max=\(DatabaseTextFormatting.fixedDecimal(maxDurationMs, fractionDigits: 2))ms
        """
    }
}

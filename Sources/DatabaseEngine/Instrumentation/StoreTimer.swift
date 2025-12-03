// StoreTimer.swift
// DatabaseEngine - Detailed instrumentation for database operations
//
// Reference: FDB Record Layer StoreTimer.java
// Provides fine-grained timing and counting for database operations.

import Foundation
import Metrics
import Synchronization

// MARK: - StoreTimerEvent

/// Events that can be timed/counted in database operations
///
/// Based on FDB Record Layer's StoreTimer.Events
/// Reference: https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBStoreTimer.java
public struct StoreTimerEvent: Hashable, Sendable, CustomStringConvertible {
    public let name: String
    public let isCount: Bool
    public let isSize: Bool

    private init(name: String, isCount: Bool = false, isSize: Bool = false) {
        self.name = name
        self.isCount = isCount
        self.isSize = isSize
    }

    public var description: String { name }

    // MARK: - Transaction Events

    /// Time to get read version from FDB
    public static let getReadVersion = StoreTimerEvent(name: "get_read_version")

    /// Time to commit transaction
    public static let commit = StoreTimerEvent(name: "commit")

    /// Time waiting for commit (latency)
    public static let commitWait = StoreTimerEvent(name: "commit_wait")

    /// Total transaction duration
    public static let transactionDuration = StoreTimerEvent(name: "transaction_duration")

    /// Number of retries
    public static let retries = StoreTimerEvent(name: "retries", isCount: true)

    // MARK: - Record Operations

    /// Time to save a record
    public static let saveRecord = StoreTimerEvent(name: "save_record")

    /// Time to load a record
    public static let loadRecord = StoreTimerEvent(name: "load_record")

    /// Time to delete a record
    public static let deleteRecord = StoreTimerEvent(name: "delete_record")

    /// Number of records saved
    public static let recordsSaved = StoreTimerEvent(name: "records_saved", isCount: true)

    /// Number of records loaded
    public static let recordsLoaded = StoreTimerEvent(name: "records_loaded", isCount: true)

    /// Number of records deleted
    public static let recordsDeleted = StoreTimerEvent(name: "records_deleted", isCount: true)

    // MARK: - Index Operations

    /// Time to update index entries
    public static let updateIndex = StoreTimerEvent(name: "update_index")

    /// Time to scan index
    public static let scanIndex = StoreTimerEvent(name: "scan_index")

    /// Number of index entries written
    public static let indexEntriesWritten = StoreTimerEvent(name: "index_entries_written", isCount: true)

    /// Number of index entries read
    public static let indexEntriesRead = StoreTimerEvent(name: "index_entries_read", isCount: true)

    /// Number of index entries deleted
    public static let indexEntriesDeleted = StoreTimerEvent(name: "index_entries_deleted", isCount: true)

    // MARK: - Range Operations

    /// Time to perform range scan
    public static let rangeScan = StoreTimerEvent(name: "range_scan")

    /// Number of ranges scanned
    public static let rangesScanned = StoreTimerEvent(name: "ranges_scanned", isCount: true)

    /// Number of key-value pairs read in range
    public static let rangeKeyValues = StoreTimerEvent(name: "range_key_values", isCount: true)

    // MARK: - Serialization

    /// Time to serialize record
    public static let serialize = StoreTimerEvent(name: "serialize")

    /// Time to deserialize record
    public static let deserialize = StoreTimerEvent(name: "deserialize")

    /// Bytes serialized
    public static let bytesSerialized = StoreTimerEvent(name: "bytes_serialized", isSize: true)

    /// Bytes deserialized
    public static let bytesDeserialized = StoreTimerEvent(name: "bytes_deserialized", isSize: true)

    // MARK: - Compression

    /// Time to compress data
    public static let compress = StoreTimerEvent(name: "compress")

    /// Time to decompress data
    public static let decompress = StoreTimerEvent(name: "decompress")

    /// Compression ratio (compressed / original)
    public static let compressionRatio = StoreTimerEvent(name: "compression_ratio")

    // MARK: - Query Planning

    /// Time to plan query
    public static let planQuery = StoreTimerEvent(name: "plan_query")

    /// Time to execute plan
    public static let executePlan = StoreTimerEvent(name: "execute_plan")

    /// Number of plans evaluated
    public static let plansEvaluated = StoreTimerEvent(name: "plans_evaluated", isCount: true)

    // MARK: - Online Indexing

    /// Time for single online index batch
    public static let onlineIndexBatch = StoreTimerEvent(name: "online_index_batch")

    /// Number of records indexed
    public static let recordsIndexed = StoreTimerEvent(name: "records_indexed", isCount: true)

    // MARK: - Cache

    /// Cache hit
    public static let cacheHit = StoreTimerEvent(name: "cache_hit", isCount: true)

    /// Cache miss
    public static let cacheMiss = StoreTimerEvent(name: "cache_miss", isCount: true)
}

// MARK: - StoreTimer

/// Timer for recording database operation metrics
///
/// Provides fine-grained timing and counting similar to FDB Record Layer's StoreTimer.
/// Integrates with swift-metrics for backend flexibility.
///
/// **Usage**:
/// ```swift
/// let timer = StoreTimer()
/// timer.record(.saveRecord, duration: 5_000_000) // 5ms in nanoseconds
/// timer.increment(.recordsSaved, by: 10)
///
/// // Or use the scoped timing API
/// let result = try await timer.time(.loadRecord) {
///     try await loadRecordFromDatabase()
/// }
/// ```
///
/// **Thread Safety**: This class is thread-safe.
public final class StoreTimer: Sendable {

    // MARK: - State

    private struct State: Sendable {
        var events: [StoreTimerEvent: EventData] = [:]
    }

    private struct EventData: Sendable {
        var count: Int = 0
        var totalNanos: UInt64 = 0
        var minNanos: UInt64 = .max
        var maxNanos: UInt64 = 0
    }

    private let state: Mutex<State>

    /// Prefix for metrics labels
    public let metricsPrefix: String

    /// Whether to emit metrics to swift-metrics backend
    public let emitMetrics: Bool

    // MARK: - Pre-created Metrics

    /// Pre-created timer metrics for O(1) lookup
    /// Reference: swift-metrics recommends reusing metric instances
    private let timerMetrics: [StoreTimerEvent: Metrics.Timer]

    /// Pre-created counter metrics for O(1) lookup
    private let counterMetrics: [StoreTimerEvent: Counter]

    // MARK: - Initialization

    public init(metricsPrefix: String = "fdb", emitMetrics: Bool = true) {
        self.metricsPrefix = metricsPrefix
        self.emitMetrics = emitMetrics
        self.state = Mutex(State())

        // Pre-create all metrics for efficiency
        if emitMetrics {
            // Timer events (timing-based)
            let timerEvents: [StoreTimerEvent] = [
                .getReadVersion, .commit, .commitWait, .transactionDuration,
                .saveRecord, .loadRecord, .deleteRecord,
                .updateIndex, .scanIndex,
                .rangeScan,
                .serialize, .deserialize,
                .compress, .decompress, .compressionRatio,
                .planQuery, .executePlan,
                .onlineIndexBatch
            ]

            var timers: [StoreTimerEvent: Metrics.Timer] = [:]
            for event in timerEvents {
                timers[event] = Metrics.Timer(label: "\(metricsPrefix)_\(event.name)_nanoseconds")
            }
            self.timerMetrics = timers

            // Counter events (count/size-based)
            let counterEvents: [StoreTimerEvent] = [
                .retries,
                .recordsSaved, .recordsLoaded, .recordsDeleted,
                .indexEntriesWritten, .indexEntriesRead, .indexEntriesDeleted,
                .rangesScanned, .rangeKeyValues,
                .bytesSerialized, .bytesDeserialized,
                .plansEvaluated,
                .recordsIndexed,
                .cacheHit, .cacheMiss
            ]

            var counters: [StoreTimerEvent: Counter] = [:]
            for event in counterEvents {
                counters[event] = Counter(label: "\(metricsPrefix)_\(event.name)_total")
            }
            self.counterMetrics = counters
        } else {
            self.timerMetrics = [:]
            self.counterMetrics = [:]
        }
    }

    // MARK: - Recording

    /// Record a timing event
    ///
    /// - Parameters:
    ///   - event: The event to record
    ///   - duration: Duration in nanoseconds
    public func record(_ event: StoreTimerEvent, duration nanoseconds: UInt64) {
        state.withLock { state in
            var data = state.events[event] ?? EventData()
            data.count += 1
            data.totalNanos += nanoseconds
            data.minNanos = min(data.minNanos, nanoseconds)
            data.maxNanos = max(data.maxNanos, nanoseconds)
            state.events[event] = data
        }

        // Emit to pre-created swift-metrics timer (O(1) lookup)
        timerMetrics[event]?.recordNanoseconds(Int64(nanoseconds))
    }

    /// Increment a count event
    ///
    /// - Parameters:
    ///   - event: The event to increment
    ///   - count: Amount to increment by (default 1)
    public func increment(_ event: StoreTimerEvent, by count: Int = 1) {
        state.withLock { state in
            var data = state.events[event] ?? EventData()
            data.count += count
            state.events[event] = data
        }

        // Emit to pre-created swift-metrics counter (O(1) lookup)
        counterMetrics[event]?.increment(by: count)
    }

    /// Record a size event
    ///
    /// - Parameters:
    ///   - event: The event to record
    ///   - bytes: Size in bytes
    public func recordSize(_ event: StoreTimerEvent, bytes: Int) {
        increment(event, by: bytes)
    }

    // MARK: - Scoped Timing

    /// Time a synchronous operation
    ///
    /// - Parameters:
    ///   - event: The event to time
    ///   - operation: The operation to execute
    /// - Returns: The result of the operation
    public func time<T>(_ event: StoreTimerEvent, _ operation: () throws -> T) rethrows -> T {
        let start = DispatchTime.now()
        defer {
            let duration = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            record(event, duration: duration)
        }
        return try operation()
    }

    /// Time an async operation
    ///
    /// - Parameters:
    ///   - event: The event to time
    ///   - operation: The async operation to execute
    /// - Returns: The result of the operation
    public func time<T>(_ event: StoreTimerEvent, _ operation: () async throws -> T) async rethrows -> T {
        let start = DispatchTime.now()
        defer {
            let duration = DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds
            record(event, duration: duration)
        }
        return try await operation()
    }

    // MARK: - Querying

    /// Get the count for an event
    public func getCount(_ event: StoreTimerEvent) -> Int {
        state.withLock { $0.events[event]?.count ?? 0 }
    }

    /// Get the total time in nanoseconds for an event
    public func getTotalNanos(_ event: StoreTimerEvent) -> UInt64 {
        state.withLock { $0.events[event]?.totalNanos ?? 0 }
    }

    /// Get statistics for an event
    public func getStats(_ event: StoreTimerEvent) -> EventStats? {
        state.withLock { state in
            guard let data = state.events[event], data.count > 0 else { return nil }
            return EventStats(
                event: event,
                count: data.count,
                totalNanos: data.totalNanos,
                minNanos: data.minNanos == .max ? 0 : data.minNanos,
                maxNanos: data.maxNanos
            )
        }
    }

    /// Get all recorded events
    public func getAllStats() -> [EventStats] {
        state.withLock { state in
            state.events.compactMap { event, data in
                guard data.count > 0 else { return nil }
                return EventStats(
                    event: event,
                    count: data.count,
                    totalNanos: data.totalNanos,
                    minNanos: data.minNanos == .max ? 0 : data.minNanos,
                    maxNanos: data.maxNanos
                )
            }
        }
    }

    // MARK: - Reset

    /// Reset all recorded data
    public func reset() {
        state.withLock { $0.events.removeAll() }
    }

    /// Reset a specific event
    public func reset(_ event: StoreTimerEvent) {
        state.withLock { _ = $0.events.removeValue(forKey: event) }
    }

    // MARK: - Merging

    /// Add statistics from another timer
    public func add(_ other: StoreTimer) {
        let otherEvents = other.state.withLock { $0.events }

        state.withLock { state in
            for (event, otherData) in otherEvents {
                var data = state.events[event] ?? EventData()
                data.count += otherData.count
                data.totalNanos += otherData.totalNanos
                data.minNanos = min(data.minNanos, otherData.minNanos)
                data.maxNanos = max(data.maxNanos, otherData.maxNanos)
                state.events[event] = data
            }
        }
    }
}

// MARK: - EventStats

/// Statistics for a single event
public struct EventStats: Sendable {
    public let event: StoreTimerEvent
    public let count: Int
    public let totalNanos: UInt64
    public let minNanos: UInt64
    public let maxNanos: UInt64

    /// Average time in nanoseconds
    public var avgNanos: Double {
        count > 0 ? Double(totalNanos) / Double(count) : 0
    }

    /// Total time in milliseconds
    public var totalMs: Double {
        Double(totalNanos) / 1_000_000
    }

    /// Average time in milliseconds
    public var avgMs: Double {
        avgNanos / 1_000_000
    }

    /// Min time in milliseconds
    public var minMs: Double {
        Double(minNanos) / 1_000_000
    }

    /// Max time in milliseconds
    public var maxMs: Double {
        Double(maxNanos) / 1_000_000
    }
}

// MARK: - StoreTimerSnapshot

/// Immutable snapshot of timer state
public struct StoreTimerSnapshot: Sendable {
    public let timestamp: Date
    public let stats: [StoreTimerEvent: EventStats]

    /// Create a snapshot from a timer
    public init(from timer: StoreTimer) {
        self.timestamp = Date()
        var stats: [StoreTimerEvent: EventStats] = [:]
        for stat in timer.getAllStats() {
            stats[stat.event] = stat
        }
        self.stats = stats
    }

    /// Get difference between this snapshot and another
    public func difference(from earlier: StoreTimerSnapshot) -> [StoreTimerEvent: EventStats] {
        var result: [StoreTimerEvent: EventStats] = [:]

        for (event, current) in stats {
            let earlier = earlier.stats[event]
            let countDiff = current.count - (earlier?.count ?? 0)
            let totalDiff = current.totalNanos - (earlier?.totalNanos ?? 0)

            if countDiff > 0 {
                result[event] = EventStats(
                    event: event,
                    count: countDiff,
                    totalNanos: totalDiff,
                    minNanos: current.minNanos,
                    maxNanos: current.maxNanos
                )
            }
        }

        return result
    }
}

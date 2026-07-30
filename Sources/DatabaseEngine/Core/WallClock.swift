import DatabaseTypes

/// Supplies absolute time for persisted database values and observable events.
///
/// Scheduling and latency measurement use `StorageMonotonicClock` instead.
/// Keeping these capabilities separate prevents wall-clock adjustments from
/// affecting deadlines, retry delays, and performance measurements.
public protocol WallClock: Sendable {
    var now: Timestamp { get }
}

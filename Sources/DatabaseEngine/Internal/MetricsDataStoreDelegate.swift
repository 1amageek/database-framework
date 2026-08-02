import DatabaseKit
import StorageKit

/// Internal delegate that collects data store metrics through the container's
/// explicitly selected metrics destination.
///
/// This delegate is the default implementation used by DatabaseDataStore.
/// It emits operation counts and durations to the configured metrics backend.
///
/// **Metrics Recorded**:
/// - `database_persistence_operations_total` (Counter): Total operation count by type and status
/// - `database_persistence_operation_duration_seconds` (Timer): Operation duration by type
/// - `database_persistence_items_total` (Counter): Total items processed by operation type
///
/// **Labels/Dimensions**:
/// - `operation`: save, fetch, delete, batch
/// - `item_type`: The persistable type name (e.g., "User", "Product")
/// - `status`: success, failure
///
/// **Usage**: Automatically used by DatabaseDataStore. No user configuration required.
/// Users can configure the metrics backend via `MetricsSystem.bootstrap()`.
final class MetricsDataStoreDelegate: DataStoreDelegate, Sendable {
    // MARK: - Metrics

    // Operation counters
    private let metrics: DatabaseMetricsConfiguration
    private let saveCounter: DatabaseMetricCounter
    private let fetchCounter: DatabaseMetricCounter
    private let deleteCounter: DatabaseMetricCounter
    private let batchCounter: DatabaseMetricCounter

    // Error counters
    private let saveErrorCounter: DatabaseMetricCounter
    private let fetchErrorCounter: DatabaseMetricCounter
    private let deleteErrorCounter: DatabaseMetricCounter
    private let batchErrorCounter: DatabaseMetricCounter

    // Timers
    private let saveTimer: DatabaseMetricTimer
    private let fetchTimer: DatabaseMetricTimer
    private let deleteTimer: DatabaseMetricTimer
    private let batchTimer: DatabaseMetricTimer

    // Item counters
    private let itemsSavedCounter: DatabaseMetricCounter
    private let itemsFetchedCounter: DatabaseMetricCounter
    private let itemsDeletedCounter: DatabaseMetricCounter

    // MARK: - Initialization

    init(metrics: DatabaseMetricsConfiguration) {
        self.metrics = metrics
        // Initialize counters
        self.saveCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "save"), ("status", "success")]
        )
        self.fetchCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "fetch"), ("status", "success")]
        )
        self.deleteCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "delete"), ("status", "success")]
        )
        self.batchCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "batch"), ("status", "success")]
        )

        // Initialize error counters
        self.saveErrorCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "save"), ("status", "failure")]
        )
        self.fetchErrorCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "fetch"), ("status", "failure")]
        )
        self.deleteErrorCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "delete"), ("status", "failure")]
        )
        self.batchErrorCounter = metrics.counter(
            label: "database_persistence_operations_total",
            dimensions: [("operation", "batch"), ("status", "failure")]
        )

        // Initialize timers
        self.saveTimer = metrics.timer(
            label: "database_persistence_operation_duration_seconds",
            dimensions: [("operation", "save")]
        )
        self.fetchTimer = metrics.timer(
            label: "database_persistence_operation_duration_seconds",
            dimensions: [("operation", "fetch")]
        )
        self.deleteTimer = metrics.timer(
            label: "database_persistence_operation_duration_seconds",
            dimensions: [("operation", "delete")]
        )
        self.batchTimer = metrics.timer(
            label: "database_persistence_operation_duration_seconds",
            dimensions: [("operation", "batch")]
        )

        // Initialize item counters
        self.itemsSavedCounter = metrics.counter(
            label: "database_persistence_items_total",
            dimensions: [("operation", "save")]
        )
        self.itemsFetchedCounter = metrics.counter(
            label: "database_persistence_items_total",
            dimensions: [("operation", "fetch")]
        )
        self.itemsDeletedCounter = metrics.counter(
            label: "database_persistence_items_total",
            dimensions: [("operation", "delete")]
        )
    }

    // MARK: - DataStoreDelegate

    func didSave(itemType: String, count: Int, duration: UInt64) {
        saveCounter.increment()
        saveTimer.recordNanoseconds(duration)
        itemsSavedCounter.increment(by: count)

        // Record per-type counter
        metrics.counter(
            label: "database_persistence_items_by_type_total",
            dimensions: [("operation", "save"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailSave(itemType: String, error: Error, duration: UInt64) {
        saveErrorCounter.increment()
        saveTimer.recordNanoseconds(duration)

        // Record per-type error
        metrics.counter(
            label: "database_persistence_errors_total",
            dimensions: [("operation", "save"), ("item_type", itemType), ("error_type", Self.metricsErrorType(for: error))]
        ).increment()
    }

    func didFetch(itemType: String, count: Int, duration: UInt64) {
        fetchCounter.increment()
        fetchTimer.recordNanoseconds(duration)
        itemsFetchedCounter.increment(by: count)

        // Record per-type counter
        metrics.counter(
            label: "database_persistence_items_by_type_total",
            dimensions: [("operation", "fetch"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailFetch(itemType: String, error: Error, duration: UInt64) {
        fetchErrorCounter.increment()
        fetchTimer.recordNanoseconds(duration)

        // Record per-type error
        metrics.counter(
            label: "database_persistence_errors_total",
            dimensions: [("operation", "fetch"), ("item_type", itemType), ("error_type", Self.metricsErrorType(for: error))]
        ).increment()
    }

    func didDelete(itemType: String, count: Int, duration: UInt64) {
        deleteCounter.increment()
        deleteTimer.recordNanoseconds(duration)
        itemsDeletedCounter.increment(by: count)

        // Record per-type counter
        metrics.counter(
            label: "database_persistence_items_by_type_total",
            dimensions: [("operation", "delete"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailDelete(itemType: String, error: Error, duration: UInt64) {
        deleteErrorCounter.increment()
        deleteTimer.recordNanoseconds(duration)

        // Record per-type error
        metrics.counter(
            label: "database_persistence_errors_total",
            dimensions: [("operation", "delete"), ("item_type", itemType), ("error_type", Self.metricsErrorType(for: error))]
        ).increment()
    }

    func didExecuteBatch(insertCount: Int, deleteCount: Int, duration: UInt64) {
        batchCounter.increment()
        batchTimer.recordNanoseconds(duration)
        itemsSavedCounter.increment(by: insertCount)
        itemsDeletedCounter.increment(by: deleteCount)
    }

    func didFailBatch(error: Error, duration: UInt64) {
        batchErrorCounter.increment()
        batchTimer.recordNanoseconds(duration)

        // Record error type
        metrics.counter(
            label: "database_persistence_errors_total",
            dimensions: [("operation", "batch"), ("error_type", Self.metricsErrorType(for: error))]
        ).increment()
    }

    // MARK: - Helpers

    /// Returns one bounded label so untrusted error types cannot create an
    /// unbounded metrics-cardinality surface. Detailed errors belong in logs.
    static func metricsErrorType(for error: Error) -> String {
        _ = error
        return "operation_failure"
    }
}

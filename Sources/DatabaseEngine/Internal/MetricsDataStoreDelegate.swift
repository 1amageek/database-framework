import Foundation
import Metrics

/// Internal delegate that records data store metrics using swift-metrics
///
/// This delegate is the default implementation used by FDBDataStore.
/// It records operation counts and durations to the configured metrics backend.
///
/// **Metrics Recorded**:
/// - `fdb_datastore_operations_total` (Counter): Total operation count by type and status
/// - `fdb_datastore_operation_duration_seconds` (Timer): Operation duration by type
/// - `fdb_datastore_items_total` (Counter): Total items processed by operation type
///
/// **Labels/Dimensions**:
/// - `operation`: save, fetch, delete, batch
/// - `item_type`: The persistable type name (e.g., "User", "Product")
/// - `status`: success, failure
///
/// **Usage**: Automatically used by FDBDataStore. No user configuration required.
/// Users can configure the metrics backend via `MetricsSystem.bootstrap()`.
final class MetricsDataStoreDelegate: DataStoreDelegate, Sendable {
    // MARK: - Singleton

    /// Shared instance (singleton for efficiency)
    static let shared = MetricsDataStoreDelegate()

    // MARK: - Metrics

    // Operation counters
    private let saveCounter: Counter
    private let fetchCounter: Counter
    private let deleteCounter: Counter
    private let batchCounter: Counter

    // Error counters
    private let saveErrorCounter: Counter
    private let fetchErrorCounter: Counter
    private let deleteErrorCounter: Counter
    private let batchErrorCounter: Counter

    // Timers
    private let saveTimer: Metrics.Timer
    private let fetchTimer: Metrics.Timer
    private let deleteTimer: Metrics.Timer
    private let batchTimer: Metrics.Timer

    // Item counters
    private let itemsSavedCounter: Counter
    private let itemsFetchedCounter: Counter
    private let itemsDeletedCounter: Counter

    // MARK: - Initialization

    private init() {
        // Initialize counters
        self.saveCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "save"), ("status", "success")]
        )
        self.fetchCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "fetch"), ("status", "success")]
        )
        self.deleteCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "delete"), ("status", "success")]
        )
        self.batchCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "batch"), ("status", "success")]
        )

        // Initialize error counters
        self.saveErrorCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "save"), ("status", "failure")]
        )
        self.fetchErrorCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "fetch"), ("status", "failure")]
        )
        self.deleteErrorCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "delete"), ("status", "failure")]
        )
        self.batchErrorCounter = Counter(
            label: "fdb_datastore_operations_total",
            dimensions: [("operation", "batch"), ("status", "failure")]
        )

        // Initialize timers
        self.saveTimer = Metrics.Timer(
            label: "fdb_datastore_operation_duration_seconds",
            dimensions: [("operation", "save")]
        )
        self.fetchTimer = Metrics.Timer(
            label: "fdb_datastore_operation_duration_seconds",
            dimensions: [("operation", "fetch")]
        )
        self.deleteTimer = Metrics.Timer(
            label: "fdb_datastore_operation_duration_seconds",
            dimensions: [("operation", "delete")]
        )
        self.batchTimer = Metrics.Timer(
            label: "fdb_datastore_operation_duration_seconds",
            dimensions: [("operation", "batch")]
        )

        // Initialize item counters
        self.itemsSavedCounter = Counter(
            label: "fdb_datastore_items_total",
            dimensions: [("operation", "save")]
        )
        self.itemsFetchedCounter = Counter(
            label: "fdb_datastore_items_total",
            dimensions: [("operation", "fetch")]
        )
        self.itemsDeletedCounter = Counter(
            label: "fdb_datastore_items_total",
            dimensions: [("operation", "delete")]
        )
    }

    // MARK: - DataStoreDelegate

    func didSave(itemType: String, count: Int, duration: UInt64) {
        saveCounter.increment()
        saveTimer.recordNanoseconds(Int64(duration))
        itemsSavedCounter.increment(by: count)

        // Record per-type counter
        Counter(
            label: "fdb_datastore_items_by_type_total",
            dimensions: [("operation", "save"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailSave(itemType: String, error: Error, duration: UInt64) {
        saveErrorCounter.increment()
        saveTimer.recordNanoseconds(Int64(duration))

        // Record per-type error
        Counter(
            label: "fdb_datastore_errors_total",
            dimensions: [("operation", "save"), ("item_type", itemType), ("error_type", errorType(error))]
        ).increment()
    }

    func didFetch(itemType: String, count: Int, duration: UInt64) {
        fetchCounter.increment()
        fetchTimer.recordNanoseconds(Int64(duration))
        itemsFetchedCounter.increment(by: count)

        // Record per-type counter
        Counter(
            label: "fdb_datastore_items_by_type_total",
            dimensions: [("operation", "fetch"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailFetch(itemType: String, error: Error, duration: UInt64) {
        fetchErrorCounter.increment()
        fetchTimer.recordNanoseconds(Int64(duration))

        // Record per-type error
        Counter(
            label: "fdb_datastore_errors_total",
            dimensions: [("operation", "fetch"), ("item_type", itemType), ("error_type", errorType(error))]
        ).increment()
    }

    func didDelete(itemType: String, count: Int, duration: UInt64) {
        deleteCounter.increment()
        deleteTimer.recordNanoseconds(Int64(duration))
        itemsDeletedCounter.increment(by: count)

        // Record per-type counter
        Counter(
            label: "fdb_datastore_items_by_type_total",
            dimensions: [("operation", "delete"), ("item_type", itemType)]
        ).increment(by: count)
    }

    func didFailDelete(itemType: String, error: Error, duration: UInt64) {
        deleteErrorCounter.increment()
        deleteTimer.recordNanoseconds(Int64(duration))

        // Record per-type error
        Counter(
            label: "fdb_datastore_errors_total",
            dimensions: [("operation", "delete"), ("item_type", itemType), ("error_type", errorType(error))]
        ).increment()
    }

    func didExecuteBatch(insertCount: Int, deleteCount: Int, duration: UInt64) {
        batchCounter.increment()
        batchTimer.recordNanoseconds(Int64(duration))
        itemsSavedCounter.increment(by: insertCount)
        itemsDeletedCounter.increment(by: deleteCount)
    }

    func didFailBatch(error: Error, duration: UInt64) {
        batchErrorCounter.increment()
        batchTimer.recordNanoseconds(Int64(duration))

        // Record error type
        Counter(
            label: "fdb_datastore_errors_total",
            dimensions: [("operation", "batch"), ("error_type", errorType(error))]
        ).increment()
    }

    // MARK: - Helpers

    /// Extract a safe error type string for metrics
    private func errorType(_ error: Error) -> String {
        // Use type name to avoid exposing sensitive error details
        let typeName = String(describing: type(of: error))

        // Sanitize for metrics label (remove special characters)
        let sanitized = typeName.replacingOccurrences(of: "[^a-zA-Z0-9_]", with: "_", options: .regularExpression)

        // Limit length for label cardinality
        return String(sanitized.prefix(50))
    }
}

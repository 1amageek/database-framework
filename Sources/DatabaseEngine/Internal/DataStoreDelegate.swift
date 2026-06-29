/// Internal delegate protocol for data store operation callbacks
///
/// This protocol enables separation of metrics collection from core data store logic.
/// The delegate is called at key operation points to record metrics without
/// polluting the main business logic.
///
/// **Design Decision**: Internal protocol, not exposed to users.
/// Default implementation (MetricsDataStoreDelegate) uses swift-metrics.
protocol DataStoreDelegate: Sendable {
    // MARK: - Save Operations

    /// Called after a successful save operation
    ///
    /// - Parameters:
    ///   - itemType: The type of items saved (e.g., "User", "Product")
    ///   - count: Number of items saved
    ///   - duration: Operation duration in nanoseconds
    func didSave(itemType: String, count: Int, duration: UInt64)

    /// Called after a save operation fails
    ///
    /// - Parameters:
    ///   - itemType: The type of items that failed to save
    ///   - error: The error that occurred
    ///   - duration: Operation duration in nanoseconds
    func didFailSave(itemType: String, error: Error, duration: UInt64)

    // MARK: - Fetch Operations

    /// Called after a successful fetch operation
    ///
    /// - Parameters:
    ///   - itemType: The type of items fetched
    ///   - count: Number of items returned
    ///   - duration: Operation duration in nanoseconds
    func didFetch(itemType: String, count: Int, duration: UInt64)

    /// Called after a fetch operation fails
    ///
    /// - Parameters:
    ///   - itemType: The type of items that failed to fetch
    ///   - error: The error that occurred
    ///   - duration: Operation duration in nanoseconds
    func didFailFetch(itemType: String, error: Error, duration: UInt64)

    // MARK: - Delete Operations

    /// Called after a successful delete operation
    ///
    /// - Parameters:
    ///   - itemType: The type of items deleted
    ///   - count: Number of items deleted
    ///   - duration: Operation duration in nanoseconds
    func didDelete(itemType: String, count: Int, duration: UInt64)

    /// Called after a delete operation fails
    ///
    /// - Parameters:
    ///   - itemType: The type of items that failed to delete
    ///   - error: The error that occurred
    ///   - duration: Operation duration in nanoseconds
    func didFailDelete(itemType: String, error: Error, duration: UInt64)

    // MARK: - Batch Operations

    /// Called after a successful batch operation
    ///
    /// - Parameters:
    ///   - insertCount: Number of items inserted
    ///   - deleteCount: Number of items deleted
    ///   - duration: Operation duration in nanoseconds
    func didExecuteBatch(insertCount: Int, deleteCount: Int, duration: UInt64)

    /// Called after a batch operation fails
    ///
    /// - Parameters:
    ///   - error: The error that occurred
    ///   - duration: Operation duration in nanoseconds
    func didFailBatch(error: Error, duration: UInt64)
}

// MARK: - Default Implementations

extension DataStoreDelegate {
    // Provide empty default implementations so delegates can implement only what they need

    func didSave(itemType: String, count: Int, duration: UInt64) {}
    func didFailSave(itemType: String, error: Error, duration: UInt64) {}
    func didFetch(itemType: String, count: Int, duration: UInt64) {}
    func didFailFetch(itemType: String, error: Error, duration: UInt64) {}
    func didDelete(itemType: String, count: Int, duration: UInt64) {}
    func didFailDelete(itemType: String, error: Error, duration: UInt64) {}
    func didExecuteBatch(insertCount: Int, deleteCount: Int, duration: UInt64) {}
    func didFailBatch(error: Error, duration: UInt64) {}
}

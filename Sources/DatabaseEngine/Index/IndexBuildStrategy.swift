import DatabaseKit
import StorageKit

/// Protocol for custom index build strategies
///
/// Some index types (e.g., HNSW vector indexes) require specialized batch build logic
/// that differs from the standard scan-based approach. This protocol allows index
/// maintainers to provide custom build strategies.
///
/// **When to Use**:
/// - Index requires bulk construction (e.g., HNSW graph building)
/// - Standard item-by-item scanning is inefficient
/// - Need access to all data at once for optimization
///
/// **When NOT to Use**:
/// - Standard VALUE indexes (use default scan-based build)
/// - Aggregation indexes that can be built incrementally
/// - Any index that works efficiently with `scanItem()`
///
/// **Example** (HNSW bulk build in fdb-indexes):
/// ```swift
/// public struct HNSWBuildStrategy<Item: Sendable>: IndexBuildStrategy {
///     private let maintainer: HNSWIndexMaintainer<Item>
///
///     public func buildIndex(context: IndexBuildContext) async throws {
///         // Read count- and byte-bounded pages with context.readItems(...),
///         // then persist physical index state.
///         // physical state with context.withIndexTransaction(...).
///     }
/// }
/// ```
public protocol IndexBuildStrategy<Item>: Sendable {
    associatedtype Item: PersistedEntityValue

    /// Build the index using custom strategy
    ///
    /// This method is called by OnlineIndexer when the associated IndexMaintainer
    /// provides a custom build strategy. Implementations should:
    /// 1. Load entity data through count- and byte-bounded `readItems` pages
    /// 2. Build index data structures efficiently
    /// 3. Write index data through `withIndexTransaction`
    ///
    /// **Important**:
    /// - Use multiple transactions if needed (avoid timeouts)
    /// - Be mindful of transaction size limits
    /// - Consider batch processing for large datasets
    ///
    /// - Parameter context:
    ///   The entity-read and index-write capabilities for this build.
    /// - Throws: Error if build fails
    ///
    /// **Note**: Use `DataAccess.serialize()`, `DataAccess.deserialize()`, and `DataAccess.evaluate()` to work with items
    func buildIndex(context: IndexBuildContext) async throws
}

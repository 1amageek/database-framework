import FoundationDB
import Core

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
///     public func buildIndex(
///         container: FDBContainer,
///         itemSubspace: Subspace,
///         indexSubspace: Subspace,
///         itemType: String,
///         index: Index,
///         dataAccess: any DataAccess<Item>
///     ) async throws {
///         // 1. Load all vectors
///         let allVectors = try await loadAllVectors(...)
///
///         // 2. Save to flat index
///         try await saveVectorsToFlatIndex(allVectors)
///
///         // 3. Build HNSW graph efficiently
///         try await buildHNSWGraph(allVectors)
///     }
/// }
/// ```
public protocol IndexBuildStrategy<Item>: Sendable {
    associatedtype Item: Persistable

    /// Build the index using custom strategy
    ///
    /// This method is called by OnlineIndexer when the associated IndexMaintainer
    /// provides a custom build strategy. Implementations should:
    /// 1. Load all necessary data from itemSubspace
    /// 2. Build index data structures efficiently
    /// 3. Write index data to indexSubspace
    ///
    /// **Important**:
    /// - Use multiple transactions if needed (avoid timeouts)
    /// - Be mindful of transaction size limits
    /// - Consider batch processing for large datasets
    ///
    /// - Parameters:
    ///   - container: FDBContainer for transaction execution
    ///   - itemSubspace: Subspace where items are stored ([R]/)
    ///   - indexSubspace: Subspace where index data is stored ([I]/)
    ///   - itemType: Type name of items to index (e.g., "User", "Product")
    ///   - index: Index definition (name, kind, rootExpression)
    /// - Throws: Error if build fails
    ///
    /// **Note**: Use `DataAccess.serialize()`, `DataAccess.deserialize()`, and `DataAccess.evaluate()` to work with items
    func buildIndex(
        container: FDBContainer,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        index: Index
    ) async throws
}

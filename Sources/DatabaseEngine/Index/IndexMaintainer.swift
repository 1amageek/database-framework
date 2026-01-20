import FoundationDB
import Core

/// Protocol for maintaining an index
///
/// IndexMaintainer provides the interface for updating and building indexes.
/// Concrete implementations are provided by upper layers (fdb-record-layer, etc.).
///
/// **Responsibilities**:
/// - Update index entries when items change
/// - Build index entries during batch indexing
/// - Use DataAccess to extract field values
///
/// **Design**:
/// - Protocol definition only in FDBIndexing
/// - Concrete implementations in upper layers (fdb-record-layer, fdb-document-layer, etc.)
/// - Each data model layer provides its own implementations
///
/// **Usage Example** (fdb-record-layer):
/// ```swift
/// struct ValueIndexMaintainer<Item: Persistable>: IndexMaintainer {
///     func updateIndex(
///         oldItem: Item?,
///         newItem: Item?,
///         transaction: any TransactionProtocol
///     ) async throws {
///         // Remove old index entries
///         if let old = oldItem {
///             let oldValues = try DataAccess.evaluate(item: old, expression: index.rootExpression)
///             // Remove from index...
///         }
///
///         // Add new index entries
///         if let new = newItem {
///             let newValues = try DataAccess.evaluate(item: new, expression: index.rootExpression)
///             // Add to index...
///         }
///     }
///
///     func scanItem(
///         _ item: Item,
///         id: Tuple,
///         transaction: any TransactionProtocol
///     ) async throws {
///         // Build index entries for this item
///         let values = try DataAccess.evaluate(item: item, expression: index.rootExpression)
///         // Add to index...
///     }
/// }
/// ```
public protocol IndexMaintainer<Item>: Sendable {
    associatedtype Item: Persistable

    /// Update index entries when an item changes
    ///
    /// This method is called when an item is inserted, updated, or deleted.
    /// The implementation should:
    /// 1. Remove old index entries (if oldItem is not nil)
    /// 2. Add new index entries (if newItem is not nil)
    ///
    /// - Parameters:
    ///   - oldItem: The old item (nil if inserting)
    ///   - newItem: The new item (nil if deleting)
    ///   - transaction: The transaction to use
    /// - Throws: Error if index update fails
    ///
    /// **Note**: Use `DataAccess.extractField()` and `DataAccess.evaluate()` to access item fields
    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws

    /// Scan and build index entries for an item
    ///
    /// This method is called during batch index building (OnlineIndexer).
    /// The implementation should build all index entries for the given item.
    ///
    /// - Parameters:
    ///   - item: The item to scan
    ///   - id: The item's unique identifier
    ///   - transaction: The transaction to use
    /// - Throws: Error if index building fails
    ///
    /// **Note**: Use `DataAccess.extractField()` and `DataAccess.evaluate()` to access item fields
    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws

    /// Optional custom build strategy for this index
    ///
    /// Some index types (e.g., HNSW) require specialized bulk build logic that
    /// differs from the standard scan-based approach. If provided, OnlineIndexer
    /// will use this strategy instead of calling scanItem() for each item.
    ///
    /// **Default**: nil (use standard scan-based build via scanItem())
    ///
    /// **When to Provide**:
    /// - Index requires bulk construction (e.g., HNSW graph building)
    /// - Standard item-by-item scanning is inefficient
    /// - Need access to all data at once for optimization
    ///
    /// **Example** (HNSW):
    /// ```swift
    /// public var customBuildStrategy: (any IndexBuildStrategy<Item>)? {
    ///     return HNSWBuildStrategy(maintainer: self)
    /// }
    /// ```
    var customBuildStrategy: (any IndexBuildStrategy<Item>)? { get }

    /// Compute expected index keys for an item
    ///
    /// This method computes the index keys that should exist for a given item
    /// WITHOUT actually writing them. Used by OnlineIndexScrubber for verification.
    ///
    /// - Parameters:
    ///   - item: The item to compute keys for
    ///   - id: The item's unique identifier
    /// - Returns: Array of index keys that should exist for this item
    /// - Throws: Error if computation fails
    ///
    /// **Default**: Empty array (verification skipped for this maintainer)
    ///
    /// **Implementation Notes**:
    /// - For VALUE indexes: Return key like [indexSubspace]/[indexName]/[value]/[id]
    /// - For aggregation indexes: Return key(s) for aggregation buckets
    /// - For complex indexes: Return all keys that should reference this item
    func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes]

    /// Compute expected index keys for an item with transaction access
    ///
    /// This method is similar to `computeIndexKeys(for:id:)` but provides
    /// transaction access for indexes that need to load related data.
    ///
    /// - Parameters:
    ///   - item: The item to compute keys for
    ///   - id: The item's unique identifier
    ///   - transaction: The transaction for loading related data
    /// - Returns: Array of index keys that should exist for this item
    /// - Throws: Error if computation fails
    ///
    /// **Default**: Calls the non-transaction version
    ///
    /// **When to Override**:
    /// - RelationshipIndex: Needs to load related items to compute index keys
    /// - Cross-type indexes: Require data from multiple types
    func computeIndexKeys(
        for item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [FDB.Bytes]
}

// MARK: - Default Implementations

extension IndexMaintainer {
    /// Default: no custom build strategy (use standard scan-based build)
    public var customBuildStrategy: (any IndexBuildStrategy<Item>)? {
        return nil
    }

    /// Default: empty array (verification skipped for this maintainer)
    ///
    /// Concrete implementations should override this to enable scrubber verification.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return []
    }

    /// Default: calls the non-transaction version
    ///
    /// Override this for indexes that need transaction access (e.g., RelationshipIndex).
    public func computeIndexKeys(
        for item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [FDB.Bytes] {
        return try await computeIndexKeys(for: item, id: id)
    }
}

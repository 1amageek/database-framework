// IndexQueryContext.swift
// DatabaseEngine - Context for index-based queries

import Foundation
import FoundationDB
import Core

/// Context for executing index-based queries
///
/// This struct provides access to low-level storage operations needed by
/// index-specific query builders. It wraps FDBContext and the underlying
/// storage to expose a public interface for query execution.
///
/// **Usage** (from index modules):
/// ```swift
/// // In FullTextIndex module
/// extension FDBContext {
///     public func search<T: Persistable>(_ type: T.Type) -> FullTextEntryPoint<T> {
///         FullTextEntryPoint(queryContext: indexQueryContext, type: type)
///     }
/// }
///
/// // Query builder uses IndexQueryContext
/// let results = try await queryContext.executeFullTextSearch(
///     type: Article.self,
///     indexName: "idx_content",
///     terms: ["swift"],
///     matchMode: .all
/// )
/// ```
public struct IndexQueryContext: Sendable {

    /// The FDBContext this query context wraps
    public let context: FDBContext

    /// Create an index query context
    public init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Item Access

    /// Fetch items by their IDs
    ///
    /// - Parameters:
    ///   - ids: Array of item IDs (as Tuples)
    ///   - type: The item type
    /// - Returns: Array of fetched items (in same order as IDs, skipping not found)
    public func fetchItems<T: Persistable>(
        ids: [Tuple],
        type: T.Type
    ) async throws -> [T] {
        var results: [T] = []
        for id in ids {
            // Extract the first element as the primary ID
            if let idElement = id[0] {
                if let item = try await context.model(for: idElement, as: type) {
                    results.append(item)
                }
            }
        }
        return results
    }

    /// Fetch a single item by ID
    ///
    /// - Parameters:
    ///   - id: The item ID (as Tuple)
    ///   - type: The item type
    /// - Returns: The item if found
    public func fetchItem<T: Persistable>(
        id: Tuple,
        type: T.Type
    ) async throws -> T? {
        guard let idElement = id[0] else { return nil }
        return try await context.model(for: idElement, as: type)
    }

    /// Get the item subspace for a type (for transaction-scoped operations)
    ///
    /// This returns the subspace where items of the given type are stored.
    /// Use this when you need to fetch items within an existing transaction
    /// to maintain snapshot consistency.
    ///
    /// - Parameter type: The persistable type
    /// - Returns: Subspace for items of this type
    public func itemSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let store = try await context.container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw IndexQueryError.unsupportedStore
        }
        return fdbStore.itemSubspace
    }

    /// Fetch item by ID within a transaction
    ///
    /// Use this method when you need to fetch an item within an existing
    /// transaction to maintain snapshot consistency.
    ///
    /// - Parameters:
    ///   - id: The item ID (as Tuple)
    ///   - type: The item type
    ///   - transaction: The transaction to use
    /// - Returns: The item if found
    public func fetchItem<T: Persistable>(
        id: Tuple,
        type: T.Type,
        transaction: any TransactionProtocol
    ) async throws -> T? {
        let itemSubspace = try await itemSubspace(for: type)
        let key = itemSubspace.subspace(T.persistableType).pack(id)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try DataAccess.deserialize(data)
    }

    /// Batch fetch items by their IDs using optimized BatchFetcher
    ///
    /// This method is more efficient than `fetchItems` for large result sets
    /// because it batches the fetches and processes them together within
    /// a single transaction.
    ///
    /// - Parameters:
    ///   - ids: Array of item IDs (as Tuples)
    ///   - type: The item type
    ///   - configuration: Batch fetch configuration
    /// - Returns: Array of fetched items (order may not match input order)
    public func batchFetchItems<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        configuration: BatchFetchConfiguration = .default
    ) async throws -> [T] {
        guard !ids.isEmpty else { return [] }

        let store = try await context.container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            // Fall back to sequential fetch for non-FDB stores
            return try await fetchItems(ids: ids, type: type)
        }

        let fetcher = BatchFetcher<T>(
            itemSubspace: fdbStore.itemSubspace,
            itemType: T.persistableType,
            configuration: configuration
        )

        return try await context.container.database.withTransaction { transaction in
            try await fetcher.fetch(primaryKeys: ids, transaction: transaction)
        }
    }

    // MARK: - Index Search Operations

    /// Execute a full-text search
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - indexName: Name of the full-text index
    ///   - terms: Search terms
    ///   - matchMode: How to match terms (all/any)
    ///   - limit: Maximum results (nil for unlimited)
    /// - Returns: Array of matching items
    public func executeFullTextSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?
    ) async throws -> [T] {
        let searcher = FullTextIndexSearcher()
        let query = FullTextIndexQuery(terms: terms, matchMode: matchMode, limit: limit)

        // Get subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(indexName)

        let store = try await context.container.store(for: type)
        let reader = createStorageReader(store: store)

        let entries = try await searcher.search(query: query, in: indexSubspace, using: reader)
        return try await fetchItems(ids: entries.map { $0.itemID }, type: type)
    }

    /// Execute a vector similarity search
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - indexName: Name of the vector index
    ///   - queryVector: The query vector
    ///   - k: Number of nearest neighbors
    ///   - dimensions: Vector dimensions
    ///   - metric: Distance metric
    /// - Returns: Array of (item, distance) tuples
    public func executeVectorSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        queryVector: [Float],
        k: Int,
        dimensions: Int,
        metric: VectorDistanceMetric
    ) async throws -> [(item: T, distance: Double)] {
        let searcher = VectorIndexSearcher(dimensions: dimensions, metric: metric)
        let query = VectorIndexQuery(queryVector: queryVector, k: k)

        // Get subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(indexName)

        let store = try await context.container.store(for: type)
        let reader = createStorageReader(store: store)

        let entries = try await searcher.search(query: query, in: indexSubspace, using: reader)

        var results: [(item: T, distance: Double)] = []
        for entry in entries {
            if let item = try await fetchItem(id: entry.itemID, type: type) {
                results.append((item: item, distance: entry.score ?? Double.infinity))
            }
        }
        return results
    }

    /// Execute a spatial search
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - indexName: Name of the spatial index
    ///   - constraint: Spatial constraint (bounds, radius, etc.)
    ///   - limit: Maximum results (nil for unlimited)
    /// - Returns: Array of matching items
    public func executeSpatialSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        constraint: SpatialConstraint,
        limit: Int?
    ) async throws -> [T] {
        let searcher = SpatialIndexSearcher()
        let query = SpatialIndexQuery(constraint: constraint, limit: limit)

        // Get subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(indexName)

        let store = try await context.container.store(for: type)
        let reader = createStorageReader(store: store)

        let entries = try await searcher.search(query: query, in: indexSubspace, using: reader)
        return try await fetchItems(ids: entries.map { $0.itemID }, type: type)
    }

    // MARK: - Schema Access

    /// Get the schema for accessing index definitions
    public var schema: Schema {
        context.container.schema
    }

    /// Find index descriptors for a type
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: Array of index descriptors
    public func indexDescriptors<T: Persistable>(for type: T.Type) -> [IndexDescriptor] {
        schema.indexDescriptors(for: T.persistableType)
    }

    /// Find an index by kind identifier
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - kindIdentifier: The index kind identifier (e.g., "rank", "count", "sum")
    /// - Returns: Array of matching IndexDescriptors
    public func findIndexes<T: Persistable>(
        for type: T.Type,
        kindIdentifier: String
    ) -> [IndexDescriptor] {
        let descriptors = indexDescriptors(for: type)
        return descriptors.filter { descriptor in
            let kindType = Swift.type(of: descriptor.kind)
            return kindType.identifier == kindIdentifier
        }
    }

    /// Find an index by name
    ///
    /// - Parameter name: The index name
    /// - Returns: The matching IndexDescriptor if found
    public func findIndex(named name: String) -> IndexDescriptor? {
        schema.indexDescriptor(named: name)
    }

    // MARK: - Database Access

    /// Execute a closure within a transaction
    ///
    /// - Parameters:
    ///   - body: Closure that takes a transaction
    /// - Returns: Result of the closure
    public func withTransaction<R: Sendable>(
        _ body: @Sendable @escaping (any TransactionProtocol) async throws -> R
    ) async throws -> R {
        return try await context.container.database.withTransaction(body)
    }

    /// Get the index subspace for a type
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: The index subspace
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let store = try await context.container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw IndexQueryContextError.unsupportedStoreType
        }
        return fdbStore.indexSubspace
    }

    // MARK: - Private Helpers

    /// Create a StorageReader from a DataStore
    private func createStorageReader(store: any DataStore) -> StorageReader {
        FDBStorageReaderAdapter(store: store)
    }
}

/// Errors for IndexQueryContext operations
public enum IndexQueryContextError: Error, CustomStringConvertible {
    case unsupportedStoreType
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .unsupportedStoreType:
            return "Store type is not FDBDataStore"
        case .indexNotFound(let name):
            return "Index not found: \(name)"
        }
    }
}

// MARK: - FDB Storage Reader Adapter

/// Adapter that wraps FDBDataStore to provide StorageReader interface
///
/// **Note**: Index subspace is NOT provided here. Use `IndexQueryContext.indexSubspace(for:)`
/// to get subspace via DirectoryLayer based on Persistable type.
internal struct FDBStorageReaderAdapter: StorageReader {

    private let store: any DataStore

    init(store: any DataStore) {
        self.store = store
    }

    func fetchItem<T: Persistable & Codable>(id: any TupleElement, type: T.Type) async throws -> T? {
        return try await store.fetch(type, id: id)
    }

    func scanItems<T: Persistable & Codable>(type: T.Type) -> AsyncThrowingStream<T, Error> {
        return AsyncThrowingStream { continuation in
            Task {
                do {
                    let items = try await store.fetchAll(type)
                    for item in items {
                        continuation.yield(item)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    func scanRange(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) -> AsyncThrowingStream<(key: [UInt8], value: [UInt8]), Error> {
        // Delegate to FDBDataStore's raw range scan
        if let fdbStore = store as? FDBDataStore {
            return fdbStore.scanRangeRaw(
                subspace: subspace,
                start: start,
                end: end,
                startInclusive: startInclusive,
                endInclusive: endInclusive,
                reverse: reverse
            )
        }
        // Return empty stream for non-FDB stores
        return AsyncThrowingStream { continuation in
            continuation.finish()
        }
    }

    func getValue(key: [UInt8]) async throws -> [UInt8]? {
        if let fdbStore = store as? FDBDataStore {
            return try await fdbStore.getValueRaw(key: key)
        }
        return nil
    }
}

// MARK: - FDBDataStore Extensions for StorageReader Support

extension FDBDataStore {

    /// Scan a range within a subspace (raw key-value access)
    ///
    /// - Parameters:
    ///   - subspace: The subspace to scan
    ///   - start: Start tuple (nil = beginning of subspace)
    ///   - end: End tuple (nil = end of subspace)
    ///   - startInclusive: Whether start is inclusive
    ///   - endInclusive: Whether end is inclusive
    ///   - reverse: Whether to scan in reverse order
    ///   - limit: Maximum number of entries to return (nil = unlimited)
    ///   - streamingMode: FDB streaming mode (nil = auto-select based on limit)
    func scanRangeRaw(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool,
        limit: Int? = nil,
        streamingMode: FDB.StreamingMode? = nil
    ) -> AsyncThrowingStream<(key: [UInt8], value: [UInt8]), Error> {
        // Select optimal StreamingMode if not specified
        let mode = streamingMode ?? FDB.StreamingMode.forQuery(limit: limit)
        let effectiveLimit = limit ?? 0  // 0 = unlimited in FDB

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    try await database.withTransaction { transaction in
                        // Build range
                        let beginKey: [UInt8]
                        let endKey: [UInt8]

                        if let startTuple = start {
                            let packed = subspace.pack(startTuple)
                            if startInclusive {
                                beginKey = packed
                            } else {
                                // Next key after packed
                                beginKey = self.incrementKey(packed)
                            }
                        } else {
                            beginKey = subspace.prefix
                        }

                        if let endTuple = end {
                            let packed = subspace.pack(endTuple)
                            if endInclusive {
                                // Include this key by going to next
                                endKey = self.incrementKey(packed)
                            } else {
                                endKey = packed
                            }
                        } else {
                            // End of subspace
                            endKey = self.incrementKey(subspace.prefix)
                        }

                        // Apply limit pushdown and streaming mode optimization
                        // Use appropriate key selectors for forward vs reverse iteration
                        let fromSelector: FDB.KeySelector
                        let toSelector: FDB.KeySelector

                        if reverse {
                            // For reverse iteration, use lastLessOrEqual for efficient positioning
                            // Start from end (exclusive) and go backwards to begin (inclusive)
                            fromSelector = FDB.KeySelector.lastLessThan(endKey)
                            toSelector = FDB.KeySelector.firstGreaterOrEqual(beginKey)
                        } else {
                            // Forward iteration uses standard firstGreaterOrEqual
                            fromSelector = FDB.KeySelector.firstGreaterOrEqual(beginKey)
                            toSelector = FDB.KeySelector.firstGreaterOrEqual(endKey)
                        }

                        let sequence = transaction.getRange(
                            from: fromSelector,
                            to: toSelector,
                            limit: effectiveLimit,
                            reverse: reverse,
                            snapshot: true,
                            streamingMode: mode
                        )
                        for try await (key, value) in sequence {
                            continuation.yield((key: key, value: value))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Get a single value by key (raw access)
    func getValueRaw(key: [UInt8]) async throws -> [UInt8]? {
        return try await database.withTransaction { transaction in
            return try await transaction.getValue(for: key, snapshot: true)
        }
    }

    /// Increment the last byte of a key (for range end)
    private func incrementKey(_ key: [UInt8]) -> [UInt8] {
        var result = key
        if result.isEmpty {
            result.append(0x00)
        } else {
            // Find rightmost byte that can be incremented
            var i = result.count - 1
            while i >= 0 {
                if result[i] < 0xFF {
                    result[i] += 1
                    return result
                }
                i -= 1
            }
            // All bytes are 0xFF, append 0x00
            result.append(0x00)
        }
        return result
    }
}

// MARK: - IndexQueryError

/// Errors that can occur during index query operations
public enum IndexQueryError: Error, CustomStringConvertible {
    /// The data store type is not supported for this operation
    case unsupportedStore

    public var description: String {
        switch self {
        case .unsupportedStore:
            return "Unsupported data store type for index query operation"
        }
    }
}

// MARK: - Fusion Support Methods

extension IndexQueryContext {

    // MARK: - Item Fetching by String IDs

    /// Fetch items by string IDs (for Fusion)
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - ids: Array of ID strings
    /// - Returns: Array of items
    public func fetchItemsByStringIds<T: Persistable>(
        type: T.Type,
        ids: [String]
    ) async throws -> [T] {
        // Try to fetch using the ID type
        var results: [T] = []
        for idString in ids {
            // Try String ID directly
            if let item = try await context.model(for: idString, as: type) {
                results.append(item)
                continue
            }
            // Try Int64 ID
            if let intId = Int64(idString), let item = try await context.model(for: intId, as: type) {
                results.append(item)
                continue
            }
            // Try Int ID
            if let intId = Int(idString), let item = try await context.model(for: intId, as: type) {
                results.append(item)
            }
        }
        return results
    }

    // MARK: - Scalar Index Search

    /// Execute a scalar index equality search (for Fusion Filter)
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - indexName: Name of the scalar index
    ///   - fieldName: Field name being searched
    ///   - value: Value to match
    /// - Returns: Array of matching items
    public func executeScalarIndexSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        fieldName: String,
        value: any Sendable & Hashable
    ) async throws -> [T] {
        let typeSubspace = try await indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await context.container.database.withTransaction { transaction in
            // Build the key for this value
            let valueSubspace: Subspace
            if let stringValue = value as? String {
                valueSubspace = indexSubspace.subspace(stringValue)
            } else if let intValue = value as? Int {
                valueSubspace = indexSubspace.subspace(Int64(intValue))
            } else if let int64Value = value as? Int64 {
                valueSubspace = indexSubspace.subspace(int64Value)
            } else if let boolValue = value as? Bool {
                valueSubspace = indexSubspace.subspace(boolValue ? 1 : 0)
            } else {
                // Fallback: use string representation
                valueSubspace = indexSubspace.subspace("\(value)")
            }

            // Scan for all items with this value
            var itemIds: [Tuple] = []
            let range = transaction.getRange(
                from: FDB.KeySelector.firstGreaterOrEqual(valueSubspace.prefix),
                to: FDB.KeySelector.firstGreaterOrEqual(incrementKey(valueSubspace.prefix)),
                snapshot: true
            )

            for try await (key, _) in range {
                // Extract primary key from index key
                if let unpacked = try? valueSubspace.unpack(key), !unpacked.isEmpty {
                    itemIds.append(Tuple(unpacked))
                }
            }

            return try await self.fetchItems(ids: itemIds, type: type)
        }
    }

    /// Execute a scalar index range search (for Fusion Filter)
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - indexName: Name of the scalar index
    ///   - fieldName: Field name being searched
    ///   - min: Minimum value (nil for no lower bound)
    ///   - max: Maximum value (nil for no upper bound)
    ///   - minInclusive: Whether min is inclusive
    ///   - maxInclusive: Whether max is inclusive
    /// - Returns: Array of matching items
    public func executeScalarRangeSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        fieldName: String,
        min: (any Sendable)?,
        max: (any Sendable)?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) async throws -> [T] {
        let typeSubspace = try await indexSubspace(for: type)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await context.container.database.withTransaction { transaction in
            // Build range keys
            let beginKey: [UInt8]
            let endKey: [UInt8]

            if let minValue = min {
                let minSubspace = self.subspaceForValue(indexSubspace, value: minValue)
                if minInclusive {
                    beginKey = minSubspace.prefix
                } else {
                    beginKey = self.incrementKey(minSubspace.prefix)
                }
            } else {
                beginKey = indexSubspace.prefix
            }

            if let maxValue = max {
                let maxSubspace = self.subspaceForValue(indexSubspace, value: maxValue)
                if maxInclusive {
                    endKey = self.incrementKey(maxSubspace.prefix)
                } else {
                    endKey = maxSubspace.prefix
                }
            } else {
                endKey = self.incrementKey(indexSubspace.prefix)
            }

            // Scan range
            var itemIds: [Tuple] = []
            let range = transaction.getRange(
                from: FDB.KeySelector.firstGreaterOrEqual(beginKey),
                to: FDB.KeySelector.firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in range {
                // Extract primary key from index key
                if let unpacked = try? indexSubspace.unpack(key), unpacked.count >= 2 {
                    // Skip the value element (first), take the rest as primary key
                    var pkElements: [any TupleElement] = []
                    for i in 1..<unpacked.count {
                        if let element = unpacked[i] {
                            pkElements.append(element)
                        }
                    }
                    if !pkElements.isEmpty {
                        itemIds.append(Tuple(pkElements))
                    }
                }
            }

            return try await self.fetchItems(ids: itemIds, type: type)
        }
    }

    // MARK: - Fetch All Items

    /// Fetch all items of a type (expensive, use with caution)
    ///
    /// - Parameter type: The persistable type
    /// - Returns: Array of all items
    public func fetchAllItems<T: Persistable>(type: T.Type) async throws -> [T] {
        let store = try await context.container.store(for: type)
        return try await store.fetchAll(type)
    }

    // MARK: - Private Helpers

    private func subspaceForValue(_ base: Subspace, value: any Sendable) -> Subspace {
        if let stringValue = value as? String {
            return base.subspace(stringValue)
        } else if let intValue = value as? Int {
            return base.subspace(Int64(intValue))
        } else if let int64Value = value as? Int64 {
            return base.subspace(int64Value)
        } else if let doubleValue = value as? Double {
            return base.subspace(doubleValue)
        } else if let floatValue = value as? Float {
            return base.subspace(Double(floatValue))
        } else {
            return base.subspace("\(value)")
        }
    }

    private func incrementKey(_ key: [UInt8]) -> [UInt8] {
        var result = key
        if result.isEmpty {
            result.append(0x00)
        } else {
            var i = result.count - 1
            while i >= 0 {
                if result[i] < 0xFF {
                    result[i] += 1
                    return result
                }
                i -= 1
            }
            result.append(0x00)
        }
        return result
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Get an index query context for executing index-based queries
    ///
    /// This is used by index module extensions to access storage.
    ///
    /// **Usage**:
    /// ```swift
    /// // In FullTextIndex module
    /// let results = try await context.indexQueryContext.executeFullTextSearch(
    ///     type: Article.self,
    ///     indexName: "idx_content",
    ///     terms: ["swift"],
    ///     matchMode: .all,
    ///     limit: nil
    /// )
    /// ```
    public var indexQueryContext: IndexQueryContext {
        IndexQueryContext(context: self)
    }
}

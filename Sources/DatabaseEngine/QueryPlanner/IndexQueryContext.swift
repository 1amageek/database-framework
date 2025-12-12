// IndexQueryContext.swift
// DatabaseEngine - Context for index-based queries

import Foundation
import FoundationDB
import Core

/// Context for executing index-based queries
///
/// This struct provides low-level storage access for index-specific query builders.
/// Each index module is responsible for its own search logic.
///
/// **Design Principle**:
/// - IndexQueryContext provides storage access only
/// - Each FusionQuery implementation reads its index structure directly
/// - Unified pattern across all index types
///
/// **Usage** (from index modules):
/// ```swift
/// // In VectorIndex/Fusion/Similar.swift
/// func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
///     let indexSubspace = try await queryContext.indexSubspace(for: T.self)
///         .subspace(indexName)
///     let reader = try await queryContext.storageReader(for: T.self)
///     // Read index directly...
///     return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
/// }
/// ```
public struct IndexQueryContext: Sendable {

    /// The FDBContext this query context wraps
    public let context: FDBContext

    /// Create an index query context
    public init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Storage Access

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

    /// Get a StorageReader for a type
    ///
    /// Use this to access index data via IndexSearcher classes.
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: A StorageReader for index access
    public func storageReader<T: Persistable>(for type: T.Type) async throws -> StorageReader {
        let store = try await context.container.store(for: type)
        return FDBStorageReaderAdapter(store: store)
    }

    /// Get the item subspace for a type (for transaction-scoped operations)
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

    /// Execute a closure within a transaction
    ///
    /// - Parameter body: Closure that takes a transaction
    /// - Returns: Result of the closure
    public func withTransaction<R: Sendable>(
        _ body: @Sendable @escaping (any TransactionProtocol) async throws -> R
    ) async throws -> R {
        return try await context.container.database.withTransaction(body)
    }

    // MARK: - Item Fetching

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

    /// Fetch item by ID within a transaction
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
        let item: T = try DataAccess.deserialize(data)
        // Security: Evaluate GET for the retrieved item
        try context.container.securityDelegate?.evaluateGet(item)
        return item
    }

    /// Fetch items by string IDs
    ///
    /// - Parameters:
    ///   - type: The persistable type
    ///   - ids: Array of ID strings
    /// - Returns: Array of items
    public func fetchItemsByStringIds<T: Persistable>(
        type: T.Type,
        ids: [String]
    ) async throws -> [T] {
        var results: [T] = []
        for idString in ids {
            if let item = try await context.model(for: idString, as: type) {
                results.append(item)
                continue
            }
            if let intId = Int64(idString), let item = try await context.model(for: intId, as: type) {
                results.append(item)
                continue
            }
            if let intId = Int(idString), let item = try await context.model(for: intId, as: type) {
                results.append(item)
            }
        }
        return results
    }

    /// Fetch all items of a type (expensive, use with caution)
    ///
    /// - Parameter type: The persistable type
    /// - Returns: Array of all items
    public func fetchAllItems<T: Persistable>(type: T.Type) async throws -> [T] {
        let store = try await context.container.store(for: type)
        return try await store.fetchAll(type)
    }

    /// Batch fetch items by their IDs using optimized BatchFetcher
    ///
    /// - Parameters:
    ///   - ids: Array of item IDs (as Tuples)
    ///   - type: The item type
    ///   - configuration: Batch fetch configuration
    /// - Returns: Array of fetched items
    public func batchFetchItems<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        configuration: BatchFetchConfiguration = .default
    ) async throws -> [T] {
        guard !ids.isEmpty else { return [] }

        // Security: Evaluate LIST before fetching
        try context.container.securityDelegate?.evaluateList(
            type: type,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )

        let store = try await context.container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            return try await fetchItems(ids: ids, type: type)
        }

        let fetcher = BatchFetcher<T>(
            itemSubspace: fdbStore.itemSubspace,
            itemType: T.persistableType,
            configuration: configuration
        )

        let items = try await context.container.database.withTransaction { transaction in
            try await fetcher.fetch(primaryKeys: ids, transaction: transaction)
        }

        // Security: Evaluate GET for each fetched item
        for item in items {
            try context.container.securityDelegate?.evaluateGet(item)
        }

        return items
    }

    // MARK: - Schema Access

    /// Get the schema for accessing index definitions
    public var schema: Schema {
        context.container.schema
    }

    /// Find index descriptors for a type
    public func indexDescriptors<T: Persistable>(for type: T.Type) -> [IndexDescriptor] {
        schema.indexDescriptors(for: T.persistableType)
    }

    /// Find an index by kind identifier
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
    public func findIndex(named name: String) -> IndexDescriptor? {
        schema.indexDescriptor(named: name)
    }
}

// MARK: - Errors

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

/// Errors that can occur during index query operations
public enum IndexQueryError: Error, CustomStringConvertible {
    case unsupportedStore

    public var description: String {
        switch self {
        case .unsupportedStore:
            return "Unsupported data store type for index query operation"
        }
    }
}

// MARK: - FDB Storage Reader Adapter

/// Adapter that wraps FDBDataStore to provide StorageReader interface
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

    func scanSubspace(_ subspace: Subspace) -> AsyncThrowingStream<(key: [UInt8], value: [UInt8]), Error> {
        return scanRange(
            subspace: subspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: false
        )
    }
}

// MARK: - FDBDataStore Extensions

extension FDBDataStore {

    /// Scan a range within a subspace (raw key-value access)
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
        let mode = streamingMode ?? FDB.StreamingMode.forQuery(limit: limit)
        let effectiveLimit = limit ?? 0

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    try await database.withTransaction { transaction in
                        let beginKey: [UInt8]
                        let endKey: [UInt8]

                        if let startTuple = start {
                            let packed = subspace.pack(startTuple)
                            if startInclusive {
                                beginKey = packed
                            } else {
                                beginKey = self.incrementKey(packed)
                            }
                        } else {
                            beginKey = subspace.prefix
                        }

                        if let endTuple = end {
                            let packed = subspace.pack(endTuple)
                            if endInclusive {
                                endKey = self.incrementKey(packed)
                            } else {
                                endKey = packed
                            }
                        } else {
                            endKey = self.incrementKey(subspace.prefix)
                        }

                        let fromSelector: FDB.KeySelector
                        let toSelector: FDB.KeySelector

                        if reverse {
                            fromSelector = FDB.KeySelector.lastLessThan(endKey)
                            toSelector = FDB.KeySelector.firstGreaterOrEqual(beginKey)
                        } else {
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
    public var indexQueryContext: IndexQueryContext {
        IndexQueryContext(context: self)
    }
}

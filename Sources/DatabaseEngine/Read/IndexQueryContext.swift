// IndexQueryContext.swift
// DatabaseEngine context for index-based reads

import StorageKit
import DatabaseKit
import DatabaseTypes

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
/// func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
///     let indexSubspace = try await queryContext.indexSubspace(for: T.self)
///         .subspace(indexName)
///     let reader = try await queryContext.storageReader(for: T.self)
///     // Read index directly...
///     return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
/// }
/// ```
///
/// **Partition Support**:
/// For types with dynamic directories, use `withPartition()` to create a partition-scoped context:
/// ```swift
/// let partitionedContext = queryContext.withPartition(
///     #field(\Order.tenantID),
///     equals: "tenant_123"
/// )
/// let indexSubspace = try await partitionedContext.indexSubspace(for: Order.self)
/// ```
public struct IndexQueryContext: Sendable {

    /// The DatabaseContext this query context wraps
    public let context: DatabaseContext

    /// Canonical partition values for dynamic directories.
    private let partitions: FieldObject?

    /// Create an index query context
    public init(context: DatabaseContext) {
        self.context = context
        self.partitions = nil
    }

    /// Create an index query context with partition binding
    private init(context: DatabaseContext, partitions: FieldObject?) {
        self.context = context
        self.partitions = partitions
    }

    // MARK: - Partition Support

    /// Create a partition-scoped index query context
    ///
    /// For types with dynamic directories in `#Directory`,
    /// this creates a context scoped to the specified partition.
    ///
    /// **Usage**:
    /// ```swift
    /// let partitionedContext = queryContext.withPartition(
    ///     #field(\Order.tenantID),
    ///     equals: "tenant_123"
    /// )
    /// let indexSubspace = try await partitionedContext.indexSubspace(for: Order.self)
    /// ```
    ///
    /// - Parameters:
    ///   - field: The compiled partition field
    ///   - value: The partition value
    /// - Returns: A partition-scoped IndexQueryContext
    public func withPartition<
        T: Persistable,
        V: Sendable & FieldValueRepresentable
    >(
        _ field: Field<T, V>,
        equals value: V
    ) throws -> IndexQueryContext {
        var fields = partitions?.fields ?? []
        fields.removeAll { $0.key == field.name }
        fields.append((key: field.name, value: value.fieldValue))
        return IndexQueryContext(
            context: context,
            partitions: try FieldObject(fields)
        )
    }

    /// Create a partition-scoped query context from canonical wire values.
    ///
    /// The wire layer carries directory fields as strings keyed by field name.
    /// DatabaseEngine owns the binding to concrete directory path components.
    public func withPartitions<T: Persistable>(
        _ partitions: FieldObject,
        for type: T.Type
    ) throws -> IndexQueryContext {
        guard try CanonicalPartitionBinding.makeBinding(
            for: type,
            partitions: partitions
        ) != nil else {
            return self
        }
        return IndexQueryContext(context: context, partitions: partitions)
    }

    /// Get the partition binding for a specific type (if set)
    public func partitionBinding<T: Persistable>(
        for type: T.Type
    ) throws -> DirectoryPath<T>? {
        guard let partitions else { return nil }
        return try CanonicalPartitionBinding.makeBinding(
            for: type,
            partitions: partitions
        )
    }

    // MARK: - Storage Access

    /// Get the index subspace for a type
    ///
    /// For types with dynamic directories, uses the partition binding if set.
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: The index subspace
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        return store.indexSubspace
    }

    /// Resolves a declared index using only the caller's read transaction.
    ///
    /// Unlike `indexSubspace(for:)`, this API does not create a directory or
    /// initialize index state. `nil` means the logical partition has never
    /// existed and therefore has no rows to scan.
    public func readableIndexSubspace<T: Persistable>(
        named indexName: String,
        for type: T.Type,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        guard try type.indexDescriptors.contains(
            where: { $0.name == indexName }
        ) else {
            throw IndexQueryContextError.indexNotFound(indexName)
        }
        let path: AnyDirectoryPath?
        if let binding = try partitionBinding(for: type) {
            path = try AnyDirectoryPath(binding)
        } else {
            path = nil
        }
        return try await context.container.readableIndexSubspace(
            named: indexName,
            for: type,
            path: path,
            transaction: transaction
        )
    }

    /// Resolves a declared index by entity name using only the caller's read
    /// transaction. A missing logical partition is an empty dataset and returns
    /// `nil`; this method never creates directory or index metadata.
    public func readableIndexSubspace(
        named indexName: String,
        forEntityName entityName: String,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        guard let entity = schema.entitiesByName[entityName] else {
            throw IndexQueryContextError.indexNotFound(entityName)
        }
        let path = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        return try await context.container.readableIndexSubspace(
            named: indexName,
            for: entity,
            path: path,
            transaction: transaction
        )
    }

    /// Get the index subspace for an entity resolved by name.
    ///
    /// This is the schema-driven counterpart of `indexSubspace(for:)`. Callers
    /// that don't hold a generic `T` (for example federated SPARQL queries) use
    /// canonical entity and partition metadata directly.
    ///
    /// - Parameter entityName: The entity name as registered in the schema
    /// - Returns: The index subspace for that entity
    /// - Throws: `IndexQueryContextError.indexNotFound` when the entity is absent
    ///   or its `persistableType` is nil (e.g. wire-decoded schemas).
    public func indexSubspace(forEntityName entityName: String) async throws -> Subspace {
        try await indexSubspace(
            forEntityName: entityName,
            partitions: FieldObject()
        )
    }

    /// Get an index subspace for a runtime-resolved entity and canonical partition.
    public func indexSubspace(
        forEntityName entityName: String,
        partitions: FieldObject
    ) async throws -> Subspace {
        guard let entity = schema.entitiesByName[entityName] else {
            throw IndexQueryContextError.indexNotFound(entityName)
        }
        let path = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        let store = try await context.container.store(
            for: entity,
            path: path
        )
        return store.indexSubspace
    }

    /// Get a StorageReader for a type
    ///
    /// Feature-owned readers use this for physical index access.
    /// For types with dynamic directories, uses the partition binding if set.
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: A StorageReader for index access
    public func storageReader<T: Persistable>(for type: T.Type) async throws -> StorageReader {
        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        return DatabaseStorageReaderAdapter(store: store)
    }

    /// Get the item subspace for a type (for transaction-scoped operations)
    ///
    /// For types with dynamic directories, uses the partition binding if set.
    ///
    /// - Parameter type: The persistable type
    /// - Returns: Subspace for items of this type
    public func itemSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        return store.itemSubspace
    }

    /// Execute a closure within a transaction
    ///
    /// Uses `context.withStorageAccess()` internally to benefit from
    /// `ReadVersionCache` while withholding lifecycle authority.
    ///
    /// - Parameter body: Closure that takes a transaction
    /// - Returns: Result of the closure
    public func withTransaction<R: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (any TransactionAccess) async throws -> R
    ) async throws -> R {
        return try await context.withStorageAccess(configuration: configuration) { transaction in
            try await body(transaction)
        }
    }

    // MARK: - Item Fetching

    /// Fetch items by their IDs
    ///
    /// For types with dynamic directories, uses the partition binding if set via `withPartition()`.
    ///
    /// - Parameters:
    ///   - ids: Array of item IDs (as Tuples)
    ///   - type: The item type
    /// - Returns: Array of fetched items (in same order as IDs, skipping not found)
    public func fetchItems<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> [T] {
        // Security: Evaluate LIST before fetching
        try context.container.securityDelegate?.evaluateList(
            entity: T.persistableType,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )

        var results: [T] = []

        // Use partition binding if available
        if let binding = try partitionBinding(for: type) {
            for identifierTuple in ids {
                if let item = try await context.model(
                    forIdentifierTuple: identifierTuple,
                    as: type,
                    partition: binding,
                    cachePolicy: cachePolicy
                ) {
                    results.append(item)
                }
            }
        } else {
            for identifierTuple in ids {
                if let item = try await context.model(
                    forIdentifierTuple: identifierTuple,
                    as: type,
                    cachePolicy: cachePolicy
                ) {
                    results.append(item)
                }
            }
        }

        return results
    }

    /// Fetches application-level identifiers without erasing their declared
    /// model identifier type.
    public func fetchItems<T: Persistable>(
        identifiers: [T.ID],
        type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> [T] {
        try context.container.securityDelegate?.evaluateList(
            entity: T.persistableType,
            limit: identifiers.count,
            offset: nil,
            orderBy: nil
        )

        var results: [T] = []
        results.reserveCapacity(identifiers.count)
        if let binding = try partitionBinding(for: type) {
            for identifier in identifiers {
                if let item = try await context.model(
                    for: identifier,
                    as: type,
                    partition: binding,
                    cachePolicy: cachePolicy
                ) {
                    results.append(item)
                }
            }
        } else {
            for identifier in identifiers {
                if let item = try await context.model(
                    for: identifier,
                    as: type,
                    cachePolicy: cachePolicy
                ) {
                    results.append(item)
                }
            }
        }
        return results
    }

    /// Fetch items by their IDs, preserving input order (nil for missing).
    ///
    /// Unlike `fetchItems`, this returns `T?` slots so callers can detect which
    /// IDs were missing without their later indexes shifting. Use this when the
    /// caller attaches per-index metadata (rank, score offset, etc.) that must
    /// stay aligned with the original ID list.
    ///
    /// - Parameters:
    ///   - ids: Array of item IDs (as Tuples)
    ///   - type: The item type
    ///   - cachePolicy: Cache policy for each fetch
    /// - Returns: `[T?]` in the same order as `ids`; nil for IDs that do not resolve.
    public func fetchItemsPreservingOrder<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> [T?] {
        try context.container.securityDelegate?.evaluateList(
            entity: T.persistableType,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )

        var results: [T?] = []
        results.reserveCapacity(ids.count)

        if let binding = try partitionBinding(for: type) {
            for identifierTuple in ids {
                let item = try await context.model(
                    forIdentifierTuple: identifierTuple,
                    as: type,
                    partition: binding,
                    cachePolicy: cachePolicy
                )
                results.append(item)
            }
        } else {
            for identifierTuple in ids {
                let item = try await context.model(
                    forIdentifierTuple: identifierTuple,
                    as: type,
                    cachePolicy: cachePolicy
                )
                results.append(item)
            }
        }

        return results
    }

    /// Fetch a single item by ID
    ///
    /// For types with dynamic directories, uses the partition binding if set via `withPartition()`.
    ///
    /// - Parameters:
    ///   - id: The item ID (as Tuple)
    ///   - type: The item type
    /// - Returns: The item if found
    public func fetchItem<T: Persistable>(
        id: Tuple,
        type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        // Use partition binding if available
        if let binding = try partitionBinding(for: type) {
            return try await context.model(
                forIdentifierTuple: id,
                as: type,
                partition: binding,
                cachePolicy: cachePolicy
            )
        } else {
            return try await context.model(
                forIdentifierTuple: id,
                as: type,
                cachePolicy: cachePolicy
            )
        }
    }

    /// Fetch item by ID within a transaction
    ///
    /// For types with dynamic directories, uses the partition binding if set via `withPartition()`.
    ///
    /// - Parameters:
    ///   - id: The item ID (as Tuple)
    ///   - type: The item type
    ///   - transaction: The transaction to use
    /// - Returns: The item if found
    public func fetchItem<T: Persistable>(
        id: Tuple,
        type: T.Type,
        transaction: any TransactionAccess
    ) async throws -> T? {
        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        _ = try PersistableIdentifierKeyCodec.value(
            from: id,
            expectedType: T.persistableIdentifierType
        )
        return try await store.fetchByIdentifierTupleInTransaction(
            type,
            identifier: id,
            transaction: transaction,
            snapshot: true
        )
    }

    /// Fetch all items of a type (expensive, use with caution)
    ///
    /// For types with dynamic directories, uses the partition binding if set via `withPartition()`.
    ///
    /// - Parameter type: The persistable type
    /// - Returns: Array of all items
    public func fetchAllItems<T: Persistable>(type: T.Type) async throws -> [T] {
        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        return try await store.fetchAll(type)
    }

    /// Batch fetch items by their IDs using optimized BatchFetcher
    ///
    /// For types with dynamic directories, uses the partition binding if set via `withPartition()`.
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
            entity: T.persistableType,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )

        let store: DatabaseDataStore
        if let binding = try partitionBinding(for: type) {
            store = try await context.container.store(for: type, path: binding)
        } else {
            store = try await context.container.store(for: type)
        }
        let fetcher = BatchFetcher<T>(
            itemSubspace: store.itemSubspace,
            blobsSubspace: store.blobsSubspace,
            itemType: T.persistableType,
            itemStorageFactory: context.container.itemStorageFactory,
            configuration: configuration
        )

        let items = try await context.withStorageAccess(configuration: .default) { transaction in
            try await fetcher.fetch(primaryKeys: ids, transaction: transaction)
        }

        // Security: Evaluate GET for each fetched item
        for item in items {
            try context.container.securityDelegate?.evaluateGet(
                try PersistedModel(item)
            )
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
            descriptor.kind.identifier == kindIdentifier
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
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Index not found: \(name)"
        }
    }
}

// MARK: - Storage Reader Adapter

/// Adapter that wraps DatabaseDataStore to provide StorageReader interface
internal struct DatabaseStorageReaderAdapter: StorageReader {

    private let store: DatabaseDataStore

    init(store: DatabaseDataStore) {
        self.store = store
    }

    func scanRange(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) -> AsyncThrowingStream<(key: ByteString, value: ByteString), Error> {
        store.scanRangeRaw(
            subspace: subspace,
            start: start,
            end: end,
            startInclusive: startInclusive,
            endInclusive: endInclusive,
            reverse: reverse
        )
    }

    func getValue(key: ByteString) async throws -> ByteString? {
        try await store.getValueRaw(key: key)
    }

}

// MARK: - DatabaseDataStore Extensions

extension DatabaseDataStore {

    /// Scan a range within a subspace (raw key-value access)
    func scanRangeRaw(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool,
        limit: Int? = nil,
        streamingMode: StreamingMode? = nil
    ) -> AsyncThrowingStream<(key: ByteString, value: ByteString), Error> {
        let mode = streamingMode ?? StreamingMode.forQuery(limit: limit)
        let effectiveLimit = limit ?? 0

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    try await self.container.transactionExecutor.withTransaction(configuration: .default, clock: self.container.monotonicClock) { transaction in
                        let beginKey: ByteString
                        let endKey: ByteString

                        if let startTuple = start {
                            let packed = subspace.pack(startTuple)
                            if startInclusive {
                                beginKey = packed
                            } else {
                                beginKey = self.keyAfter(packed)
                            }
                        } else {
                            beginKey = subspace.prefix
                        }

                        if let endTuple = end {
                            let packed = subspace.pack(endTuple)
                            if endInclusive {
                                endKey = self.keyAfter(packed)
                            } else {
                                endKey = packed
                            }
                        } else {
                            endKey = try strinc(subspace.prefix)
                        }

                        let fromSelector: KeySelector
                        let toSelector: KeySelector

                        if reverse {
                            fromSelector = KeySelector.lastLessThan(endKey)
                            toSelector = KeySelector.firstGreaterOrEqual(beginKey)
                        } else {
                            fromSelector = KeySelector.firstGreaterOrEqual(beginKey)
                            toSelector = KeySelector.firstGreaterOrEqual(endKey)
                        }

                        let sequence = try await transaction.collectRange(
                            from: fromSelector,
                            to: toSelector,
                            limit: effectiveLimit,
                            reverse: reverse,
                            snapshot: true,
                            streamingMode: mode
                        )
                        for (key, value) in sequence {
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
    func getValueRaw(key: ByteString) async throws -> ByteString? {
        return try await self.container.transactionExecutor.withTransaction(configuration: .default, clock: self.container.monotonicClock) { transaction in
            return try await transaction.getValue(for: key, snapshot: true)
        }
    }

    /// Return the first key strictly greater than one exact key.
    private func keyAfter(_ key: ByteString) -> ByteString {
        key.appending(0x00)
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Get an index query context for executing index-based queries
    public var indexQueryContext: IndexQueryContext {
        IndexQueryContext(context: self)
    }
}

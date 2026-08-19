// IndexQueryContext.swift
// DatabaseEngine context for index-based reads

import DatabaseKit
import DatabaseTypes
import StorageKit

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
/// // In a feature-owned index reader
/// func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
///     try await queryContext.withReadableIndex(
///         named: indexName,
///         indexType: indexType,
///         for: T.self
///     ) { index, transaction in
///         // Read index directly with the admitted subspace and transaction.
///     }
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
/// let rows = try await partitionedContext.withReadableIndex(
///     named: indexName,
///     indexType: indexType,
///     for: Order.self
/// ) { index, transaction in
///     // Read the admitted index in transaction.
/// }
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
    /// let rows = try await partitionedContext.withReadableIndex(
    ///     named: indexName,
    ///     indexType: indexType,
    ///     for: Order.self
    /// ) { index, transaction in
    ///     // Read the admitted index in transaction.
    /// }
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

    /// Resolves and admits a declared index using only the caller's transaction.
    ///
    /// This API does not create a directory or initialize index state. `nil`
    /// means the logical partition has never existed and has no rows to scan.
    public func readableIndex<T: Persistable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        transaction: any TransactionAccess
    ) async throws -> ReadableIndex? {
        #if DATABASE_MULTI_BASE
        _ = try context.requireOperationDataRoot()
        #endif
        let descriptor = try indexDescriptor(
            named: indexName,
            indexType: indexType,
            for: type
        )
        let path: AnyDirectoryPath?
        if let binding = try partitionBinding(for: type) {
            path = try AnyDirectoryPath(binding)
        } else {
            path = nil
        }
        guard let subspace = try await context.container.readableIndexSubspace(
            named: descriptor.name,
            for: type,
            path: path,
            transaction: transaction
        ) else {
            return nil
        }
        return ReadableIndex(
            descriptor: descriptor,
            subspace: subspace
        )
    }

    /// Resolves a declared index by entity name using only the caller's read
    /// transaction. A missing logical partition is an empty dataset and returns
    /// `nil`; this method never creates directory or index metadata.
    public func readableIndex(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> ReadableIndex? {
        #if DATABASE_MULTI_BASE
        _ = try context.requireOperationDataRoot()
        #endif
        let descriptor = try indexDescriptor(
            named: indexName,
            indexType: indexType,
            forEntityName: entityName
        )
        guard let entity = schema.entitiesByName[entityName] else {
            throw IndexQueryContextError.entityNotFound(entityName)
        }
        let path = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        guard let subspace = try await context.container.readableIndexSubspace(
            named: descriptor.name,
            for: entity,
            path: path,
            transaction: transaction
        ) else {
            return nil
        }
        return ReadableIndex(
            descriptor: descriptor,
            subspace: subspace
        )
    }

    /// Owns the read transaction so lifecycle admission and the physical index
    /// operation always observe the same read version.
    public func withReadableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await withTransaction(configuration: configuration) { transaction in
            let index = try await readableIndex(
                named: indexName,
                indexType: indexType,
                for: type,
                transaction: transaction
            )
            return try await operation(index, transaction)
        }
    }

    /// Owns a read transaction and resolves an index through schema metadata,
    /// without requiring a compiled model type.
    public func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await withTransaction(configuration: configuration) { transaction in
            let index = try await readableIndex(
                named: indexName,
                indexType: indexType,
                forEntityName: entityName,
                partitions: partitions,
                transaction: transaction
            )
            return try await operation(index, transaction)
        }
    }

    /// Bind raw storage reads to one already-admitted transaction.
    package func storageReader(
        transaction: any TransactionAccess
    ) -> TransactionStorageReader {
        TransactionStorageReader(transaction: transaction)
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
        return try await context.withStorageAccess(
            requiredAccess: .read,
            configuration: configuration
        ) { transaction in
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

    /// Fetches index-emitted identifiers without opening nested transactions.
    /// The caller owns the transaction and therefore the read snapshot.
    package func fetchItemsPreservingOrder<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        transaction: any TransactionAccess
    ) async throws -> [T?] {
        try context.container.securityDelegate?.evaluateList(
            entity: T.persistableType,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )

        let partitionValues = partitions ?? FieldObject()
        var results: [T?] = []
        results.reserveCapacity(ids.count)
        for identifierTuple in ids {
            results.append(
                try await context.model(
                    forIdentifierTuple: identifierTuple,
                    as: type,
                    partitions: partitionValues,
                    transaction: transaction
                )
            )
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
        #if DATABASE_MULTI_BASE
        _ = try context.requireOperationDataRoot()
        #endif
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
        var query = Query<T>()
        if let binding = try partitionBinding(for: type) {
            query.partitionBinding = binding
        }
        return try await context.fetch(query)
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

        return try await context.withDataOperation { [self] in

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

        let items = try await context.withStorageAccess(
            requiredAccess: .read,
            configuration: .default
        ) { transaction in
            try await fetcher.fetch(primaryKeys: ids, transaction: transaction)
        }

        // Security: Evaluate GET for each fetched item
        for item in items {
            try context.container.securityDelegate?.evaluateGet(
                try PersistedModel(item),
                fields: nil
            )
        }

        return items
        }
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

    /// Resolve an exact schema-owned index for a concrete entity type.
    public func indexDescriptor<T: Persistable>(
        named name: String,
        indexType: IndexType,
        for type: T.Type
    ) throws -> IndexDescriptor {
        try indexDescriptor(
            named: name,
            indexType: indexType,
            forEntityName: T.persistableType
        )
    }

    /// Resolve an exact schema-owned index for a runtime entity name.
    public func indexDescriptor(
        named name: String,
        indexType: IndexType,
        forEntityName entityName: String
    ) throws -> IndexDescriptor {
        guard let entity = schema.entitiesByName[entityName] else {
            throw IndexQueryContextError.entityNotFound(entityName)
        }
        guard let descriptor = entity.indexDescriptors.first(
            where: { $0.name == name }
        ) else {
            throw IndexQueryContextError.indexNotFound(
                indexName: name,
                entityName: entityName
            )
        }
        guard descriptor.type == indexType else {
            throw IndexQueryContextError.indexTypeMismatch(
                indexName: name,
                expected: descriptor.type,
                actual: indexType
            )
        }
        return descriptor
    }

    /// Find indexes by semantic type.
    public func findIndexes<T: Persistable>(
        for type: T.Type,
        indexType: IndexType
    ) -> [IndexDescriptor] {
        let descriptors = indexDescriptors(for: type)
        return descriptors.filter { descriptor in
            descriptor.type == indexType
        }
    }

}

// MARK: - Errors

/// Errors for IndexQueryContext operations
public enum IndexQueryContextError: Error, Sendable, Equatable, CustomStringConvertible {
    case entityNotFound(String)
    case indexNotFound(indexName: String, entityName: String)
    case polymorphicIndexNotFound(indexName: String, groupIdentifier: String)
    case indexTypeMismatch(indexName: String, expected: IndexType, actual: IndexType)
    case missingDirectory(entityName: String)

    public var description: String {
        switch self {
        case .entityNotFound(let name):
            return "Entity not found: \(name)"
        case .indexNotFound(let indexName, let entityName):
            return "Index '\(indexName)' is not declared by entity '\(entityName)'"
        case .polymorphicIndexNotFound(let indexName, let groupIdentifier):
            return "Index '\(indexName)' is not declared by polymorphic group '\(groupIdentifier)'"
        case .indexTypeMismatch(let indexName, let expected, let actual):
            return "Index '\(indexName)' has type '\(expected.diagnosticName)', not '\(actual.diagnosticName)'"
        case .missingDirectory(let entityName):
            return "Registered directory for entity '\(entityName)' is missing"
        }
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Get an index query context for executing index-based queries
    public var indexQueryContext: IndexQueryContext {
        IndexQueryContext(context: self)
    }
}

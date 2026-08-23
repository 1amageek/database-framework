// IndexQueryContext.swift
// DatabaseEngine context for index-based reads

import DatabaseKit
import DatabaseTypes
import StorageKit

/// Exact LIST policy shape for one direct index operation.
public struct IndexReadAuthorization: Sendable, Hashable {
    public let limit: Int?
    public let offset: Int?
    public let orderBy: [String]?

    public init(
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) {
        self.limit = limit
        self.offset = offset
        self.orderBy = orderBy
    }

    package init(selectQuery: SelectQuery) throws {
        self.limit = try Self.runtimeWindowValue(
            selectQuery.limit,
            name: "limit"
        )
        self.offset = try Self.runtimeWindowValue(
            selectQuery.offset,
            name: "offset"
        )
        self.orderBy = try selectQuery.requiredOrderByColumnNames()
    }

    package init(sparqlSelectQuery: SelectQuery) throws {
        self.limit = try Self.runtimeWindowValue(
            sparqlSelectQuery.limit,
            name: "limit"
        )
        self.offset = try Self.runtimeWindowValue(
            sparqlSelectQuery.offset,
            name: "offset"
        )
        self.orderBy = sparqlSelectQuery
            .orderByVariableNamesForAuthorization()
    }

    package init(modifiers: SPARQLSolutionModifiers) throws {
        self.limit = try Self.runtimeWindowValue(
            modifiers.limit,
            name: "limit"
        )
        self.offset = try Self.runtimeWindowValue(
            modifiers.offset,
            name: "offset"
        )
        self.orderBy = modifiers.orderByVariableNamesForAuthorization()
    }

    private static func runtimeWindowValue(
        _ value: UInt64?,
        name: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let converted = Int(exactly: value) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: name,
                value: value
            )
        }
        return converted
    }
}

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

    /// The database execution context retained by this capability facade.
    package let context: DatabaseContext

    /// Canonical partition values for dynamic directories.
    private let partitions: FieldObject?

    package var partitionValues: FieldObject {
        partitions ?? FieldObject()
    }

    /// Performs entity LIST admission for direct index execution paths that
    /// do not construct a canonical SelectQuery envelope.
    package func authorizeListAccess(
        entityName: String,
        authorization: IndexReadAuthorization
    ) throws {
        if try ActiveDatabaseReadAuthorizationAdmission.admission?.coversList(
            entityName: entityName,
            authorization: authorization,
            context: context
        ) == true {
            return
        }
        try RequestAuthorization.$context.withValue(context.authorization) {
            try context.container.securityDelegate?.evaluateList(
                entity: entityName,
                limit: authorization.limit,
                offset: authorization.offset,
                orderBy: authorization.orderBy
            )
        }
    }

    /// Admits every field that can influence or be exposed by one direct
    /// index read. This preflight is intentionally independent of storage so
    /// a denied read cannot use index existence as an oracle.
    package func authorizeIndexRead(
        entityName: String,
        descriptor: IndexDescriptor,
        authorization: IndexReadAuthorization
    ) throws {
        try RequestAuthorization.$context.withValue(context.authorization) {
            try authorizeListAccess(
                entityName: entityName,
                authorization: authorization
            )
            guard let entity = schema.entitiesByName[entityName] else {
                throw IndexQueryContextError.entityNotFound(entityName)
            }
            try context.authorizeFieldReads(
                .index(entity: entity, descriptor: descriptor)
            )
        }
    }

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
            return IndexQueryContext(context: context, partitions: nil)
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
    package func readableIndex<T: Persistable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
    ) async throws -> ReadableIndex? {
        try await context.withDataOperation { [self] in
            let descriptor = try indexDescriptor(
                named: indexName,
                indexType: indexType,
                for: type
            )
            try authorizeIndexRead(
                entityName: T.persistableType,
                descriptor: descriptor,
                authorization: authorization
            )
            return try await resolveReadableIndex(
                named: indexName,
                indexType: indexType,
                for: type,
                transaction: transaction
            )
        }
    }

    private func resolveReadableIndex<T: Persistable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        transaction: any TransactionReadAccess
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
    package func readableIndex(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
    ) async throws -> ReadableIndex? {
        try await context.withDataOperation { [self] in
            let descriptor = try indexDescriptor(
                named: indexName,
                indexType: indexType,
                forEntityName: entityName
            )
            try authorizeIndexRead(
                entityName: entityName,
                descriptor: descriptor,
                authorization: authorization
            )
            return try await resolveReadableIndex(
                named: indexName,
                indexType: indexType,
                forEntityName: entityName,
                partitions: partitions,
                transaction: transaction
            )
        }
    }

    /// Resolves a declared index for an admitted cross-package database
    /// execution without exposing write or transaction-control authority.
    @_spi(DatabaseExecution)
    public func readableIndexForExecution(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
    ) async throws -> ReadableIndex? {
        try await readableIndex(
            named: indexName,
            indexType: indexType,
            forEntityName: entityName,
            partitions: partitions,
            authorization: authorization,
            transaction: transaction
        )
    }

    private func resolveReadableIndex(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
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
    package func withReadableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        authorization: IndexReadAuthorization,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await context.withDataOperation { [self] in
            let descriptor = try indexDescriptor(
                named: indexName,
                indexType: indexType,
                for: type
            )
            try authorizeIndexRead(
                entityName: T.persistableType,
                descriptor: descriptor,
                authorization: authorization
            )
            return try await context.withReadStorageAccess(
                configuration: configuration
            ) { transaction in
                let index = try await resolveReadableIndex(
                    named: indexName,
                    indexType: indexType,
                    for: type,
                    transaction: transaction
                )
                return try await operation(
                    index,
                    ScopedIndexReadAccess(
                        queryContext: self,
                        transaction: transaction,
                        index: index
                    )
                )
            }
        }
    }

    /// Owns a read transaction and resolves an index through schema metadata,
    /// without requiring a compiled model type.
    package func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await context.withDataOperation { [self] in
            let descriptor = try indexDescriptor(
                named: indexName,
                indexType: indexType,
                forEntityName: entityName
            )
            try authorizeIndexRead(
                entityName: entityName,
                descriptor: descriptor,
                authorization: authorization
            )
            return try await context.withReadStorageAccess(
                configuration: configuration
            ) { transaction in
                let index = try await resolveReadableIndex(
                    named: indexName,
                    indexType: indexType,
                    forEntityName: entityName,
                    partitions: partitions,
                    transaction: transaction
                )
                return try await operation(
                    index,
                    ScopedIndexReadAccess(
                        queryContext: self,
                        transaction: transaction,
                        index: index
                    )
                )
            }
        }
    }

    /// Owns a mutation-authorized transaction while resolving one schema-
    /// admitted physical index. The feature callback receives storage authority
    /// confined to that index and cannot access the enclosing database root.
    package func withWritableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        requiredAccess: DatabaseMutationAuthorization = .write,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            ReadableIndex,
            any IndexMaintenanceTransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await context.withWriteStorageAccess(
            requiredAccess: requiredAccess,
            configuration: configuration
        ) { transaction in
            let index = try await resolveReadableIndex(
                named: indexName,
                indexType: indexType,
                for: type,
                transaction: transaction
            )
            guard let index else {
                throw IndexQueryContextError.indexNotReadable(
                    indexName: indexName,
                    entityName: T.persistableType
                )
            }
            return try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: index.subspace
            ) { maintenanceTransaction in
                try await operation(index, maintenanceTransaction)
            }
        }
    }

    /// Bind raw storage reads to one already-admitted transaction.
    package func storageReader(
        transaction: any TransactionReadAccess
    ) -> TransactionStorageReader {
        TransactionStorageReader(transaction: transaction)
    }

    /// Resolves semantic read capabilities on one hidden storage snapshot.
    /// Feature packages can compose multiple index reads without recovering
    /// the database-root transaction.
    package func withQuerySnapshot<R: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            any IndexQuerySnapshotAccess
        ) async throws -> R
    ) async throws -> R {
        return try await context.withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            try await body(
                ScopedIndexQuerySnapshotAccess(
                    queryContext: self,
                    transaction: transaction
                )
            )
        }
    }

    /// Executes one feature-owned auxiliary read inside a data-root-relative
    /// namespace while withholding every other database key range.
    package func withAuxiliaryReadStorage<R: Sendable>(
        namespace: ByteString,
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> R
    ) async throws -> R {
        guard !namespace.isEmpty else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        return try await context.withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            let subspace = try context.operationDataRoot()
                .subspace("data")
                .subspace(namespace)
            return try await body(
                subspace,
                ScopedIndexReadAccess(
                    transaction: transaction,
                    subspace: subspace
                )
            )
        }
    }

    package func withAuxiliaryReadStorage<R: Sendable>(
        path: [String],
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> R
    ) async throws -> R {
        guard !path.isEmpty, path.allSatisfy({ !$0.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        return try await context.withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            var subspace = try context.operationDataRoot().subspace("data")
            for component in path {
                subspace = subspace.subspace(component)
            }
            return try await body(
                subspace,
                ScopedIndexReadAccess(
                    transaction: transaction,
                    subspace: subspace
                )
            )
        }
    }

    /// Executes one feature-owned auxiliary mutation inside a
    /// data-root-relative namespace.
    package func withAuxiliaryWriteStorage<R: Sendable>(
        namespace: ByteString,
        requiredAccess: DatabaseMutationAuthorization,
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            Subspace,
            any IndexMaintenanceTransactionAccess
        ) async throws -> R
    ) async throws -> R {
        guard !namespace.isEmpty else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        return try await context.withWriteStorageAccess(
            requiredAccess: requiredAccess,
            configuration: configuration
        ) { transaction in
            let subspace = try context.operationDataRoot()
                .subspace("data")
                .subspace(namespace)
            return try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: subspace
            ) { scoped in
                try await body(subspace, scoped)
            }
        }
    }

    package func withAuxiliaryWriteStorage<R: Sendable>(
        path: [String],
        requiredAccess: DatabaseMutationAuthorization,
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            Subspace,
            any IndexMaintenanceTransactionAccess
        ) async throws -> R
    ) async throws -> R {
        guard !path.isEmpty, path.allSatisfy({ !$0.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        return try await context.withWriteStorageAccess(
            requiredAccess: requiredAccess,
            configuration: configuration
        ) { transaction in
            var subspace = try context.operationDataRoot().subspace("data")
            for component in path {
                subspace = subspace.subspace(component)
            }
            let scopedSubspace = subspace
            return try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: scopedSubspace
            ) { scoped in
                try await body(scopedSubspace, scoped)
            }
        }
    }

    /// Executes persistence projection on one Engine-owned read snapshot
    /// without exposing root-wide physical storage access to feature targets.
    package func withPersistenceRead<R: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (
            any PersistenceQueryReadAccess
        ) async throws -> R
    ) async throws -> R {
        try await context.withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            try await body(
                ScopedPersistenceQueryReadAccess(
                    context: context,
                    transaction: transaction
                )
            )
        }
    }

    /// Binds nested semantic query operations to one snapshot without
    /// disclosing that snapshot's root storage capability.
    package func withReadSnapshot<R: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping () async throws -> R
    ) async throws -> R {
        try await context.withReadStorageAccess(
            configuration: configuration
        ) { _ in
            try await body()
        }
    }

    /// Execute an index mutation within an explicitly write-authorized
    /// transaction while withholding lifecycle authority.
    package func withWriteTransaction<R: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (any TransactionAccess) async throws -> R
    ) async throws -> R {
        try await context.withWriteStorageAccess(
            requiredAccess: .write,
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
        try await context.withDataOperation { [self] in
            try authorizeListAccess(
                entityName: T.persistableType,
                authorization: IndexReadAuthorization(
                    limit: ids.count,
                    offset: nil,
                    orderBy: nil
                )
            )

            var results: [T] = []

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
    }

    /// Fetches application-level identifiers without erasing their declared
    /// model identifier type.
    public func fetchItems<T: Persistable>(
        identifiers: [T.ID],
        type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> [T] {
        try await context.withDataOperation { [self] in
            try authorizeListAccess(
                entityName: T.persistableType,
                authorization: IndexReadAuthorization(
                    limit: identifiers.count,
                    offset: nil,
                    orderBy: nil
                )
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
        try await context.withDataOperation { [self] in
            try authorizeListAccess(
                entityName: T.persistableType,
                authorization: IndexReadAuthorization(
                    limit: ids.count,
                    offset: nil,
                    orderBy: nil
                )
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
    }

    /// Fetches index-emitted identifiers without opening nested transactions.
    /// The caller owns the transaction and therefore the read snapshot.
    package func fetchItemsPreservingOrder<T: Persistable>(
        ids: [Tuple],
        type: T.Type,
        transaction: any TransactionReadAccess
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
    package func fetchItem<T: Persistable>(
        id: Tuple,
        type: T.Type,
        transaction: any TransactionReadAccess
    ) async throws -> T? {
        #if DATABASE_MULTI_BASE
        _ = try context.requireOperationDataRoot()
        #endif
        return try await context.model(
            forIdentifierTuple: id,
            as: type,
            partitions: partitions ?? FieldObject(),
            transaction: transaction
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
        return try await context.withDataOperation { [self] in

        // Security: Evaluate LIST before fetching
        try context.container.securityDelegate?.evaluateList(
            entity: T.persistableType,
            limit: ids.count,
            offset: nil,
            orderBy: nil
        )
        guard !ids.isEmpty else { return [] }

        let items: [T] = try await context.withReadStorageAccess(
            configuration: .default
        ) { transaction in
            guard let entity = self.schema.entity(named: T.persistableType)
            else {
                throw IndexQueryContextError.entityNotFound(
                    T.persistableType
                )
            }
            let path: AnyDirectoryPath?
            if let binding = try self.partitionBinding(for: type) {
                path = try AnyDirectoryPath(binding)
            } else {
                path = nil
            }
            guard let store = try await self.context.container.openStore(
                for: entity,
                path: path,
                transaction: transaction
            ) else {
                return []
            }
            return try await BatchFetcher<T>(
                itemSubspace: store.itemSubspace,
                blobsSubspace: store.blobsSubspace,
                itemType: T.persistableType,
                itemStorageFactory: self.context.container.itemStorageFactory,
                configuration: configuration
            ).fetch(primaryKeys: ids, transaction: transaction)
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
    case indexNotReadable(indexName: String, entityName: String)
    case polymorphicIndexNotFound(indexName: String, groupIdentifier: String)
    case indexTypeMismatch(indexName: String, expected: IndexType, actual: IndexType)
    case missingDirectory(entityName: String)

    public var description: String {
        switch self {
        case .entityNotFound(let name):
            return "Entity not found: \(name)"
        case .indexNotFound(let indexName, let entityName):
            return "Index '\(indexName)' is not declared by entity '\(entityName)'"
        case .indexNotReadable(let indexName, let entityName):
            return "Index '\(indexName)' for entity '\(entityName)' is not readable"
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

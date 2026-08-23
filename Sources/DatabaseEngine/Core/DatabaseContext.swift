import StorageKit
import DatabaseKit
import DatabaseTypes
import Synchronization

/// DatabaseContext - Central API for model persistence
///
/// A model context is central to the database runtime because it manages
/// the entire lifecycle of your persistent models. You use a context to insert
/// new models, track and persist changes to those models, and to delete those
/// models when you no longer need them.
///
/// **Architecture** (Context-Centric Design):
/// - DatabaseContext provides high-level API for persistence
/// - **DatabaseContext owns transactions and ReadVersionCache** (not DBContainer)
/// - Container resolves directories from Persistable type's `#Directory` declaration
/// - DatabaseDataStore applies schema-aware persistence over the configured StorageEngine
///
/// **Transaction Management**:
/// - Use `context.withTransaction()` for explicit transaction control
/// - ReadVersionCache is per-context for proper scoping per unit of work
/// - System operations (DirectoryLayer, Migration) use `(try container.requireDataTransactionExecutor()).withTransaction()`
///
/// **Usage**:
/// ```swift
/// let context = container.newContext(authorization: authorization)
///
/// // Insert models (type-independent)
/// try context.insert(user)      // User: Persistable
/// try context.insert(product)   // Product: Persistable
///
/// // Save all changes atomically
/// try await context.save()
///
/// // Fetch models with Fluent API
/// let users = try await context.fetch(User.self)
///     .where(User.fields.isActive == true)
///     .orderBy(User.fields.name)
///     .limit(10)
///     .execute()
///
/// // Explicit transaction
/// try await context.withTransaction(configuration: .default) { tx in
///     let user = try await tx.fetch(User.self, identifiedBy: userId)
///     // ...
/// }
///
/// // Get by ID
/// if let user = try await context.model(for: userId, as: User.self) {
///     print(user.name)
/// }
///
/// // Delete
/// try context.delete(user)
/// try await context.save()
/// ```
public final class DatabaseContext: Sendable {
    // MARK: - Properties

    /// The container that owns this context
    package let container: DBContainer

    #if DATABASE_MULTI_BASE
    /// Security and storage resource selected for every Base operation.
    package let resource: Security.Resource

    /// Base identity selected for every operation executed by this context.
    public var baseID: Base.ID {
        guard case .base(let id) = resource else {
            preconditionFailure("A MultiBase context must target a Base")
        }
        return id
    }
    #endif

    package let authorization: AuthorizationContext

    /// Read version cache for CachePolicy
    ///
    /// Each context owns its own cache, scoped to the unit of work.
    /// This follows the design principle that cache lifetime = context lifetime.
    ///
    /// **Use Cases**:
    /// - Web app: 1 request = 1 context → cache effective within request
    /// - Batch processing: long-lived context → cache shared across batch
    /// - Parallel processing: each context has independent cache → no interference
    ///
    /// **Cache Usage by Operation**:
    ///
    /// | Operation | Uses Cache | CachePolicy |
    /// |-----------|------------|-------------|
    /// | `fetch()` | Configurable | `.server` (default), `.cached`, `.stale(duration)` |
    /// | `fetchCount()` | Configurable | Same as fetch() |
    /// | `withTransaction()` | ✅ Yes | Via TransactionConfiguration.cachePolicy |
    /// | `save()` | ❌ No | Write operations need latest data for consistency |
    ///
    /// **Usage**:
    /// ```swift
    /// // Default: strict consistency (no cache)
    /// let users = try await context.fetch(User.self).execute()
    ///
    /// // Use cache if available (no time limit)
    /// let users = try await context.fetch(User.self)
    ///     .cachePolicy(.cached)
    ///     .execute()
    ///
    /// // Use cache only if younger than 30 seconds
    /// let users = try await context.fetch(User.self)
    ///     .cachePolicy(.stale(.seconds(30)))
    ///     .execute()
    /// ```
    private let readVersionCache: ReadVersionCache

    /// Change tracking state
    private let stateLock: Mutex<ContextState>

    /// Database event logger selected by the container configuration.
    private let logger: DatabaseLogger

    /// Error handler for autosave failures
    ///
    /// When set, this handler is called if autosave fails. This allows the caller
    /// to be notified of save failures that would otherwise be silently logged.
    ///
    /// **Usage**:
    /// ```swift
    /// let context = session.base(baseID).newContext(autosaveEnabled: true)
    /// context.autosaveErrorHandler = { error in
    ///     print("Autosave failed: \(error)")
    ///     // Show user notification, retry, etc.
    /// }
    /// ```
    public var autosaveErrorHandler: (@Sendable (Error) -> Void)? {
        get { stateLock.withLock { $0.autosaveErrorHandler } }
        set { stateLock.withLock { $0.autosaveErrorHandler = newValue } }
    }

    // MARK: - Initialization

    /// Initialize DatabaseContext
    ///
    /// - Parameters:
    ///   - container: The DBContainer to use for storage
    ///   - autosaveEnabled: Whether to automatically save after insert/delete (default: false)
    #if DATABASE_MULTI_BASE
    package init(
        container: DBContainer,
        resource: Security.Resource,
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) {
        self.container = container
        self.resource = resource
        self.authorization = authorization
        self.readVersionCache = ReadVersionCache(
            monotonicClock: container.monotonicClock
        )
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
        self.logger = container.configuration.logging.logger(
            label: "com.database.framework.context"
        )
    }
    #else
    package init(
        container: DBContainer,
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) {
        self.container = container
        self.authorization = authorization
        self.readVersionCache = ReadVersionCache(
            monotonicClock: container.monotonicClock
        )
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
        self.logger = container.configuration.logging.logger(
            label: "com.database.framework.context"
        )
    }
    #endif

    // MARK: - State

    /// Net mutation staged for one persisted identity before `save()`.
    internal enum PendingMutation: Sendable {
        case save(
            model: PersistedModel,
            precondition: WritePrecondition
        )
        case delete(
            model: PersistedModel,
            precondition: WritePrecondition
        )

        var precondition: WritePrecondition {
            switch self {
            case .save(_, let precondition),
                 .delete(_, let precondition):
                return precondition
            }
        }
    }

    private enum StagedMutationIntent: Sendable {
        case save(
            model: PersistedModel,
            precondition: WritePrecondition
        )
        case delete(
            model: PersistedModel,
            precondition: WritePrecondition
        )

        var model: PersistedModel {
            switch self {
            case .save(let model, _), .delete(let model, _):
                return model
            }
        }
    }

    private struct StagedMutation: Sendable {
        let identity: EntityReference
        let intent: StagedMutationIntent
    }

    private struct ActiveSave: Sendable {
        let identifier: UInt64
        let capturedMutations: ContextPendingMutations
        var followupMutations: [StagedMutation]

        init(
            identifier: UInt64,
            capturedMutations: ContextPendingMutations
        ) {
            self.identifier = identifier
            self.capturedMutations = capturedMutations
            self.followupMutations = []
        }
    }

    private struct ContextState: Sendable {
        var pending = ContextPendingMutations()
        var activeSave: ActiveSave?
        var nextSaveIdentifier: UInt64 = 1
        var commitOutcomeUnknown = false

        /// Whether to automatically save after insert/delete operations
        var autosaveEnabled: Bool

        /// Whether an autosave task is already scheduled
        var autosaveScheduled: Bool = false

        /// Error handler for autosave failures
        var autosaveErrorHandler: (@Sendable (Error) -> Void)?

        /// Whether the context has unsaved changes
        var hasChanges: Bool {
            !pending.isEmpty || activeSave != nil
        }

        init(autosaveEnabled: Bool = false) {
            self.autosaveEnabled = autosaveEnabled
        }
    }

    /// Resolves read storage inside the caller-owned transaction. Read paths
    /// never create directories, initialize lifecycle state, or open a nested
    /// transaction merely to construct a store facade.
    private func openStoreForRead<T: Persistable>(
        for type: T.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseDataStore? {
        guard let entity = container.schema.entity(named: T.persistableType)
        else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        return try await container.openStore(
            for: entity,
            path: path,
            transaction: transaction
        )
    }

    private func ensureUsable() throws {
        try stateLock.withLock { state in
            guard !state.commitOutcomeUnknown else {
                throw DatabaseContextError.commitOutcomeUnknown
            }
        }
    }

    private func pendingModelLookup<T: Persistable>(
        for id: T.ID,
        as type: T.Type,
        partitions: FieldObject
    ) throws -> (inserted: T?, isDeleted: Bool) {
        try pendingModelLookup(
            for: id.persistableIdentifierValue,
            as: type,
            partitions: partitions
        )
    }

    private func pendingModelLookup<T: Persistable>(
        for identifier: ReferenceIdentifier,
        as type: T.Type,
        partitions: FieldObject
    ) throws -> (inserted: T?, isDeleted: Bool) {
        try stateLock.withLock { state in
            guard state.hasChanges else {
                return (nil, false)
            }

            let identity = try EntityReference(
                entity: T.persistableType,
                id: identifier,
                partitions: partitions
            )
            let mutation =
                state.pending[identity]
                ?? state.activeSave?.capturedMutations[identity]
            guard let mutation else {
                return (nil, false)
            }

            switch mutation {
            case .save(let model, _):
                return (try model.decode(as: T.self), false)
            case .delete:
                return (nil, true)
            }
        }
    }

    // MARK: - Public Properties

    /// Whether the context has unsaved changes
    public var hasChanges: Bool {
        stateLock.withLock { state in
            state.hasChanges
        }
    }

    /// Whether to automatically save after insert/delete operations
    public var autosaveEnabled: Bool {
        get {
            stateLock.withLock { state in
                state.autosaveEnabled
            }
        }
        set {
            stateLock.withLock { state in
                state.autosaveEnabled = newValue
            }
        }
    }

    // MARK: - Write Operations

    /// Stage a strict insert.
    public func insert<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .notExists
    ) throws {
        try stage(
            identity: EntityReferenceEncoder.encode(model),
            intent: .save(
                model: PersistedModel(model),
                precondition: precondition
            )
        )
    }

    /// Stage a write that may create or replace the persisted value.
    public func upsert<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .none
    ) throws {
        try stage(
            identity: EntityReferenceEncoder.encode(model),
            intent: .save(
                model: PersistedModel(model),
                precondition: precondition
            )
        )
    }

    /// Stage an update that requires a persisted value.
    public func update<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .exists
    ) throws {
        try stage(
            identity: EntityReferenceEncoder.encode(model),
            intent: .save(
                model: PersistedModel(model),
                precondition: precondition
            )
        )
    }

    /// Stage a delete that requires a persisted value.
    public func delete<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .exists
    ) throws {
        try stage(
            identity: EntityReferenceEncoder.encode(model),
            intent: .delete(
                model: PersistedModel(model),
                precondition: precondition
            )
        )
    }

    private func stage(
        identity: EntityReference,
        intent: StagedMutationIntent
    ) throws {
        let stagedMutation = StagedMutation(
            identity: identity,
            intent: intent
        )
        let shouldScheduleAutosave = try stateLock.withLock {
            state -> Bool in
            guard !state.commitOutcomeUnknown else {
                throw DatabaseContextError.commitOutcomeUnknown
            }
            Self.apply(stagedMutation, to: &state.pending)
            if var activeSave = state.activeSave {
                activeSave.followupMutations.append(stagedMutation)
                state.activeSave = activeSave
                return false
            }
            guard state.autosaveEnabled,
                  !state.autosaveScheduled,
                  !state.pending.isEmpty else {
                return false
            }
            state.autosaveScheduled = true
            return true
        }

        if shouldScheduleAutosave {
            scheduleAutosave()
        }
    }

    private static func apply(
        _ stagedMutation: StagedMutation,
        to mutations: inout ContextPendingMutations
    ) {
        let identity = stagedMutation.identity
        switch stagedMutation.intent {
        case .save(let model, let precondition):
            let initialPrecondition =
                mutations[identity]?.precondition ?? precondition
            mutations[identity] = .save(
                model: model,
                precondition: initialPrecondition
            )
        case .delete(let model, let precondition):
            guard let current = mutations[identity] else {
                mutations[identity] = .delete(
                    model: model,
                    precondition: precondition
                )
                return
            }
            switch current {
            case .save(_, let initialPrecondition):
                if initialPrecondition == .notExists {
                    mutations.remove(identifiedBy: identity)
                } else {
                    mutations[identity] = .delete(
                        model: model,
                        precondition: initialPrecondition
                    )
                }
            case .delete(_, let initialPrecondition):
                mutations[identity] = .delete(
                    model: model,
                    precondition: initialPrecondition
                )
            }
        }
    }

    /// Delete all models of a type matching a predicate
    public func delete<T: Persistable>(
        _ type: T.Type,
        where predicate: Predicate<T>
    ) async throws {
        let query = Query<T>().where(predicate)
        let models = try await fetch(query)
        for model in models {
            try delete(model)
        }
    }

    /// Delete all models of a type
    ///
    /// For types with dynamic directories, use `deleteAll(_:partition:equals:)` instead.
    ///
    /// - Throws: `DirectoryPathError.dynamicFieldsRequired` if type has dynamic directory
    public func deleteAll<T: Persistable>(_ type: T.Type) async throws {
        // Validate: dynamic directory types require partition
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }
        let models = try await fetch(Query<T>())
        for model in models {
            try delete(model)
        }
    }

    /// Delete all models of a type within a partition
    ///
    /// For types with dynamic directories, use this method to specify the partition.
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.deleteAll(Order.self, partition: \.tenantID, equals: "tenant_123")
    /// try await context.save()
    /// ```
    public func deleteAll<T: Persistable, V: Sendable & Equatable & FieldValueRepresentable>(
        _ type: T.Type,
        partition field: Field<T, V>,
        equals value: V
    ) async throws {
        let models = try await fetch(Query<T>().partition(field, equals: value))
        for model in models {
            try delete(model)
        }
    }

    // MARK: - Fetch

    /// Create a query executor for fetching models with Fluent API
    public func fetch<T: Persistable>(_ type: T.Type) -> QueryExecutor<T> {
        QueryExecutor(context: self, query: Query<T>())
    }

    // MARK: - Cursor (Paginated Fetch)

    /// Create a cursor for paginated fetching with continuation support
    ///
    /// Unlike `fetch()` which returns all results at once, cursors return results
    /// in batches with continuation tokens for efficient pagination.
    ///
    /// **Usage**:
    /// ```swift
    /// // First page
    /// let result = try await context.cursor(User.self)
    ///     .where(User.fields.isActive == true)
    ///     .orderBy(User.fields.createdAt, .descending)
    ///     .batchSize(20)
    ///     .next()
    ///
    /// displayUsers(result.items)
    ///
    /// // Next page (can be in a different request/session)
    /// if let continuation = result.continuation {
    ///     let nextResult = try await context.cursor(User.self, continuation: continuation).next()
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to query
    ///   - continuation: Optional continuation token to resume from
    /// - Returns: A CursorQueryBuilder for configuring the query
    public func cursor<T: Persistable>(
        _ type: T.Type,
        continuation: ContinuationToken? = nil
    ) -> CursorQueryBuilder<T> {
        CursorQueryBuilder(context: self, continuation: continuation)
    }

    /// Fetch models matching a query (internal use)
    ///
    /// Returns only persisted data. Pending changes (inserts/deletes) are not included.
    /// Use `pendingInserts()` and `pendingDeletes()` to access uncommitted changes.
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// the query must include partition binding via `.partition()`.
    ///
    /// **Cache Policy**: Uses `query.cachePolicy` to determine read version caching:
    /// - `.server`: Always fetch latest data (no cache) - **default**
    /// - `.cached`: Use cached read version if available (no time limit)
    /// - `.stale(N)`: Use cache only if younger than N seconds
    internal func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        try await withFetchedModelsInTransaction(query) { models, _ in
            models
        }
    }

    /// Fetch persisted models through a caller-owned storage transaction.
    ///
    /// This path is used when an upper-layer operation composes multiple reads
    /// into one authoritative snapshot. Directory resolution, index admission,
    /// and entity reads all remain inside the supplied transaction.
    package func fetch<T: Persistable>(
        _ query: Query<T>,
        transaction: any TransactionReadAccess
    ) async throws -> [T] {
        try ensureUsable()
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif

        let path: AnyDirectoryPath?
        if T.hasDynamicDirectory {
            guard let binding = query.partitionBinding else {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: T.persistableType,
                    fields: T.directoryFieldNames
                )
            }
            try binding.validate()
            path = try AnyDirectoryPath(binding)
        } else {
            path = nil
        }

        guard let entity = container.schema.entity(named: T.persistableType) else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        guard let store = try await container.openStore(
            for: entity,
            path: path,
            transaction: transaction
        ) else { return [] }
        return try await store.fetchInTransaction(
            query,
            transaction: transaction
        )
    }

    /// Counts persisted models through a caller-owned storage transaction.
    ///
    /// Composition execution uses this entry point so every partial count is
    /// evaluated at the same per-domain snapshot as member authorization.
    package func fetchCount<T: Persistable>(
        _ query: Query<T>,
        transaction: any TransactionReadAccess
    ) async throws -> Int {
        try ensureUsable()
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif

        let path: AnyDirectoryPath?
        if T.hasDynamicDirectory {
            guard let binding = query.partitionBinding else {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: T.persistableType,
                    fields: T.directoryFieldNames
                )
            }
            try binding.validate()
            path = try AnyDirectoryPath(binding)
        } else {
            path = nil
        }

        guard let entity = container.schema.entity(named: T.persistableType) else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        guard let store = try await container.openStore(
            for: entity,
            path: path,
            transaction: transaction
        ) else { return 0 }
        return try await store.fetchCountInTransaction(
            query,
            transaction: transaction
        )
    }

    /// Resolve the storage access path that the current runtime would use.
    internal func executionPlan<T: Persistable>(
        for query: Query<T>
    ) async throws -> QueryAccessPlan {
        try await withDataOperation { [self] in
        if T.hasDynamicDirectory {
            guard let binding = query.partitionBinding else {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: T.persistableType,
                    fields: T.directoryFieldNames
                )
            }
            try binding.validate()
        }

        guard let entity = container.schema.entity(named: T.persistableType)
        else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        let directoryPath = try query.partitionBinding.map(AnyDirectoryPath.init)
            ?? AnyDirectoryPath(for: entity)
        let store = DatabaseDataStore(
            container: container,
            subspace: try container.operationDataSubspace(
                relativePath: directoryPath.resolve()
            ),
            entity: entity,
            securityDelegate: container.securityDelegate,
            indexConfigurations: container.runtimeConfiguration
                .indexConfigurations
        )
        return try await withReadStorageAccess {
            transaction in
            try await store.executionPlan(
                for: query,
                transaction: transaction
            )
        }
        }
    }

    /// Executes a model query and dependent reads at one transaction read version.
    package func withFetchedModelsInTransaction<T: Persistable, Result: Sendable>(
        _ query: Query<T>,
        _ operation: @Sendable @escaping (
            [T],
            any DatabaseTransactionPersistenceReading
        ) async throws -> Result
    ) async throws -> Result {
        return try await withDataOperation { [self] in
        // Check if type requires partition and validate
        if T.hasDynamicDirectory {
            guard let binding = query.partitionBinding else {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: T.persistableType,
                    fields: T.directoryFieldNames
                )
            }
            try binding.validate()
        }

        // Create TransactionConfiguration with CachePolicy
        let config = TransactionConfiguration(cachePolicy: query.cachePolicy)

        // Execute fetch within transaction (uses ReadVersionCache)
        return try await self.withPersistenceReadTransaction(
            configuration: config
        ) { transaction, storageAccess in
            let models = try await self.fetch(
                query,
                transaction: storageAccess
            )
            return try await operation(models, transaction)
        }
        }
    }

    /// Fetch count of models matching a query (internal use)
    ///
    /// Returns count of persisted data only. Pending changes are not included.
    ///
    /// For types with dynamic directories, the query must include partition binding.
    ///
    /// **Cache Policy**: Uses `query.cachePolicy` to determine read version caching:
    /// - `.server`: Always fetch latest count (no cache) - **default**
    /// - `.cached`: Use cached read version if available (no time limit)
    /// - `.stale(N)`: Use cache only if younger than N seconds
    internal func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        return try await withDataOperation { [self] in
        // Check if type requires partition and validate
        if T.hasDynamicDirectory {
            guard let binding = query.partitionBinding else {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: T.persistableType,
                    fields: T.directoryFieldNames
                )
            }
            try binding.validate()
        }

        // Create TransactionConfiguration with CachePolicy
        let config = TransactionConfiguration(cachePolicy: query.cachePolicy)

        // Execute count within transaction (uses ReadVersionCache)
        return try await self.withReadStorageAccess(
            configuration: config
        ) { transaction in
            try await self.fetchCount(
                query,
                transaction: transaction
            )
        }
        }
    }

    // MARK: - Get by ID

    /// Get a single model by its identifier
    ///
    /// Performs a direct key lookup (O(1)) rather than a query scan.
    ///
    /// **Cache Policy**: Controls whether to use cached read version:
    /// - `.server`: Always fetch latest data (no cache) - **default**
    /// - `.cached`: Use cached read version if available (no time limit)
    /// - `.stale(N)`: Use cache only if younger than N seconds
    ///
    /// **Usage**:
    /// ```swift
    /// // Default: strict consistency
    /// let user = try await context.model(for: userId, as: User.self)
    ///
    /// // With cache (for read-heavy scenarios)
    /// let user = try await context.model(for: userId, as: User.self, cachePolicy: .cached)
    /// ```
    ///
    /// - Parameters:
    ///   - id: The model's identifier
    ///   - type: The model type
    ///   - cachePolicy: Cache policy for this read (default: `.server`)
    /// - Returns: The model if found, nil if not found
    public func model<T: Persistable>(
        for id: T.ID,
        as type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        try await withDataOperation { [self] in
            try await modelInActiveBase(
                for: id,
                as: type,
                cachePolicy: cachePolicy
            )
        }
    }

    private func modelInActiveBase<T: Persistable>(
        for id: T.ID,
        as type: T.Type,
        cachePolicy: CachePolicy
    ) async throws -> T? {
        try ensureUsable()

        // A dynamic type has no unambiguous in-memory identity without its partition.
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }

        let pendingResult = try pendingModelLookup(
            for: id,
            as: type,
            partitions: FieldObject()
        )

        if let inserted = pendingResult.inserted {
            try await requireAccess(.read)
            // Evaluate GET security for pending insert (not via DataStore)
            try container.securityDelegate?.evaluateGet(
                try PersistedModel(inserted),
                fields: nil
            )
            return inserted
        }

        if pendingResult.isDeleted {
            try await requireAccess(.read)
            return nil
        }

        let configuration = TransactionConfiguration(cachePolicy: cachePolicy)
        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            guard let store = try await self.openStoreForRead(
                for: type,
                transaction: transaction
            ) else { return nil }
            return try await store.fetchByIDInTransaction(
                type,
                id: id,
                transaction: transaction
            )
        }
    }

    /// Resolves an identifier tuple emitted by an index without weakening the
    /// application-facing `T.ID` contract.
    ///
    /// The schema-guided decode is also used to match staged mutations. The
    /// original tuple is retained for the storage read so its bytes are not
    /// reconstructed through a dynamically guessed Swift type.
    func model<T: Persistable>(
        forIdentifierTuple identifierTuple: Tuple,
        as type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        try await withDataOperation { [self] in
            try await modelInActiveBase(
                forIdentifierTuple: identifierTuple,
                as: type,
                cachePolicy: cachePolicy
            )
        }
    }

    private func modelInActiveBase<T: Persistable>(
        forIdentifierTuple identifierTuple: Tuple,
        as type: T.Type,
        cachePolicy: CachePolicy
    ) async throws -> T? {
        try ensureUsable()

        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }

        let identifier = try PersistableIdentifierKeyCodec.value(
            from: identifierTuple,
            expectedType: T.persistableIdentifierType
        )
        let pendingResult = try pendingModelLookup(
            for: identifier,
            as: type,
            partitions: FieldObject()
        )

        if let inserted = pendingResult.inserted {
            try await requireAccess(.read)
            try container.securityDelegate?.evaluateGet(
                try PersistedModel(inserted),
                fields: nil
            )
            return inserted
        }
        if pendingResult.isDeleted {
            try await requireAccess(.read)
            return nil
        }

        let configuration = TransactionConfiguration(cachePolicy: cachePolicy)
        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            guard let store = try await self.openStoreForRead(
                for: type,
                transaction: transaction
            ) else { return nil }
            return try await store.fetchByIdentifierTupleInTransaction(
                type,
                identifier: identifierTuple,
                transaction: transaction
            )
        }
    }

    /// Resolves an index-emitted identifier in the caller-owned transaction.
    /// Directory admission, staged-mutation visibility, and the entity read all
    /// remain on that transaction's snapshot.
    package func model<T: Persistable>(
        forIdentifierTuple identifierTuple: Tuple,
        as type: T.Type,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> T? {
        try ensureUsable()
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif

        let identifier = try PersistableIdentifierKeyCodec.value(
            from: identifierTuple,
            expectedType: T.persistableIdentifierType
        )
        let pendingResult = try pendingModelLookup(
            for: identifier,
            as: type,
            partitions: partitions
        )
        if let inserted = pendingResult.inserted {
            try container.securityDelegate?.evaluateGet(
                try PersistedModel(inserted),
                fields: nil
            )
            return inserted
        }
        if pendingResult.isDeleted {
            return nil
        }

        guard let entity = container.schema.entity(named: T.persistableType) else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        let path = try AnyDirectoryPath(
            entity: entity,
            partitions: partitions
        )
        guard let store = try await container.openStore(
            for: entity,
            path: path,
            transaction: transaction
        ) else { return nil }
        return try await store.fetchByIdentifierTupleInTransaction(
            type,
            identifier: identifierTuple,
            transaction: transaction
        )
    }

    /// Get a single model by its identifier from a partitioned directory
    ///
    /// For types with dynamic directories (`Field(\.keyPath)` in `#Directory`),
    /// you must provide partition binding.
    ///
    /// **Cache Policy**: Controls whether to use cached read version:
    /// - `.server`: Always fetch latest data (no cache) - **default**
    /// - `.cached`: Use cached read version if available (no time limit)
    /// - `.stale(N)`: Use cache only if younger than N seconds
    ///
    /// **Example**:
    /// ```swift
    /// var binding = DirectoryPath<Order>()
    /// binding.set(\.tenantID, to: "tenant_123")
    /// let order = try await context.model(for: orderId, as: Order.self, partition: binding)
    ///
    /// // With cache
    /// let order = try await context.model(
    ///     for: orderId, as: Order.self, partition: binding, cachePolicy: .cached
    /// )
    /// ```
    public func model<T: Persistable>(
        for id: T.ID,
        as type: T.Type,
        partition path: DirectoryPath<T>,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        try await withDataOperation { [self] in
            try await modelInActiveBase(
                for: id,
                as: type,
                partition: path,
                cachePolicy: cachePolicy
            )
        }
    }

    private func modelInActiveBase<T: Persistable>(
        for id: T.ID,
        as type: T.Type,
        partition path: DirectoryPath<T>,
        cachePolicy: CachePolicy
    ) async throws -> T? {
        try ensureUsable()

        let pendingResult = try pendingModelLookup(
            for: id,
            as: type,
            partitions: try path.canonicalPartitions()
        )

        if let inserted = pendingResult.inserted {
            try await requireAccess(.read)
            // Evaluate GET security for pending insert (not via DataStore)
            try container.securityDelegate?.evaluateGet(
                try PersistedModel(inserted),
                fields: nil
            )
            return inserted
        }

        if pendingResult.isDeleted {
            try await requireAccess(.read)
            return nil
        }

        let directoryPath = try AnyDirectoryPath(path)
        let configuration = TransactionConfiguration(cachePolicy: cachePolicy)
        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            guard let store = try await self.openStoreForRead(
                for: type,
                path: directoryPath,
                transaction: transaction
            ) else { return nil }
            return try await store.fetchByIDInTransaction(
                type,
                id: id,
                transaction: transaction
            )
        }
    }

    /// Resolves an index identifier tuple inside a dynamic directory binding.
    func model<T: Persistable>(
        forIdentifierTuple identifierTuple: Tuple,
        as type: T.Type,
        partition path: DirectoryPath<T>,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        try await withDataOperation { [self] in
            try await modelInActiveBase(
                forIdentifierTuple: identifierTuple,
                as: type,
                partition: path,
                cachePolicy: cachePolicy
            )
        }
    }

    private func modelInActiveBase<T: Persistable>(
        forIdentifierTuple identifierTuple: Tuple,
        as type: T.Type,
        partition path: DirectoryPath<T>,
        cachePolicy: CachePolicy
    ) async throws -> T? {
        try ensureUsable()

        let identifier = try PersistableIdentifierKeyCodec.value(
            from: identifierTuple,
            expectedType: T.persistableIdentifierType
        )
        let partitions = try path.canonicalPartitions()
        let pendingResult = try pendingModelLookup(
            for: identifier,
            as: type,
            partitions: partitions
        )

        if let inserted = pendingResult.inserted {
            try await requireAccess(.read)
            try container.securityDelegate?.evaluateGet(
                try PersistedModel(inserted),
                fields: nil
            )
            return inserted
        }
        if pendingResult.isDeleted {
            try await requireAccess(.read)
            return nil
        }

        let directoryPath = try AnyDirectoryPath(path)
        let configuration = TransactionConfiguration(cachePolicy: cachePolicy)
        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            guard let store = try await self.openStoreForRead(
                for: type,
                path: directoryPath,
                transaction: transaction
            ) else { return nil }
            return try await store.fetchByIdentifierTupleInTransaction(
                type,
                identifier: identifierTuple,
                transaction: transaction
            )
        }
    }

    // MARK: - Save

    /// Persist all pending changes atomically
    ///
    /// All changes are persisted in a single transaction for atomicity.
    /// Security evaluation is performed via the container's securityDelegate.
    public func save() async throws {
        enum SaveCheckResult {
            case noChanges
            case alreadySaving
            case commitOutcomeUnknown
            case identifierExhausted
            case start(
                identifier: UInt64,
                mutations: ContextPendingMutations
            )
        }

        let checkResult = stateLock.withLock { state -> SaveCheckResult in
            guard state.activeSave == nil else {
                return .alreadySaving
            }
            guard !state.commitOutcomeUnknown else {
                return .commitOutcomeUnknown
            }
            guard !state.pending.isEmpty else {
                state.autosaveScheduled = false
                return .noChanges
            }
            guard state.nextSaveIdentifier < UInt64.max else {
                return .identifierExhausted
            }

            let identifier = state.nextSaveIdentifier
            state.nextSaveIdentifier += 1
            let capturedMutations = state.pending
            state.pending.removeAll()
            state.activeSave = ActiveSave(
                identifier: identifier,
                capturedMutations: capturedMutations
            )
            state.autosaveScheduled = false
            return .start(
                identifier: identifier,
                mutations: capturedMutations
            )
        }

        switch checkResult {
        case .noChanges:
            return
        case .alreadySaving:
            throw DatabaseContextError.concurrentSaveNotAllowed
        case .commitOutcomeUnknown:
            throw DatabaseContextError.commitOutcomeUnknown
        case .identifierExhausted:
            throw DatabaseContextError.saveIdentifierExhausted
        case .start(let identifier, let mutations):
            do {
                try await saveGeneralPath(
                    mutations: Self.persistableMutations(from: mutations)
                )
                let shouldScheduleAutosave = try stateLock.withLock {
                    state -> Bool in
                    guard state.activeSave?.identifier == identifier else {
                        throw DatabaseContextError.invalidSaveState
                    }
                    state.activeSave = nil
                    guard state.autosaveEnabled,
                          !state.autosaveScheduled,
                          !state.pending.isEmpty else {
                        return false
                    }
                    state.autosaveScheduled = true
                    return true
                }
                if shouldScheduleAutosave {
                    scheduleAutosave()
                }
            } catch {
                try stateLock.withLock { state in
                    guard let activeSave = state.activeSave,
                          activeSave.identifier == identifier else {
                        throw DatabaseContextError.invalidSaveState
                    }
                    if !state.commitOutcomeUnknown {
                        var restored = activeSave.capturedMutations
                        for mutation in activeSave.followupMutations {
                            Self.apply(mutation, to: &restored)
                        }
                        state.pending = restored
                    }
                    state.activeSave = nil
                }
                throw error
            }
        }
    }

    private static func persistableMutations(
        from mutations: ContextPendingMutations
    ) -> [PersistableMutation] {
        var result: [PersistableMutation] = []
        result.reserveCapacity(mutations.count)
        mutations.forEach { identity, mutation in
            switch mutation {
            case .save(let model, let precondition):
                result.append(
                    .save(
                        identity: identity,
                        model: model,
                        precondition: precondition
                    )
                )
            case .delete(let model, let precondition):
                result.append(
                    .delete(
                        identity: identity,
                        model: model,
                        precondition: precondition
                    )
                )
            }
        }
        return result
    }

    /// Applies the captured unit-of-work through the canonical logical
    /// transaction. Every primary mutation, derived index, relationship rule,
    /// and final-state check therefore shares one owner and one commit.
    private func saveGeneralPath(
        mutations: [PersistableMutation]
    ) async throws {
        try await withTransaction { transaction in
            try await transaction.apply(mutations)
        }
    }

    // MARK: - Rollback

    /// Discard pending changes when no save outcome is in flight or ambiguous.
    public func rollback() throws {
        try stateLock.withLock { state in
            guard state.activeSave == nil else {
                throw DatabaseContextError.rollbackDuringSaveNotAllowed
            }
            guard !state.commitOutcomeUnknown else {
                throw DatabaseContextError.commitOutcomeUnknown
            }
            state.pending.removeAll()
            state.autosaveScheduled = false
        }
    }

    // MARK: - Autosave

    private func scheduleAutosave() {
        let clock = container.monotonicClock
        Task { [self, clock] in
            do {
                try await clock.sleep(for: .milliseconds(10))
            } catch {
                if Task.isCancelled {
                    return
                }
                self.logger.error("Autosave scheduling failed")
                let errorHandler = self.stateLock.withLock {
                    $0.autosaveErrorHandler
                }
                errorHandler?(error)
                return
            }

            let (shouldSave, errorHandler) = self.stateLock.withLock { state in
                (state.hasChanges && state.autosaveEnabled, state.autosaveErrorHandler)
            }

            if shouldSave {
                do {
                    try await self.save()
                } catch {
                    self.logger.error("Autosave failed")

                    // Notify caller via error handler
                    errorHandler?(error)

                    // Disable autosave after failure to prevent repeated errors
                    self.stateLock.withLock { state in
                        state.autosaveEnabled = false
                    }
                    self.logger.warning("Autosave disabled due to failure. Call save() manually or re-enable autosave.")
                }
            } else {
                // Reset flag when no save occurred (no changes or autosave disabled)
                // This allows future changes to schedule autosave again
                self.stateLock.withLock { state in
                    state.autosaveScheduled = false
                }
            }
        }
    }

    // MARK: - Perform and Save

    /// Execute operations and automatically save changes
    public func performAndSave(
        block: () throws -> Void
    ) async throws {
        try block()
        try await save()
    }

    // MARK: - Enumerate

    /// Enumerate all models of a type
    ///
    /// For types with dynamic directories, use `enumerate(_:partition:equals:block:)` instead.
    ///
    /// - Throws: `DirectoryPathError.dynamicFieldsRequired` if type has dynamic directory
    public func enumerate<T: Persistable>(
        _ type: T.Type,
        block: (T) throws -> Void
    ) async throws {
        try ensureUsable()

        // Validate: dynamic directory types require partition
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }
        let models = try await fetch(Query<T>())
        for model in models {
            try block(model)
        }
    }

    /// Enumerate all models of a type within a partition
    ///
    /// For types with dynamic directories, use this method to specify the partition.
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.enumerate(Order.self, partition: \.tenantID, equals: "tenant_123") { order in
    ///     print(order.id)
    /// }
    /// ```
    public func enumerate<T: Persistable, V: Sendable & Equatable & FieldValueRepresentable>(
        _ type: T.Type,
        partition field: Field<T, V>,
        equals value: V,
        block: (T) throws -> Void
    ) async throws {
        try ensureUsable()

        let models = try await fetch(Query<T>().partition(field, equals: value))
        for model in models {
            try block(model)
        }
    }
}

// MARK: - Errors

public enum DatabaseContextError: Error, Sendable, Equatable, CustomStringConvertible {
    case concurrentSaveNotAllowed
    case rollbackDuringSaveNotAllowed
    case commitOutcomeUnknown
    case saveIdentifierExhausted
    case invalidSaveState

    /// A `WritePrecondition` was violated at commit time.
    ///
    /// - `identity`: Typed logical identity of the offending model.
    /// - `precondition`: The precondition that failed.
    /// - `reason`: Human-readable explanation (e.g. "row already exists",
    ///   "row not found", "stored version mismatch").
    case preconditionFailed(
        identity: EntityReference,
        precondition: WritePrecondition,
        reason: String
    )

    public var description: String {
        switch self {
        case .concurrentSaveNotAllowed:
            return "DatabaseContextError: Cannot save while another save operation is in progress"
        case .rollbackDuringSaveNotAllowed:
            return "DatabaseContextError: Cannot roll back staged changes while a save operation is in progress"
        case .commitOutcomeUnknown:
            return "DatabaseContextError: The previous commit outcome is unknown; this context cannot be reused"
        case .saveIdentifierExhausted:
            return "DatabaseContextError: Save operation identifier space is exhausted"
        case .invalidSaveState:
            return "DatabaseContextError: Save lifecycle state is inconsistent"
        case .preconditionFailed(let identity, let precondition, let reason):
            return "DatabaseContextError: Precondition \(precondition) failed for \(identity.entity): \(reason)"
        }
    }
}

// MARK: - Transaction API

extension DatabaseContext {
    /// Execute a transactional operation with configurable retry and timeout
    ///
    /// Provides Firestore-like transaction semantics with explicit read isolation control.
    /// The closure may be retried on conflict - avoid side effects inside the closure.
    ///
    /// **Read Modes**:
    /// - `tx.fetch(consistency: .serializable)` (default): Transactional read that adds read conflict.
    ///   If another transaction writes to this data before commit, this transaction
    ///   will conflict and retry.
    /// - `tx.fetch(consistency: .snapshot)`: Snapshot read with no conflict tracking.
    ///   May return stale data, but won't cause conflicts. Use for non-critical reads.
    ///
    /// **ReadVersionCache**:
    /// This method uses the context's own ReadVersionCache for cache policies.
    /// When `configuration.cachePolicy` is `.cached` or `.stale(N)`, the transaction
    /// may reuse a cached read version instead of calling `getReadVersion()`, reducing
    /// network round-trips at the cost of potential staleness.
    ///
    /// **Usage**:
    /// ```swift
    /// // Read-modify-write with conflict detection
    /// try await context.withTransaction { tx in
    ///     guard let user = try await tx.fetch(User.self, identifiedBy: userId) else { throw ... }
    ///     var updated = user
    ///     updated.balance -= amount
    ///     try await tx.save(updated)
    /// }
    ///
    /// // Mixed reads
    /// try await context.withTransaction { tx in
    ///     let user = try await tx.fetch(User.self, identifiedBy: userId)           // Transactional
    ///     let stats = try await tx.fetch(Stats.self, identifiedBy: id, consistency: .snapshot) // Snapshot
    ///     try await tx.save(updated)
    /// }
    ///
    /// // Batch configuration
    /// try await context.withTransaction(configuration: .batch) { tx in
    ///     for item in items {
    ///         try await tx.save(item)
    ///     }
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority)
    ///   - operation: The transactional operation to execute
    /// - Returns: The result of the operation
    /// - Throws: Error if the transaction cannot be committed after retries
    ///
    /// **Reference**: Cloud Firestore transaction model, FDB Record Layer FDBRecordContext
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
    ) async throws -> T {
        try await withModelTransaction(
            requiredAccess: [.read, .write],
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    private func withModelTransaction<T: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
    ) async throws -> T {
        try await withModelTransactionCapabilities(
            authorization: .request(requiredAccess),
            configuration: configuration,
            executionDeadline: executionDeadline,
            exposesControlMetadata: false
        ) { transaction, _ in
            try await operation(transaction)
        }
    }

    private func withModelTransactionCapabilities<T: Sendable>(
        authorization: DatabaseModelTransactionAuthorization,
        configuration: TransactionConfiguration,
        executionDeadline: TransactionExecutionDeadline?,
        exposesControlMetadata: Bool,
        _ operation: @Sendable @escaping (
            DatabaseTransaction,
            (any TransactionAccess)?
        ) async throws -> T
    ) async throws -> T {
        // A DatabaseTransaction exposes both model reads and model mutations.
        // Every callback receiving that capability must therefore be admitted
        // for both operations, in addition to any domain-specific access such
        // as administration requested by the caller.
        let grantedAccess = authorization.grantedAccess
        try ensureUsable()
        return try await withDataOperation {
            if let binding = ActiveDatabaseTransactionContext.binding {
                guard !exposesControlMetadata else {
                    throw DatabaseControlMetadataTransactionError
                        .requiresIndependentTransaction
                }
                try binding.identity.validateReuse(by: self)
                guard binding.accessMode.allowsMutation else {
                    throw DatabaseReadTransactionError
                        .mutationRequiresWriteAccess
                }
                guard !authorization.requiresPersistedGrant
                        || binding.grantedAccess.isSuperset(of: grantedAccess)
                else {
                    #if DATABASE_MULTI_BASE
                    throw DatabaseGrantAuthorizationError.denied(
                        resource: self.resource,
                        required: grantedAccess
                    )
                    #else
                    throw DatabaseReadTransactionError
                        .mutationRequiresWriteAccess
                    #endif
                }
                #if DATABASE_MULTI_BASE
                guard binding.resource == self.resource,
                      binding.authorization == self.authorization
                else {
                    throw DatabaseGrantAuthorizationError.denied(
                        resource: self.resource,
                        required: grantedAccess
                    )
                }
                #endif
                let reuseLease = try binding.operationScope.beginRead()
                defer { reuseLease.finish() }
                guard let databaseTransaction = binding.databaseTransaction
                else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                return try await operation(databaseTransaction, nil)
            }
            #if DATABASE_MULTI_BASE
            let lease = try self.requireOperationDataRoot()
            let selectedTransactionExecutor = lease.transactionExecutor
            #else
            let selectedTransactionExecutor = (try self.container.requireDataTransactionExecutor())
            #endif
            let runner = TransactionRunner(
                transactionExecutor: selectedTransactionExecutor,
                clock: self.container.monotonicClock,
                logging: self.container.configuration.logging,
                metrics: self.container.configuration.metrics
            )
            return try await runner.run(
                configuration: configuration,
                executionDeadline: executionDeadline,
                readVersionCache: self.readVersionCache,
                operationDescription: "DatabaseContext.withTransaction",
                onCommitOutcomeUnknown: { [self] in
                    stateLock.withLock { $0.commitOutcomeUnknown = true }
                }
            ) { storageAccess in
                #if DATABASE_MULTI_BASE
                if authorization.requiresPersistedGrant {
                    try await self.requireGrant(
                        grantedAccess,
                        lease: lease,
                        transaction: storageAccess
                    )
                }
                #endif
                let executionIdentity = try DatabaseTransactionExecutionIdentity(
                    context: self
                )
                let controlStorage = self.container.controlStorage()
                if exposesControlMetadata,
                   executionIdentity.storageDomainIdentifier
                    != controlStorage.domainIdentifier {
                    throw DatabaseControlMetadataTransactionError
                        .storageDomainMismatch
                }
                let operationScope = DatabaseReadScopeGate()
                let validationScope = DatabaseReadScopeGate()
                let accessMode: DatabaseTransactionAccessMode = grantedAccess
                    .contains(.write) ? .readWrite : .readOnly
                let admittedStorageAccess = DataRootTransactionAccess.admitted(
                    storageAccess,
                    dataRoot: executionIdentity.dataRoot,
                    accessMode: accessMode,
                    readScope: operationScope
                )
                let validationStorageAccess = DataRootTransactionAccess.admitted(
                    storageAccess,
                    dataRoot: executionIdentity.dataRoot,
                    accessMode: .readOnly,
                    readScope: validationScope
                )
                let transaction = DatabaseTransaction(
                    storageAccess: admittedStorageAccess,
                    validationStorageAccess: validationStorageAccess,
                    container: self.container
                )
                let controlMetadataAccess = exposesControlMetadata
                    ? DataRootTransactionAccess.admitted(
                        storageAccess,
                        dataRoot: controlStorage.root,
                        accessMode: .readWrite,
                        readScope: operationScope
                    )
                    : nil
                #if DATABASE_MULTI_BASE
                let executionBinding = DatabaseTransactionExecutionBinding(
                    identity: executionIdentity,
                    transaction: admittedStorageAccess,
                    grantedAccess: grantedAccess,
                    accessMode: accessMode,
                    operationScope: operationScope,
                    resource: self.resource,
                    authorization: self.authorization,
                    databaseTransaction: transaction
                )
                #else
                let executionBinding = DatabaseTransactionExecutionBinding(
                    identity: executionIdentity,
                    transaction: admittedStorageAccess,
                    grantedAccess: grantedAccess,
                    accessMode: accessMode,
                    operationScope: operationScope,
                    databaseTransaction: transaction
                )
                #endif
                return try await ActiveDatabaseTransactionContext.$binding
                    .withValue(executionBinding) {
                        do {
                            let result = try await operation(
                                transaction,
                                controlMetadataAccess
                            )
                            try await operationScope.closeAndWait()
                            controlMetadataAccess?.revoke()
                            admittedStorageAccess.revoke()
                            try await transaction.prepareForCommit()
                            try await validationScope.closeAndWait()
                            validationStorageAccess.revoke()
                            return result
                        } catch {
                            var terminalError: any Error = error
                            for scope in [operationScope, validationScope] {
                                do {
                                    try await scope.closeAndWait()
                                } catch let cleanupError as DatabaseReadScopeCleanupError {
                                    terminalError = cleanupError.preserving(
                                        operationError: terminalError
                                    )
                                }
                            }
                            controlMetadataAccess?.revoke()
                            admittedStorageAccess.revoke()
                            validationStorageAccess.revoke()
                            await transaction.invalidate()
                            throw terminalError
                        }
                    }
                }
        }
    }

    /// Executes dependent model reads in one transaction while erasing every
    /// persistence mutation method at the package boundary.
    package func withPersistenceReadTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any DatabaseTransactionPersistenceReading,
            any TransactionReadAccess
        ) async throws -> T
    ) async throws -> T {
        try await withAdmittedStorageAccess(
            requiredAccess: .read,
            mode: .readOnly,
            configuration: configuration,
            executionDeadline: executionDeadline
        ) { storageAccess in
            guard let admitted = storageAccess as? DataRootTransactionAccess
            else {
                throw DatabaseRuntimeError.internalError(
                    "Persistence reads require admitted read storage access"
                )
            }
            let transaction = DatabaseTransaction(
                storageAccess: admitted,
                container: self.container
            )
            do {
                let result = try await operation(
                    DatabaseTransactionPersistenceReadProjection(
                        transaction: transaction
                    ),
                    admitted.readProjection()
                )
                await transaction.invalidate()
                return result
            } catch {
                await transaction.invalidate()
                throw error
            }
        }
    }

    #if DATABASE_MULTI_BASE
    /// Executes server-owned read work with a capability that cannot express
    /// model or storage mutation.
    @_spi(DatabaseExecution)
    public func withExecutionReadTransaction<T: Sendable>(
        requiredAccess: Security.Access = .read,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseExecutionReadTransaction
        ) async throws -> T
    ) async throws -> T {
        try await withExecutionReadTransactionImplementation(
            requiredAccess: requiredAccess,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    @_spi(DatabaseExecution)
    public func withExecutionTransaction<T: Sendable>(
        requiredAccess: Security.Access = .write,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
    ) async throws -> T {
        return try await withModelTransaction(
            requiredAccess: requiredAccess,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    #else
    /// Executes server-owned read work with a capability that cannot express
    /// model or storage mutation.
    @_spi(DatabaseExecution)
    public func withExecutionReadTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseExecutionReadTransaction
        ) async throws -> T
    ) async throws -> T {
        try await withExecutionReadTransactionImplementation(
            requiredAccess: .read,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    @_spi(DatabaseExecution)
    public func withExecutionTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
        ) async throws -> T {
        try await withModelTransaction(
            requiredAccess: [.read, .write],
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }
    #endif

    /// Executes data semantics and control-metadata mutations in one physical
    /// transaction while preserving independent root-scoped capabilities.
    ///
    /// This is the framework-owned atomic boundary used by an execution host
    /// for durable control state that must commit with one data operation. It
    /// rejects cross-domain composition; those operations require a durable
    /// checkpoint protocol instead of weakening either root boundary.
    @_spi(DatabaseExecution)
    public func withExecutionDataAndControlMetadataTransaction<T: Sendable>(
        requiredAccess: Security.Access = .write,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction,
            any TransactionAccess
        ) async throws -> T
    ) async throws -> T {
        try await withModelTransactionCapabilities(
            authorization: .request(requiredAccess),
            configuration: configuration,
            executionDeadline: executionDeadline,
            exposesControlMetadata: true
        ) { transaction, controlMetadataAccess in
            guard let controlMetadataAccess else {
                throw DatabaseControlMetadataTransactionError
                    .requiresIndependentTransaction
            }
            return try await operation(transaction, controlMetadataAccess)
        }
    }

    /// Atomically commits durable-operation cleanup with its control state.
    ///
    /// The execution host receives no data mutation capability until
    /// `validateOwnership` confirms the current operation lease through the
    /// read-only control-metadata projection in the same physical transaction.
    /// Cross-domain work remains unsupported and must use a checkpointed
    /// protocol.
    @_spi(DatabaseExecution)
    public func withExecutionOperationOwnedDataAndControlMetadataTransaction<
        Ownership: Sendable,
        T: Sendable
    >(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        validateOwnership: @Sendable @escaping (
            any TransactionReadAccess
        ) async throws -> Ownership,
        _ operation: @Sendable @escaping (
            Ownership,
            DatabaseTransaction,
            any TransactionAccess
        ) async throws -> T
    ) async throws -> T {
        try await withModelTransactionCapabilities(
            authorization: .durableOperationOwner,
            configuration: configuration,
            executionDeadline: executionDeadline,
            exposesControlMetadata: true
        ) { transaction, controlMetadataAccess in
            guard let controlMetadataAccess else {
                throw DatabaseControlMetadataTransactionError
                    .requiresIndependentTransaction
            }
            let ownership = try await validateOwnership(controlMetadataAccess)
            return try await operation(
                ownership,
                transaction,
                controlMetadataAccess
            )
        }
    }

    private func withExecutionReadTransactionImplementation<T: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration,
        executionDeadline: TransactionExecutionDeadline?,
        _ operation: @Sendable @escaping (
            DatabaseExecutionReadTransaction
        ) async throws -> T
    ) async throws -> T {
        try await withAdmittedStorageAccess(
            requiredAccess: requiredAccess,
            mode: .readOnly,
            configuration: configuration,
            executionDeadline: executionDeadline
        ) { storageAccess in
            guard let admitted = storageAccess as? DataRootTransactionAccess
            else {
                throw DatabaseRuntimeError.internalError(
                    "Execution reads require admitted data-root access"
                )
            }
            let transaction = DatabaseTransaction(
                storageAccess: admitted,
                container: self.container
            )
            do {
                let result = try await operation(
                    transaction.executionReadTransaction
                )
                await transaction.invalidate()
                return result
            } catch {
                await transaction.invalidate()
                throw error
            }
        }
    }

    @_spi(DatabaseExecution)
    public func withExecutionDataOperation<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await withDataOperation(operation)
    }

    /// Execute an operation with storage access and runner-owned lifecycle.
    ///
    /// **Design Intent**:
    /// This is an internal API for DatabaseContext operations that need key-value
    /// capabilities such as `clearRange` without receiving commit or
    /// cancellation authority.
    ///
    /// Uses the context's ReadVersionCache for weak read semantics optimization.
    ///
    /// **For public API users**:
    /// - Use `withTransaction(_:)` for high-level DatabaseTransaction API
    /// - Use `(try container.requireDataTransactionExecutor()).withTransaction(_:)` for storage-level work
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority)
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: Error if the transaction cannot be committed after retries
    package func withAdmittedStorageAccess<T: Sendable>(
        requiredAccess: Security.Access,
        mode: DatabaseTransactionAccessMode,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        requestedReadPoint: DatabaseExecutionReadPoint? = nil,
        _ operation: @Sendable @escaping (any TransactionAccess) async throws -> T
    ) async throws -> T {
        try ensureUsable()

        return try await withDataOperation {
            if let binding = ActiveDatabaseTransactionContext.binding {
                guard requestedReadPoint == nil else {
                    throw DatabaseExecutionReadPointError
                        .restoreRequiresIndependentTransaction
                }
                try binding.identity.validateReuse(by: self)
                guard mode == .readOnly || binding.accessMode.allowsMutation
                else {
                    throw DatabaseReadTransactionError
                        .mutationRequiresWriteAccess
                }
                guard binding.grantedAccess.isSuperset(of: requiredAccess)
                else {
                    #if DATABASE_MULTI_BASE
                    throw DatabaseGrantAuthorizationError.denied(
                        resource: self.resource,
                        required: requiredAccess
                    )
                    #else
                    throw DatabaseReadTransactionError
                        .mutationRequiresWriteAccess
                    #endif
                }
                #if DATABASE_MULTI_BASE
                guard binding.resource == self.resource,
                      binding.authorization == self.authorization
                else {
                    throw DatabaseGrantAuthorizationError.denied(
                        resource: self.resource,
                        required: requiredAccess
                    )
                }
                #endif
                let reuseLease = try binding.operationScope.beginRead()
                defer { reuseLease.finish() }
                let nestedOperationScope = DatabaseReadScopeGate()
                let admittedTransaction = DataRootTransactionAccess.admitted(
                    binding.transaction,
                    dataRoot: binding.identity.dataRoot,
                    accessMode: mode,
                    readScope: nestedOperationScope
                )
                #if DATABASE_MULTI_BASE
                let nestedBinding = DatabaseTransactionExecutionBinding(
                    identity: binding.identity,
                    transaction: admittedTransaction,
                    grantedAccess: binding.grantedAccess,
                    accessMode: mode,
                    operationScope: nestedOperationScope,
                    resource: binding.resource,
                    authorization: binding.authorization,
                    databaseTransaction: mode.allowsMutation
                        ? binding.databaseTransaction
                        : nil
                )
                #else
                let nestedBinding = DatabaseTransactionExecutionBinding(
                    identity: binding.identity,
                    transaction: admittedTransaction,
                    grantedAccess: binding.grantedAccess,
                    accessMode: mode,
                    operationScope: nestedOperationScope,
                    databaseTransaction: mode.allowsMutation
                        ? binding.databaseTransaction
                        : nil
                )
                #endif
                let result: T
                do {
                    result = try await ActiveDatabaseTransactionContext.$binding
                        .withValue(nestedBinding) {
                            try await operation(admittedTransaction)
                        }
                } catch {
                    var terminalError: any Error = error
                    do {
                        try await nestedOperationScope.closeAndWait()
                    } catch let cleanupError as DatabaseReadScopeCleanupError {
                        terminalError = cleanupError.preserving(
                            operationError: terminalError
                        )
                    }
                    admittedTransaction.revoke()
                    throw terminalError
                }
                do {
                    try await nestedOperationScope.closeAndWait()
                } catch {
                    admittedTransaction.revoke()
                    throw error
                }
                admittedTransaction.revoke()
                return result
            }
            #if DATABASE_MULTI_BASE
            let lease = try self.requireOperationDataRoot()
            let selectedTransactionExecutor = lease.transactionExecutor
            #else
            let selectedTransactionExecutor = (try self.container.requireDataTransactionExecutor())
            #endif
            let runner = TransactionRunner(
                transactionExecutor: selectedTransactionExecutor,
                clock: self.container.monotonicClock,
                logging: self.container.configuration.logging,
                metrics: self.container.configuration.metrics
            )
            #if DATABASE_MULTI_BASE
            if requestedReadPoint != nil {
                let authorizationRunner = TransactionRunner(
                    transactionExecutor: selectedTransactionExecutor,
                    clock: self.container.monotonicClock,
                    logging: self.container.configuration.logging,
                    metrics: self.container.configuration.metrics
                )
                _ = try await authorizationRunner.run(
                    configuration: configuration,
                    executionDeadline: executionDeadline,
                    readVersionCache: nil,
                    operationDescription:
                        "DatabaseContext.withStorageAccess.currentGrant",
                    onCommitOutcomeUnknown: {}
                ) { authorizationTransaction in
                    try await self.requireGrant(
                        requiredAccess,
                        lease: lease,
                        transaction: authorizationTransaction
                    )
                }
            }
            #endif
            return try await runner.run(
                configuration: configuration,
                executionDeadline: executionDeadline,
                readVersionCache: requestedReadPoint == nil
                    ? self.readVersionCache
                    : nil,
                operationDescription: "DatabaseContext.withStorageAccess",
                onCommitOutcomeUnknown: { [self] in
                    stateLock.withLock { $0.commitOutcomeUnknown = true }
                }
            ) { transaction in
                let executionIdentity = try DatabaseTransactionExecutionIdentity(
                    context: self
                )
                if let requestedReadPoint {
                    guard requestedReadPoint.domainIdentifier
                            == executionIdentity.storageDomainIdentifier else {
                        throw DatabaseExecutionReadPointError.domainMismatch
                    }
                    guard case .version(let version) =
                            requestedReadPoint.position else {
                        throw DatabaseExecutionReadPointError
                            .opaquePositionCannotBeRestored
                    }
                    guard transaction.capabilities.historicalReadVersion else {
                        throw DatabaseExecutionReadPointError
                            .historicalReadUnsupported
                    }
                    guard let signedVersion = Int64(exactly: version) else {
                        throw DatabaseExecutionReadPointError
                            .invalidVersion(version)
                    }
                    try transaction.setReadVersion(signedVersion)
                }
                #if DATABASE_MULTI_BASE
                if requestedReadPoint == nil {
                    try await self.requireGrant(
                        requiredAccess,
                        lease: lease,
                        transaction: transaction
                    )
                }
                #endif
                let operationScope = DatabaseReadScopeGate()
                let admittedTransaction = DataRootTransactionAccess.admitted(
                    transaction,
                    dataRoot: executionIdentity.dataRoot,
                    accessMode: mode,
                    readScope: operationScope
                )
                #if DATABASE_MULTI_BASE
                let executionBinding = DatabaseTransactionExecutionBinding(
                    identity: executionIdentity,
                    transaction: admittedTransaction,
                    grantedAccess: requiredAccess,
                    accessMode: mode,
                    operationScope: operationScope,
                    resource: self.resource,
                    authorization: self.authorization,
                    databaseTransaction: nil
                )
                #else
                let executionBinding = DatabaseTransactionExecutionBinding(
                    identity: executionIdentity,
                    transaction: admittedTransaction,
                    grantedAccess: requiredAccess,
                    accessMode: mode,
                    operationScope: operationScope,
                    databaseTransaction: nil
                )
                #endif
                return try await ActiveDatabaseTransactionContext.$binding
                    .withValue(executionBinding) {
                        let result: T
                        do {
                            result = try await operation(
                                admittedTransaction
                            )
                        } catch {
                            var terminalError: any Error = error
                            do {
                                try await operationScope.closeAndWait()
                            } catch let cleanupError as DatabaseReadScopeCleanupError {
                                terminalError = cleanupError.preserving(
                                    operationError: terminalError
                                )
                            }
                            admittedTransaction.revoke()
                            throw terminalError
                        }
                        do {
                            try await operationScope.closeAndWait()
                            admittedTransaction.revoke()
                            return result
                        } catch {
                            admittedTransaction.revoke()
                            throw error
                        }
                    }
            }
        }
    }

    /// Executes one read-only storage operation with both the selected data
    /// root and the absence of mutation capability represented by the value
    /// passed to the caller.
    internal func withReadStorageAccess<T: Sendable>(
        requiredAccess: Security.Access = .read,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionReadAccess
        ) async throws -> T
    ) async throws -> T {
        try await withAdmittedStorageAccess(
            requiredAccess: requiredAccess,
            mode: .readOnly,
            configuration: configuration,
            executionDeadline: executionDeadline
        ) { transaction in
            guard let admitted = transaction
                as? DataRootTransactionAccess else {
                throw DatabaseRuntimeError.internalError(
                    "Read storage access was not admitted through its data root"
                )
            }
            return try await operation(admitted.readProjection())
        }
    }

    /// Executes one mutation-capable storage operation after independently
    /// validating the caller's required authorization level.
    internal func withWriteStorageAccess<T: Sendable>(
        requiredAccess: DatabaseMutationAuthorization,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> T
    ) async throws -> T {
        try await withAdmittedStorageAccess(
            requiredAccess: requiredAccess.securityAccess,
            mode: .readWrite,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    /// Reads the backend snapshot identifier for diagnostics while retaining
    /// the same authorization, transaction runner, and data-root admission as
    /// an ordinary read. No control capability escapes this method.
    internal func currentStorageReadVersion(
        configuration: TransactionConfiguration = .default
    ) async throws -> Int64 {
        try await withAdmittedStorageAccess(
            requiredAccess: .read,
            mode: .readOnly,
            configuration: configuration
        ) { transaction in
            guard let admitted = transaction
                as? DataRootTransactionAccess else {
                throw DatabaseRuntimeError.internalError(
                    "Read version diagnostics require admitted storage access"
                )
            }
            return try await admitted.currentReadVersionForDiagnostics()
        }
    }

    private func requireAccess(
        _ access: Security.Access
    ) async throws {
        try await withReadStorageAccess(requiredAccess: access) { _ in () }
    }

    /// Executes one higher-level database operation while retaining the
    /// context's schema generation and, when enabled, its selected Base root.
    ///
    /// This SPI is consumed by operation adapters such as database-server. It
    /// does not expose transport, listener, framing, or process lifecycle.
    @_spi(DatabaseExecution)
    public func withDataOperation<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await container.withOperationSchemaLease { _ in
            #if DATABASE_MULTI_BASE
            if let dataRootBinding = ActiveDatabaseDataRootContext.binding {
                let current = dataRootBinding.lease
                guard current.resource == resource else {
                    throw DatabaseTransactionExecutionScopeError
                        .dataRootMismatch
                }
                let dataRootOperationLease = try dataRootBinding
                    .operationScope.beginRead()
                defer { dataRootOperationLease.finish() }
                switch resource {
                case .database:
                    break
                case .base(let baseID):
                    guard let binding = ActiveDatabaseBaseContext.binding,
                          binding.baseID == baseID,
                          binding.placementGeneration == current.generation,
                          binding.root == current.root,
                          binding.operationScope === dataRootBinding.operationScope
                    else {
                        throw DatabaseTransactionExecutionScopeError
                            .dataRootMismatch
                    }
                }
                return try await RequestAuthorization.$context.withValue(
                    authorization
                ) {
                    try await operation()
                }
            }
            let execute: @Sendable () async throws -> Result = {
                try await RequestAuthorization.$context.withValue(
                    self.authorization
                ) {
                    try await operation()
                }
            }
            switch resource {
            case .database:
                return try await container.withDatabaseDataRoot(execute)
            case .base(let baseID):
                let lease = try container.acquireBaseLease(baseID)
                return try await container.withBaseLease(lease, execute)
            }
            #else
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await operation()
            }
            #endif
        }
    }

    /// Root of the database data selected for this context. The lightweight
    /// runtime has one fixed root; `MultiBase` resolves the operation-bound
    /// Base lease before returning its root.
    package func operationDataRoot() throws -> Subspace {
        #if DATABASE_MULTI_BASE
        try requireOperationDataRoot().root
        #else
        container.databaseRoot
        #endif
    }

    @_spi(DatabaseExecution)
    public func executionStorage() throws -> DatabaseExecutionStorage {
        #if DATABASE_MULTI_BASE
        let lease = try requireOperationDataRoot()
        return DatabaseExecutionStorage(
            root: lease.root,
            resource: lease.resource,
            generation: lease.generation,
            domainIdentifier: lease.domain.id.value
        )
        #else
        return try container.executionStorage()
        #endif
    }

    #if DATABASE_MULTI_BASE
    package func requireOperationDataRoot() throws -> DatabaseDataRootLease {
        guard let dataRootBinding = ActiveDatabaseDataRootContext.binding else {
            throw DatabaseTransactionExecutionScopeError.dataRootMismatch
        }
        let lease = dataRootBinding.lease
        guard lease.resource == resource else {
            throw DatabaseTransactionExecutionScopeError.dataRootMismatch
        }
        try dataRootBinding.operationScope.validateOpen()
        if case .base(let baseID) = resource {
            guard let binding = ActiveDatabaseBaseContext.binding,
                  binding.baseID == baseID,
                  binding.placementGeneration == lease.generation,
                  binding.root == lease.root,
                  binding.operationScope === dataRootBinding.operationScope
            else {
                throw DatabaseTransactionExecutionScopeError.dataRootMismatch
            }
        }
        return lease
    }

    private func requireGrant(
        _ access: Security.Access,
        lease: DatabaseDataRootLease,
        transaction: any TransactionAccess
    ) async throws {
        try await DatabaseGrantStore(
            resource: resource,
            root: lease.root
        ).require(
            access,
            authorization: authorization,
            transaction: transaction
        )
    }
    #endif

    // MARK: - Cache Management

    /// Clear the read version cache
    ///
    /// Use after:
    /// - Schema changes that affect read compatibility
    /// - Testing scenarios requiring fresh reads
    /// - Recovery from errors
    public func clearReadVersionCache() {
        readVersionCache.clear()
    }

    /// Get current cache info for debugging/metrics
    ///
    /// - Returns: Tuple of (version, ageMillis), or nil if no cached value
    public func readVersionCacheInfo() -> (version: Int64, ageMillis: Int64)? {
        readVersionCache.currentCacheInfo()
    }
}

// MARK: - CustomStringConvertible

extension DatabaseContext: CustomStringConvertible {
    public var description: String {
        let stateDescription = stateLock.withLock { state in
            var saves = 0
            var deletes = 0
            state.pending.forEach { _, mutation in
                switch mutation {
                case .save:
                    saves += 1
                case .delete:
                    deletes += 1
                }
            }
            return (
                saves: saves,
                deletes: deletes,
                saveInProgress: state.activeSave != nil,
                commitOutcomeUnknown: state.commitOutcomeUnknown
            )
        }

        return """
        DatabaseContext(
            pendingSaves: \(stateDescription.saves),
            pendingDeletes: \(stateDescription.deletes),
            saveInProgress: \(stateDescription.saveInProgress),
            commitOutcomeUnknown: \(stateDescription.commitOutcomeUnknown),
            hasChanges: \(hasChanges)
        )
        """
    }
}

// MARK: - Polymorphic Fetch API

extension DatabaseContext {
    /// Create a polymorphic fetch builder for querying multiple types via a shared protocol
    ///
    /// Enables querying all concrete types that conform to a `@Polymorphable` protocol.
    /// Types are automatically discovered from the Schema.
    ///
    /// **Usage**:
    /// ```swift
    /// // Define polymorphic protocol
    /// @Polymorphable
    /// protocol Document {
    ///     var id: String { get }
    ///     var title: String { get }
    ///     #Directory<Document>("app", "documents")
    /// }
    ///
    /// // Conforming types are automatically discovered from Schema
    /// let schema = try Schema(
    ///     entities: [
    ///         try Article.schemaEntity,
    ///         try Report.schemaEntity,
    ///         try User.schemaEntity,
    ///     ],
    ///     version: .init(1, 0, 0)
    /// )
    ///
    /// // Fetch all documents
    /// let docs = try await context.fetchPolymorphic(Document.self)
    ///
    /// for snapshot in docs {
    ///     switch snapshot.entity {
    ///     case Article.persistableType:
    ///         let article = try snapshot.decode(as: Article.self)
    ///         print("Article: \(article.content)")
    ///     case Report.persistableType:
    ///         let report = try snapshot.decode(as: Report.self)
    ///         print("Report: \(report.data.count) bytes")
    ///     default:
    ///         throw PolymorphicQueryError.unknownType(snapshot.entity)
    ///     }
    /// }
    /// ```
    ///
    /// - Parameter protocolType: The Polymorphable protocol to query
    /// - Returns: Array of all items conforming to the protocol
    /// - Throws: Error if no types are found or if fetch fails
    public func fetchPolymorphic<P: Polymorphable>(
        _ protocolType: P.Type
    ) async throws -> [PersistedModel] {
        guard let group = container.schema.polymorphicGroup(
            identifier: P.polymorphableType
        ) else {
            throw PolymorphicRuntimeError.missingGroup(
                identifier: P.polymorphableType
            )
        }
        return try await scanPolymorphicItems(group: group).map { $0.item }
    }

    /// Fetch a specific polymorphic item by ID
    ///
    /// Searches all types in the Schema that conform to the protocol to find an item with the specified ID.
    ///
    /// - Parameters:
    ///   - protocolType: The Polymorphable protocol
    ///   - id: The ID to search for
    /// - Returns: The item if found, nil otherwise
    public func fetchPolymorphic<P: Polymorphable>(
        _ protocolType: P.Type,
        id: String
    ) async throws -> PersistedModel? {
        guard let group = container.schema.polymorphicGroup(
            identifier: P.polymorphableType
        ) else {
            throw PolymorphicRuntimeError.missingGroup(
                identifier: P.polymorphableType
            )
        }
        let identifiers = try polymorphicTypeMap(for: group).keys.map {
            Tuple([$0, id])
        }
        return try await fetchPolymorphicItems(
            group: group,
            ids: identifiers
        ).first?.item
    }

}

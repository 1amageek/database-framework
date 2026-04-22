import Foundation
import StorageKit
import Core
import Synchronization
import Logging

/// FDBContext - Central API for model persistence
///
/// A model context is central to fdb-runtime as it's responsible for managing
/// the entire lifecycle of your persistent models. You use a context to insert
/// new models, track and persist changes to those models, and to delete those
/// models when you no longer need them.
///
/// **Architecture** (Context-Centric Design):
/// - FDBContext provides high-level API for persistence
/// - **FDBContext owns transactions and ReadVersionCache** (not DBContainer)
/// - Container resolves directories from Persistable type's `#Directory` declaration
/// - FDBDataStore performs low-level FDB operations in the resolved directory
///
/// **Transaction Management**:
/// - Use `context.withTransaction()` for explicit transaction control
/// - ReadVersionCache is per-context for proper scoping per unit of work
/// - System operations (DirectoryLayer, Migration) use `container.engine.withTransaction()`
///
/// **Usage**:
/// ```swift
/// let context = container.newContext()
///
/// // Insert models (type-independent)
/// context.insert(user)      // User: Persistable
/// context.insert(product)   // Product: Persistable
///
/// // Save all changes atomically
/// try await context.save()
///
/// // Fetch models with Fluent API
/// let users = try await context.fetch(User.self)
///     .where(\.isActive == true)
///     .orderBy(\.name)
///     .limit(10)
///     .execute()
///
/// // Explicit transaction
/// try await context.withTransaction(configuration: .default) { tx in
///     let user = try await tx.get(User.self, id: userId)
///     // ...
/// }
///
/// // Get by ID
/// if let user = try await context.model(for: userId, as: User.self) {
///     print(user.name)
/// }
///
/// // Delete
/// context.delete(user)
/// try await context.save()
/// ```
public final class FDBContext: Sendable {
    // MARK: - Properties

    /// The container that owns this context
    public let container: DBContainer

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
    /// | `fetch()` | Configurable | `.server` (default), `.cached`, `.stale(N)` |
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
    ///     .cachePolicy(.stale(30))
    ///     .execute()
    /// ```
    private let readVersionCache: ReadVersionCache

    /// Change tracking state
    private let stateLock: Mutex<ContextState>

    /// Logger
    private let logger: Logger

    /// Cached stores keyed by (typeName, partitionPath) to avoid re-creation on every save()
    private let storeCache: Mutex<[StoreKey: FDBDataStore]> = Mutex([:])

    /// Error handler for autosave failures
    ///
    /// When set, this handler is called if autosave fails. This allows the caller
    /// to be notified of save failures that would otherwise be silently logged.
    ///
    /// **Usage**:
    /// ```swift
    /// let context = container.newContext(autosaveEnabled: true)
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

    /// Initialize FDBContext
    ///
    /// - Parameters:
    ///   - container: The DBContainer to use for storage
    ///   - autosaveEnabled: Whether to automatically save after insert/delete (default: false)
    public init(container: DBContainer, autosaveEnabled: Bool = false) {
        self.container = container
        self.readVersionCache = ReadVersionCache()
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
        self.logger = Logger(label: "com.fdb.runtime.context")
    }

    // MARK: - State

    /// Pending mutation for a ModelKey prior to `save()`.
    ///
    /// Introduced in Phase 1 (PendingMutation redesign) to replace the dual
    /// `insertedModels` / `deletedModels` maps, which silently dropped the old
    /// value when `delete(old) + insert(new)` collided on the same ID.
    ///
    /// Phase 2 attaches a `WritePrecondition` to every variant so the save path
    /// can enforce explicit existence / version checks instead of silent
    /// fallbacks. Producers:
    /// - `create(_:)`        → `.insert(new, .notExists)` (strict insert)
    /// - `upsert(_:)` / legacy `insert(_:)` → `.upsert(new, .none)` (blind write)
    /// - `replace(old:with:)` → `.replace(old, new, .exists)`
    /// - `delete(_:)`        → `.delete(old, .none)` by default (legacy idempotent)
    ///                         or `.exists` when the caller opts in
    internal enum PendingMutation: Sendable {
        case insert(new: any Persistable, precondition: WritePrecondition)
        case upsert(new: any Persistable, precondition: WritePrecondition)
        case delete(old: any Persistable, precondition: WritePrecondition)
        case replace(old: any Persistable, new: any Persistable, precondition: WritePrecondition)

        var precondition: WritePrecondition {
            switch self {
            case .insert(_, let p), .upsert(_, let p), .delete(_, let p), .replace(_, _, let p):
                return p
            }
        }
    }

    /// Snapshot of a single staged insert/upsert as captured from the pending map
    /// at `save()` time. Preserves strictness (create vs upsert) and precondition
    /// so the storage layer can enforce the user's intent and so an error rollback
    /// can restore the exact same pending state.
    internal struct StagedInsert: Sendable {
        let model: any Persistable
        let strict: Bool
        let precondition: WritePrecondition
    }

    /// Snapshot of a single staged replace (explicit old → new pair).
    internal struct StagedReplace: Sendable {
        let old: any Persistable
        let new: any Persistable
        let precondition: WritePrecondition
    }

    /// Snapshot of a single staged delete.
    internal struct StagedDelete: Sendable {
        let model: any Persistable
        let precondition: WritePrecondition
    }

    private struct ContextState: Sendable {
        /// Mutations keyed by ModelKey. Merge rules are applied on every
        /// `insert()` / `delete()` call so the map always holds the fused
        /// final intent for each key.
        var pending: [ModelKey: PendingMutation] = [:]

        /// Whether a save operation is currently in progress
        var isSaving: Bool = false

        /// Whether to automatically save after insert/delete operations
        var autosaveEnabled: Bool

        /// Whether an autosave task is already scheduled
        var autosaveScheduled: Bool = false

        /// Error handler for autosave failures
        var autosaveErrorHandler: (@Sendable (Error) -> Void)?

        /// Whether the context has unsaved changes
        var hasChanges: Bool {
            return !pending.isEmpty
        }

        init(autosaveEnabled: Bool = false) {
            self.autosaveEnabled = autosaveEnabled
        }
    }

    /// Key for grouping by (type, partition path) in save() and store caching
    private struct StoreKey: Hashable, Sendable {
        let typeName: String
        let resolvedPath: String
    }

    private func cachedStore<T: Persistable>(
        for type: T.Type
    ) async throws -> FDBDataStore {
        let storeKey = StoreKey(typeName: T.persistableType, resolvedPath: "")
        if let cached = storeCache.withLock({ $0[storeKey] }) {
            return cached
        }

        let store = try await container.fdbStore(for: type)
        storeCache.withLock { $0[storeKey] = store }
        return store
    }

    /// Point reads with `.server` consistency do not benefit from per-context store caching.
    ///
    /// Fresh contexts used by one-shot reads would otherwise pay local cache bookkeeping
    /// every time, even though DBContainer already shares resolved stores globally.
    private func pointReadStore<T: Persistable>(
        for type: T.Type
    ) async throws -> FDBDataStore {
        try await container.fdbStore(for: type)
    }

    private func cachedStore<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T>
    ) async throws -> FDBDataStore {
        let resolvedPath = AnyDirectoryPath(path).resolve().joined(separator: "/")
        let storeKey = StoreKey(typeName: T.persistableType, resolvedPath: resolvedPath)
        if let cached = storeCache.withLock({ $0[storeKey] }) {
            return cached
        }

        let store = try await container.fdbStore(for: type, path: path)
        storeCache.withLock { $0[storeKey] = store }
        return store
    }

    private func pointReadStore<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T>
    ) async throws -> FDBDataStore {
        try await container.fdbStore(for: type, path: path)
    }

    /// Single-model write/delete fast paths do not benefit from per-context store caching.
    ///
    /// One-shot contexts used by insert/update/delete benchmarks would otherwise pay local
    /// cache mutex and dictionary overhead even though DBContainer already shares stores.
    private func singleOperationStore(
        for type: any Persistable.Type
    ) async throws -> FDBDataStore {
        try await container.fdbStore(for: type)
    }

    private func pendingModelLookup<T: Persistable>(
        for id: any TupleElement,
        as type: T.Type
    ) -> (inserted: T?, isDeleted: Bool) {
        stateLock.withLock { state in
            guard state.hasChanges else {
                return (nil, false)
            }

            let key = ModelKey(persistableType: T.persistableType, id: id)
            guard let mutation = state.pending[key] else {
                return (nil, false)
            }

            switch mutation {
            case .insert(let new, _), .upsert(let new, _):
                if let typed = new as? T {
                    return (typed, false)
                }
                return (nil, false)
            case .replace(_, let new, _):
                if let typed = new as? T {
                    return (typed, false)
                }
                return (nil, false)
            case .delete:
                return (nil, true)
            }
        }
    }

    /// Key for tracking models
    private struct ModelKey: Hashable, Sendable {
        let persistableType: String
        let idBytes: [UInt8]

        init<T: Persistable>(_ model: T) {
            self.persistableType = T.persistableType
            self.idBytes = Self.packID(model.id)
        }

        init(persistableType: String, id: any Sendable) {
            self.persistableType = persistableType
            self.idBytes = Self.packID(id)
        }

        private static func packID(_ id: any Sendable) -> [UInt8] {
            // Tuple types (compound keys)
            if let tuple = id as? Tuple {
                return tuple.pack()
            }
            // TupleElement types (String, Int, UUID, etc.)
            if let element = id as? any TupleElement {
                return Tuple([element]).pack()
            }
            // UUID (common ID type, not TupleElement by default)
            if let uuid = id as? UUID {
                return Tuple([uuid.uuidString]).pack()
            }
            // Fallback: Use string representation
            // WARNING: This may cause inconsistencies for custom types
            // Consider making your ID type conform to TupleElement
            return Tuple([String(describing: id)]).pack()
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

    // MARK: - Write Operations (Phase 2 explicit API)

    /// Stage a strict insert. Fails at `save()` if a row with the same id already
    /// exists in storage (unless the caller overrides the precondition).
    ///
    /// Default precondition: `.notExists`.
    ///
    /// - Parameters:
    ///   - model: The model to insert.
    ///   - precondition: Assertion checked against stored state at commit time.
    public func create<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .notExists
    ) {
        mergeInsert(model: model, strict: true, precondition: precondition)
    }

    /// Stage a blind upsert. Writes the new value without existence checks.
    ///
    /// Default precondition: `.none`. Matches the legacy `insert()` behavior.
    ///
    /// - Parameters:
    ///   - model: The model to write.
    ///   - precondition: Assertion checked against stored state at commit time.
    public func upsert<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .none
    ) {
        mergeInsert(model: model, strict: false, precondition: precondition)
    }

    /// Stage an explicit old→new replacement. The supplied `old` is trusted for
    /// index maintenance — the framework does NOT re-read the pre-image from
    /// storage. Use this when the caller already holds the pre-image (e.g. read
    /// earlier in the same context) to avoid an extra storage round-trip.
    ///
    /// Default precondition: `.exists` — if the row is missing at commit time,
    /// `preconditionFailed` is thrown rather than silently downgrading to an
    /// insert.
    ///
    /// - Parameters:
    ///   - old: The pre-image (trusted; used for index maintenance).
    ///   - new: The post-image to write.
    ///   - precondition: Assertion checked against stored state at commit time.
    public func replace<T: Persistable>(
        old: T,
        with new: T,
        precondition: WritePrecondition = .exists
    ) {
        let key = ModelKey(new)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            let merged: PendingMutation
            switch state.pending[key] {
            case .none:
                merged = .replace(old: old, new: new, precondition: precondition)
            case .some(.insert):
                // A prior strict insert has not yet hit storage. Replacing its
                // new value in-place preserves the strict-insert intent.
                merged = .insert(new: new, precondition: precondition)
            case .some(.upsert):
                merged = .replace(old: old, new: new, precondition: precondition)
            case .some(.delete(let origOld, _)):
                merged = .replace(old: origOld, new: new, precondition: precondition)
            case .some(.replace(let origOld, _, _)):
                // Keep the original pre-image (the one before any staged mutation)
                // so index maintenance can accurately remove the on-disk entries.
                merged = .replace(old: origOld, new: new, precondition: precondition)
            }
            state.pending[key] = merged

            if state.autosaveEnabled && !state.autosaveScheduled {
                state.autosaveScheduled = true
                return true
            }
            return false
        }

        if shouldScheduleAutosave {
            scheduleAutosave()
        }
    }

    // MARK: - Insert (legacy alias)

    /// Register a model for persistence. Legacy entry point equivalent to
    /// `upsert(_:)` — retained for source compatibility.
    ///
    /// Merge rules against an existing pending mutation on the same ModelKey:
    /// - none               → `.upsert(new)`
    /// - `.upsert(_)`       → `.upsert(new)` (last write wins)
    /// - `.insert(_)`       → `.insert(new)` (preserve strict-insert intent)
    /// - `.delete(old)`     → `.replace(old, new)`  (Phase 1 bug fix)
    /// - `.replace(old, _)` → `.replace(old, new)`
    ///
    /// - Parameter model: The model to insert
    public func insert<T: Persistable>(_ model: T) {
        upsert(model)
    }

    /// Merge a new insert/upsert/create into the pending map.
    ///
    /// - Parameters:
    ///   - model: The model to stage.
    ///   - strict: `true` for `create(_:)` (produces `.insert`); `false` for
    ///             `upsert(_:)` / legacy `insert(_:)` (produces `.upsert`).
    ///   - precondition: Precondition for the staged operation (stored as-is
    ///                   unless a prior `.delete` / `.replace` is present, in
    ///                   which case `.replace` takes precedence).
    private func mergeInsert<T: Persistable>(
        model: T,
        strict: Bool,
        precondition: WritePrecondition
    ) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            let merged: PendingMutation
            switch state.pending[key] {
            case .none:
                merged = strict
                    ? .insert(new: model, precondition: precondition)
                    : .upsert(new: model, precondition: precondition)
            case .some(.upsert):
                // Upsert + create / upsert → keep the new variant's strictness.
                merged = strict
                    ? .insert(new: model, precondition: precondition)
                    : .upsert(new: model, precondition: precondition)
            case .some(.insert):
                // A prior strict insert upgrades any follow-up to strict too,
                // preserving the notExists intent.
                merged = .insert(new: model, precondition: precondition)
            case .some(.delete(let old, _)):
                merged = .replace(old: old, new: model, precondition: precondition)
            case .some(.replace(let old, _, _)):
                merged = .replace(old: old, new: model, precondition: precondition)
            }
            state.pending[key] = merged

            if state.autosaveEnabled && !state.autosaveScheduled {
                state.autosaveScheduled = true
                return true
            }
            return false
        }

        if shouldScheduleAutosave {
            scheduleAutosave()
        }
    }

    // MARK: - Delete

    /// Mark a model for deletion.
    ///
    /// Merge rules against an existing pending mutation on the same ModelKey:
    /// - none               → `.delete(old)`
    /// - `.insert(_)`       → erase entry (cancel the uncommitted insert; warn if
    ///                         the key may refer to an existing stored row).
    /// - `.upsert(_)`       → `.delete(old)` (last write wins; the upsert is dropped)
    /// - `.delete(_)`       → `.delete(old)` (last write wins)
    /// - `.replace(origOld, _)` → `.delete(origOld)` (keep the pre-replace old)
    ///
    /// - Parameters:
    ///   - model: The model to delete.
    ///   - precondition: Assertion checked against stored state at commit time.
    ///                   Defaults to `.none` for source-compatibility with the
    ///                   pre-Phase-2 idempotent behavior. Pass `.exists` to
    ///                   detect deletes that target a missing row.
    public func delete<T: Persistable>(
        _ model: T,
        precondition: WritePrecondition = .none
    ) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            switch state.pending[key] {
            case .none:
                state.pending[key] = .delete(old: model, precondition: precondition)
            case .some(.insert):
                state.pending.removeValue(forKey: key)
                self.logger.warning(
                    "delete() cancelled a pending insert for \(T.persistableType). If a stored row exists for this id, its delete intent has been dropped."
                )
            case .some(.upsert):
                state.pending[key] = .delete(old: model, precondition: precondition)
            case .some(.delete):
                state.pending[key] = .delete(old: model, precondition: precondition)
            case .some(.replace(let origOld, _, _)):
                state.pending[key] = .delete(old: origOld, precondition: precondition)
            }

            if state.autosaveEnabled && !state.autosaveScheduled && state.hasChanges {
                state.autosaveScheduled = true
                return true
            }
            return false
        }

        if shouldScheduleAutosave {
            scheduleAutosave()
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
            delete(model)
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
            delete(model)
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
    public func deleteAll<T: Persistable, V: Sendable & Equatable & FieldValueConvertible>(
        _ type: T.Type,
        partition keyPath: KeyPath<T, V>,
        equals value: V
    ) async throws {
        let models = try await fetch(Query<T>().partition(keyPath, equals: value))
        for model in models {
            delete(model)
        }
    }

    /// Clear all data for a type without decoding (useful for tests and schema migrations)
    ///
    /// Unlike `deleteAll`, this method directly clears the subspace range without
    /// loading/decoding any data. Use this when:
    /// - Schema has changed and old data cannot be decoded
    /// - You need efficient bulk deletion without per-record operations
    ///
    /// **Security**: Admin-only operation. Requires admin role in auth context.
    ///
    /// **Note**: This does NOT track deletions in the context. Changes are applied immediately.
    /// **Note**: Also clears polymorphic directory data if type conforms to Polymorphable.
    /// **Note**: For types with dynamic directories, use `clearAll(_:partition:equals:)` instead.
    ///
    /// - Parameter type: The Persistable type to clear
    /// - Throws: SecurityError if not admin, DirectoryPathError if type has dynamic directory
    public func clearAll<T: Persistable>(_ type: T.Type) async throws {
        // Validate: dynamic directory types require partition
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }

        // Use DataStore.clearAll() which evaluates admin security
        let store = try await cachedStore(for: type)
        try await store.clearAll(type)

        // Also clear polymorphic directory data if applicable
        if let polymorphicType = T.self as? any Polymorphable.Type {
            let typeDirectory = T.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
            let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")

            if typeDirectory != polyDirectory {
                // Different directory - need to clear polymorphic data too
                let polySubspace = try await container.resolvePolymorphicDirectory(for: polymorphicType)
                let typeCode = polymorphicType.typeCode(for: T.persistableType)
                let polyItemSubspace = polySubspace.subspace(SubspaceKey.items).subspace(typeCode)

                try await self.withRawTransaction(configuration: .batch) { transaction in
                    let (polyBegin, polyEnd) = polyItemSubspace.range()
                    transaction.clearRange(beginKey: polyBegin, endKey: polyEnd)
                }
            }
        }
    }

    /// Clear all data for a type within a partition without decoding
    ///
    /// For types with dynamic directories, use this method to specify the partition.
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.clearAll(Order.self, partition: \.tenantID, equals: "tenant_123")
    /// ```
    public func clearAll<T: Persistable, V: Sendable & Equatable & FieldValueConvertible>(
        _ type: T.Type,
        partition keyPath: KeyPath<T, V>,
        equals value: V
    ) async throws {
        var binding = DirectoryPath<T>()
        binding.set(keyPath, to: value)
        let store = try await cachedStore(for: type, path: binding)
        try await store.clearAll(type)
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
    ///     .where(\.isActive == true)
    ///     .orderBy(\.createdAt, .descending)
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
    public func cursor<T: Persistable & Codable>(
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

        // Get store for this type (partition-aware if needed)
        let store: any DataStore
        if let binding = query.partitionBinding {
            store = try await cachedStore(for: T.self, path: binding)
        } else {
            store = try await cachedStore(for: T.self)
        }

        // Create TransactionConfiguration with CachePolicy
        let config = TransactionConfiguration(cachePolicy: query.cachePolicy)

        // Execute fetch within transaction (uses ReadVersionCache)
        return try await self.withRawTransaction(configuration: config) { transaction in
            guard let fdbStore = store as? FDBDataStore else {
                // Fall back to original behavior if not FDBDataStore
                return try await store.fetch(query)
            }
            return try await fdbStore.fetchInTransaction(query, transaction: transaction)
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

        // Get store (partition-aware if needed)
        let store: any DataStore
        if let binding = query.partitionBinding {
            store = try await cachedStore(for: T.self, path: binding)
        } else {
            store = try await cachedStore(for: T.self)
        }

        // Create TransactionConfiguration with CachePolicy
        let config = TransactionConfiguration(cachePolicy: query.cachePolicy)

        // Execute count within transaction (uses ReadVersionCache)
        return try await self.withRawTransaction(configuration: config) { transaction in
            guard let fdbStore = store as? FDBDataStore else {
                // Fall back to original behavior if not FDBDataStore
                return try await store.fetchCount(query)
            }
            return try await fdbStore.fetchCountInTransaction(query, transaction: transaction)
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
        for id: any TupleElement,
        as type: T.Type,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        let pendingResult = pendingModelLookup(for: id, as: type)

        if let inserted = pendingResult.inserted {
            // Evaluate GET security for pending insert (not via DataStore)
            try container.securityDelegate?.evaluateGet(inserted)
            return inserted
        }

        if pendingResult.isDeleted {
            return nil
        }

        // Validate: dynamic directory types require partition
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }

        // Auto-commit for default cache policy (single point read, no transaction needed).
        // Non-default cache policies require TransactionRunner for ReadVersionCache support.
        if case .server = cachePolicy {
            let store = try await pointReadStore(for: type)
            return try await store.withAutoCommit { transaction in
                try await store.fetchByIdInTransaction(type, id: id, transaction: transaction)
            }
        } else {
            let store = try await cachedStore(for: type)
            let config = TransactionConfiguration(cachePolicy: cachePolicy)
            return try await self.withRawTransaction(configuration: config) { transaction in
                try await store.fetchByIdInTransaction(type, id: id, transaction: transaction)
            }
        }
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
        for id: any TupleElement,
        as type: T.Type,
        partition path: DirectoryPath<T>,
        cachePolicy: CachePolicy = .server
    ) async throws -> T? {
        let pendingResult = pendingModelLookup(for: id, as: type)

        if let inserted = pendingResult.inserted {
            // Evaluate GET security for pending insert (not via DataStore)
            try container.securityDelegate?.evaluateGet(inserted)
            return inserted
        }

        if pendingResult.isDeleted {
            return nil
        }

        // Auto-commit for default cache policy (single point read, no transaction needed).
        // Non-default cache policies require TransactionRunner for ReadVersionCache support.
        if case .server = cachePolicy {
            let store = try await pointReadStore(for: type, path: path)
            return try await store.withAutoCommit { transaction in
                try await store.fetchByIdInTransaction(type, id: id, transaction: transaction)
            }
        } else {
            let store = try await cachedStore(for: type, path: path)
            let config = TransactionConfiguration(cachePolicy: cachePolicy)
            return try await self.withRawTransaction(configuration: config) { transaction in
                try await store.fetchByIdInTransaction(type, id: id, transaction: transaction)
            }
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
            case singleInsert(StagedInsert)
            case singleDelete(StagedDelete)
            case proceed(
                inserts: [StagedInsert],
                replaces: [StagedReplace],
                deletes: [StagedDelete]
            )
        }

        let checkResult = stateLock.withLock { state -> SaveCheckResult in
            guard !state.isSaving else {
                return .alreadySaving
            }

            guard state.hasChanges else {
                state.autosaveScheduled = false
                return .noChanges
            }

            // Fast path: exactly one mutation in the pending map, and that mutation
            // is a pure upsert or pure delete WITH `.none` precondition. `.replace`
            // always needs the general path so the user-provided old value reaches
            // IndexMaintenanceService; non-`.none` preconditions need the general
            // path so precondition evaluation happens against storage.
            if state.pending.count == 1, let (_, only) = state.pending.first {
                switch only {
                case .upsert(let model, let p) where p == .none:
                    state.pending.removeAll()
                    state.isSaving = true
                    state.autosaveScheduled = false
                    return .singleInsert(StagedInsert(model: model, strict: false, precondition: p))
                case .insert, .upsert:
                    break  // fall through to general path (precondition enforcement)
                case .delete(let model, let p) where p == .none:
                    state.pending.removeAll()
                    state.isSaving = true
                    state.autosaveScheduled = false
                    return .singleDelete(StagedDelete(model: model, precondition: p))
                case .delete:
                    break  // fall through to general path (precondition enforcement)
                case .replace:
                    break  // fall through to general path
                }
            }

            var inserts: [StagedInsert] = []
            var replaces: [StagedReplace] = []
            var deletes: [StagedDelete] = []

            for (_, mutation) in state.pending {
                switch mutation {
                case .insert(let new, let p):
                    inserts.append(StagedInsert(model: new, strict: true, precondition: p))
                case .upsert(let new, let p):
                    inserts.append(StagedInsert(model: new, strict: false, precondition: p))
                case .delete(let old, let p):
                    deletes.append(StagedDelete(model: old, precondition: p))
                case .replace(let old, let new, let p):
                    replaces.append(StagedReplace(old: old, new: new, precondition: p))
                }
            }

            state.pending.removeAll()
            state.isSaving = true
            state.autosaveScheduled = false

            return .proceed(inserts: inserts, replaces: replaces, deletes: deletes)
        }

        switch checkResult {
        case .noChanges:
            return
        case .alreadySaving:
            throw FDBContextError.concurrentSaveNotAllowed
        case .singleInsert(let staged):
            do {
                if let fastResult = try await saveSingleOperationFastPathIfEligible(
                    staged.model,
                    isDelete: false
                ) {
                    _ = fastResult
                } else {
                    try await saveGeneralPath(
                        insertsSnapshot: [staged],
                        replacesSnapshot: [],
                        deletesSnapshot: []
                    )
                }

                stateLock.withLock { state in
                    state.isSaving = false
                }
            } catch {
                restoreSingleInsert(staged)
                throw error
            }
            return
        case .singleDelete(let staged):
            do {
                if let fastResult = try await saveSingleOperationFastPathIfEligible(
                    staged.model,
                    isDelete: true
                ) {
                    _ = fastResult
                } else {
                    try await saveGeneralPath(
                        insertsSnapshot: [],
                        replacesSnapshot: [],
                        deletesSnapshot: [staged]
                    )
                }

                stateLock.withLock { state in
                    state.isSaving = false
                }
            } catch {
                restoreSingleDelete(staged)
                throw error
            }
            return
        case .proceed(let inserts, let replaces, let deletes):
            guard !inserts.isEmpty || !replaces.isEmpty || !deletes.isEmpty else {
                stateLock.withLock { state in
                    state.isSaving = false
                }
                return
            }

            do {
                try await saveGeneralPath(
                    insertsSnapshot: inserts,
                    replacesSnapshot: replaces,
                    deletesSnapshot: deletes
                )

                stateLock.withLock { state in
                    state.isSaving = false
                }
            } catch {
                stateLock.withLock { state in
                    for staged in inserts {
                        let key = ModelKey(
                            persistableType: type(of: staged.model).persistableType,
                            id: staged.model.id
                        )
                        state.pending[key] = staged.strict
                            ? .insert(new: staged.model, precondition: staged.precondition)
                            : .upsert(new: staged.model, precondition: staged.precondition)
                    }
                    for staged in replaces {
                        let key = ModelKey(
                            persistableType: type(of: staged.new).persistableType,
                            id: staged.new.id
                        )
                        state.pending[key] = .replace(
                            old: staged.old,
                            new: staged.new,
                            precondition: staged.precondition
                        )
                    }
                    for staged in deletes {
                        let key = ModelKey(
                            persistableType: type(of: staged.model).persistableType,
                            id: staged.model.id
                        )
                        state.pending[key] = .delete(
                            old: staged.model,
                            precondition: staged.precondition
                        )
                    }
                    state.isSaving = false
                }
                throw error
            }
        }
    }

    // MARK: - Save Fast Path

    /// Attempt fast path for single-model save with static directory.
    ///
    /// Returns `true` if the fast path was taken, `nil` if not eligible.
    /// Eligible when: exactly 1 insert + 0 deletes (or vice versa),
    /// static directory (no dynamic partition), and not Polymorphable.
    private func saveSingleOperationFastPathIfEligible(
        _ model: any Persistable,
        isDelete: Bool
    ) async throws -> Bool? {
        let modelType = type(of: model)

        // Not eligible if dynamic directory (needs partition path resolution)
        guard !hasDynamicDirectory(modelType) else { return nil }

        // Not eligible if Polymorphable (needs dual-write processing)
        guard !(modelType is any Polymorphable.Type) else { return nil }

        // Not eligible if model has indexes (index updates need atomicity with data write)
        guard modelType.indexDescriptors.isEmpty else { return nil }

        // Not eligible if security is enabled (CREATE/UPDATE evaluation needs existing record check)
        guard container.securityDelegate == nil else { return nil }

        // Reuse the container-wide store cache directly for single-operation contexts.
        let store = try await singleOperationStore(for: modelType)

        // Execute in auto-commit mode (no BEGIN/COMMIT for single operations)
        if !isDelete {
            _ = try await store.withAutoCommit { transaction in
                try await store.executeBatchInTransaction(
                    inserts: [model],
                    deletes: [],
                    transaction: transaction,
                    skipExistingCheck: true
                )
            }
        } else {
            _ = try await store.withAutoCommit { transaction in
                try await store.executeBatchInTransaction(
                    inserts: [],
                    deletes: [model],
                    transaction: transaction,
                    skipExistingCheck: true
                )
            }
        }

        return true
    }

    private func restoreSingleInsert(_ staged: StagedInsert) {
        stateLock.withLock { state in
            let key = ModelKey(
                persistableType: type(of: staged.model).persistableType,
                id: staged.model.id
            )
            state.pending[key] = staged.strict
                ? .insert(new: staged.model, precondition: staged.precondition)
                : .upsert(new: staged.model, precondition: staged.precondition)
            state.isSaving = false
        }
    }

    private func restoreSingleDelete(_ staged: StagedDelete) {
        stateLock.withLock { state in
            let key = ModelKey(
                persistableType: type(of: staged.model).persistableType,
                id: staged.model.id
            )
            state.pending[key] = .delete(old: staged.model, precondition: staged.precondition)
            state.isSaving = false
        }
    }

    /// General path for multi-model or dynamic-directory save operations.
    private func saveGeneralPath(
        insertsSnapshot: [StagedInsert],
        replacesSnapshot: [StagedReplace],
        deletesSnapshot: [StagedDelete]
    ) async throws {
        // Group models by (type, partition) for partition-aware batching
        var insertsByStore: [StoreKey: [StagedInsert]] = [:]
        var replacesByStore: [StoreKey: [StagedReplace]] = [:]
        var deletesByStore: [StoreKey: [StagedDelete]] = [:]

        for staged in insertsSnapshot {
            let modelType = type(of: staged.model)
            let typeName = modelType.persistableType
            let resolvedPath = resolvePartitionPath(for: staged.model)
            let key = StoreKey(typeName: typeName, resolvedPath: resolvedPath)
            insertsByStore[key, default: []].append(staged)
        }

        for staged in replacesSnapshot {
            // Replace keys are derived from `new` (the post-state); `old` and `new`
            // share the same ModelKey by construction (merge rule invariant).
            let modelType = type(of: staged.new)
            let typeName = modelType.persistableType
            let resolvedPath = resolvePartitionPath(for: staged.new)
            let key = StoreKey(typeName: typeName, resolvedPath: resolvedPath)
            replacesByStore[key, default: []].append(staged)
        }

        for staged in deletesSnapshot {
            let modelType = type(of: staged.model)
            let typeName = modelType.persistableType
            let resolvedPath = resolvePartitionPath(for: staged.model)
            let key = StoreKey(typeName: typeName, resolvedPath: resolvedPath)
            deletesByStore[key, default: []].append(staged)
        }

        let allStoreKeys = Set(insertsByStore.keys)
            .union(replacesByStore.keys)
            .union(deletesByStore.keys)

        // Resolve stores with caching (avoids FDBDataStore re-creation across save() calls)
        var resolvedStores: [StoreKey: FDBDataStore] = [:]
        for storeKey in allStoreKeys {
            // Check cache first
            if let cached = storeCache.withLock({ $0[storeKey] }) {
                resolvedStores[storeKey] = cached
                continue
            }

            let sampleModel: (any Persistable)? =
                insertsByStore[storeKey]?.first?.model
                ?? replacesByStore[storeKey]?.first?.new
                ?? deletesByStore[storeKey]?.first?.model
            guard let model = sampleModel else { continue }

            let modelType = type(of: model)
            let store: FDBDataStore

            if hasDynamicDirectory(modelType) {
                let binding = buildAnyDirectoryPath(from: model)
                store = try await container.fdbStore(for: modelType, path: binding)
            } else {
                store = try await container.fdbStore(for: modelType)
            }
            resolvedStores[storeKey] = store
            storeCache.withLock { $0[storeKey] = store }
        }
        let storesByKey = resolvedStores

        // Make immutable copies for Sendable closure
        let insertsForTransaction = insertsByStore
        let replacesForTransaction = replacesByStore
        let deletesForTransaction = deletesByStore

        // Get any store to provide the transaction (all stores share the same database)
        guard storesByKey.values.first != nil else {
            return
        }

        // Route through the context's own raw transaction so the commit updates
        // the ReadVersionCache. Using FDBDataStore.withRawTransaction here would
        // bypass the cache and leave subsequent `.cached` reads observing the
        // pre-commit version (visible as: delete+insert RMW appearing to "lose"
        // the update until a fresh read version is acquired).
        try await self.withRawTransaction { transaction in
            var allSerializedModels: [SerializedModel] = []

            // Batch process inserts per (type, partition)
            // skipExistingCheck: inserts from FDBContext are known new records,
            // so we can skip the storage.read() + clearAllBlobs() overhead. The
            // storage layer is responsible for evaluating `staged.precondition`
            // (Phase 2) and throwing `FDBContextError.preconditionFailed` on
            // violations.
            for (storeKey, stagedItems) in insertsForTransaction {
                guard let store = storesByKey[storeKey] else { continue }
                let models = stagedItems.map { $0.model }
                let preconditions = stagedItems.map { $0.precondition }
                let serialized = try await store.executeBatchInTransactionWithPreconditions(
                    inserts: models,
                    deletes: [],
                    transaction: transaction,
                    skipExistingCheck: true,
                    insertPreconditions: preconditions,
                    deletePreconditions: []
                )
                allSerializedModels.append(contentsOf: serialized)
            }

            // Batch process replaces per (type, partition) — old is user-provided,
            // so index maintenance skips the storage re-read. skipExistingCheck is
            // false because replace semantically expects the row to exist and we
            // want the storage layer to detect the mismatch via ItemStorage write.
            for (storeKey, stagedItems) in replacesForTransaction {
                guard let store = storesByKey[storeKey] else { continue }
                let pairs = stagedItems.map { (old: $0.old, new: $0.new) }
                let preconditions = stagedItems.map { $0.precondition }
                let serialized = try await store.executeReplaceInTransaction(
                    pairs: pairs,
                    transaction: transaction,
                    preconditions: preconditions
                )
                allSerializedModels.append(contentsOf: serialized)
            }

            // Batch process deletes per (type, partition)
            // skipExistingCheck: enables blob cleanup skip for no-index, no-security types
            for (storeKey, stagedItems) in deletesForTransaction {
                guard let store = storesByKey[storeKey] else { continue }
                let models = stagedItems.map { $0.model }
                let preconditions = stagedItems.map { $0.precondition }
                try await store.executeBatchInTransactionWithPreconditions(
                    inserts: [],
                    deletes: models,
                    transaction: transaction,
                    skipExistingCheck: true,
                    insertPreconditions: [],
                    deletePreconditions: preconditions
                )
            }

            // Batch handle Polymorphable dual-write (reusing serialized data)
            try await self.processDualWrites(
                serializedInserts: allSerializedModels,
                deletes: deletesSnapshot.map { $0.model },
                transaction: transaction
            )
        }
    }

    // MARK: - Partition Helpers

    /// Check if a type has dynamic directory (contains Field components)
    ///
    /// Uses `DynamicDirectoryElement` protocol for explicit Field identification.
    private func hasDynamicDirectory(_ type: any Persistable.Type) -> Bool {
        type.directoryPathComponents.contains { $0 is any DynamicDirectoryElement }
    }

    /// Resolve the partition path for a model (empty string for static directories)
    private func resolvePartitionPath(for model: any Persistable) -> String {
        let modelType = type(of: model)
        guard hasDynamicDirectory(modelType) else {
            return ""
        }

        // Build path by extracting Field values from model
        var path: [String] = []
        for component in modelType.directoryPathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            } else if let dynamicElement = component as? any DynamicDirectoryElement {
                // Extract field value using anyKeyPath from DynamicDirectoryElement
                let fieldName = modelType.fieldName(for: dynamicElement.anyKeyPath)
                if let value = model[dynamicMember: fieldName] {
                    path.append(directoryPathString(from: value))
                }
            }
        }
        return path.joined(separator: "/")
    }

    /// Build type-erased partition binding from a model instance
    ///
    /// Uses `DynamicDirectoryElement.anyKeyPath` to extract Field keyPaths without Mirror.
    private func buildAnyDirectoryPath(from model: any Persistable) -> AnyDirectoryPath {
        let modelType = type(of: model)
        var bindings: [(keyPath: AnyKeyPath, value: any Sendable)] = []

        for component in modelType.directoryPathComponents {
            if let dynamicElement = component as? any DynamicDirectoryElement {
                let keyPath = dynamicElement.anyKeyPath
                let fieldName = modelType.fieldName(for: keyPath)
                if let value = model[dynamicMember: fieldName] {
                    bindings.append((keyPath, value))
                }
            }
        }

        return AnyDirectoryPath(fieldValues: bindings, type: modelType)
    }

    // MARK: - Polymorphable Dual-Write Support

    /// Process dual-writes for Polymorphable types
    ///
    /// Optimized batch processing:
    /// - Groups models by polymorphic protocol
    /// - Resolves directories once per protocol
    /// - Reuses pre-serialized data (no re-serialization)
    ///
    /// - Parameters:
    ///   - serializedInserts: Pre-serialized insert models from DataStore
    ///   - deletes: Models to delete
    ///   - transaction: The transaction to use
    private func processDualWrites(
        serializedInserts: [SerializedModel],
        deletes: [any Persistable],
        transaction: any Transaction
    ) async throws {
        // Group by polymorphic protocol type for efficient directory resolution
        var insertsByPolyType: [ObjectIdentifier: [(SerializedModel, any Polymorphable.Type)]] = [:]
        var deletesByPolyType: [ObjectIdentifier: [(any Persistable, Tuple, any Polymorphable.Type)]] = [:]

        // Categorize inserts
        for serialized in serializedInserts {
            let modelType = type(of: serialized.model)
            guard let polymorphicType = modelType as? any Polymorphable.Type else { continue }

            // Check if dual-write is needed (different directories)
            let typeDirectory = modelType.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
            let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
            guard typeDirectory != polyDirectory else { continue }

            let key = ObjectIdentifier(polymorphicType)
            insertsByPolyType[key, default: []].append((serialized, polymorphicType))
        }

        // Categorize deletes
        for model in deletes {
            let modelType = type(of: model)
            guard let polymorphicType = modelType as? any Polymorphable.Type else { continue }

            // Check if dual-write is needed
            let typeDirectory = modelType.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
            let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
            guard typeDirectory != polyDirectory else { continue }

            // Compute ID tuple
            guard let tupleElement = model.id as? any TupleElement else { continue }
            let idTuple = (tupleElement as? Tuple) ?? Tuple([tupleElement])

            let key = ObjectIdentifier(polymorphicType)
            deletesByPolyType[key, default: []].append((model, idTuple, polymorphicType))
        }

        // Process inserts by protocol (resolve directory once per protocol)
        for (_, items) in insertsByPolyType {
            guard let (_, polymorphicType) = items.first else { continue }

            let group = try container.polymorphicGroup(identifier: polymorphicType.polymorphableType)
            let polySubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
            let itemSubspace = polySubspace.subspace(SubspaceKey.items)
            let blobsSubspace = polySubspace.subspace(SubspaceKey.blobs)
            let maintenanceService = makePolymorphicIndexMaintenanceService(
                group: group,
                subspace: polySubspace
            )

            // Use ItemStorage for large value handling (stores chunks in blobs subspace)
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )

            for (serialized, _) in items {
                let modelType = type(of: serialized.model)
                let typeCode = polymorphicType.typeCode(for: modelType.persistableType)
                let compositeID = makePolymorphicCompositeID(
                    typeCode: typeCode,
                    idTuple: serialized.idTuple
                )
                let polyKey = itemSubspace.pack(compositeID)

                let oldModel: (any Persistable)?
                if let existingData = try await storage.read(for: polyKey) {
                    oldModel = try DataAccess.deserializeAny(existingData, as: modelType)
                } else {
                    oldModel = nil
                }

                // Write using pre-serialized data (handles compression + external storage for >90KB)
                try await storage.write(serialized.data, for: polyKey)
                let descriptors = container.schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberType: modelType
                )
                try await maintenanceService.updateIndexesUntyped(
                    oldModel: oldModel,
                    newModel: serialized.model,
                    id: compositeID,
                    descriptors: descriptors,
                    logicalTypeName: group.identifier,
                    transaction: transaction
                )
            }
        }

        // Process deletes by protocol
        for (_, items) in deletesByPolyType {
            guard let (_, _, polymorphicType) = items.first else { continue }

            let group = try container.polymorphicGroup(identifier: polymorphicType.polymorphableType)
            let polySubspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
            let itemSubspace = polySubspace.subspace(SubspaceKey.items)
            let blobsSubspace = polySubspace.subspace(SubspaceKey.blobs)
            let maintenanceService = makePolymorphicIndexMaintenanceService(
                group: group,
                subspace: polySubspace
            )

            // Use ItemStorage for large value handling
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )

            for (model, idTuple, _) in items {
                let modelType = type(of: model)
                let typeCode = polymorphicType.typeCode(for: modelType.persistableType)
                let compositeID = makePolymorphicCompositeID(typeCode: typeCode, idTuple: idTuple)
                let polyKey = itemSubspace.pack(compositeID)

                // Delete (handles external blob chunks)
                let descriptors = container.schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberType: modelType
                )
                try await maintenanceService.updateIndexesUntyped(
                    oldModel: model,
                    newModel: nil as (any Persistable)?,
                    id: compositeID,
                    descriptors: descriptors,
                    logicalTypeName: group.identifier,
                    transaction: transaction
                )
                try await storage.delete(for: polyKey)
            }
        }
    }

    private func makePolymorphicCompositeID(
        typeCode: Int64,
        idTuple: Tuple
    ) -> Tuple {
        var elements: [any TupleElement] = [typeCode]
        for index in 0..<idTuple.count {
            if let element = idTuple[index] {
                elements.append(element)
            }
        }
        return Tuple(elements)
    }

    private func makePolymorphicIndexMaintenanceService(
        group: PolymorphicGroup,
        subspace: Subspace
    ) -> IndexMaintenanceService {
        let groupConfigurations = group.indexes.flatMap { index in
            container.indexConfigurations[index.name] ?? []
        }
        return IndexMaintenanceService(
            indexStateManager: IndexStateManager(container: container, subspace: subspace),
            violationTracker: UniquenessViolationTracker(
                container: container,
                metadataSubspace: subspace.subspace(SubspaceKey.metadata)
            ),
            indexSubspace: subspace.subspace(SubspaceKey.indexes),
            configurations: groupConfigurations
        )
    }

    // MARK: - Rollback

    /// Discard all pending changes
    public func rollback() {
        stateLock.withLock { state in
            state.pending.removeAll()
            state.isSaving = false
            state.autosaveScheduled = false
        }
    }

    // MARK: - Autosave

    private func scheduleAutosave() {
        Task { [weak self] in
            do {
                try await Task.sleep(nanoseconds: 10_000_000)  // 10ms
            } catch {
                return
            }

            guard let self = self else { return }

            let (shouldSave, errorHandler) = self.stateLock.withLock { state in
                (state.hasChanges && state.autosaveEnabled, state.autosaveErrorHandler)
            }

            if shouldSave {
                do {
                    try await self.save()
                } catch {
                    self.logger.error("Autosave failed: \(error)")

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
        // Validate: dynamic directory types require partition
        if T.hasDynamicDirectory {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: T.persistableType,
                fields: T.directoryFieldNames
            )
        }
        let store = try await cachedStore(for: type)
        let models = try await store.fetchAll(type)
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
    public func enumerate<T: Persistable, V: Sendable & Equatable & FieldValueConvertible>(
        _ type: T.Type,
        partition keyPath: KeyPath<T, V>,
        equals value: V,
        block: (T) throws -> Void
    ) async throws {
        var binding = DirectoryPath<T>()
        binding.set(keyPath, to: value)
        let store = try await cachedStore(for: type, path: binding)
        let models = try await store.fetchAll(type)
        for model in models {
            try block(model)
        }
    }
}

// MARK: - Errors

public enum FDBContextError: Error, CustomStringConvertible {
    case concurrentSaveNotAllowed
    case modelNotFound(String)
    case transactionTooLarge(currentSize: Int, limit: Int, hint: String)

    /// A `WritePrecondition` was violated at commit time.
    ///
    /// - `typeName`: `Persistable.persistableType` of the offending model.
    /// - `idDescription`: String form of the primary key (for diagnostics only — the raw
    ///   id bytes are intentionally not exposed to avoid leaking binary key material).
    /// - `precondition`: The precondition that failed.
    /// - `reason`: Human-readable explanation (e.g. "row already exists",
    ///   "row not found", "stored version mismatch").
    case preconditionFailed(
        typeName: String,
        idDescription: String,
        precondition: WritePrecondition,
        reason: String
    )

    public var description: String {
        switch self {
        case .concurrentSaveNotAllowed:
            return "FDBContextError: Cannot save while another save operation is in progress"
        case .modelNotFound(let type):
            return "FDBContextError: Model of type '\(type)' not found"
        case .transactionTooLarge(let currentSize, let limit, let hint):
            return "FDBContextError: Transaction size (\(currentSize) bytes) approaching limit (\(limit) bytes). \(hint)"
        case .preconditionFailed(let typeName, let idDescription, let precondition, let reason):
            return "FDBContextError: Precondition \(precondition) failed for \(typeName) id=\(idDescription): \(reason)"
        }
    }
}

// MARK: - Transaction API

extension FDBContext {
    /// Execute a transactional operation with configurable retry and timeout
    ///
    /// Provides Firestore-like transaction semantics with explicit read isolation control.
    /// The closure may be retried on conflict - avoid side effects inside the closure.
    ///
    /// **Read Modes**:
    /// - `tx.get(snapshot: false)` (default): Transactional read that adds read conflict.
    ///   If another transaction writes to this data before commit, this transaction
    ///   will conflict and retry.
    /// - `tx.get(snapshot: true)`: Snapshot read with no conflict tracking.
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
    ///     guard let user = try await tx.get(User.self, id: userId) else { throw ... }
    ///     var updated = user
    ///     updated.balance -= amount
    ///     try await tx.set(updated)
    /// }
    ///
    /// // Mixed reads
    /// try await context.withTransaction { tx in
    ///     let user = try await tx.get(User.self, id: userId)           // Transactional
    ///     let stats = try await tx.get(Stats.self, id: id, snapshot: true) // Snapshot
    ///     try await tx.set(updated)
    /// }
    ///
    /// // Batch configuration
    /// try await context.withTransaction(configuration: .batch) { tx in
    ///     for item in items {
    ///         try await tx.set(item)
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
        _ operation: @Sendable @escaping (TransactionContext) async throws -> T
    ) async throws -> T {
        // Use TransactionRunner with context's own ReadVersionCache
        let runner = TransactionRunner(database: container.engine)
        return try await runner.run(
            configuration: configuration,
            readVersionCache: readVersionCache
        ) { transaction in
            let context = TransactionContext(
                transaction: transaction,
                container: self.container
            )
            return try await operation(context)
        }
    }

    /// Execute a raw transaction with configurable retry and timeout (internal use only)
    ///
    /// **Design Intent**:
    /// This is an internal API for FDBContext's own operations (clearAll, fetchPolymorphic, etc.)
    /// that need direct Transaction access for low-level operations like `clearRange`.
    ///
    /// Uses the context's ReadVersionCache for weak read semantics optimization.
    ///
    /// **For public API users**:
    /// - Use `withTransaction(_:)` for high-level TransactionContext API
    /// - Use `container.engine.withTransaction(_:)` if raw access is truly needed (no cache)
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (timeout, retry, priority)
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: Error if the transaction cannot be committed after retries
    internal func withRawTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (any Transaction) async throws -> T
    ) async throws -> T {
        let runner = TransactionRunner(database: container.engine)
        return try await runner.run(
            configuration: configuration,
            readVersionCache: readVersionCache,
            operation: operation
        )
    }

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

extension FDBContext: CustomStringConvertible {
    public var description: String {
        let (insertCount, deleteCount, replaceCount) = stateLock.withLock { state in
            var inserts = 0
            var deletes = 0
            var replaces = 0
            for mutation in state.pending.values {
                switch mutation {
                case .insert, .upsert: inserts += 1
                case .delete: deletes += 1
                case .replace: replaces += 1
                }
            }
            return (inserts, deletes, replaces)
        }

        return """
        FDBContext(
            inserts: \(insertCount),
            deletes: \(deleteCount),
            replaces: \(replaceCount),
            hasChanges: \(hasChanges)
        )
        """
    }
}

// MARK: - Polymorphic Fetch API

extension FDBContext {
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
    /// let schema = Schema([Article.self, Report.self, User.self])
    ///
    /// // Fetch all documents
    /// let docs = try await context.fetchPolymorphic(Document.self)
    ///
    /// for doc in docs {
    ///     switch doc {
    ///     case let article as Article:
    ///         print("Article: \(article.content)")
    ///     case let report as Report:
    ///         print("Report: \(report.data.count) bytes")
    ///     default:
    ///         print("Unknown type")
    ///     }
    /// }
    /// ```
    ///
    /// - Parameter protocolType: The Polymorphable protocol to query
    /// - Returns: Array of all items conforming to the protocol
    /// - Throws: Error if no types are found or if fetch fails
    public func fetchPolymorphic<P: Polymorphable>(_ protocolType: P.Type) async throws -> [any Persistable] {
        // Find entities that conform to this polymorphic protocol
        let conformingEntities = container.schema.entities.filter { entity in
            guard let polyType = entity.persistableType as? any Polymorphable.Type else { return false }
            return polyType.polymorphableType == P.polymorphableType
        }

        guard !conformingEntities.isEmpty else {
            throw FDBRuntimeError.internalError(
                "No types found in Schema for polymorphic protocol '\(P.polymorphableType)'. " +
                "Ensure conforming types are included in Schema initialization."
            )
        }

        // Security: Evaluate LIST for each conforming type
        for entity in conformingEntities {
            guard let persistableType = entity.persistableType else { continue }
            try container.securityDelegate?.evaluateList(
                type: persistableType,
                limit: nil,
                offset: nil,
                orderBy: nil
            )
        }

        // Resolve polymorphic directory
        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        let results: [any Persistable] = try await self.withRawTransaction(configuration: .default) { transaction in
            // Use ItemStorage.scan to properly handle external (large) values
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )

            var items: [any Persistable] = []

            // Scan all type codes for this protocol
            for entity in conformingEntities {
                guard let persistableType = entity.persistableType,
                      let polyType = persistableType as? any Polymorphable.Type else { continue }
                let typeCode = polyType.typeCode(for: entity.name)
                let codableType = persistableType

                let typeSubspace = itemSubspace.subspace(typeCode)
                let (begin, end) = typeSubspace.range()

                // ItemStorage.scan handles both inline and external (split) values transparently
                for try await (_, data) in storage.scan(begin: begin, end: end, snapshot: true) {
                    let item = try self.deserializePolymorphic(
                        bytes: data,
                        as: codableType
                    )
                    // Security: Evaluate GET for each retrieved item
                    try self.container.securityDelegate?.evaluateGet(item)
                    items.append(item)
                }
            }

            return items
        }

        return results
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
    ) async throws -> (any Persistable)? {
        // Find entities that conform to this polymorphic protocol
        let conformingEntities = container.schema.entities.filter { entity in
            guard let polyType = entity.persistableType as? any Polymorphable.Type else { return false }
            return polyType.polymorphableType == P.polymorphableType
        }

        guard !conformingEntities.isEmpty else {
            return nil
        }

        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let idTuple = Tuple([id])

        // Search all type subspaces for this ID
        let result: (any Persistable)? = try await self.withRawTransaction(configuration: .default) { transaction in
            // Use ItemStorage for proper handling of large values
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )

            for entity in conformingEntities {
                guard let persistableType = entity.persistableType,
                      let polyType = persistableType as? any Polymorphable.Type else { continue }
                let typeCode = polyType.typeCode(for: entity.name)
                let codableType = persistableType

                let typeSubspace = itemSubspace.subspace(typeCode)
                let key = typeSubspace.pack(idTuple)

                if let data = try await storage.read(for: key) {
                    let item = try self.deserializePolymorphic(
                        bytes: data,
                        as: codableType
                    )
                    return item
                }
            }
            return nil
        }

        // Security: Evaluate GET for the retrieved item
        if let item = result {
            try container.securityDelegate?.evaluateGet(item)
        }

        return result
    }

    /// Deserialize bytes into a concrete Persistable type
    private func deserializePolymorphic(
        bytes: [UInt8],
        as type: any Persistable.Type
    ) throws -> any Persistable {
        // Persistable types are always Codable (via @Persistable macro)
        // The type system sees `any Persistable.Type` as `any (Persistable & Codable).Type`
        try DataAccess.deserializeAny(bytes, as: type)
    }

    // MARK: - Polymorphic Save/Delete

    /// Save a polymorphic item to the shared directory
    ///
    /// Saves the item to the polymorphic protocol's directory with the correct type code prefix.
    /// The type must be included in the Schema and conform to the Polymorphable protocol.
    ///
    /// **Usage**:
    /// ```swift
    /// let article = Article(title: "Hello", content: "World")
    /// try await context.savePolymorphic(article, as: Document.self)
    /// ```
    ///
    /// - Parameters:
    ///   - item: The item to save
    ///   - protocolType: The Polymorphable protocol it conforms to
    /// - Throws: Error if type is not in Schema or save fails
    public func savePolymorphic<T: Persistable & Codable, P: Polymorphable>(
        _ item: T,
        as protocolType: P.Type
    ) async throws {
        let typeName = T.persistableType

        // Verify type is in Schema
        guard container.schema.entity(for: T.self) != nil else {
            throw FDBRuntimeError.internalError(
                "Type '\(typeName)' is not found in Schema. " +
                "Ensure '\(typeName)' is included in Schema initialization."
            )
        }

        // Get typeCode from the type directly
        guard let polyType = T.self as? any Polymorphable.Type else {
            throw FDBRuntimeError.internalError(
                "Type '\(typeName)' does not conform to Polymorphable. " +
                "Ensure '\(typeName)' conforms to '\(P.polymorphableType)'."
            )
        }
        let typeCode = polyType.typeCode(for: typeName)

        let group = try container.polymorphicGroup(identifier: P.polymorphableType)
        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        let validatedID = try item.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])
        let compositeID = makePolymorphicCompositeID(typeCode: typeCode, idTuple: idTuple)
        let key = itemSubspace.pack(compositeID)

        let data = try DataAccess.serialize(item)

        // Security: Check if this is CREATE or UPDATE
        let oldData: Bytes? = try await self.withRawTransaction(configuration: .default) { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            return try await storage.read(for: key)
        }
        let oldItem: T?
        if let oldBytes = oldData {
            let decodedOldItem: T = try DataAccess.deserialize(oldBytes)
            try container.securityDelegate?.evaluateUpdate(decodedOldItem, newResource: item)
            oldItem = decodedOldItem
        } else {
            try container.securityDelegate?.evaluateCreate(item)
            oldItem = nil
        }

        try await self.withRawTransaction(configuration: .default) { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            // Save (handles compression + external storage for >90KB)
            try await storage.write(data, for: key)

            let descriptors = self.container.schema.polymorphicIndexDescriptors(
                identifier: group.identifier,
                memberType: T.self
            )
            let maintenanceService = self.makePolymorphicIndexMaintenanceService(
                group: group,
                subspace: subspace
            )
            try await maintenanceService.updateIndexesUntyped(
                oldModel: oldItem,
                newModel: item,
                id: compositeID,
                descriptors: descriptors,
                logicalTypeName: group.identifier,
                transaction: transaction
            )
        }
    }

    /// Delete a polymorphic item from the shared directory
    ///
    /// Deletes the item from the polymorphic protocol's directory.
    /// The type must be included in the Schema and conform to the Polymorphable protocol.
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.deletePolymorphic(Article.self, id: articleId, as: Document.self)
    /// ```
    ///
    /// - Parameters:
    ///   - type: The concrete type of the item
    ///   - id: The ID of the item to delete
    ///   - protocolType: The Polymorphable protocol it conforms to
    /// - Throws: Error if type is not in Schema or delete fails
    public func deletePolymorphic<T: Persistable, P: Polymorphable>(
        _ type: T.Type,
        id: String,
        as protocolType: P.Type
    ) async throws {
        let typeName = T.persistableType

        // Verify type is in Schema
        guard container.schema.entity(for: T.self) != nil else {
            throw FDBRuntimeError.internalError(
                "Type '\(typeName)' is not found in Schema."
            )
        }

        // Get typeCode from the type directly
        guard let polyType = T.self as? any Polymorphable.Type else {
            throw FDBRuntimeError.internalError(
                "Type '\(typeName)' does not conform to Polymorphable."
            )
        }
        let typeCode = polyType.typeCode(for: typeName)

        let group = try container.polymorphicGroup(identifier: P.polymorphableType)
        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        let idTuple = Tuple([id])
        let compositeID = makePolymorphicCompositeID(typeCode: typeCode, idTuple: idTuple)
        let key = itemSubspace.pack(compositeID)

        // Security: Fetch existing item to evaluate DELETE permission
        let oldItem: T?
        if let oldData: Bytes = try await self.withRawTransaction(configuration: .default, { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            return try await storage.read(for: key)
        }) {
            let decodedOldItem: T = try DataAccess.deserialize(oldData)
            try container.securityDelegate?.evaluateDelete(decodedOldItem)
            oldItem = decodedOldItem
        } else {
            oldItem = nil
        }

        try await self.withRawTransaction(configuration: .default) { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            if let oldItem {
                let descriptors = self.container.schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberType: T.self
                )
                let maintenanceService = self.makePolymorphicIndexMaintenanceService(
                    group: group,
                    subspace: subspace
                )
                try await maintenanceService.updateIndexesUntyped(
                    oldModel: oldItem,
                    newModel: nil as (any Persistable)?,
                    id: compositeID,
                    descriptors: descriptors,
                    logicalTypeName: group.identifier,
                    transaction: transaction
                )
            }
            // Delete (handles external blob chunks)
            try await storage.delete(for: key)
        }
    }
}

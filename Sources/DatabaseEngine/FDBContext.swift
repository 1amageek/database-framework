import Foundation
import FoundationDB
import Core
import Synchronization
import Logging

/// FDBContext - Central API for model persistence (like SwiftData's ModelContext)
///
/// A model context is central to fdb-runtime as it's responsible for managing
/// the entire lifecycle of your persistent models. You use a context to insert
/// new models, track and persist changes to those models, and to delete those
/// models when you no longer need them.
///
/// **Architecture**:
/// - FDBContext provides SwiftData-like high-level API
/// - Container resolves directories from Persistable type's `#Directory` declaration
/// - FDBDataStore performs low-level FDB operations in the resolved directory
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
    public let container: FDBContainer

    /// Change tracking state
    private let stateLock: Mutex<ContextState>

    /// Logger
    private let logger: Logger

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
    ///   - container: The FDBContainer to use for storage
    ///   - autosaveEnabled: Whether to automatically save after insert/delete (default: false)
    public init(container: FDBContainer, autosaveEnabled: Bool = false) {
        self.container = container
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
        self.logger = Logger(label: "com.fdb.runtime.context")
    }

    // MARK: - State

    private struct ContextState: Sendable {
        /// Models pending insertion (type-erased)
        var insertedModels: [ModelKey: any Persistable] = [:]

        /// Models pending deletion (type-erased)
        var deletedModels: [ModelKey: any Persistable] = [:]

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
            return !insertedModels.isEmpty || !deletedModels.isEmpty
        }

        init(autosaveEnabled: Bool = false) {
            self.autosaveEnabled = autosaveEnabled
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
            if let tuple = id as? Tuple {
                return tuple.pack()
            }
            if let element = id as? any TupleElement {
                return Tuple([element]).pack()
            }
            return Array(String(describing: id).utf8)
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

    // MARK: - Insert

    /// Register a model for persistence
    ///
    /// The model is not persisted until `save()` is called.
    ///
    /// - Parameter model: The model to insert
    public func insert<T: Persistable>(_ model: T) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            state.insertedModels[key] = model
            state.deletedModels.removeValue(forKey: key)

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

    /// Mark a model for deletion
    ///
    /// - Parameter model: The model to delete
    public func delete<T: Persistable>(_ model: T) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            if state.insertedModels.removeValue(forKey: key) != nil {
                // Model was inserted in this context - just cancel the insert
            } else {
                state.deletedModels[key] = model
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
    public func deleteAll<T: Persistable>(_ type: T.Type) async throws {
        let models = try await fetch(Query<T>())
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
    /// **Note**: This does NOT track deletions in the context. Changes are applied immediately.
    /// **Note**: Also clears polymorphic directory data if type conforms to Polymorphable.
    ///
    /// - Parameter type: The Persistable type to clear
    public func clearAll<T: Persistable>(_ type: T.Type) async throws {
        let subspace = try await container.resolveDirectory(for: type)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let indexSubspace = subspace.subspace(SubspaceKey.indexes)
        let typeSubspace = itemSubspace.subspace(T.persistableType)

        // Check if type conforms to Polymorphable and needs dual-clear
        let polymorphicSubspace: Subspace? = try await {
            guard let polymorphicType = T.self as? any Polymorphable.Type else {
                return nil
            }
            let typeDirectory = T.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
            let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
            guard typeDirectory != polyDirectory else {
                return nil  // Same directory, no dual-clear needed
            }
            let polySubspace = try await container.resolvePolymorphicDirectory(for: polymorphicType)
            let typeCode = polymorphicType.typeCode(for: T.persistableType)
            return polySubspace.subspace(SubspaceKey.items).subspace(Tuple([typeCode]))
        }()

        try await container.database.withTransaction { transaction in
            // Clear all items for this type
            let (itemBegin, itemEnd) = typeSubspace.range()
            transaction.clearRange(beginKey: itemBegin, endKey: itemEnd)

            // Clear all indexes for this type
            for descriptor in T.indexDescriptors {
                let indexRange = indexSubspace.subspace(descriptor.name).range()
                transaction.clearRange(beginKey: indexRange.0, endKey: indexRange.1)
            }

            // Clear polymorphic directory data if applicable
            if let polySubspace = polymorphicSubspace {
                let (polyBegin, polyEnd) = polySubspace.range()
                transaction.clearRange(beginKey: polyBegin, endKey: polyEnd)
            }
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
    internal func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        let (pendingInserts, pendingDeleteKeys) = stateLock.withLock { state -> ([T], Set<ModelKey>) in
            let inserts = state.insertedModels.values.compactMap { $0 as? T }
            let deleteKeys = state.deletedModels.keys
                .filter { $0.persistableType == T.persistableType }
            return (inserts, Set(deleteKeys))
        }

        // Get store for this type and fetch
        let store = try await container.store(for: T.self)
        var results = try await store.fetch(query)

        // Exclude models pending deletion
        if !pendingDeleteKeys.isEmpty {
            results = results.filter { model in
                !pendingDeleteKeys.contains(ModelKey(model))
            }
        }

        // Include models pending insertion
        if !pendingInserts.isEmpty {
            let existingKeys = Set(results.map { ModelKey($0) })
            for model in pendingInserts {
                if !existingKeys.contains(ModelKey(model)) {
                    results.append(model)
                }
            }
        }

        return results
    }

    /// Fetch count of models matching a query (internal use)
    internal func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        let (hasInserts, hasDeletes) = stateLock.withLock { state -> (Bool, Bool) in
            let inserts = state.insertedModels.values.contains { $0 is T }
            let deletes = state.deletedModels.keys.contains { $0.persistableType == T.persistableType }
            return (inserts, deletes)
        }

        if !hasInserts && !hasDeletes {
            let store = try await container.store(for: T.self)
            return try await store.fetchCount(query)
        }

        let results = try await fetch(query)
        return results.count
    }

    // MARK: - Get by ID

    /// Get a single model by its identifier
    public func model<T: Persistable>(
        for id: any TupleElement,
        as type: T.Type
    ) async throws -> T? {
        let pendingResult = stateLock.withLock { state -> (inserted: T?, isDeleted: Bool) in
            let insertKey = ModelKey(persistableType: T.persistableType, id: id)
            if let inserted = state.insertedModels[insertKey] as? T {
                return (inserted, false)
            }

            let deleteKey = ModelKey(persistableType: T.persistableType, id: id)
            if state.deletedModels[deleteKey] != nil {
                return (nil, true)
            }

            return (nil, false)
        }

        if let inserted = pendingResult.inserted {
            return inserted
        }

        if pendingResult.isDeleted {
            return nil
        }

        let store = try await container.store(for: type)
        return try await store.fetch(type, id: id)
    }

    // MARK: - Save

    /// Persist all pending changes atomically
    ///
    /// Groups models by type, resolves each type's directory, and saves all changes
    /// in a single transaction.
    public func save() async throws {
        enum SaveCheckResult {
            case noChanges
            case alreadySaving
            case proceed(inserts: [any Persistable], deletes: [any Persistable])
        }

        let checkResult = stateLock.withLock { state -> SaveCheckResult in
            guard !state.isSaving else {
                return .alreadySaving
            }

            guard state.hasChanges else {
                state.autosaveScheduled = false
                return .noChanges
            }

            let inserts = Array(state.insertedModels.values)
            let deletes = Array(state.deletedModels.values)

            state.insertedModels.removeAll()
            state.deletedModels.removeAll()
            state.isSaving = true
            state.autosaveScheduled = false

            return .proceed(inserts: inserts, deletes: deletes)
        }

        let insertsSnapshot: [any Persistable]
        let deletesSnapshot: [any Persistable]

        switch checkResult {
        case .noChanges:
            return
        case .alreadySaving:
            throw FDBContextError.concurrentSaveNotAllowed
        case .proceed(let inserts, let deletes):
            insertsSnapshot = inserts
            deletesSnapshot = deletes
        }

        guard !insertsSnapshot.isEmpty || !deletesSnapshot.isEmpty else {
            stateLock.withLock { state in
                state.isSaving = false
            }
            return
        }

        do {
            // Group models by type
            let insertsByType = groupByType(insertsSnapshot)
            let deletesByType = groupByType(deletesSnapshot)

            // Get all unique types
            let allTypes = Set(insertsByType.keys).union(deletesByType.keys)

            // Create encoder once for all models (Sendable, reusable)
            let encoder = ProtobufEncoder()

            // Transaction size limits (FDB max is 10MB)
            let sizeWarningThreshold = 8_000_000  // 8MB - warn before approaching limit
            let sizeErrorThreshold = 9_500_000   // 9.5MB - error to prevent silent failure

            // Execute all operations in a single transaction
            try await container.database.withTransaction { transaction in
                for typeName in allTypes {
                    let typeInserts = insertsByType[typeName] ?? []
                    let typeDeletes = deletesByType[typeName] ?? []

                    guard let firstModel = typeInserts.first ?? typeDeletes.first else { continue }

                    // Resolve directory for this type
                    let modelType = type(of: firstModel)
                    let subspace = try await self.container.resolveDirectory(for: modelType)

                    // Create store for this subspace and execute operations
                    let store = FDBDataStore(
                        database: self.container.database,
                        subspace: subspace,
                        schema: self.container.schema
                    )

                    // Save inserts
                    for model in typeInserts {
                        try await self.saveModel(model, store: store, transaction: transaction, encoder: encoder)
                    }

                    // Monitor transaction size after processing each type
                    let currentSize = try await transaction.getApproximateSize()
                    if currentSize > sizeErrorThreshold {
                        throw FDBContextError.transactionTooLarge(
                            currentSize: currentSize,
                            limit: 10_000_000,
                            hint: "Consider using batch operations or reducing the number of changes per save()"
                        )
                    } else if currentSize > sizeWarningThreshold {
                        self.logger.warning(
                            "Transaction approaching size limit",
                            metadata: [
                                "currentSize": "\(currentSize)",
                                "threshold": "\(sizeWarningThreshold)",
                                "typeName": "\(typeName)"
                            ]
                        )
                    }

                    // Delete deletes
                    for model in typeDeletes {
                        try await self.deleteModel(model, store: store, transaction: transaction)
                    }
                }
            }

            stateLock.withLock { state in
                state.isSaving = false
            }
        } catch {
            // Restore changes on error
            stateLock.withLock { state in
                for model in insertsSnapshot {
                    let key = ModelKey(persistableType: type(of: model).persistableType, id: model.id)
                    state.insertedModels[key] = model
                }
                for model in deletesSnapshot {
                    let key = ModelKey(persistableType: type(of: model).persistableType, id: model.id)
                    state.deletedModels[key] = model
                }
                state.isSaving = false
            }
            throw error
        }
    }

    /// Group models by their persistableType
    private func groupByType(_ models: [any Persistable]) -> [String: [any Persistable]] {
        var result: [String: [any Persistable]] = [:]
        for model in models {
            let typeName = type(of: model).persistableType
            result[typeName, default: []].append(model)
        }
        return result
    }

    /// Save a single model using the provided store and transaction
    ///
    /// Uses FDBDataStore's IndexMaintenanceService for full index maintenance
    /// including aggregations and uniqueness constraints.
    /// Handles Polymorphable dual-write at this layer.
    internal func saveModel(
        _ model: any Persistable,
        store: FDBDataStore,
        transaction: any TransactionProtocol,
        encoder: ProtobufEncoder
    ) async throws {
        let modelType = type(of: model)
        let persistableType = modelType.persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Serialize using Protobuf
        let data = Array(try encoder.encode(model))

        // Build key
        let typeSubspace = store.itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Get existing record for diff-based index update
        let oldData = try await transaction.getValue(for: key, snapshot: false)

        // Write record
        transaction.setValue(data, for: key)

        // Update indexes via IndexMaintenanceService (full support including aggregations)
        try await store.indexMaintenanceService.updateIndexesUntyped(
            oldData: oldData,
            newModel: model,
            id: idTuple,
            transaction: transaction
        )

        // Dual write: If type conforms to Polymorphable and has its own directory,
        // also save to the polymorphic directory
        try await saveDualWriteIfNeeded(
            model: model,
            data: data,
            idTuple: idTuple,
            transaction: transaction
        )
    }

    /// Save to polymorphic directory if dual-write is needed
    ///
    /// Dual-write occurs when:
    /// 1. The type conforms to a Polymorphable protocol
    /// 2. The type has its own #Directory (different from the protocol's directory)
    private func saveDualWriteIfNeeded(
        model: any Persistable,
        data: [UInt8],
        idTuple: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)

        // Check if type conforms to Polymorphable
        guard let polymorphicType = modelType as? any Polymorphable.Type else {
            return
        }

        // Check if the type has its own directory (different from polymorphic shared directory)
        let typeDirectory = modelType.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")

        guard typeDirectory != polyDirectory else {
            // Same directory, no dual-write needed
            return
        }

        // Resolve polymorphic directory and save
        let polySubspace = try await container.resolvePolymorphicDirectory(for: polymorphicType)
        let itemSubspace = polySubspace.subspace(SubspaceKey.items)

        // Get typeCode for this type
        let typeCode = polymorphicType.typeCode(for: modelType.persistableType)
        let typeCodeSubspace = itemSubspace.subspace(Tuple([typeCode]))
        let polyKey = typeCodeSubspace.pack(idTuple)

        // Save to polymorphic directory
        transaction.setValue(data, for: polyKey)
    }

    /// Delete a single model using the provided store and transaction
    ///
    /// Uses FDBDataStore's IndexMaintenanceService for full index cleanup.
    /// Handles Polymorphable dual-delete at this layer.
    internal func deleteModel(
        _ model: any Persistable,
        store: FDBDataStore,
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)
        let persistableType = modelType.persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Build key
        let typeSubspace = store.itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first via IndexMaintenanceService
        try await store.indexMaintenanceService.updateIndexesUntyped(
            oldData: nil,
            newModel: nil,
            id: idTuple,
            transaction: transaction,
            deletingModel: model
        )

        // Delete record
        transaction.clear(key: key)

        // Dual delete: If type conforms to Polymorphable and has its own directory,
        // also delete from the polymorphic directory
        try await deleteDualWriteIfNeeded(
            model: model,
            idTuple: idTuple,
            transaction: transaction
        )
    }

    /// Delete from polymorphic directory if dual-write was used
    ///
    /// Dual-delete occurs when:
    /// 1. The type conforms to a Polymorphable protocol
    /// 2. The type has its own #Directory (different from the protocol's directory)
    private func deleteDualWriteIfNeeded(
        model: any Persistable,
        idTuple: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)

        // Check if type conforms to Polymorphable
        guard let polymorphicType = modelType as? any Polymorphable.Type else {
            return
        }

        // Check if the type has its own directory (different from polymorphic shared directory)
        let typeDirectory = modelType.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let polyDirectory = polymorphicType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")

        guard typeDirectory != polyDirectory else {
            // Same directory, no dual-delete needed
            return
        }

        // Resolve polymorphic directory and delete
        let polySubspace = try await container.resolvePolymorphicDirectory(for: polymorphicType)
        let itemSubspace = polySubspace.subspace(SubspaceKey.items)

        // Get typeCode for this type
        let typeCode = polymorphicType.typeCode(for: modelType.persistableType)
        let typeCodeSubspace = itemSubspace.subspace(Tuple([typeCode]))
        let polyKey = typeCodeSubspace.pack(idTuple)

        // Delete from polymorphic directory
        transaction.clear(key: polyKey)
    }

    /// Validate ID is a TupleElement
    private func validateID(_ id: any Sendable, for typeName: String) throws -> any TupleElement {
        if let tupleElement = id as? any TupleElement {
            return tupleElement
        }
        throw FDBRuntimeError.internalError("ID for \(typeName) must conform to TupleElement")
    }

    // MARK: - Rollback

    /// Discard all pending changes
    public func rollback() {
        stateLock.withLock { state in
            state.insertedModels.removeAll()
            state.deletedModels.removeAll()
            state.isSaving = false
            state.autosaveScheduled = false
        }
    }

    // MARK: - Autosave

    private func scheduleAutosave() {
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: 10_000_000)  // 10ms

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
    public func enumerate<T: Persistable>(
        _ type: T.Type,
        block: (T) throws -> Void
    ) async throws {
        let store = try await container.store(for: type)
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

    public var description: String {
        switch self {
        case .concurrentSaveNotAllowed:
            return "FDBContextError: Cannot save while another save operation is in progress"
        case .modelNotFound(let type):
            return "FDBContextError: Model of type '\(type)' not found"
        case .transactionTooLarge(let currentSize, let limit, let hint):
            return "FDBContextError: Transaction size (\(currentSize) bytes) approaching limit (\(limit) bytes). \(hint)"
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
        _ operation: @Sendable (TransactionContext) async throws -> T
    ) async throws -> T {
        let runner = TransactionRunner(database: container.database)

        // Pass readVersionCache for weak read semantics support
        return try await runner.run(
            configuration: configuration,
            readVersionCache: container.readVersionCache
        ) { transaction in
            let context = TransactionContext(
                transaction: transaction,
                container: self.container
            )
            return try await operation(context)
        }
    }
}

// MARK: - CustomStringConvertible

extension FDBContext: CustomStringConvertible {
    public var description: String {
        let (insertedCount, deletedCount) = stateLock.withLock { state in
            (state.insertedModels.count, state.deletedModels.count)
        }

        return """
        FDBContext(
            insertedModels: \(insertedCount),
            deletedModels: \(deletedCount),
            hasChanges: \(hasChanges)
        )
        """
    }
}

// MARK: - Uniqueness Violation API

extension FDBContext {
    /// Scan uniqueness violations for an index
    ///
    /// Returns all violations for the specified index on the given Persistable type.
    /// Use this after online indexing completes to review any uniqueness violations
    /// that were tracked during the build process.
    ///
    /// **Usage**:
    /// ```swift
    /// let violations = try await context.scanUniquenessViolations(
    ///     for: User.self,
    ///     indexName: "email_idx"
    /// )
    /// for violation in violations {
    ///     print("Duplicate value: \(violation.valueDescription)")
    ///     print("Conflicting records: \(violation.primaryKeys.count)")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to scan
    ///   - indexName: Name of the index to scan for violations
    ///   - limit: Maximum number of violations to return (nil = all)
    /// - Returns: Array of uniqueness violations
    public func scanUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String,
        limit: Int? = nil
    ) async throws -> [UniquenessViolation] {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }
        return try await fdbStore.violationTracker.scanViolations(
            indexName: indexName,
            limit: limit
        )
    }

    /// Check if an index has any uniqueness violations
    ///
    /// Fast check without loading all violations.
    ///
    /// **Usage**:
    /// ```swift
    /// if try await context.hasUniquenessViolations(for: User.self, indexName: "email_idx") {
    ///     print("Index has violations - review before making readable")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index to check
    /// - Returns: True if violations exist
    public func hasUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws -> Bool {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }
        return try await fdbStore.violationTracker.hasViolations(indexName: indexName)
    }

    /// Get a summary of uniqueness violations for an index
    ///
    /// Returns violation count and total conflicting records without loading
    /// all violation details.
    ///
    /// **Usage**:
    /// ```swift
    /// let summary = try await context.uniquenessViolationSummary(
    ///     for: User.self,
    ///     indexName: "email_idx"
    /// )
    /// if summary.hasViolations {
    ///     print("\(summary.violationCount) duplicate values")
    ///     print("\(summary.totalConflictingRecords) total conflicting records")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    /// - Returns: Violation summary
    public func uniquenessViolationSummary<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws -> ViolationSummary {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }
        return try await fdbStore.violationTracker.violationSummary(indexName: indexName)
    }

    /// Clear a resolved uniqueness violation
    ///
    /// Call this after confirming the violation has been resolved by
    /// deleting or updating the duplicate records.
    ///
    /// **Usage**:
    /// ```swift
    /// // After resolving a violation
    /// try await context.clearUniquenessViolation(
    ///     for: User.self,
    ///     indexName: "email_idx",
    ///     valueKey: violation.valueKey
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to clear
    public func clearUniquenessViolation<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: [UInt8]
    ) async throws {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }
        try await fdbStore.violationTracker.clearViolation(
            indexName: indexName,
            valueKey: valueKey
        )
    }

    /// Clear all uniqueness violations for an index
    ///
    /// Use after all violations have been resolved or when resetting the index.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    public func clearAllUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }
        try await fdbStore.violationTracker.clearAllViolations(indexName: indexName)
    }

    /// Verify if a uniqueness violation has been resolved
    ///
    /// Checks the actual index to see if duplicate entries still exist.
    ///
    /// **Usage**:
    /// ```swift
    /// let resolution = try await context.verifyUniquenessViolationResolution(
    ///     for: User.self,
    ///     indexName: "email_idx",
    ///     valueKey: violation.valueKey
    /// )
    /// switch resolution {
    /// case .resolved:
    ///     try await context.clearUniquenessViolation(...)
    /// case .unresolved(let updatedViolation):
    ///     print("Still has \(updatedViolation.primaryKeys.count) duplicates")
    /// case .notFound:
    ///     print("Violation was already cleared")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to verify
    /// - Returns: Resolution status
    public func verifyUniquenessViolationResolution<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: [UInt8]
    ) async throws -> ViolationResolution {
        let store = try await container.store(for: type)
        guard let fdbStore = store as? FDBDataStore else {
            throw FDBRuntimeError.internalError("Store is not an FDBDataStore")
        }

        let indexSubspace = fdbStore.indexSubspace.subspace(indexName)
        return try await fdbStore.violationTracker.verifyResolution(
            indexName: indexName,
            valueKey: valueKey,
            indexSubspace: indexSubspace
        )
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

        // Resolve polymorphic directory
        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)

        var results: [any Persistable] = []

        try await container.database.withTransaction { transaction in
            // Scan all type codes for this protocol
            for entity in conformingEntities {
                guard let polyType = entity.persistableType as? any Polymorphable.Type else { continue }
                let typeCode = polyType.typeCode(for: entity.name)
                let codableType = entity.persistableType

                let typeSubspace = itemSubspace.subspace(Tuple([typeCode]))
                let (begin, end) = typeSubspace.range()

                let sequence = transaction.getRange(
                    from: begin,
                    to: end,
                    snapshot: true,
                    streamingMode: .wantAll
                )

                for try await (_, value) in sequence {
                    // Deserialize using the concrete type
                    let item = try self.deserializePolymorphic(
                        bytes: Array(value),
                        as: codableType
                    )
                    results.append(item)
                }
            }
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
        let idTuple = Tuple([id])

        // Search all type subspaces for this ID
        return try await container.database.withTransaction { transaction in
            for entity in conformingEntities {
                guard let polyType = entity.persistableType as? any Polymorphable.Type else { continue }
                let typeCode = polyType.typeCode(for: entity.name)
                let codableType = entity.persistableType

                let typeSubspace = itemSubspace.subspace(Tuple([typeCode]))
                let key = typeSubspace.pack(idTuple)

                if let value = try await transaction.getValue(for: key, snapshot: true) {
                    let item = try self.deserializePolymorphic(
                        bytes: Array(value),
                        as: codableType
                    )
                    return item
                }
            }
            return nil
        }
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

        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(Tuple([typeCode]))

        let validatedID = try item.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])
        let key = typeSubspace.pack(idTuple)

        let data = try DataAccess.serialize(item)

        try await container.database.withTransaction { transaction in
            transaction.setValue(data, for: key)
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

        let subspace = try await container.resolvePolymorphicDirectory(for: P.self)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(Tuple([typeCode]))

        let idTuple = Tuple([id])
        let key = typeSubspace.pack(idTuple)

        try await container.database.withTransaction { transaction in
            transaction.clear(key: key)
        }
    }
}


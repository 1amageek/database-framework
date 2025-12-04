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

    // MARK: - Fetch

    /// Create a query executor for fetching models with Fluent API
    public func fetch<T: Persistable>(_ type: T.Type) -> QueryExecutor<T> {
        QueryExecutor(context: self, query: Query<T>())
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
            try await container.withTransaction { transaction in
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
    private func saveModel(
        _ model: any Persistable,
        store: FDBDataStore,
        transaction: any TransactionProtocol,
        encoder: ProtobufEncoder
    ) async throws {
        let persistableType = type(of: model).persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Serialize using Protobuf (encoder reused across all models)
        let data = try encoder.encode(model)

        let typeSubspace = store.itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Check for existing record (for index updates)
        let oldData = try await transaction.getValue(for: key, snapshot: false)

        // Save the record
        transaction.setValue(Array(data), for: key)

        // Update indexes
        try await updateIndexes(
            oldData: oldData,
            newModel: model,
            id: idTuple,
            store: store,
            transaction: transaction
        )
    }

    /// Delete a single model using the provided store and transaction
    private func deleteModel(
        _ model: any Persistable,
        store: FDBDataStore,
        transaction: any TransactionProtocol
    ) async throws {
        let persistableType = type(of: model).persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = store.itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first
        try await updateIndexes(
            oldData: nil,
            newModel: nil,
            id: idTuple,
            store: store,
            transaction: transaction,
            deletingModel: model
        )

        // Delete the record
        transaction.clear(key: key)
    }

    /// Validate ID is a TupleElement
    private func validateID(_ id: any Sendable, for typeName: String) throws -> any TupleElement {
        if let tupleElement = id as? any TupleElement {
            return tupleElement
        }
        throw FDBRuntimeError.internalError("ID for \(typeName) must conform to TupleElement")
    }

    /// Update indexes for a model change
    private func updateIndexes(
        oldData: [UInt8]?,
        newModel: (any Persistable)?,
        id: Tuple,
        store: FDBDataStore,
        transaction: any TransactionProtocol,
        deletingModel: (any Persistable)? = nil
    ) async throws {
        let modelType: any Persistable.Type
        if let newModel = newModel {
            modelType = type(of: newModel)
        } else if let deletingModel = deletingModel {
            modelType = type(of: deletingModel)
        } else {
            return
        }

        let indexDescriptors = modelType.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        for descriptor in indexDescriptors {
            let indexSubspaceForIndex = store.indexSubspace.subspace(descriptor.name)

            // Clear old index entries if updating
            if oldData != nil {
                try await clearIndexEntriesForId(
                    indexSubspace: indexSubspaceForIndex,
                    id: id,
                    transaction: transaction
                )
            }

            // Remove old entries for delete
            if let deletingModel = deletingModel {
                let oldValues = extractIndexValues(from: deletingModel, keyPaths: descriptor.keyPaths)
                if !oldValues.isEmpty {
                    let oldIndexKey = buildIndexKey(
                        subspace: indexSubspaceForIndex,
                        values: oldValues,
                        id: id
                    )
                    transaction.clear(key: oldIndexKey)
                }
            }

            // Add new index entries
            if let newModel = newModel {
                let newValues = extractIndexValues(from: newModel, keyPaths: descriptor.keyPaths)
                if !newValues.isEmpty {
                    let newIndexKey = buildIndexKey(
                        subspace: indexSubspaceForIndex,
                        values: newValues,
                        id: id
                    )
                    transaction.setValue([], for: newIndexKey)
                }
            }
        }
    }

    /// Clear all index entries for a given ID
    private func clearIndexEntriesForId(
        indexSubspace: Subspace,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let (begin, end) = indexSubspace.range()
        // Use wantAll to minimize round-trips for full index scan
        let sequence = transaction.getRange(
            from: begin,
            to: end,
            snapshot: false,
            streamingMode: .wantAll
        )

        for try await (key, _) in sequence {
            if let extractedId = extractIDFromIndexKey(key, subspace: indexSubspace),
               extractedId.pack() == id.pack() {
                transaction.clear(key: key)
            }
        }
    }

    /// Extract ID from an index key
    private func extractIDFromIndexKey(_ key: [UInt8], subspace: Subspace) -> Tuple? {
        // Quick check: verify key belongs to this subspace before attempting unpack
        guard subspace.contains(key) else { return nil }

        do {
            let tuple = try subspace.unpack(key)
            if tuple.count >= 2, let lastElement = tuple[tuple.count - 1] {
                return Tuple([lastElement])
            } else if tuple.count == 1, let element = tuple[0] {
                return Tuple([element])
            }
        } catch {}
        return nil
    }

    /// Extract index values from a model
    private func extractIndexValues(from model: any Persistable, keyPaths: [AnyKeyPath]) -> [any TupleElement] {
        (try? DataAccess.extractFieldsUsingKeyPaths(from: model, keyPaths: keyPaths)) ?? []
    }

    /// Build index key
    private func buildIndexKey(subspace: Subspace, values: [any TupleElement], id: Tuple) -> [UInt8] {
        var elements: [any TupleElement] = values
        for i in 0..<id.count {
            if let element = id[i] {
                elements.append(element)
            }
        }
        return subspace.pack(Tuple(elements))
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

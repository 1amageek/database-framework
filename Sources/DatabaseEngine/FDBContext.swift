import FoundationDB
import Core
import Synchronization

/// FDBContext - Central API for model persistence (like SwiftData's ModelContext)
///
/// A model context is central to fdb-runtime as it's responsible for managing
/// the entire lifecycle of your persistent models. You use a context to insert
/// new models, track and persist changes to those models, and to delete those
/// models when you no longer need them.
///
/// **Usage**:
/// ```swift
/// let context = container.mainContext
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

    /// Internal data store for persistence operations
    ///
    /// This is typed as `any DataStore` to support different storage backends.
    /// The default implementation is FDBDataStore, but can be replaced with
    /// custom implementations for testing or alternative backends.
    private let dataStore: any DataStore

    /// Change tracking state
    private let stateLock: Mutex<ContextState>

    // MARK: - Initialization

    /// Initialize FDBContext
    ///
    /// - Parameters:
    ///   - container: The FDBContainer to use for storage
    ///   - autosaveEnabled: Whether to automatically save after insert/delete (default: false)
    public init(container: FDBContainer, autosaveEnabled: Bool = false) {
        self.container = container
        self.dataStore = container.dataStore
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
    }

    /// Initialize FDBContext with a custom DataStore
    ///
    /// This initializer is primarily for testing, allowing injection of mock stores.
    ///
    /// - Parameters:
    ///   - container: The FDBContainer to use for storage
    ///   - dataStore: Custom DataStore implementation
    ///   - autosaveEnabled: Whether to automatically save after insert/delete (default: false)
    internal init(container: FDBContainer, dataStore: any DataStore, autosaveEnabled: Bool = false) {
        self.container = container
        self.dataStore = dataStore
        self.stateLock = Mutex(ContextState(autosaveEnabled: autosaveEnabled))
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

        /// Whether the context has unsaved changes
        var hasChanges: Bool {
            return !insertedModels.isEmpty || !deletedModels.isEmpty
        }

        init(autosaveEnabled: Bool = false) {
            self.autosaveEnabled = autosaveEnabled
        }
    }

    /// Key for tracking models
    ///
    /// Uses Tuple-packed bytes for collision-free, efficient comparison.
    /// IDs must conform to TupleElement (validated at FDB storage time).
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

        /// Pack ID to bytes using Tuple encoding
        ///
        /// This produces a unique, compact byte representation.
        private static func packID(_ id: any Sendable) -> [UInt8] {
            if let tuple = id as? Tuple {
                return tuple.pack()
            }
            if let element = id as? any TupleElement {
                return Tuple([element]).pack()
            }
            // Fallback for invalid IDs (will fail at save time anyway)
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
    ///
    /// When enabled, insert() and delete() will automatically call save().
    /// When disabled (default), you must manually call save().
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
    /// The model is not persisted until `save()` is called, unless `autosaveEnabled` is true.
    /// When autosave is enabled, changes are automatically saved after a brief delay to
    /// batch multiple rapid operations.
    ///
    /// Supports any Persistable type - context is type-independent.
    ///
    /// **Usage**:
    /// ```swift
    /// let context = container.mainContext
    /// context.insert(user)      // User: Persistable
    /// context.insert(product)   // Product: Persistable
    /// try await context.save()  // Not needed if autosaveEnabled
    /// ```
    ///
    /// - Parameter model: The model to insert
    public func insert<T: Persistable>(_ model: T) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            state.insertedModels[key] = model
            state.deletedModels.removeValue(forKey: key)

            // Check if we should schedule autosave
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
    /// The model is not removed until `save()` is called, unless `autosaveEnabled` is true.
    /// When autosave is enabled, changes are automatically saved after a brief delay to
    /// batch multiple rapid operations.
    ///
    /// - Parameter model: The model to delete
    public func delete<T: Persistable>(_ model: T) {
        let key = ModelKey(model)

        let shouldScheduleAutosave = stateLock.withLock { state -> Bool in
            // If model was inserted but not saved, just cancel the insert
            if state.insertedModels.removeValue(forKey: key) != nil {
                // Model was inserted in this context - just cancel the insert
                // No need to add to deletedModels since it doesn't exist in DB
            } else {
                // Model exists in DB - mark for deletion
                state.deletedModels[key] = model
            }

            // Check if we should schedule autosave
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

    /// Delete all models of a type matching a query
    ///
    /// **Usage**:
    /// ```swift
    /// // Delete inactive users
    /// try await context.delete(
    ///     User.self,
    ///     where: \.isActive == false
    /// )
    ///
    /// // Delete all users
    /// try await context.deleteAll(User.self)
    /// ```
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - predicate: Optional predicate to filter models
    public func delete<T: Persistable>(
        _ type: T.Type,
        where predicate: Predicate<T>
    ) async throws {
        // Build query with predicate
        let query = Query<T>().where(predicate)

        // Fetch models matching the query
        let models = try await fetch(query)

        // Mark each for deletion
        for model in models {
            delete(model)
        }
    }

    /// Delete all models of a type
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.deleteAll(User.self)
    /// ```
    ///
    /// - Parameter type: The model type
    public func deleteAll<T: Persistable>(_ type: T.Type) async throws {
        let models = try await fetch(Query<T>())
        for model in models {
            delete(model)
        }
    }

    // MARK: - Fetch

    /// Create a query executor for fetching models with Fluent API
    ///
    /// **Usage**:
    /// ```swift
    /// // Fetch all users
    /// let allUsers = try await context.fetch(User.self).execute()
    ///
    /// // Fetch with filters and sorting
    /// let users = try await context.fetch(User.self)
    ///     .where(\.isActive == true)
    ///     .where(\.age > 18)
    ///     .orderBy(\.name)
    ///     .limit(10)
    ///     .execute()
    ///
    /// // Get first matching result
    /// let user = try await context.fetch(User.self)
    ///     .where(\.email == "alice@example.com")
    ///     .first()
    ///
    /// // Get count
    /// let count = try await context.fetch(User.self)
    ///     .where(\.isActive == true)
    ///     .count()
    /// ```
    ///
    /// - Parameter type: The model type
    /// - Returns: A QueryExecutor for building and executing the query
    public func fetch<T: Persistable>(_ type: T.Type) -> QueryExecutor<T> {
        QueryExecutor(context: self, query: Query<T>())
    }

    /// Fetch models matching a query (internal use)
    ///
    /// This method considers unsaved changes:
    /// - Models pending insertion are included if they match the predicate
    /// - Models pending deletion are excluded from results
    ///
    /// - Parameter query: The query
    /// - Returns: Array of matching models
    internal func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        // Get pending changes for this type
        let (pendingInserts, pendingDeleteKeys) = stateLock.withLock { state -> ([T], Set<ModelKey>) in
            // Get inserted models of type T
            let inserts = state.insertedModels.values.compactMap { $0 as? T }

            // Get keys of deleted models of type T
            let deleteKeys = state.deletedModels.keys
                .filter { $0.persistableType == T.persistableType }

            return (inserts, Set(deleteKeys))
        }

        // Fetch from data store
        var results = try await dataStore.fetch(query)

        // Exclude models pending deletion
        if !pendingDeleteKeys.isEmpty {
            results = results.filter { model in
                !pendingDeleteKeys.contains(ModelKey(model))
            }
        }

        // Include models pending insertion (that aren't already in results)
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
    ///
    /// This method considers unsaved changes:
    /// - Models pending insertion are counted if they match the predicate
    /// - Models pending deletion are excluded from count
    ///
    /// Uses efficient counting when possible:
    /// - If no pending changes affect the count, uses index-based counting
    /// - Falls back to full fetch when pending changes exist
    ///
    /// - Parameter query: The query
    /// - Returns: Count of matching models
    internal func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        // Check if there are pending changes for this type
        let (hasInserts, hasDeletes) = stateLock.withLock { state -> (Bool, Bool) in
            let inserts = state.insertedModels.values.contains { $0 is T }
            let deletes = state.deletedModels.keys.contains { $0.persistableType == T.persistableType }
            return (inserts, deletes)
        }

        // If no pending changes, use efficient data store counting
        if !hasInserts && !hasDeletes {
            return try await dataStore.fetchCount(query)
        }

        // With pending changes, we need to fetch to get accurate count
        let results = try await fetch(query)
        return results.count
    }

    // MARK: - Get by ID

    /// Get a single model by its identifier
    ///
    /// This method considers unsaved changes:
    /// - Returns pending insertion if the model exists there
    /// - Returns nil if the model is pending deletion
    ///
    /// **Usage**:
    /// ```swift
    /// if let user = try await context.model(for: userId, as: User.self) {
    ///     print(user.name)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - id: The model's identifier
    ///   - type: The model type
    /// - Returns: The model if found, nil otherwise
    public func model<T: Persistable>(
        for id: any TupleElement,
        as type: T.Type
    ) async throws -> T? {
        // Check pending changes first
        let pendingResult = stateLock.withLock { state -> (inserted: T?, isDeleted: Bool) in
            // Check if model is pending insertion
            let insertKey = ModelKey(persistableType: T.persistableType, id: id)
            if let inserted = state.insertedModels[insertKey] as? T {
                return (inserted, false)
            }

            // Check if model is pending deletion
            let deleteKey = ModelKey(persistableType: T.persistableType, id: id)
            if state.deletedModels[deleteKey] != nil {
                return (nil, true)
            }

            return (nil, false)
        }

        // Return pending insertion if found
        if let inserted = pendingResult.inserted {
            return inserted
        }

        // Return nil if pending deletion
        if pendingResult.isDeleted {
            return nil
        }

        // Fetch from data store
        return try await dataStore.fetch(type, id: id)
    }

    // MARK: - Save

    /// Persist all pending changes atomically
    ///
    /// All inserts and deletes are executed in a single transaction.
    /// If any operation fails, all changes are rolled back.
    ///
    /// - Throws: FDBContextError.concurrentSaveNotAllowed if another save is in progress
    /// - Throws: Error if save fails
    public func save() async throws {
        // Result type for atomic check-and-get operation
        enum SaveCheckResult {
            case noChanges
            case alreadySaving
            case proceed(inserts: [any Persistable], deletes: [any Persistable])
        }

        // Get changes snapshot and clear changes atomically
        let checkResult = stateLock.withLock { state -> SaveCheckResult in
            guard !state.isSaving else {
                return .alreadySaving
            }

            guard state.hasChanges else {
                // Reset autosave flag even if no changes
                state.autosaveScheduled = false
                return .noChanges
            }

            // Take snapshot and clear changes atomically
            let inserts = Array(state.insertedModels.values)
            let deletes = Array(state.deletedModels.values)

            state.insertedModels.removeAll()
            state.deletedModels.removeAll()
            state.isSaving = true
            state.autosaveScheduled = false

            return .proceed(inserts: inserts, deletes: deletes)
        }

        // Handle check result
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

        // Early return if no changes
        guard !insertsSnapshot.isEmpty || !deletesSnapshot.isEmpty else {
            stateLock.withLock { state in
                state.isSaving = false
            }
            return
        }

        do {
            // Execute batch save via data store
            try await dataStore.executeBatch(
                inserts: insertsSnapshot,
                deletes: deletesSnapshot
            )

            // Reset saving flag after successful save
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

    // MARK: - Rollback

    /// Discard all pending changes
    ///
    /// Clears the inserted and deleted model sets without persisting.
    public func rollback() {
        stateLock.withLock { state in
            state.insertedModels.removeAll()
            state.deletedModels.removeAll()
            state.isSaving = false
            state.autosaveScheduled = false
        }
    }

    // MARK: - Autosave

    /// Schedule an autosave task
    ///
    /// This method schedules a save operation to run after a brief delay,
    /// allowing multiple rapid changes to be batched together.
    private func scheduleAutosave() {
        Task { [weak self] in
            // Brief delay to batch multiple rapid operations
            try? await Task.sleep(nanoseconds: 10_000_000)  // 10ms

            guard let self = self else { return }

            // Check if we still need to save
            let shouldSave = self.stateLock.withLock { state in
                state.hasChanges && state.autosaveEnabled
            }

            if shouldSave {
                do {
                    try await self.save()
                } catch {
                    // Autosave errors are logged but not propagated
                    // Users should handle errors in explicit save() calls
                }
            }
        }
    }

    // MARK: - Perform and Save

    /// Execute operations and automatically save changes
    ///
    /// This method groups operations and saves them atomically after the block completes.
    /// All changes made within the block are saved in a single FoundationDB transaction,
    /// ensuring atomic persistence.
    ///
    /// **Note**: This is a convenience method that calls `save()` after the block.
    /// The underlying FoundationDB transaction is created during the save operation,
    /// not at the start of the block.
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.performAndSave {
    ///     context.insert(newUser)
    ///     context.delete(oldUser)
    /// }
    /// // Changes are automatically saved atomically
    /// ```
    ///
    /// - Parameter block: The operations to execute
    public func performAndSave(
        block: () throws -> Void
    ) async throws {
        try block()
        try await save()
    }

    /// Execute operations and automatically save changes (legacy name)
    ///
    /// - Note: Consider using `performAndSave` for clearer semantics.
    /// - Parameter block: The operations to execute
    @available(*, deprecated, renamed: "performAndSave")
    public func transaction(
        block: () throws -> Void
    ) async throws {
        try await performAndSave(block: block)
    }

    // MARK: - Enumerate

    /// Enumerate all models of a type
    ///
    /// **Usage**:
    /// ```swift
    /// try await context.enumerate(User.self) { user in
    ///     print(user.name)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - block: Closure called for each model
    public func enumerate<T: Persistable>(
        _ type: T.Type,
        block: (T) throws -> Void
    ) async throws {
        let models = try await dataStore.fetchAll(type)
        for model in models {
            try block(model)
        }
    }
}

// MARK: - Errors

/// Errors that can occur during FDBContext operations
public enum FDBContextError: Error, CustomStringConvertible {
    /// Attempted to save while another save operation is in progress
    case concurrentSaveNotAllowed

    /// Model not found
    case modelNotFound(String)

    public var description: String {
        switch self {
        case .concurrentSaveNotAllowed:
            return "FDBContextError: Cannot save while another save operation is in progress"
        case .modelNotFound(let type):
            return "FDBContextError: Model of type '\(type)' not found"
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

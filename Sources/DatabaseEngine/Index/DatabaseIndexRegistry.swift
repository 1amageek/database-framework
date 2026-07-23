import StorageKit
import Core
import Synchronization

/// Registers index definitions and coordinates their persisted lifecycle.
///
/// The registry:
/// - Registers index definitions by name for the container lifetime.
/// - Persists index lifecycle transitions through `IndexLifecycleStore`.
/// - Resolves registered definitions for query and maintenance operations.
///
/// Actual index maintenance is delegated to the registered
/// `IndexMaintainerProvider` implementations.
///
/// **⚠️ Important: Index Registration Persistence**:
///
/// Index definitions are compiled from the application schema and retained for the
/// container lifetime. The application schema owns definition persistence and versioning.
///
/// **Bootstrap Requirements**:
/// 1. **Application Startup**: You must re-register all indexes on each process start
/// 2. **Multiple Instances**: Each runtime instance must register the same schema.
/// 3. **Application Responsibility**: Schema migration owns definition versioning.
///
/// **Typical Bootstrap Pattern**:
/// ```swift
/// // 1. Load the application schema.
/// let schema = try await loadSchema()
///
/// // 2. Register all indexes from schema
/// let registeredIndexes = DatabaseIndexRegistry(database: database, subspace: subspace)
/// for indexDescriptor in schema.indexes {
///     let index = Index(
///         name: indexDescriptor.name,
///         kind: indexDescriptor.kind,
///         rootExpression: expression,
///         itemTypes: [entity.name]
///     )
///     try registeredIndexes.register(index: index)
/// }
///
/// // 3. Now ready to use
/// let state = try await registeredIndexes.state(of: "user_by_email")
/// ```
///
/// **State Management Validation**:
/// - `enable()`, `makeReadable()`, and `disable()` now validate that the index
///   is registered before allowing state transitions
/// - This prevents accidentally managing state for non-existent indexes
///
/// **Usage Example**:
/// ```swift
/// let registeredIndexes = DatabaseIndexRegistry(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// // Register an index
/// try registeredIndexes.register(index: emailIndex)
///
/// // Get index state
/// let state = try await registeredIndexes.state(of: "user_by_email")
///
/// // Enable index (transition to writeOnly)
/// try await registeredIndexes.enable("user_by_email")
///
/// // Make index readable (after building)
/// try await registeredIndexes.makeReadable("user_by_email")
/// ```
public final class DatabaseIndexRegistry: Sendable {
    // MARK: - Properties

    /// Container used for transaction execution.
    let container: DBContainer
    private let subspace: Subspace

    /// Store that owns persisted index lifecycle transitions.
    ///
    /// Exposed publicly for use by OnlineIndexer during migrations.
    /// Upper layers may also need this for custom index building workflows.
    public let lifecycleStore: IndexLifecycleStore

    private let registeredIndexes: Mutex<[String: Index]>

    // MARK: - Initialization

    /// Initialize DatabaseIndexRegistry
    ///
    /// - Parameters:
    ///   - container: DBContainer for transaction execution
    ///   - subspace: Subspace for storing index data and state
    public init(
        container: DBContainer,
        subspace: Subspace
    ) {
        self.container = container
        self.subspace = subspace
        self.lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: subspace
        )
        self.registeredIndexes = Mutex([:])
    }

    // MARK: - Index Registration

    /// Register an index
    ///
    /// Registers an index definition in the registry. This makes the index
    /// available for queries and state management.
    ///
    /// - Parameter index: The index to register
    /// - Throws: DatabaseIndexRegistryError.duplicateIndex if index already exists
    public func register(index: Index) throws {
        try registeredIndexes.withLock { registry in
            guard registry[index.name] == nil else {
                throw DatabaseIndexRegistryError.duplicateIndex(index.name)
            }
            registry[index.name] = index
        }
    }

    /// Register multiple indexes
    ///
    /// - Parameter indexes: Array of indexes to register
    /// - Throws: DatabaseIndexRegistryError.duplicateIndex if any index already exists
    public func register(indexes: [Index]) throws {
        for index in indexes {
            try register(index: index)
        }
    }

    /// Unregister an index
    ///
    /// Removes an index from the registry. This does not delete the index data
    /// from FDB or change its state.
    ///
    /// - Parameter indexName: Name of the index to unregister
    public func unregister(indexName: String) {
        _ = registeredIndexes.withLock { registry in
            registry.removeValue(forKey: indexName)
        }
    }

    // MARK: - Index Lookup

    /// Get an index by name
    ///
    /// - Parameter name: Index name
    /// - Returns: The index, or nil if not found
    public func index(named name: String) -> Index? {
        return registeredIndexes.withLock { registry in
            registry[name]
        }
    }

    /// Get all registered indexes
    ///
    /// - Returns: Array of all registered indexes
    public func allIndexes() -> [Index] {
        return registeredIndexes.withLock { registry in
            Array(registry.values)
        }
    }

    /// Get indexes for a specific item type
    ///
    /// - Parameter itemType: The item type name
    /// - Returns: Array of indexes that apply to this item type
    public func indexes(for itemType: String) -> [Index] {
        return registeredIndexes.withLock { registry in
            registry.values.filter { index in
                // Universal indexes (itemTypes == nil) apply to all types
                if index.itemTypes == nil {
                    return true
                }
                // Check if this item type is in the index's item types
                return index.itemTypes?.contains(itemType) ?? false
            }
        }
    }

    // MARK: - State Management

    /// Get the current state of an index
    ///
    /// - Parameter indexName: Name of the index
    /// - Returns: Current IndexState
    /// - Throws: Error if state read fails
    public func state(of indexName: String) async throws -> IndexState {
        return try await lifecycleStore.state(of: indexName)
    }

    /// Get states for multiple indexes
    ///
    /// - Parameter indexNames: List of index names
    /// - Returns: Dictionary mapping index names to states
    /// - Throws: Error if state read fails
    public func states(of indexNames: [String]) async throws -> [String: IndexState] {
        return try await lifecycleStore.states(of: indexNames)
    }

    /// Enable an index (transition to WRITE_ONLY state)
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: DatabaseIndexRegistryError.indexNotFound if index is not registered
    /// - Throws: IndexStateError.invalidTransition if not in DISABLED state
    public func enable(_ indexName: String) async throws {
        guard index(named: indexName) != nil else {
            throw DatabaseIndexRegistryError.indexNotFound(indexName)
        }
        try await lifecycleStore.enable(indexName)
    }

    /// Make an index readable (transition to READABLE state)
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: DatabaseIndexRegistryError.indexNotFound if index is not registered
    /// - Throws: IndexStateError.invalidTransition if not in WRITE_ONLY state
    public func makeReadable(_ indexName: String) async throws {
        guard index(named: indexName) != nil else {
            throw DatabaseIndexRegistryError.indexNotFound(indexName)
        }
        try await lifecycleStore.makeReadable(indexName)
    }

    /// Disable an index (transition to DISABLED state)
    ///
    /// - Parameter indexName: Name of the index
    /// - Throws: DatabaseIndexRegistryError.indexNotFound if index is not registered
    /// - Throws: Error if state write fails
    public func disable(_ indexName: String) async throws {
        guard index(named: indexName) != nil else {
            throw DatabaseIndexRegistryError.indexNotFound(indexName)
        }
        try await lifecycleStore.disable(indexName)
    }

    // MARK: - Subspace Management

    /// Get the subspace for a specific index
    ///
    /// Returns the subspace where this index's data is stored:
    /// `[subspace][indexName]`
    ///
    /// - Parameter indexName: The index name
    /// - Returns: The subspace for storing this index's data
    public func indexSubspace(for indexName: String) -> Subspace {
        return subspace.subspace(indexName)
    }

    /// Get the subspace for an Index struct
    ///
    /// - Parameter index: The index
    /// - Returns: The subspace for storing this index's data
    public func indexSubspace(for index: Index) -> Subspace {
        return subspace.subspace(index.subspaceKey)
    }
}

// MARK: - Errors

/// Errors raised while registering or locating index definitions.
public enum DatabaseIndexRegistryError: Error, CustomStringConvertible {
    /// Attempted to register an index that already exists
    case duplicateIndex(String)

    /// Index not found in registry
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .duplicateIndex(let name):
            return "Index '\(name)' is already registered"
        case .indexNotFound(let name):
            return "Index '\(name)' not found in registry"
        }
    }
}

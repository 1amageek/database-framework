// DataStore.swift
// FDBRuntime - SwiftData-like protocol for storage backend abstraction
//
// This protocol enables different storage backend implementations:
// - FDBDataStore: Default FoundationDB implementation
// - Custom implementations: For testing or alternative backends

import FoundationDB
import Core

/// SwiftData-like protocol for storage backend abstraction
///
/// **Purpose**: Abstract the storage layer to enable:
/// - Different storage backends (FDB, in-memory, SQLite)
/// - Easy testing with mock stores
/// - Consistent API across implementations
///
/// **SwiftData Comparison**:
/// ```
/// SwiftData                    fdb-runtime
/// ─────────                    ───────────
/// DataStore (protocol)    ←→   DataStore (protocol)
/// DefaultStore            ←→   FDBDataStore
/// DataStoreConfiguration  ←→   DataStoreConfiguration
/// ```
///
/// **Usage**:
/// ```swift
/// // Default: FDBDataStore is created automatically
/// let container = try FDBContainer(for: schema)
///
/// // Custom: Inject a different DataStore (e.g., for testing)
/// let customStore = CustomDataStore(...)
/// let container = try FDBContainer(
///     for: schema,
///     dataStore: customStore
/// )
/// ```
///
/// **Implementing a Custom DataStore**:
/// ```swift
/// final class CustomDataStore: DataStore {
///     typealias Configuration = CustomConfiguration
///
///     func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
///         // Your implementation
///     }
///     // ... implement other required methods
/// }
/// ```
public protocol DataStore: AnyObject, Sendable {

    // MARK: - Associated Types

    /// The configuration type for this data store
    associatedtype Configuration: DataStoreConfiguration

    // MARK: - Fetch Operations

    /// Fetch models matching a query
    ///
    /// This method should:
    /// - Apply predicates (where clauses)
    /// - Apply sorting (orderBy)
    /// - Apply pagination (limit, offset)
    /// - Use indexes when available for optimization
    ///
    /// - Parameter query: The query to execute
    /// - Returns: Array of matching models
    /// - Throws: Error if fetch fails
    func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T]

    /// Fetch a single model by ID
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    /// - Returns: The model if found, nil otherwise
    /// - Throws: Error if fetch fails
    func fetch<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws -> T?

    /// Fetch all models of a type
    ///
    /// **Note**: Use with caution for large datasets.
    /// Consider using `fetch(_:Query)` with pagination instead.
    ///
    /// - Parameter type: The model type
    /// - Returns: Array of all models of the type
    /// - Throws: Error if fetch fails
    func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T]

    /// Fetch count of models matching a query
    ///
    /// This method may be optimized to avoid loading full model data.
    ///
    /// - Parameter query: The query to count
    /// - Returns: Count of matching models
    /// - Throws: Error if count fails
    func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int

    // MARK: - Save/Delete Operations

    /// Execute batch save and delete operations
    ///
    /// All operations are executed atomically in a single transaction.
    /// If any operation fails, all changes are rolled back.
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    /// - Throws: Error if batch execution fails
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws
}

// MARK: - DataStoreConfiguration

/// Configuration protocol for DataStore
///
/// Defines the configuration requirements for a data store.
/// Concrete implementations provide store-specific settings.
///
/// **SwiftData Comparison**:
/// - SwiftData's `DataStoreConfiguration` requires `name` and `schema`
/// - fdb-runtime follows the same pattern
///
/// **Example Implementation**:
/// ```swift
/// struct MyDataStoreConfiguration: DataStoreConfiguration {
///     var name: String?
///     var schema: Schema?
///
///     // Custom properties
///     var connectionString: String
///     var maxConnections: Int
/// }
/// ```
public protocol DataStoreConfiguration: Sendable {
    /// Optional name for debugging and identification
    var name: String? { get }

    /// Schema defining entities and indexes
    var schema: Schema? { get }
}

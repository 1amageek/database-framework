// DataStore.swift
// FDBRuntime - SwiftData-like protocol for storage backend abstraction
//
// This protocol enables different storage backend implementations:
// - FDBDataStore: Default FoundationDB implementation
// - Custom implementations: For testing or alternative backends
//
// Security: DataStore uses DataStoreSecurityDelegate for access control.
// Auth context is obtained via TaskLocal (AuthContextKey.current).

import FoundationDB
import Core

/// SwiftData-like protocol for storage backend abstraction
///
/// **Purpose**: Abstract the storage layer to enable:
/// - Different storage backends (FDB, in-memory, SQLite)
/// - Easy testing with mock stores
/// - Consistent API across implementations
///
/// **Security**:
/// DataStore holds a security delegate that evaluates permissions.
/// Auth context is obtained via `AuthContextKey.current` (TaskLocal).
///
/// ```swift
/// // Set auth context per request
/// try await AuthContextKey.$current.withValue(userAuth) {
///     let context = container.newContext()
///     try await context.save()  // Security evaluated via delegate
/// }
/// ```
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
public protocol DataStore: AnyObject, Sendable {

    // MARK: - Associated Types

    /// The configuration type for this data store
    associatedtype Configuration: DataStoreConfiguration

    // MARK: - Security

    /// Security delegate for access control evaluation
    ///
    /// If nil, security evaluation is skipped.
    var securityDelegate: (any DataStoreSecurityDelegate)? { get }

    // MARK: - Fetch Operations

    /// Fetch models matching a query
    ///
    /// Security: LIST operation is evaluated via securityDelegate.
    ///
    /// This method should:
    /// - Evaluate LIST security (limit, offset, orderBy)
    /// - Apply predicates (where clauses)
    /// - Apply sorting (orderBy)
    /// - Apply pagination (limit, offset)
    /// - Use indexes when available for optimization
    ///
    /// - Parameter query: The query to execute
    /// - Returns: Array of matching models
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T]

    /// Fetch a single model by ID
    ///
    /// Security: GET operation is evaluated via securityDelegate after fetch.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    /// - Returns: The model if found and access is allowed, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetch<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws -> T?

    /// Fetch all models of a type
    ///
    /// Security: LIST operation is evaluated via securityDelegate.
    ///
    /// **Note**: Use with caution for large datasets.
    /// Consider using `fetch(_:Query)` with pagination instead.
    ///
    /// - Parameter type: The model type
    /// - Returns: Array of all models of the type
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T]

    /// Fetch count of models matching a query
    ///
    /// Security: LIST operation is evaluated via securityDelegate.
    ///
    /// This method may be optimized to avoid loading full model data.
    ///
    /// - Parameter query: The query to count
    /// - Returns: Count of matching models
    /// - Throws: SecurityError if LIST not allowed, or other errors on failure
    func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int

    // MARK: - Write Operations

    /// Execute batch save and delete operations
    ///
    /// Security (evaluated via securityDelegate):
    /// - CREATE operation is evaluated for new records
    /// - UPDATE operation is evaluated for existing records (with old and new values)
    /// - DELETE operation is evaluated for records being deleted
    ///
    /// All operations are executed atomically in a single transaction.
    /// If any operation fails (including security), all changes are rolled back.
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    /// - Throws: SecurityError if any operation is not allowed, or other errors on failure
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws

    /// Clear all records of a type
    ///
    /// Security: Admin-only operation (evaluated via securityDelegate.requireAdmin).
    ///
    /// **Warning**: This operation is destructive and cannot be undone.
    ///
    /// - Parameter type: The model type to clear
    /// - Throws: SecurityError if not admin, or other errors on failure
    func clearAll<T: Persistable>(_ type: T.Type) async throws

    // MARK: - Transaction Operations

    /// Execute operations within a transaction
    ///
    /// Security: Each operation within the transaction is evaluated separately
    /// via the TransactionContextProtocol methods.
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (priority, timeout, retry)
    ///   - operation: The closure to execute within the transaction
    /// - Returns: The result of the operation closure
    /// - Throws: SecurityError or other errors from the operation
    func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (any TransactionContextProtocol) async throws -> T
    ) async throws -> T

    // MARK: - Transaction-Scoped Operations

    /// Fetch a single model by ID within an externally-provided transaction
    ///
    /// Security: GET operation is evaluated via securityDelegate after fetch.
    ///
    /// This method performs a direct key lookup (O(1)) rather than a query scan.
    /// Use this when you know the exact ID of the item you want to fetch.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    ///   - transaction: The transaction to use
    /// - Returns: The model if found and access is allowed, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetchByIdInTransaction<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        transaction: any TransactionProtocol
    ) async throws -> T?

    /// Execute batch operations within an externally-provided transaction
    ///
    /// Security (evaluated via securityDelegate):
    /// - CREATE operation for new records
    /// - UPDATE operation for existing records
    /// - DELETE operation for deletions
    ///
    /// Use this for coordinating multiple DataStore operations in a single transaction.
    /// More efficient than per-model methods as it can batch reads and writes.
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    ///   - transaction: The transaction to use
    /// - Returns: Serialized data for inserted models (for dual-write optimization)
    /// - Throws: SecurityError if operation not allowed, or other errors on failure
    @discardableResult
    func executeBatchInTransaction(
        inserts: [any Persistable],
        deletes: [any Persistable],
        transaction: any TransactionProtocol
    ) async throws -> [SerializedModel]

    /// Execute operations within a raw transaction
    ///
    /// Use this for coordinating operations across multiple DataStores
    /// in a single atomic transaction.
    ///
    /// **Usage**:
    /// ```swift
    /// let userStore = try await container.store(for: User.self)
    /// let orderStore = try await container.store(for: Order.self)
    ///
    /// try await userStore.withRawTransaction { transaction in
    ///     try await userStore.executeBatchInTransaction(
    ///         inserts: [user], deletes: [], transaction: transaction
    ///     )
    ///     try await orderStore.executeBatchInTransaction(
    ///         inserts: [order], deletes: [], transaction: transaction
    ///     )
    /// }
    /// ```
    ///
    /// - Parameter body: The closure to execute within the transaction
    /// - Returns: Result of the closure
    func withRawTransaction<T: Sendable>(
        _ body: @Sendable @escaping (any TransactionProtocol) async throws -> T
    ) async throws -> T
}

// MARK: - SerializedModel

/// Serialized model data for dual-write optimization
///
/// Returned by `executeBatchInTransaction` to avoid re-serialization
/// when writing to polymorphic directories.
public struct SerializedModel: Sendable {
    /// The original model
    public let model: any Persistable
    /// Serialized data (Protobuf)
    public let data: [UInt8]
    /// ID as Tuple
    public let idTuple: Tuple

    public init(model: any Persistable, data: [UInt8], idTuple: Tuple) {
        self.model = model
        self.data = data
        self.idTuple = idTuple
    }
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

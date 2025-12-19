// FDBDatabase.swift
// DatabaseEngine - Database wrapper with read version caching
//
// Reference: FDB Record Layer FDBDatabase.java
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBDatabase.java

import Foundation
import FoundationDB

// MARK: - FDBDatabase

/// Database wrapper that provides read version caching for weak read semantics
///
/// FDBDatabase wraps a raw `DatabaseProtocol` and adds:
/// - Read version caching to reduce `getReadVersion()` network round-trips
/// - Transaction execution with proper configuration support
/// - Metrics collection for cache effectiveness
///
/// **Architecture**:
/// ```
/// FDBContainer (Application Resource Manager)
///     │
///     └── FDBDatabase (Database Session)
///             │
///             ├── database: DatabaseProtocol (FDB native)
///             └── readVersionCache: ReadVersionCache
/// ```
///
/// **Reference**: FDB Record Layer `FDBDatabase` maintains `lastSeenFDBVersion`
/// at the database level, shared across all `FDBRecordContext` instances.
///
/// **Usage**:
/// ```swift
/// let fdbDatabase = try FDBDatabase()
///
/// // Transaction with weak read semantics (uses cache)
/// let result = try await fdbDatabase.withTransaction(configuration: .readOnly) { tx in
///     try await tx.getValue(for: key, snapshot: true)
/// }
///
/// // Transaction without cache (strict consistency)
/// let result = try await fdbDatabase.withTransaction(configuration: .default) { tx in
///     try await tx.getValue(for: key, snapshot: false)
/// }
/// ```
public final class FDBDatabase: Sendable {
    // MARK: - Properties

    /// The underlying FDB database connection
    ///
    /// Thread-safe: FDB client handles thread safety internally
    nonisolated(unsafe) public let database: any DatabaseProtocol

    /// Cache for read versions to support weak read semantics
    ///
    /// Shared across all transactions created from this database.
    /// Updated on successful commits and reads (when tracking is enabled).
    public let readVersionCache: ReadVersionCache

    /// Whether to update cache after successful commits
    ///
    /// Default: true (recommended for most use cases)
    public let trackLastSeenVersionOnCommit: Bool

    /// Whether to update cache after read version is obtained
    ///
    /// Default: false (commit version is more accurate)
    public let trackLastSeenVersionOnRead: Bool

    // MARK: - Initialization

    /// Create FDBDatabase with a new database connection
    ///
    /// - Parameters:
    ///   - clusterFilePath: Path to FDB cluster file (nil for default)
    ///   - trackLastSeenVersionOnCommit: Update cache on commit (default: true)
    ///   - trackLastSeenVersionOnRead: Update cache on read (default: false)
    /// - Throws: Error if database connection fails
    public init(
        clusterFilePath: String? = nil,
        trackLastSeenVersionOnCommit: Bool = true,
        trackLastSeenVersionOnRead: Bool = false
    ) throws {
        self.database = try FDBClient.openDatabase(clusterFilePath: clusterFilePath)
        self.readVersionCache = ReadVersionCache()
        self.trackLastSeenVersionOnCommit = trackLastSeenVersionOnCommit
        self.trackLastSeenVersionOnRead = trackLastSeenVersionOnRead
    }

    /// Create FDBDatabase wrapping an existing database connection
    ///
    /// - Parameters:
    ///   - database: Existing database connection
    ///   - trackLastSeenVersionOnCommit: Update cache on commit (default: true)
    ///   - trackLastSeenVersionOnRead: Update cache on read (default: false)
    public init(
        database: any DatabaseProtocol,
        trackLastSeenVersionOnCommit: Bool = true,
        trackLastSeenVersionOnRead: Bool = false
    ) {
        self.database = database
        self.readVersionCache = ReadVersionCache()
        self.trackLastSeenVersionOnCommit = trackLastSeenVersionOnCommit
        self.trackLastSeenVersionOnRead = trackLastSeenVersionOnRead
    }

    // MARK: - Transaction Execution

    /// Execute a transaction with the specified configuration
    ///
    /// This method provides:
    /// - Exponential backoff with jitter (prevents thundering herd)
    /// - Configurable retry limits and timeouts
    /// - Weak read semantics support (when configured)
    /// - Read version cache updates
    ///
    /// **Weak Read Semantics**:
    /// When `configuration.weakReadSemantics` is set, the transaction may
    /// reuse a cached read version instead of calling `getReadVersion()`,
    /// reducing network round-trips at the cost of potential staleness.
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration to apply
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: Error if transaction fails after all retry attempts
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let runner = TransactionRunner(database: database)
        return try await runner.run(
            configuration: configuration,
            readVersionCache: readVersionCache,
            operation: operation
        )
    }

    // MARK: - Direct Database Access

    /// Create a raw transaction (for advanced use cases)
    ///
    /// **Warning**: Prefer `withTransaction(configuration:_:)` for automatic
    /// retry handling and read version caching.
    ///
    /// - Returns: A new transaction
    /// - Throws: Error if transaction creation fails
    public func createTransaction() throws -> any TransactionProtocol {
        try database.createTransaction()
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


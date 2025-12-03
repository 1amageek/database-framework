import Foundation
import FoundationDB
import Core
import Synchronization
import Logging

/// FDBContainer - SwiftData-like container for FoundationDB persistence
///
/// **Design Philosophy**:
/// FDBContainer is a lightweight coordinator that connects:
/// - **Schema**: Defines entities and indexes
/// - **Database**: Provides DirectoryLayer and transactions
/// - **Persistable types**: Define their own directory paths via `#Directory` macro
///
/// FDBContainer does NOT manage:
/// - `subspace`: Each Persistable type defines its own directory via `#Directory`
/// - `directoryLayer`: Provided by Database
/// - `dataStore`: Created dynamically based on resolved directory
///
/// **Responsibilities**:
/// - Schema management
/// - Database connection reference
/// - Directory resolution from Persistable type metadata
/// - Migration execution
///
/// **Usage**:
/// ```swift
/// // 1. Define models with #Directory
/// @Persistable
/// struct User {
///     #Directory<User>("app", "users")
///     var id: String = ULID().ulidString
///     var email: String
/// }
///
/// // 2. Create container
/// let schema = Schema([User.self])
/// let container = try FDBContainer(for: schema)
///
/// // 3. Use context (SwiftData-like API)
/// let context = container.newContext()
/// context.insert(user)
/// try await context.save()
/// ```
public final class FDBContainer: Sendable {
    // MARK: - Properties

    /// Database connection (thread-safe in FoundationDB)
    nonisolated(unsafe) public let database: any DatabaseProtocol

    /// Schema (version, entities, indexes)
    public let schema: Schema

    /// Configuration
    public let configuration: FDBConfiguration?

    /// Index configurations grouped by indexName
    public let indexConfigurations: [String: [any IndexConfiguration]]

    /// Logger
    private let logger: Logger

    /// Directory cache for performance
    private let directoryCache: Mutex<[String: Subspace]>

    /// Read version cache for GRV optimization
    /// Uses SharedReadVersionCache for consistency across all code paths
    /// (FDBContainer, FDBDataStore, IndexQueryContext, etc.)
    private var readVersionCache: ReadVersionCache {
        SharedReadVersionCache.shared.cache
    }

    /// Transaction size warning threshold (5MB)
    private static let transactionSizeWarningThreshold = 5_000_000

    /// Migration plan (SwiftData-like API)
    nonisolated(unsafe) private var _migrationPlan: (any SchemaMigrationPlan.Type)?

    // MARK: - Initialization

    /// Initialize FDBContainer with schema
    ///
    /// **Example**:
    /// ```swift
    /// let schema = Schema([User.self, Order.self])
    /// let container = try FDBContainer(for: schema)
    /// ```
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - configuration: Optional FDBConfiguration
    /// - Throws: Error if database connection fails
    public init(
        for schema: Schema,
        configuration: FDBConfiguration? = nil
    ) throws {
        guard !schema.entities.isEmpty else {
            throw FDBRuntimeError.internalError("Schema must contain at least one entity")
        }

        let database = try FDBClient.openDatabase(clusterFilePath: configuration?.url?.path)

        self.database = database
        self.schema = schema
        self.configuration = configuration
        self.indexConfigurations = Self.aggregateIndexConfigurations(configuration?.indexConfigurations ?? [])
        self._migrationPlan = nil
        self.logger = Logger(label: "com.fdb.runtime.container")
        self.directoryCache = Mutex([:])
        // readVersionCache is now a computed property using SharedReadVersionCache.shared
    }

    /// Initialize FDBContainer with database (low-level API)
    ///
    /// - Parameters:
    ///   - database: The FDB database
    ///   - schema: Schema defining entities and indexes
    ///   - configuration: Optional configuration
    ///   - indexConfigurations: Index configurations
    public init(
        database: any DatabaseProtocol,
        schema: Schema,
        configuration: FDBConfiguration? = nil,
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        precondition(!schema.entities.isEmpty, "Schema must contain at least one entity")

        self.database = database
        self.schema = schema
        self.configuration = configuration
        self.indexConfigurations = Self.aggregateIndexConfigurations(indexConfigurations)
        self._migrationPlan = nil
        self.logger = Logger(label: "com.fdb.runtime.container")
        self.directoryCache = Mutex([:])
        // readVersionCache is now a computed property using SharedReadVersionCache.shared
    }

    // MARK: - Context Management

    /// Create a new context for data operations
    ///
    /// **Example**:
    /// ```swift
    /// let context = container.newContext()
    /// context.insert(user)
    /// context.insert(order)
    /// try await context.save()
    /// ```
    ///
    /// - Parameter autosaveEnabled: Whether to automatically save after operations (default: false)
    /// - Returns: New FDBContext instance
    public func newContext(autosaveEnabled: Bool = false) -> FDBContext {
        return FDBContext(container: self, autosaveEnabled: autosaveEnabled)
    }

    // MARK: - Directory Resolution

    /// Resolve directory for a Persistable type
    ///
    /// Uses the type's `#Directory` declaration to resolve the directory path.
    /// Results are cached for performance.
    ///
    /// - Parameter type: The Persistable type
    /// - Returns: Subspace for the type's directory
    /// - Throws: Error if type has dynamic path (Field components)
    public func resolveDirectory<T: Persistable>(for type: T.Type) async throws -> Subspace {
        let cacheKey = type.persistableType

        // Check cache first
        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        // Resolve path from type's directoryPathComponents
        let pathComponents = type.directoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else {
                throw FDBRuntimeError.internalError(
                    "Type \(type.persistableType) has dynamic directory path (Field components). " +
                    "Use resolveDirectory(for:with:) with an instance instead."
                )
            }
        }

        // Resolve via DirectoryLayer (created on demand)
        let layer = convertDirectoryLayer(type.directoryLayer)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path, type: layer)
        let subspace = dirSubspace.subspace

        // Cache the result
        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
    }

    /// Resolve directory for a Persistable type (type-erased version)
    ///
    /// Used by FDBContext for batch operations with mixed types.
    ///
    /// - Parameter type: The Persistable type (existential)
    /// - Returns: Subspace for the type's directory
    internal func resolveDirectory(for type: any Persistable.Type) async throws -> Subspace {
        let cacheKey = type.persistableType

        // Check cache first
        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        // Resolve path from type's directoryPathComponents
        let pathComponents = type.directoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else {
                throw FDBRuntimeError.internalError(
                    "Type \(type.persistableType) has dynamic directory path (Field components)."
                )
            }
        }

        // Resolve via DirectoryLayer (created on demand)
        let layer = convertDirectoryLayer(type.directoryLayer)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path, type: layer)
        let subspace = dirSubspace.subspace

        // Cache the result
        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
    }

    /// Resolve directory for a Persistable instance (supports partitioned directories)
    ///
    /// **Example**:
    /// ```swift
    /// @Persistable
    /// struct Order {
    ///     #Directory<Order>("tenants", Field(\.tenantID), "orders", layer: .partition)
    ///     var tenantID: String
    /// }
    ///
    /// let subspace = try await container.resolveDirectory(for: Order.self, with: order)
    /// // Resolves to: tenants/{tenantID}/orders
    /// ```
    public func resolveDirectory<T: Persistable>(for type: T.Type, with instance: T) async throws -> Subspace {
        let pathComponents = type.directoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let fieldElement = component as? Field<T> {
                let keyPath = fieldElement.value
                let value = instance[keyPath: keyPath]
                path.append(String(describing: value))
            }
        }

        // For partitioned directories, cache key includes the resolved path
        let cacheKey = path.joined(separator: "/")

        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let layer = convertDirectoryLayer(type.directoryLayer)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path, type: layer)
        let subspace = dirSubspace.subspace

        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
    }

    /// Convert Core.DirectoryLayer to FoundationDB.DirectoryType
    private func convertDirectoryLayer(_ layer: Core.DirectoryLayer) -> DirectoryType? {
        switch layer {
        case .default:
            return nil
        case .partition:
            return .partition
        }
    }

    // MARK: - Store Access (Convenience)

    /// Get DataStore for a Persistable type
    ///
    /// **Example**:
    /// ```swift
    /// let store = try await container.store(for: User.self)
    /// ```
    public func store<T: Persistable>(for type: T.Type) async throws -> any DataStore {
        let subspace = try await resolveDirectory(for: type)
        return FDBDataStore(database: database, subspace: subspace, schema: schema)
    }

    // MARK: - Transaction Support

    /// Execute a closure with a database transaction
    ///
    /// This method provides optimized transaction execution with:
    /// - **GRV Caching**: Reduces read version latency when `useGrvCache` is enabled
    /// - **Automatic Retry**: Exponential backoff with jitter for retryable errors
    /// - **Size Monitoring**: Logs warnings for large transactions
    /// - **Version Tracking**: Updates cache with committed versions
    ///
    /// - Parameters:
    ///   - configuration: Transaction configuration (priority, timeout, etc.)
    ///   - operation: The operation to execute within the transaction
    /// - Returns: The result of the operation
    /// - Throws: `FDBError` if the transaction fails
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let maxRetries = configuration.retryLimit ?? 100

        for attempt in 0..<maxRetries {
            let transaction = try database.createTransaction()
            try transaction.apply(configuration)

            // Apply cached read version if GRV cache is enabled
            if configuration.useGrvCache {
                if let cachedVersion = readVersionCache.getCachedVersion(
                    semantics: .bounded(seconds: 5.0)
                ) {
                    transaction.setReadVersion(cachedVersion)
                }
            }

            do {
                let result = try await operation(transaction)

                // Check transaction size before commit
                let approximateSize = try await transaction.getApproximateSize()
                if approximateSize > Self.transactionSizeWarningThreshold {
                    logger.warning(
                        "Large transaction detected",
                        metadata: [
                            "size_bytes": "\(approximateSize)",
                            "threshold_bytes": "\(Self.transactionSizeWarningThreshold)"
                        ]
                    )
                }

                let committed = try await transaction.commit()

                if committed {
                    // Update cache with committed version
                    let commitVersion = try transaction.getCommittedVersion()
                    readVersionCache.recordCommitVersion(commitVersion)
                    return result
                }
            } catch {
                transaction.cancel()

                if let fdbError = error as? FDBError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
                        // Exponential backoff with jitter to prevent thundering herd
                        let maxDelay = configuration.maxRetryDelay ?? 1000
                        let baseDelay = min(maxDelay, 10 * (1 << min(attempt, 10)))
                        let jitter = Int.random(in: 0...(baseDelay / 4))
                        let delay = baseDelay + jitter
                        try await Task.sleep(nanoseconds: UInt64(delay) * 1_000_000)
                        continue
                    }
                }

                throw error
            }
        }

        throw FDBError(code: 1020)  // transaction_too_old
    }

    /// Get read version cache statistics
    public var readVersionCacheStatistics: ReadVersionCacheStatistics {
        readVersionCache.statistics
    }

    /// Invalidate read version cache
    public func invalidateReadVersionCache() {
        readVersionCache.invalidate()
    }

    // MARK: - Index Configuration Management

    /// Get a single index configuration
    public func indexConfiguration<C: IndexConfiguration>(
        for indexName: String,
        as type: C.Type
    ) -> C? {
        return indexConfigurations[indexName]?.first { $0 is C } as? C
    }

    /// Get all index configurations for an index
    public func indexConfigurations<C: IndexConfiguration>(
        for indexName: String,
        as type: C.Type
    ) -> [C] {
        return indexConfigurations[indexName]?.compactMap { $0 as? C } ?? []
    }

    /// Check if an index has configurations
    public func hasIndexConfiguration(for indexName: String) -> Bool {
        guard let configs = indexConfigurations[indexName] else { return false }
        return !configs.isEmpty
    }

    /// Aggregate index configurations by indexName
    internal static func aggregateIndexConfigurations(
        _ indexConfigurations: [any IndexConfiguration]
    ) -> [String: [any IndexConfiguration]] {
        var result: [String: [any IndexConfiguration]] = [:]
        for config in indexConfigurations {
            result[config.indexName, default: []].append(config)
        }
        return result
    }
}

// MARK: - Migration Support

extension FDBContainer {
    /// Get metadata directory for schema versioning
    private func getMetadataSubspace() async throws -> Subspace {
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: ["_metadata"])
        return dirSubspace.subspace
    }

    /// Get the current schema version from FDB
    public func getCurrentSchemaVersion() async throws -> Schema.Version? {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))

        return try await database.withTransaction(configuration: .readOnly) { transaction -> Schema.Version? in
            guard let versionBytes = try await transaction.getValue(for: versionKey, snapshot: true) else {
                return nil
            }

            let tuple = try Tuple.unpack(from: versionBytes)
            guard tuple.count == 3 else {
                throw FDBRuntimeError.internalError("Invalid version format")
            }

            func toInt(_ value: Any) -> Int? {
                if let v = value as? Int { return v }
                if let v = value as? Int64 { return Int(v) }
                if let v = value as? Int32 { return Int(v) }
                return nil
            }

            guard let major = toInt(tuple[0]),
                  let minor = toInt(tuple[1]),
                  let patch = toInt(tuple[2]) else {
                throw FDBRuntimeError.internalError("Invalid version format")
            }

            return Schema.Version(major, minor, patch)
        }
    }

    /// Set the current schema version in FDB
    public func setCurrentSchemaVersion(_ version: Schema.Version) async throws {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))

        try await database.withTransaction(configuration: .system) { transaction in
            let versionTuple = Tuple(version.major, version.minor, version.patch)
            transaction.setValue(versionTuple.pack(), for: versionKey)
        }
    }
}

// MARK: - VersionedSchema Support

extension FDBContainer {
    /// Initialize with VersionedSchema and MigrationPlan
    public convenience init<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: FDBConfiguration? = nil
    ) throws {
        try P.validate()
        let schemaInstance = S.makeSchema()
        let database = try FDBClient.openDatabase(clusterFilePath: configuration?.url?.path)

        self.init(
            database: database,
            schema: schemaInstance,
            configuration: configuration,
            indexConfigurations: configuration?.indexConfigurations ?? []
        )
        self._migrationPlan = migrationPlan
    }

    /// Migrate to the current schema version if needed
    public func migrateIfNeeded() async throws {
        guard let plan = _migrationPlan else { return }

        guard let targetVersion = plan.currentVersion else {
            throw FDBRuntimeError.internalError("Migration plan has no schemas")
        }

        try schema.validateIndexNames()

        let currentVersion = try await getCurrentSchemaVersion()

        guard let currentVersion else {
            try await setCurrentSchemaVersion(targetVersion)
            logger.info("Set initial schema version: \(targetVersion)")
            return
        }

        if currentVersion >= targetVersion { return }

        let stages = try plan.findPath(from: currentVersion, to: targetVersion)
        if stages.isEmpty { return }

        logger.info("Starting migration from \(currentVersion) to \(targetVersion)")

        for stage in stages {
            try await executeStage(stage)
        }

        logger.info("Migration complete: now at version \(targetVersion)")
    }

    private func executeStage(_ stage: MigrationStage) async throws {
        logger.info("Executing \(stage.migrationDescription)")

        let storeRegistry = try await buildStoreRegistry()
        let metadataSubspace = try await getMetadataSubspace()

        let context = MigrationContext(
            database: database,
            schema: schema,
            metadataSubspace: metadataSubspace,
            storeRegistry: storeRegistry,
            indexConfigurations: indexConfigurations
        )

        if let willMigrate = stage.willMigrate {
            try await willMigrate(context)
        }

        for descriptor in stage.addedIndexDescriptors {
            logger.info("Adding index: \(descriptor.name)")
            try await context.addIndex(descriptor)
        }

        for indexName in stage.indexChanges.removed {
            logger.info("Removing index: \(indexName)")
            try await context.removeIndex(indexName: indexName, addedVersion: stage.fromVersionIdentifier)
        }

        if let didMigrate = stage.didMigrate {
            try await didMigrate(context)
        }

        try await setCurrentSchemaVersion(stage.toVersionIdentifier)
        logger.info("Updated schema version to \(stage.toVersionIdentifier)")
    }

    private func buildStoreRegistry() async throws -> [String: MigrationStoreInfo] {
        var registry: [String: MigrationStoreInfo] = [:]
        let directoryLayer = DirectoryLayer(database: database)

        for entity in schema.entities {
            // Use entity's directory path from Persistable
            let dirSubspace = try await directoryLayer.createOrOpen(path: [entity.name])
            let info = MigrationStoreInfo(
                subspace: dirSubspace.subspace,
                indexSubspace: dirSubspace.subspace.subspace("I")
            )
            registry[entity.name] = info
        }

        return registry
    }
}

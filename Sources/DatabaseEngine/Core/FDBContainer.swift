import Foundation
import FoundationDB
import Core
import Synchronization
import Logging

/// FDBContainer - Application resource manager for FoundationDB persistence
///
/// **Design Philosophy**:
/// FDBContainer is a **resource manager** that connects:
/// - **Schema**: Defines entities and indexes
/// - **Database**: Provides DirectoryLayer access for system operations
/// - **Persistable types**: Define their own directory paths via `#Directory` macro
///
/// FDBContainer does NOT manage:
/// - `subspace`: Each Persistable type defines its own directory via `#Directory`
/// - `directoryLayer`: Used only for system-level operations
/// - `dataStore`: Created dynamically based on resolved directory
/// - **Transactions**: FDBContext manages transactions (Context-centric design)
/// - **ReadVersionCache**: FDBContext owns cache per unit of work
///
/// **Responsibilities**:
/// - Schema management
/// - Database connection for system operations (DirectoryLayer, Migration)
/// - Directory resolution from Persistable type metadata
/// - DataStore factory
///
/// **Architecture**:
/// ```
/// FDBContainer (Resource Manager)
///     ├── database: DatabaseProtocol (for system operations only)
///     ├── schema: Schema
///     └── newContext() → FDBContext (owns transactions + cache)
/// ```
///
/// **Context-Centric Design**:
/// - FDBContainer does NOT create application transactions
/// - FDBContext owns ReadVersionCache and creates transactions via TransactionRunner
/// - System operations (DirectoryLayer, Migration) use `database.withTransaction()` directly
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
/// // 2. Create container (async - connects to DB and initializes indexes)
/// let schema = Schema([User.self])
/// let container = try await FDBContainer(for: schema)
///
/// // 3. Use context
/// let context = container.newContext()
/// context.insert(user)
/// try await context.save()
/// ```
public final class FDBContainer: Sendable {
    // MARK: - Properties

    /// The underlying FDB database connection
    ///
    /// Thread-safe: FDB client handles thread safety internally.
    /// Used for system operations (DirectoryLayer, Migration).
    /// Application transactions should use FDBContext.withTransaction().
    nonisolated(unsafe) public let database: any DatabaseProtocol

    /// Schema (version, entities, indexes)
    public let schema: Schema

    /// Configuration
    public let configuration: FDBConfiguration?

    /// Security configuration
    public let securityConfiguration: SecurityConfiguration

    /// Security delegate for DataStore operations
    ///
    /// Created from securityConfiguration and uses TaskLocal for auth context.
    public let securityDelegate: (any DataStoreSecurityDelegate)?

    /// Index configurations grouped by indexName
    public let indexConfigurations: [String: [any IndexConfiguration]]

    /// Logger
    private let logger: Logger

    /// Directory cache for performance
    private let directoryCache: Mutex<[String: Subspace]>

    /// Migration plan
    nonisolated(unsafe) private var _migrationPlan: (any SchemaMigrationPlan.Type)?

    // MARK: - Initialization

    /// Initialize FDBContainer with schema
    ///
    /// Connects to FoundationDB and initializes all indexes to `readable` state.
    /// This ensures indexes are ready for both writes and queries immediately.
    ///
    /// **Example**:
    /// ```swift
    /// let schema = Schema([User.self, Order.self])
    /// let container = try await FDBContainer(for: schema)
    ///
    /// // With security enabled
    /// let secureContainer = try await FDBContainer(
    ///     for: schema,
    ///     security: .enabled(adminRoles: ["admin"])
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - configuration: Optional FDBConfiguration
    ///   - security: Security configuration (default: enabled)
    /// - Throws: Error if database connection or index initialization fails
    public init(
        for schema: Schema,
        configuration: FDBConfiguration? = nil,
        security: SecurityConfiguration = .enabled()
    ) async throws {
        guard !schema.entities.isEmpty else {
            throw FDBRuntimeError.internalError("Schema must contain at least one entity")
        }

        let database = try FDBClient.openDatabase(clusterFilePath: configuration?.url?.path)

        self.database = database
        self.schema = schema
        self.configuration = configuration
        self.securityConfiguration = security
        self.securityDelegate = security.isEnabled
            ? DefaultSecurityDelegate(configuration: security)
            : nil

        // Merge user-provided configurations with auto-generated ones
        let userConfigs = configuration?.indexConfigurations ?? []
        let autoConfigs = Self.generateAutoConfigurations(schema: schema, database: database)
        self.indexConfigurations = Self.aggregateIndexConfigurations(userConfigs + autoConfigs)

        self._migrationPlan = nil
        self.logger = Logger(label: "com.fdb.runtime.container")
        self.directoryCache = Mutex([:])

        // Initialize all indexes to readable state
        try await ensureIndexesReady()

        // Persist schema catalog for CLI and dynamic tools
        let registry = SchemaRegistry(database: database)
        try await registry.persist(schema)
    }

    /// Initialize FDBContainer with raw database
    ///
    /// - Parameters:
    ///   - database: The raw FDB database connection
    ///   - schema: Schema defining entities and indexes
    ///   - configuration: Optional configuration
    ///   - security: Security configuration (default: enabled)
    ///   - indexConfigurations: Index configurations
    public init(
        database: any DatabaseProtocol,
        schema: Schema,
        configuration: FDBConfiguration? = nil,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        precondition(!schema.entities.isEmpty, "Schema must contain at least one entity")

        self.database = database
        self.schema = schema
        self.configuration = configuration
        self.securityConfiguration = security
        self.securityDelegate = security.isEnabled
            ? DefaultSecurityDelegate(configuration: security)
            : nil

        // Merge user-provided configurations with auto-generated ones
        let autoConfigs = Self.generateAutoConfigurations(schema: schema, database: database)
        self.indexConfigurations = Self.aggregateIndexConfigurations(indexConfigurations + autoConfigs)

        self._migrationPlan = nil
        self.logger = Logger(label: "com.fdb.runtime.container")
        self.directoryCache = Mutex([:])
    }

    // MARK: - Index Initialization

    /// Ensure all indexes are in `readable` state
    ///
    /// This method transitions all indexes to `readable`:
    /// - `disabled` → `writeOnly` → `readable`
    /// - `writeOnly` → `readable`
    /// - `readable` → no-op
    ///
    /// Index state is persisted in FoundationDB. Once indexes are `readable`,
    /// subsequent calls are no-ops.
    private func ensureIndexesReady() async throws {
        for entity in schema.entities {
            guard !entity.indexDescriptors.isEmpty else { continue }

            // Resolve directory for this entity
            // Use root subspace (same as FDBDataStore) so state is consistent
            let subspace = try await resolveDirectory(for: entity.persistableType)
            let stateManager = IndexStateManager(container: self, subspace: subspace)

            // Get index names for this entity
            let indexNames = entity.indexDescriptors.map { $0.name }

            // Get current states
            let states = try await stateManager.states(of: indexNames)

            // Transition each index to readable
            for indexName in indexNames {
                let currentState = states[indexName] ?? .disabled

                switch currentState {
                case .disabled:
                    // disabled → writeOnly → readable
                    try await stateManager.enable(indexName)
                    try await stateManager.makeReadable(indexName)
                    logger.info("Initialized index '\(indexName)': disabled → readable")

                case .writeOnly:
                    // writeOnly → readable
                    try await stateManager.makeReadable(indexName)
                    logger.info("Initialized index '\(indexName)': writeOnly → readable")

                case .readable:
                    // Already readable, nothing to do
                    break
                }
            }
        }
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
    /// Unified API for both static and dynamic directories.
    /// - Static directories: Use default empty path
    /// - Dynamic directories: Provide path with field values
    ///
    /// **Usage**:
    /// ```swift
    /// // Static directory
    /// let subspace = try await container.resolveDirectory(for: User.self)
    ///
    /// // Dynamic directory
    /// var path = DirectoryPath<Order>()
    /// path.set(\.tenantID, to: "tenant_123")
    /// let subspace = try await container.resolveDirectory(for: Order.self, path: path)
    ///
    /// // From model instance
    /// let subspace = try await container.resolveDirectory(for: Order.self, path: .from(order))
    /// ```
    public func resolveDirectory<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> Subspace {
        try await resolveDirectory(for: type, path: AnyDirectoryPath(path))
    }

    /// Resolve directory (type-erased version)
    ///
    /// Used when the generic type is not known at compile time.
    public func resolveDirectory(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        let directoryPath = path ?? AnyDirectoryPath(for: type)
        try directoryPath.validate()

        let pathComponents = directoryPath.resolve()
        let cacheKey = pathComponents.joined(separator: "/")

        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let layer = convertDirectoryLayer(type.directoryLayer)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: pathComponents, type: layer)
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

    // MARK: - Store Access

    /// Get DataStore for a Persistable type
    ///
    /// **Usage**:
    /// ```swift
    /// // Static directory
    /// let store = try await container.store(for: User.self)
    ///
    /// // Dynamic directory
    /// var path = DirectoryPath<Order>()
    /// path.set(\.tenantID, to: "tenant_123")
    /// let store = try await container.store(for: Order.self, path: path)
    /// ```
    public func store<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> any DataStore {
        let subspace = try await resolveDirectory(for: type, path: path)
        return FDBDataStore(
            container: self,
            subspace: subspace,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
    }

    /// Get DataStore (type-erased version)
    internal func store(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> any DataStore {
        let subspace = try await resolveDirectory(for: type, path: path)
        return FDBDataStore(
            container: self,
            subspace: subspace,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
    }

    // MARK: - Polymorphic Directory Resolution

    /// Resolve directory for a polymorphic protocol
    ///
    /// Creates or opens the directory specified by the protocol's `#Directory` macro.
    /// Used by `FDBContext.fetchPolymorphic()` to retrieve all items of conforming types.
    ///
    /// **Example**:
    /// ```swift
    /// @Polymorphable
    /// protocol Document {
    ///     #Directory<Document>("app", "documents")
    /// }
    ///
    /// let subspace = try await container.resolvePolymorphicDirectory(for: Document.self)
    /// ```
    ///
    /// - Parameter protocolType: The Polymorphable protocol
    /// - Returns: The resolved subspace
    /// - Throws: Error if protocol has Field path components (not allowed)
    public func resolvePolymorphicDirectory<P: Polymorphable>(for protocolType: P.Type) async throws -> Subspace {
        let cacheKey = "_polymorphic_\(P.polymorphableType)"

        // Check cache first
        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let pathComponents = P.polymorphicDirectoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            } else {
                throw FDBRuntimeError.internalError(
                    "Polymorphic protocols cannot use Field path components. " +
                    "Use only static Path components (string literals) in #Directory."
                )
            }
        }

        // Create or open the directory
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path)
        let subspace = dirSubspace.subspace

        // Cache the result
        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
    }

    /// Resolve the directory for a Polymorphable protocol (type-erased version)
    ///
    /// Used when the protocol type is known only at runtime (e.g., from `polymorphicProtocol`).
    ///
    /// - Parameter protocolType: The Polymorphable protocol metatype
    /// - Returns: The resolved subspace
    /// - Throws: Error if protocol has Field path components (not allowed)
    public func resolvePolymorphicDirectory(for protocolType: any Polymorphable.Type) async throws -> Subspace {
        let cacheKey = "_polymorphic_\(protocolType.polymorphableType)"

        // Check cache first
        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let pathComponents = protocolType.polymorphicDirectoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            } else {
                throw FDBRuntimeError.internalError(
                    "Polymorphic protocols cannot use Field path components. " +
                    "Use only static Path components (string literals) in #Directory."
                )
            }
        }

        // Create or open the directory
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path)
        let subspace = dirSubspace.subspace

        // Cache the result
        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
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

    // MARK: - Auto Configuration Generation

    /// Generate configurations for indexes that support auto-configuration
    ///
    /// Scans all entities in the schema for indexes that conform to `AutoConfigurableIndexKind`
    /// and generates their configurations automatically.
    ///
    /// **Currently Supported**:
    /// - RelationshipIndexKind: Auto-generates `RelationshipIndexConfiguration` with item loader
    internal static func generateAutoConfigurations(
        schema: Schema,
        database: any DatabaseProtocol
    ) -> [any IndexConfiguration] {
        var configs: [any IndexConfiguration] = []

        for entity in schema.entities {
            for descriptor in entity.persistableType.indexDescriptors {
                // Check if the index kind supports auto-configuration
                guard let autoConfigurable = type(of: descriptor.kind) as? any AutoConfigurableIndexKind.Type else {
                    continue
                }

                // Create the item loader closure
                // Captures database (nonisolated unsafe - FDB handles thread safety internally)
                // and schema (value type)
                nonisolated(unsafe) let capturedDatabase = database
                let itemLoader: GenericItemLoader = { typeName, id, transaction in
                    try await Self.loadItemByTypeName(
                        typeName: typeName,
                        id: id,
                        schema: schema,
                        database: capturedDatabase,
                        transaction: transaction
                    )
                }

                // Generate configuration
                let config = autoConfigurable.createConfiguration(
                    indexName: descriptor.name,
                    modelTypeName: entity.name,
                    itemLoader: itemLoader
                )
                configs.append(config)
            }
        }

        return configs
    }

    /// Load an item by type name and ID
    ///
    /// Internal helper used by auto-generated configurations to load related items.
    ///
    /// - Parameters:
    ///   - typeName: Name of the Persistable type
    ///   - id: ID value of the item
    ///   - schema: Schema containing the type
    ///   - database: Database for directory resolution
    ///   - transaction: Transaction to use for reading
    /// - Returns: The loaded item, or nil if not found
    internal static func loadItemByTypeName(
        typeName: String,
        id: any Sendable,
        schema: Schema,
        database: any DatabaseProtocol,
        transaction: any TransactionProtocol
    ) async throws -> (any Persistable)? {
        // Find the entity in schema
        guard let entity = schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }

        let type = entity.persistableType

        // Resolve directory for the type
        let pathComponents = resolveStaticDirectoryPath(for: type)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: pathComponents)
        let subspace = dirSubspace.subspace

        // Build the item key
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(type.persistableType)

        let idTuple: Tuple
        if let tupleElement = id as? any TupleElement {
            idTuple = Tuple([tupleElement])
        } else if let stringId = id as? String {
            idTuple = Tuple([stringId])
        } else {
            return nil
        }

        let key = itemSubspace.pack(idTuple)

        // Read using ItemStorage
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)

        guard let data = try await storage.read(for: key) else {
            return nil
        }

        // Deserialize
        return try DataAccess.deserializeAny(data, as: type)
    }

    /// Resolve static directory path for a Persistable type
    ///
    /// For types with dynamic directories (Field components), this returns only
    /// the static portions. Used for auto-configuration where we don't have
    /// specific field values yet.
    private static func resolveStaticDirectoryPath(for type: any Persistable.Type) -> [String] {
        var path: [String] = []
        for component in type.directoryPathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            }
            // Skip Field components - we can't resolve them without a specific item
        }

        // If no path components, use the type name
        if path.isEmpty {
            path = [type.persistableType]
        }

        return path
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

        return try await database.withTransaction(configuration: .default) { transaction -> Schema.Version? in
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

        try await database.withTransaction(configuration: .batch) { transaction in
            try transaction.setOption(forOption: .accessSystemKeys)
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
            container: self,
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

        for entity in schema.entities {
            // Use resolveDirectory to respect #Directory definitions
            let subspace = try await resolveDirectory(for: entity.persistableType)
            let info = MigrationStoreInfo(
                subspace: subspace,
                indexSubspace: subspace.subspace(SubspaceKey.indexes),
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            registry[entity.name] = info
        }

        return registry
    }
}

// MARK: - Admin Context

extension FDBContainer {
    /// Create a new admin context for management operations
    ///
    /// **Usage**:
    /// ```swift
    /// let admin = container.newAdminContext()
    ///
    /// // Get collection statistics
    /// let stats = try await admin.collectionStatistics(User.self)
    ///
    /// // Explain query plan
    /// let plan = try await admin.explain(Query<User>().where(\.age > 18))
    ///
    /// // Watch for changes
    /// for await event in admin.watch(User.self, id: userId) {
    ///     // Handle event
    /// }
    /// ```
    ///
    /// - Returns: New AdminContext instance
    public func newAdminContext() -> AdminContextProtocol {
        AdminContext(container: self)
    }
}

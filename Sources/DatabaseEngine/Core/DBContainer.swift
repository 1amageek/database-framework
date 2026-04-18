import Foundation
import StorageKit
#if FOUNDATION_DB
import FDBStorage
#endif
import Core
import Synchronization
import Logging

/// DBContainer - Application resource manager for database persistence
///
/// **Design Philosophy**:
/// DBContainer is a **resource manager** that connects:
/// - **Schema**: Defines entities and indexes
/// - **StorageEngine**: Provides transaction and directory capabilities
/// - **Persistable types**: Define their own directory paths via `#Directory` macro
///
/// DBContainer does NOT manage:
/// - `subspace`: Each Persistable type defines its own directory via `#Directory`
/// - `directoryLayer`: Used only for system-level operations
/// - `dataStore`: Created dynamically based on resolved directory
/// - **Transactions**: FDBContext manages transactions (Context-centric design)
/// - **ReadVersionCache**: FDBContext owns cache per unit of work
///
/// **Responsibilities**:
/// - Schema management
/// - StorageEngine lifecycle (creates engine from configuration)
/// - Directory resolution from Persistable type metadata
/// - DataStore factory
///
/// **Architecture**:
/// ```
/// DBContainer (Resource Manager)
///     ├── engine: StorageEngine (for system operations only)
///     ├── schema: Schema
///     └── newContext() → FDBContext (owns transactions + cache)
/// ```
///
/// **Context-Centric Design**:
/// - DBContainer does NOT create application transactions
/// - FDBContext owns ReadVersionCache and creates transactions via TransactionRunner
/// - System operations (DirectoryLayer, Migration) use `engine.withTransaction()` directly
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
/// let container = try await DBContainer(for: schema)
///
/// // 3. Use context
/// let context = container.newContext()
/// context.insert(user)
/// try await context.save()
/// ```
public final class DBContainer: Sendable {
    // MARK: - Properties

    /// The underlying storage engine
    ///
    /// Thread-safe: storage engines handle thread safety internally.
    /// Used for system operations (DirectoryLayer, Migration).
    /// Application transactions should use FDBContext.withTransaction().
    nonisolated(unsafe) public let engine: any StorageEngine

    /// Schema (version, entities, indexes)
    public let schema: Schema

    /// Configuration
    public let configuration: DBConfiguration

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

    /// DataStore cache keyed by resolved directory path
    ///
    /// Stores are immutable wrappers around a resolved subspace, so sharing them
    /// avoids rebuilding helper services on repeated point reads and saves.
    private let dataStoreCache: Mutex<[String: FDBDataStore]>

    /// Migration plan
    nonisolated(unsafe) private var _migrationPlan: (any SchemaMigrationPlan.Type)?

    // MARK: - Initialization

    /// Initialize DBContainer with schema and configuration
    ///
    /// Creates the storage engine based on the configuration's backend,
    /// then initializes all indexes to `readable` state.
    ///
    /// **Example**:
    /// ```swift
    /// // Default FDB backend
    /// let container = try await DBContainer(for: schema)
    ///
    /// // With security
    /// let container = try await DBContainer(
    ///     for: schema,
    ///     security: .enabled(adminRoles: ["admin"])
    /// )
    ///
    /// // Custom backend
    /// let engine = try SQLiteStorageEngine(configuration: .inMemory)
    /// let container = try await DBContainer(
    ///     for: schema,
    ///     configuration: .init(backend: .custom(engine))
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - configuration: Database configuration (default: FDB backend)
    ///   - security: Security configuration (default: enabled)
    /// - Throws: Error if engine creation or index initialization fails
    ///
    /// - Note: This initializer performs two side effects:
    ///   1. **Index initialization** — transitions all indexes to `readable` state via `ensureIndexesReady()`
    ///   2. **Schema persistence** — writes `Schema.Entity` via `SchemaRegistry.persist()`,
    ///      enabling CLI and dynamic tools to discover schemas without compiled Swift types
    #if FOUNDATION_DB
    /// Initialize with default FDB backend
    public convenience init(
        for schema: Schema,
        security: SecurityConfiguration = .enabled()
    ) async throws {
        try await self.init(for: schema, configuration: DBConfiguration(), security: security)
    }
    #endif

    /// Initialize with explicit configuration
    public convenience init(
        for schema: Schema,
        configuration: DBConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws {
        try await self.init(
            for: schema,
            configuration: configuration,
            security: security,
            persistSchemaCatalog: true
        )
    }

    internal init(
        for schema: Schema,
        configuration: DBConfiguration,
        security: SecurityConfiguration,
        persistSchemaCatalog: Bool,
        initializeIndexes: Bool = true
    ) async throws {
        guard !schema.entities.isEmpty else {
            throw FDBRuntimeError.internalError("Schema must contain at least one entity")
        }

        // Create engine based on backend configuration
        switch configuration.backend {
        #if FOUNDATION_DB
        case .fdb(let fdbConfig):
            self.engine = try await FDBStorageEngine(configuration: fdbConfig)
        #endif
        case .custom(let engine):
            self.engine = engine
        }

        self.schema = schema
        self.configuration = configuration
        self.securityConfiguration = security
        self.securityDelegate = security.isEnabled
            ? DefaultSecurityDelegate(configuration: security)
            : nil

        // Merge user-provided configurations with auto-generated ones
        let userConfigs = configuration.indexConfigurations
        let autoConfigs = Self.generateAutoConfigurations(schema: schema, database: engine)
        self.indexConfigurations = Self.aggregateIndexConfigurations(userConfigs + autoConfigs)

        self._migrationPlan = nil
        self.logger = Logger(label: "com.db.runtime.container")
        self.directoryCache = Mutex([:])
        self.dataStoreCache = Mutex([:])

        registerSchemaTypesForIndexBuilding(schema)

        if initializeIndexes {
            // Initialize all indexes to readable state
            try await ensureIndexesReady()
        }

        if persistSchemaCatalog {
            // Persist schema catalog (entities + ontology) for CLI and dynamic tools
            let registry = SchemaRegistry(database: engine)
            try await registry.persist(schema)
        }
    }

    // MARK: - Index Initialization

    /// Ensure all indexes are in `readable` state
    ///
    /// Uses `IndexStateManager.ensureReadable()` to atomically set all indexes
    /// to `readable` in a single transaction per entity. Idempotent and safe
    /// for concurrent execution from multiple DBContainer instances.
    ///
    /// - `disabled` → `readable` (direct)
    /// - `writeOnly` → `readable` (recovery from abandoned build)
    /// - `readable` → no-op
    public func ensureIndexesReady() async throws {
        for entity in schema.entities {
            guard !entity.indexDescriptors.isEmpty else { continue }
            guard let persistableType = entity.persistableType else { continue }
            let subspace = try await resolveDirectory(for: persistableType)
            let stateManager = IndexStateManager(container: self, subspace: subspace)
            let indexNames = entity.indexDescriptors.map { $0.name }
            try await stateManager.ensureReadable(indexNames)
        }
        for group in schema.polymorphicGroups {
            let descriptors = schema.polymorphicIndexDescriptors(identifier: group.identifier)
            guard !descriptors.isEmpty else { continue }
            let subspace = try await resolvePolymorphicDirectory(for: group.identifier)
            let stateManager = IndexStateManager(container: self, subspace: subspace)
            let indexNames = descriptors.map(\.name)
            try await stateManager.ensureReadable(indexNames)
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

        let subspace = try await engine.directoryService.createOrOpen(path: pathComponents)

        directoryCache.withLock { $0[cacheKey] = subspace }
        return subspace
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
        try await fdbStore(for: type, path: path)
    }

    internal func fdbStore<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> FDBDataStore {
        let cacheKey = storeCacheKey(for: type, path: AnyDirectoryPath(path))
        if let cached = dataStoreCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let subspace = try await resolveDirectory(for: type, path: path)
        let store = FDBDataStore(
            container: self,
            subspace: subspace,
            persistableType: type.persistableType,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
        dataStoreCache.withLock { $0[cacheKey] = store }
        return store
    }

    /// Get DataStore (type-erased version)
    internal func store(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> any DataStore {
        try await fdbStore(for: type, path: path)
    }

    private func registerSchemaTypesForIndexBuilding(_ schema: Schema) {
        for entity in schema.entities {
            guard let persistableType = entity.persistableType else {
                continue
            }
            registerIndexBuilderIfPossible(for: persistableType)
        }
    }

    private func registerIndexBuilderIfPossible(for type: any Persistable.Type) {
        func helper<T: Persistable>(_ concreteType: T.Type) {
            IndexBuilderRegistry.shared.register(concreteType)
        }
        _openExistential(type, do: helper)
    }

    internal func fdbStore(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> FDBDataStore {
        let cacheKey = storeCacheKey(for: type, path: path)
        if let cached = dataStoreCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let subspace = try await resolveDirectory(for: type, path: path)
        let store = FDBDataStore(
            container: self,
            subspace: subspace,
            persistableType: type.persistableType,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
        dataStoreCache.withLock { $0[cacheKey] = store }
        return store
    }

    private func storeCacheKey(
        for type: any Persistable.Type,
        path: AnyDirectoryPath?
    ) -> String {
        let components = (path ?? AnyDirectoryPath(for: type)).resolve()
        return components.joined(separator: "/")
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
        let subspace = try await engine.directoryService.createOrOpen(path: path)

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
        let subspace = try await engine.directoryService.createOrOpen(path: path)

        // Cache the result
        directoryCache.withLock { $0[cacheKey] = subspace }

        return subspace
    }

    /// Resolve a polymorphic group by its logical identifier.
    public func polymorphicGroup(identifier: String) throws -> PolymorphicGroup {
        guard let group = schema.polymorphicGroup(identifier: identifier) else {
            throw FDBRuntimeError.internalError(
                "Polymorphic group '\(identifier)' is not registered in Schema."
            )
        }
        return group
    }

    /// Resolve the directory for a polymorphic group identifier.
    public func resolvePolymorphicDirectory(for identifier: String) async throws -> Subspace {
        let group = try polymorphicGroup(identifier: identifier)
        let cacheKey = "_polymorphic_\(group.identifier)"

        if let cached = directoryCache.withLock({ $0[cacheKey] }) {
            return cached
        }

        let path = try group.resolvedDirectoryPath()
        let subspace = try await engine.directoryService.createOrOpen(path: path)
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
        database: any StorageEngine
    ) -> [any IndexConfiguration] {
        var configs: [any IndexConfiguration] = []

        for entity in schema.entities {
            guard let persistableType = entity.persistableType else { continue }
            for descriptor in persistableType.indexDescriptors {
                // Check if the index kind supports auto-configuration
                guard let autoConfigurable = type(of: descriptor.kind) as? any AutoConfigurableIndexKind.Type else {
                    continue
                }

                // Create the item loader closure
                // Captures database (nonisolated unsafe - storage engines handle thread safety internally)
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
    ///   - database: StorageEngine for directory resolution
    ///   - transaction: Transaction to use for reading
    /// - Returns: The loaded item, or nil if not found
    internal static func loadItemByTypeName(
        typeName: String,
        id: any Sendable,
        schema: Schema,
        database: any StorageEngine,
        transaction: any Transaction
    ) async throws -> (any Persistable)? {
        // Find the entity in schema
        guard let entity = schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }

        guard let type = entity.persistableType else {
            return nil
        }

        // Resolve directory for the type
        let pathComponents = resolveStaticDirectoryPath(for: type)
        let subspace = try await database.directoryService.createOrOpen(path: pathComponents)

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

extension DBContainer {
    /// Get metadata directory for schema versioning
    private func getMetadataSubspace() async throws -> Subspace {
        try await engine.directoryService.createOrOpen(path: ["_metadata"])
    }

    /// Get the current schema version from storage
    public func getCurrentSchemaVersion() async throws -> Schema.Version? {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))

        return try await engine.withTransaction(configuration: .default) { transaction -> Schema.Version? in
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

    /// Set the current schema version in storage
    public func setCurrentSchemaVersion(_ version: Schema.Version) async throws {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))

        try await engine.withTransaction(configuration: .batch) { transaction in
            try transaction.setOption(forOption: .accessSystemKeys)
            let versionTuple = Tuple(version.major, version.minor, version.patch)
            transaction.setValue(versionTuple.pack(), for: versionKey)
        }
    }
}

// MARK: - VersionedSchema Support

extension DBContainer {
    /// Initialize with VersionedSchema and MigrationPlan
    public convenience init<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: DBConfiguration
    ) async throws {
        try P.validate()
        let schemaInstance = S.makeSchema()
        try await self.init(
            for: schemaInstance,
            configuration: configuration,
            security: .enabled(),
            persistSchemaCatalog: false,
            initializeIndexes: false
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
        let registry = SchemaRegistry(database: engine)

        guard let currentVersion else {
            try await registry.persist(schema)
            try await setCurrentSchemaVersion(targetVersion)
            try await ensureIndexesReady()
            logger.info("Set initial schema version: \(targetVersion)")
            return
        }

        if currentVersion >= targetVersion {
            try await registry.persist(schema)
            try await ensureIndexesReady()
            return
        }

        let stages = try plan.findPath(from: currentVersion, to: targetVersion)
        if stages.isEmpty { return }

        logger.info("Starting migration from \(currentVersion) to \(targetVersion)")

        for stage in stages {
            try await executeStage(stage)
        }

        try await ensureIndexesReady()

        logger.info("Migration complete: now at version \(targetVersion)")
    }

    private func executeStage(_ stage: MigrationStage) async throws {
        logger.info("Executing \(stage.migrationDescription)")

        let sourceSchema = stage.fromVersion.makeSchema()
        let targetSchema = stage.toVersion.makeSchema()
        // Build per-schema registries. Same entity name may resolve to
        // different subspaces when the source and target versions declare
        // different `#Directory` paths — so we can't dedup across schemas.
        let sourceStoreRegistry = try await buildStoreRegistry(for: sourceSchema)
        let targetStoreRegistry = try await buildStoreRegistry(for: targetSchema)
        let metadataSubspace = try await getMetadataSubspace()
        let stageIndexConfigurations = Self.aggregateIndexConfigurations(
            configuration.indexConfigurations + Self.generateAutoConfigurations(schema: targetSchema, database: engine)
        )

        let context = MigrationContext(
            container: self,
            schema: targetSchema,
            sourceSchema: sourceSchema,
            metadataSubspace: metadataSubspace,
            sourceStoreRegistry: sourceStoreRegistry,
            targetStoreRegistry: targetStoreRegistry,
            indexConfigurations: stageIndexConfigurations
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

        let registry = SchemaRegistry(database: engine)
        let persistMode: SchemaRegistryPersistMode = if stage.isLightweight {
            .strict
        } else {
            .allowBreakingChanges(entityNames: stage.entitiesRequiringCustomMigration)
        }
        try await registry.persist(targetSchema, mode: persistMode)
        try await setCurrentSchemaVersion(stage.toVersionIdentifier)
        logger.info("Updated schema version to \(stage.toVersionIdentifier)")
    }

    private func buildStoreRegistry(for schema: Schema) async throws -> [String: MigrationStoreInfo] {
        var registry: [String: MigrationStoreInfo] = [:]

        for entity in schema.entities {
            guard let persistableType = entity.persistableType else { continue }
            // Use resolveDirectory to respect #Directory definitions declared
            // by *this schema's* Swift type — V1 and V2 with the same entity
            // name may point to different directories.
            let subspace = try await resolveDirectory(for: persistableType)
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

extension DBContainer {
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

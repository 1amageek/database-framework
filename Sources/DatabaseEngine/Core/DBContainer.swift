import DatabaseTypes
import StorageKit
#if FOUNDATION_DB
import FDBStorage
#endif
import DatabaseKit
import Synchronization

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
/// - **Transactions**: DatabaseContext manages transactions (Context-centric design)
/// - **ReadVersionCache**: DatabaseContext owns cache per unit of work
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
///     └── newContext() → DatabaseContext (owns transactions + cache)
/// ```
///
/// **Context-Centric Design**:
/// - DBContainer does NOT create application transactions
/// - DatabaseContext owns ReadVersionCache and creates transactions via TransactionRunner
/// - System operations (DirectoryLayer, Migration) use `transactionExecutor.withTransaction()` directly
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
/// let schema = try Schema(
///     entities: [try User.schemaEntity],
///     version: .init(1, 0, 0)
/// )
/// let runtime = try DatabaseFrameworkRuntime.configuration(
///     entityRuntimes: [User.self]
/// )
/// let container = try await DBContainer.open(
///     for: schema,
///     configuration: DBConfiguration(backend: .custom(engine)),
///     runtimeConfiguration: runtime
/// )
///
/// // 3. Use context
/// let context = container.newContext()
/// try context.insert(user)
/// try await context.save()
/// ```
public final class DBContainer: Sendable {
    private struct PreparedStorage: Sendable {
        let engine: any StorageEngine
        let format: DatabaseFormatDescriptor
        let partitionCatalog: DatabasePartitionCatalog
        let metadataSubspace: Subspace
    }

    private struct DataStoreCache: Sendable {
        var stores = DatabaseStoreCache<DatabaseDataStore>()
    }

    // MARK: - Properties

    /// The underlying storage engine
    ///
    /// Thread-safe: storage engines handle thread safety internally.
    /// Used for system operations (DirectoryLayer, Migration).
    /// Application transactions should use DatabaseContext.withTransaction().
    public let engine: any StorageEngine

    /// Typed transaction execution over the dynamically selected storage engine.
    ///
    /// This concrete boundary avoids invoking generic protocol-extension methods
    /// on an existential engine in Embedded Swift.
    public let transactionExecutor: StorageTransactionExecutor

    /// Schema (version, entities, indexes)
    public let schema: Schema

    /// Configuration
    public let configuration: DBConfiguration

    /// Container-scoped monotonic time source.
    public var monotonicClock: any StorageMonotonicClock {
        configuration.monotonicClock
    }

    public var wallClock: any WallClock {
        configuration.wallClock
    }

    /// Container-scoped runtime extensions and operation dependencies.
    public let runtimeConfiguration: DatabaseRuntimeConfiguration

    /// Container-scoped factory for the database's canonical entity format.
    public let itemStorageFactory: ItemStorageFactory

    /// Persisted database-wide physical format source of truth.
    public let databaseFormat: DatabaseFormatDescriptor

    /// Security configuration
    public let securityConfiguration: SecurityConfiguration

    /// Security delegate for DataStore operations
    ///
    /// Created from securityConfiguration and uses TaskLocal for auth context.
    public let securityDelegate: (any DataStoreSecurityDelegate)?

    /// Container-scoped observer for data store metrics.
    internal let dataStoreDelegate: any DataStoreDelegate

    /// Index configurations grouped by indexName
    public let indexConfigurations: [String: [any IndexRuntimeConfiguration]]

    /// Database event logger selected by the container configuration.
    private let logger: DatabaseLogger

    /// DataStore cache keyed by resolved directory path
    ///
    /// Stores are immutable wrappers around a resolved subspace, so sharing them
    /// avoids rebuilding helper services on repeated point reads and saves.
    private let dataStoreCache: Mutex<DataStoreCache>

    /// Persistent catalog of every resolved dynamic partition.
    private let partitionCatalog: DatabasePartitionCatalog

    /// Stable metadata namespace used by schema lifecycle operations.
    private let metadataSubspace: Subspace

    /// Migration plan
    private let migrationPlanStorage: Mutex<(any SchemaMigrationPlan.Type)?>

    // MARK: - Initialization

    /// Opens a database container after preparing and validating its durable
    /// storage dependencies.
    public static func open(
        for schema: Schema,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: true,
            initializeIndexes: true
        )
    }

    internal static func open(
        testing schema: Schema,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        initializeIndexes: Bool = true
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: false,
            initializeIndexes: initializeIndexes
        )
    }

    private static func open(
        for schema: Schema,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration,
        persistSchemaCatalog: Bool,
        initializeIndexes: Bool
    ) async throws -> DBContainer {
        guard !schema.entities.isEmpty else {
            throw DatabaseRuntimeError.internalError(
                "Schema must contain at least one entity"
            )
        }
        try runtimeConfiguration.validate(schema: schema)
        try IndexRuntimeConfigurationValidator.validate(
            configuration.indexConfigurations,
            schema: schema,
            entityRuntimes: runtimeConfiguration.entityRuntimes
        )

        let preparedStorage = try await prepareStorage(
            configuration: configuration
        )
        let container = DBContainer(
            schema: schema,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            preparedStorage: preparedStorage
        )

        if initializeIndexes {
            try await container.ensureIndexesReady()
        }

        if persistSchemaCatalog {
            let registry = SchemaRegistry(
                database: container.engine,
                clock: container.monotonicClock
            )
            try await registry.persist(schema)
        }
        return container
    }

    /// Opens DBContainer with schema and configuration.
    ///
    /// Creates the storage engine based on the configuration's backend,
    /// then initializes all indexes to `readable` state.
    ///
    /// **Example**:
    /// ```swift
    /// // Explicit backend configuration
    /// let container = try await DBContainer.open(
    ///     for: schema,
    ///     configuration: .init(backend: .custom(engine))
    /// )
    ///
    /// // With security
    /// let container = try await DBContainer.open(
    ///     for: schema,
    ///     configuration: .init(backend: .custom(engine)),
    ///     security: .enabled(adminRoles: ["admin"])
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - configuration: Database configuration
    ///   - security: Security configuration (default: enabled)
    /// - Throws: Error if engine creation or index initialization fails
    ///
    /// - Note: Opening performs two side effects:
    ///   1. **Index validation** — initializes index metadata only for empty stores and rejects incomplete indexes
    ///   2. **Schema persistence** — writes `Schema.Entity` via `SchemaRegistry.persist()`,
    ///      enabling CLI and dynamic tools to discover schemas without compiled Swift types
    private static func prepareStorage(
        configuration: DBConfiguration
    ) async throws -> PreparedStorage {
        let resolvedEngine: any StorageEngine
        switch configuration.backend {
        #if FOUNDATION_DB
        case .fdb(let fdbConfig):
            resolvedEngine = try await FDBStorageEngine(configuration: fdbConfig)
        #endif
        case .custom(let engine):
            resolvedEngine = engine
        }
        let expectedFormat = DatabaseFormatDescriptor.v1(
            itemStorage: configuration.itemStorage
        )
        let persistedFormat = try await DatabaseFormatCatalog(
            database: resolvedEngine,
            clock: configuration.monotonicClock
        ).installIfEmptyOrValidate(expectedFormat)
        let partitionCatalog = try await DatabasePartitionCatalog(
            engine: resolvedEngine,
            clock: configuration.monotonicClock
        )
        let metadataSubspace = try await resolvedEngine.resolveOrCreateNamespace(
            path: ["_metadata"]
        )
        return PreparedStorage(
            engine: resolvedEngine,
            format: persistedFormat,
            partitionCatalog: partitionCatalog,
            metadataSubspace: metadataSubspace
        )
    }

    private init(
        schema: Schema,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration,
        preparedStorage: PreparedStorage
    ) {
        self.engine = preparedStorage.engine
        self.transactionExecutor = StorageTransactionExecutor(
            engine: preparedStorage.engine
        )
        self.schema = schema
        self.configuration = configuration
        self.runtimeConfiguration = runtimeConfiguration
        self.itemStorageFactory = ItemStorageFactory(
            configuration: preparedStorage.format.itemStorage
        )
        self.databaseFormat = preparedStorage.format
        self.securityConfiguration = security
        self.securityDelegate = security.isEnabled
            ? RequestSecurityPolicyDelegate(
                configuration: security,
                policies: runtimeConfiguration.authorizationPolicies
            )
            : nil
        self.dataStoreDelegate = MetricsDataStoreDelegate(
            metrics: configuration.metrics
        )

        self.indexConfigurations = Self.aggregateIndexConfigurations(
            configuration.indexConfigurations
        )

        self.logger = configuration.logging.logger(
            label: "com.database.framework.container"
        )
        self.migrationPlanStorage = Mutex(nil)
        self.dataStoreCache = Mutex(DataStoreCache())
        self.partitionCatalog = preparedStorage.partitionCatalog
        self.metadataSubspace = preparedStorage.metadataSubspace
    }

    // MARK: - Index Initialization

    /// Validate all indexes and initialize indexes only for empty stores.
    ///
    /// Existing incomplete states fail fast. A missing state is initialized
    /// only when its source entity range is empty in the same transaction.
    public func ensureIndexesReady() async throws {
        for entity in schema.entities {
            guard !entity.indexDescriptors.isEmpty else { continue }
            guard !entity.hasDynamicDirectory else { continue }
            guard let persistableType = runtimeConfiguration.entityRuntimes.modelType(
                named: entity.name
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            let subspace = try await resolveDirectory(for: persistableType)
            let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
            let indexNames = entity.indexDescriptors.map { $0.name }
            try await lifecycleStore.ensureReadable(
                indexNames,
                entityRange: subspace
                    .subspace(SubspaceKey.items)
                    .subspace(persistableType.persistableType)
                    .range()
            )
        }
        for group in schema.polymorphicGroups {
            guard !group.indexes.isEmpty else { continue }
            let subspace = try await resolvePolymorphicDirectory(for: group.identifier)
            let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
            let indexNames = group.indexes.map { $0.name }
            try await lifecycleStore.ensureReadable(
                indexNames,
                entityRange: subspace.subspace(SubspaceKey.items).range()
            )
        }
    }

    // MARK: - Context Management

    /// Create a new context for data operations
    ///
    /// **Example**:
    /// ```swift
    /// let context = container.newContext()
    /// try context.insert(user)
    /// try context.insert(order)
    /// try await context.save()
    /// ```
    ///
    /// - Parameter autosaveEnabled: Whether to automatically save after operations (default: false)
    /// - Returns: New DatabaseContext instance
    public func newContext(autosaveEnabled: Bool = false) -> DatabaseContext {
        return DatabaseContext(container: self, autosaveEnabled: autosaveEnabled)
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
        try await resolveDirectory(for: type, path: try AnyDirectoryPath(path))
    }

    /// Resolve directory (type-erased version)
    ///
    /// Used when the generic type is not known at compile time.
    public func resolveDirectory(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        let entity = try schemaEntity(named: type.persistableType)
        return try await resolveDirectory(for: entity, path: path)
    }

    public func resolveDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: monotonicClock
        ) { transaction in
            try await self.resolveDirectory(
                for: entity,
                path: path,
                transaction: transaction
            )
        }
    }

    /// Resolve a model directory and partition catalog entry in one caller-owned
    /// transaction.
    package func resolveDirectory(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: schemaEntity(named: type.persistableType),
            path: path,
            transaction: transaction
        )
    }

    package func resolveDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()

        let subspace = try await engine.namespaceResolver.resolveOrCreate(
            path: directoryPath.resolve(),
            transaction: transaction
        )

        let partitions = directoryPath.canonicalPartitions()
        if !partitions.isEmpty {
            try await partitionCatalog.register(
                entity: entity.name,
                partitions: partitions,
                transaction: transaction
            )
        }

        return subspace
    }

    /// Open an existing model directory without creating namespace metadata or
    /// registering partition catalog entries.
    package func openDirectory(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await openDirectory(
            for: schemaEntity(named: type.persistableType),
            path: path,
            transaction: transaction
        )
    }

    package func openDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        return try await engine.namespaceResolver.resolveExisting(
            path: directoryPath.resolve(),
            transaction: transaction
        )
    }

    /// Resolves one declared index in the caller's read transaction.
    ///
    /// The read path never creates directory metadata, partition catalog
    /// entries, or index state. A directory that has never existed represents
    /// an empty logical partition and therefore returns `nil`.
    internal func readableIndexSubspace(
        named indexName: String,
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        try await readableIndexSubspace(
            named: indexName,
            for: schemaEntity(named: type.persistableType),
            path: path,
            transaction: transaction
        )
    }

    internal func readableIndexSubspace(
        named indexName: String,
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        let components = directoryPath.resolve()
        guard try await engine.namespaceResolver.namespaceExists(
            path: components,
            transaction: transaction
        ) else {
            return nil
        }

        let subspace = try await engine.namespaceResolver.resolveExisting(
            path: components,
            transaction: transaction
        )
        let lifecycleStore = IndexLifecycleStore(
            container: self,
            subspace: subspace
        )
        try await lifecycleStore.validateReadableForRead(
            [indexName],
            entityRange: subspace
                .subspace(SubspaceKey.items)
                .subspace(entity.name)
                .range(),
            transaction: transaction
        )
        return subspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
    }

    package func partitionCatalogPage(
        entity: String? = nil,
        continuation: ByteString? = nil,
        limit: Int
    ) async throws -> DatabasePartitionCatalogPage {
        try await partitionCatalog.page(
            entity: entity,
            continuation: continuation,
            limit: limit
        )
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
    package func store<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> DatabaseDataStore {
        let cacheKey = try storeCacheKey(for: type, path: AnyDirectoryPath(path))
        if let cached = dataStoreCache.withLock({
            $0.stores.value(for: cacheKey)
        }) {
            return cached
        }

        let subspace = try await resolveDirectory(for: type, path: path)
        try await initializeIndexStates(for: type, subspace: subspace)
        let store = DatabaseDataStore(
            container: self,
            subspace: subspace,
            persistableType: type.persistableType,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
        dataStoreCache.withLock { $0.stores.insert(store, for: cacheKey) }
        return store
    }

    /// Get DataStore (type-erased version)
    internal func store(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil
    ) async throws -> DatabaseDataStore {
        try await store(
            for: schemaEntity(named: type.persistableType),
            path: path
        )
    }

    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> DatabaseDataStore {
        let cacheKey = try storeCacheKey(for: entity, path: path)
        if let cached = dataStoreCache.withLock({
            $0.stores.value(for: cacheKey)
        }) {
            return cached
        }

        let subspace = try await resolveDirectory(for: entity, path: path)
        try await initializeIndexStates(for: entity, subspace: subspace)
        let store = DatabaseDataStore(
            container: self,
            subspace: subspace,
            persistableType: entity.name,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
        dataStoreCache.withLock { $0.stores.insert(store, for: cacheKey) }
        return store
    }

    /// Build a store whose directory and index state participate in the caller's
    /// transaction. The store is intentionally not inserted into the global
    /// cache until that transaction has committed.
    internal func store(
        for type: any Persistable.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        try await store(
            for: schemaEntity(named: type.persistableType),
            path: path,
            transaction: transaction
        )
    }

    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        let subspace = try await resolveDirectory(
            for: entity,
            path: path,
            transaction: transaction
        )
        try await initializeIndexStates(
            for: entity,
            subspace: subspace,
            transaction: transaction
        )
        return DatabaseDataStore(
            container: self,
            subspace: subspace,
            persistableType: entity.name,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
    }

    private func initializeIndexStates(
        for type: any Persistable.Type,
        subspace: Subspace
    ) async throws {
        try await initializeIndexStates(
            for: schemaEntity(named: type.persistableType),
            subspace: subspace
        )
    }

    private func initializeIndexStates(
        for entity: Schema.Entity,
        subspace: Subspace
    ) async throws {
        let indexNames = entity.indexDescriptors.map { $0.name }
        guard !indexNames.isEmpty else { return }

        let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
        try await lifecycleStore.initializeMissingStates(
            indexNames,
            entityRange: subspace
                .subspace(SubspaceKey.items)
                .subspace(entity.name)
                .range()
        )
    }

    private func initializeIndexStates(
        for type: any Persistable.Type,
        subspace: Subspace,
        transaction: any TransactionAccess
    ) async throws {
        try await initializeIndexStates(
            for: schemaEntity(named: type.persistableType),
            subspace: subspace,
            transaction: transaction
        )
    }

    private func initializeIndexStates(
        for entity: Schema.Entity,
        subspace: Subspace,
        transaction: any TransactionAccess
    ) async throws {
        let indexNames = entity.indexDescriptors.map { $0.name }
        guard !indexNames.isEmpty else { return }

        let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
        try await lifecycleStore.initializeMissingStates(
            indexNames,
            entityRange: subspace
                .subspace(SubspaceKey.items)
                .subspace(entity.name)
                .range(),
            transaction: transaction
        )
    }

    private func storeCacheKey(
        for type: any Persistable.Type,
        path: AnyDirectoryPath?
    ) throws -> DatabaseStoreCacheKey {
        try storeCacheKey(
            for: schemaEntity(named: type.persistableType),
            path: path
        )
    }

    private func storeCacheKey(
        for entity: Schema.Entity,
        path: AnyDirectoryPath?
    ) throws -> DatabaseStoreCacheKey {
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        let components = directoryPath.resolve()
        return DatabaseStoreCacheKey(
            entity: entity.name,
            components: components
        )
    }

    private func schemaEntity(
        named entityName: String
    ) throws -> Schema.Entity {
        guard let entity = schema.entity(named: entityName) else {
            throw ContainerSchemaError.entityNotFound(entityName)
        }
        return entity
    }

    // MARK: - Polymorphic Directory Resolution

    /// Resolve directory for a polymorphic protocol
    ///
    /// Creates or opens the directory specified by the protocol's `#Directory` macro.
    /// Used by `DatabaseContext.fetchPolymorphic()` to retrieve all items of conforming types.
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
        let pathComponents = P.polymorphicDirectoryPathComponents
        var path: [String] = []

        for component in pathComponents {
            switch component {
            case .staticPath(let value):
                path.append(value)
            case .dynamicField:
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic protocols cannot use Field path components. " +
                    "Use only static Path components (string literals) in #Directory."
                )
            }
        }

        return try await engine.resolveOrCreateNamespace(path: path)
    }

    /// Resolve a polymorphic group by its logical identifier.
    public func polymorphicGroup(identifier: String) throws -> PolymorphicGroup {
        guard let group = schema.polymorphicGroup(identifier: identifier) else {
            throw DatabaseRuntimeError.internalError(
                "Polymorphic group '\(identifier)' is not registered in Schema."
            )
        }
        return group
    }

    /// Resolve the directory for a polymorphic group identifier.
    public func resolvePolymorphicDirectory(for identifier: String) async throws -> Subspace {
        let group = try polymorphicGroup(identifier: identifier)
        let path = try group.resolvedDirectoryPath()
        return try await engine.resolveOrCreateNamespace(path: path)
    }

    /// Resolves a polymorphic projection directory in the caller-owned
    /// transaction so namespace creation and projected writes commit atomically.
    package func resolvePolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let group = try polymorphicGroup(identifier: identifier)
        return try await engine.namespaceResolver.resolveOrCreate(
            path: group.resolvedDirectoryPath(),
            transaction: transaction
        )
    }

    /// Opens an existing polymorphic projection without mutating namespace
    /// metadata. An absent directory represents an empty projection.
    package func openPolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        let group = try polymorphicGroup(identifier: identifier)
        let path = try group.resolvedDirectoryPath()
        guard try await engine.namespaceResolver.namespaceExists(
            path: path,
            transaction: transaction
        ) else {
            return nil
        }
        return try await engine.namespaceResolver.resolveExisting(
            path: path,
            transaction: transaction
        )
    }

    // MARK: - Index Configuration Management

    /// Check if an index has configurations
    public func hasIndexConfiguration(for indexName: String) -> Bool {
        guard let configs = indexConfigurations[indexName] else { return false }
        return !configs.isEmpty
    }

    /// Aggregate index configurations by indexName
    internal static func aggregateIndexConfigurations(
        _ indexConfigurations: [any IndexRuntimeConfiguration]
    ) -> [String: [any IndexRuntimeConfiguration]] {
        var result: [String: [any IndexRuntimeConfiguration]] = [:]
        for config in indexConfigurations {
            result[config.indexName, default: []].append(config)
        }
        return result
    }

}

// MARK: - Migration Support

extension DBContainer {
    /// Get metadata directory for schema versioning
    private func getMetadataSubspace() async throws -> Subspace {
        metadataSubspace
    }

    /// Get the current schema version from storage
    public func getCurrentSchemaVersion() async throws -> Schema.Version? {
        return try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: monotonicClock
        ) { transaction -> Schema.Version? in
            try await self.getCurrentSchemaVersion(transaction: transaction)
        }
    }

    package func getCurrentSchemaVersion(
        transaction: any TransactionAccess
    ) async throws -> Schema.Version? {
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        guard let versionBytes = try await transaction.getValue(
            for: versionKey,
            snapshot: false
        ) else {
            return nil
        }

        let tuple = try Tuple(packed: versionBytes)
        guard tuple.count == 3 else {
            throw DatabaseRuntimeError.internalError("Invalid version format")
        }

        guard case .signedInteger(let majorValue) = try tuple.value(at: 0),
              case .signedInteger(let minorValue) = try tuple.value(at: 1),
              case .signedInteger(let patchValue) = try tuple.value(at: 2),
              let major = UInt32(exactly: majorValue),
              let minor = UInt32(exactly: minorValue),
              let patch = UInt32(exactly: patchValue) else {
            throw DatabaseRuntimeError.internalError("Invalid version format")
        }
        return Schema.Version(major, minor, patch)
    }

    /// Installs one compiled schema snapshot for migration test setup.
    package func installSchemaSnapshot(
        for version: Schema.Version
    ) async throws {
        let installedSchema = try schemaDefinition(for: version)
        let metadataSubspace = try await getMetadataSubspace()
        try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            try Self.setCurrentSchemaSnapshot(
                installedSchema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
        }
    }

    private static func setCurrentSchemaSnapshot(
        _ schema: Schema,
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) throws {
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        let fingerprintKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("fingerprint"))
        try transaction.setValue(
            Tuple(
                Int(schema.version.major),
                Int(schema.version.minor),
                Int(schema.version.patch)
            ).pack(),
            for: versionKey
        )
        try transaction.setValue(
            try DatabaseSchemaFingerprint.compute(schema),
            for: fingerprintKey
        )
    }
}

// MARK: - VersionedSchema Support

extension DBContainer {
    /// Opens an application-compiled schema and attaches its migration plan.
    public static func open<P: SchemaMigrationPlan>(
        for schema: Schema,
        migrationPlan: P.Type,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        try P.validate()
        let container = try await open(
            for: schema,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: false,
            initializeIndexes: false
        )
        container.migrationPlanStorage.withLock { $0 = migrationPlan }
        return container
    }

    /// Opens a versioned schema and attaches its migration plan.
    public static func open<
        S: VersionedSchema,
        P: SchemaMigrationPlan
    >(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        try P.validate()
        let schemaInstance = try S.makeSchema()
        let container = try await open(
            for: schemaInstance,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: false,
            initializeIndexes: false
        )
        container.migrationPlanStorage.withLock { $0 = migrationPlan }
        return container
    }

    /// Return exact pending migration identifiers for the compiled schema.
    public func migrationStatus(
        targetVersion requestedTarget: Schema.Version? = nil
    ) async throws -> DatabaseMigrationStatus {
        let targetVersion = try migrationTarget(requestedTarget)
        return try await transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            try await self.migrationStatus(
                targetVersion: targetVersion,
                transaction: transaction
            )
        }
    }

    /// Resolves migration status in a caller-owned transaction.
    package func migrationStatus(
        targetVersion requestedTarget: Schema.Version? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseMigrationStatus {
        let targetVersion = try migrationTarget(requestedTarget)
        let currentVersion = try await getCurrentSchemaVersion(
            transaction: transaction
        )
        guard let currentVersion else {
            return DatabaseMigrationStatus(
                currentVersion: nil,
                targetVersion: targetVersion,
                pendingMigrationIdentifiers: ["bootstrap:\(targetVersion)"]
            )
        }
        if currentVersion == targetVersion {
            try await validatePersistedSchemaFingerprint(
                schema,
                transaction: transaction
            )
            return DatabaseMigrationStatus(
                currentVersion: currentVersion,
                targetVersion: targetVersion,
                pendingMigrationIdentifiers: []
            )
        }
        guard currentVersion < targetVersion else {
            throw MigrationPlanError.downgradeNotSupported(
                from: currentVersion,
                to: targetVersion
            )
        }
        guard let plan = migrationPlanStorage.withLock({ $0 }) else {
            throw MigrationPlanError.missingMigrationPlan(
                current: currentVersion,
                target: targetVersion
            )
        }
        guard let currentSchemaType = plan.schemas.first(where: {
            $0.versionIdentifier == currentVersion
        }) else {
            throw MigrationPlanError.schemaDefinitionNotFound(currentVersion)
        }
        try await validatePersistedSchemaFingerprint(
            currentSchemaType.makeSchema(),
            transaction: transaction
        )
        let stages = try plan.findPath(
            from: currentVersion,
            to: targetVersion
        )
        return DatabaseMigrationStatus(
            currentVersion: currentVersion,
            targetVersion: targetVersion,
            pendingMigrationIdentifiers: stages.map { $0.identifier }
        )
    }

    /// Execute at most `maximumStageCount` persisted migration transitions.
    public func runMigrations(
        targetVersion requestedTarget: Schema.Version? = nil,
        maximumStageCount: UInt64
    ) async throws -> DatabaseMigrationExecutionResult {
        guard maximumStageCount > 0 else {
            return DatabaseMigrationExecutionResult(
                completedStageCount: 0,
                isComplete: try await migrationStatus(
                    targetVersion: requestedTarget
                ).pendingMigrationIdentifiers.isEmpty
            )
        }
        let targetVersion = try migrationTarget(requestedTarget)
        let registry = SchemaRegistry(
            database: engine,
            clock: monotonicClock
        )
        var completedStageCount: UInt64 = 0

        while completedStageCount < maximumStageCount {
            let status = try await migrationStatus(
                targetVersion: targetVersion
            )
            guard !status.pendingMigrationIdentifiers.isEmpty else {
                try await registry.persist(schema)
                try await ensureIndexesReady()
                return DatabaseMigrationExecutionResult(
                    completedStageCount: completedStageCount,
                    isComplete: true
                )
            }

            if status.currentVersion == nil {
                let bootstrapped = try await bootstrapInitialSchemaIfNeeded(
                    targetVersion: targetVersion,
                    registry: registry
                )
                if bootstrapped {
                    completedStageCount += 1
                    logger.info("Set initial schema version: \(targetVersion)")
                }
                continue
            }

            guard let currentVersion = status.currentVersion,
                  let plan = migrationPlanStorage.withLock({ $0 }),
                  let stage = try plan.findPath(
                    from: currentVersion,
                    to: targetVersion
                  ).first else {
                throw DatabaseRuntimeError.internalError(
                    "Migration status has no executable stage"
                )
            }
            try await executeStage(stage)
            completedStageCount += 1
        }

        let isComplete = try await migrationStatus(
            targetVersion: targetVersion
        ).pendingMigrationIdentifiers.isEmpty
        if isComplete {
            try await ensureIndexesReady()
            logger.info("Migration complete: now at version \(targetVersion)")
        }
        return DatabaseMigrationExecutionResult(
            completedStageCount: completedStageCount,
            isComplete: isComplete
        )
    }

    /// Migrate to the current compiled schema version if needed.
    public func migrateIfNeeded() async throws {
        _ = try await runMigrations(maximumStageCount: .max)
    }

    private func migrationTarget(
        _ requestedTarget: Schema.Version?
    ) throws -> Schema.Version {
        let compiledVersion = schema.version
        let targetVersion = requestedTarget ?? compiledVersion
        guard targetVersion == compiledVersion else {
            throw MigrationPlanError.targetVersionDoesNotMatchCompiledSchema(
                requested: targetVersion,
                compiled: compiledVersion
            )
        }
        if let plan = migrationPlanStorage.withLock({ $0 }),
           plan.currentVersion != compiledVersion {
            throw DatabaseRuntimeError.internalError(
                "Migration plan target does not match the compiled schema"
            )
        }
        return targetVersion
    }

    private func schemaDefinition(
        for version: Schema.Version
    ) throws -> Schema {
        if schema.version == version {
            return schema
        }
        guard let plan = migrationPlanStorage.withLock({ $0 }),
              let versionedSchema = plan.schemas.first(where: {
                  $0.versionIdentifier == version
              }) else {
            throw MigrationPlanError.schemaDefinitionNotFound(version)
        }
        return try versionedSchema.makeSchema()
    }

    private func validatePersistedSchemaFingerprint(
        _ expectedSchema: Schema,
        transaction: any TransactionAccess
    ) async throws {
        let fingerprintKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("fingerprint"))
        guard let storedFingerprint = try await transaction.getValue(
            for: fingerprintKey,
            snapshot: false
        ) else {
            throw MigrationPlanError.schemaFingerprintMissing(
                expectedSchema.version
            )
        }
        let expectedFingerprint = try DatabaseSchemaFingerprint.compute(
            expectedSchema
        )
        guard storedFingerprint == expectedFingerprint else {
            throw MigrationPlanError.schemaFingerprintMismatch(
                expectedSchema.version
            )
        }
    }

    private func bootstrapInitialSchemaIfNeeded(
        targetVersion: Schema.Version,
        registry: SchemaRegistry
    ) async throws -> Bool {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        var staticStores: [(
            entity: String,
            range: (begin: ByteString, end: ByteString),
            lifecycleStore: IndexLifecycleStore,
            indexNames: [String]
        )] = []
        for entity in schema.entities {
            guard !entity.hasDynamicDirectory else {
                continue
            }
            guard let persistableType = runtimeConfiguration.entityRuntimes.modelType(
                named: entity.name
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            let subspace = try await resolveDirectory(for: persistableType)
            staticStores.append((
                entity: entity.name,
                range: subspace
                    .subspace(SubspaceKey.items)
                    .subspace(persistableType.persistableType)
                    .range(),
                lifecycleStore: IndexLifecycleStore(
                    container: self,
                    subspace: subspace
                ),
                indexNames: entity.indexDescriptors.map { $0.name }
            ))
        }
        let stores = staticStores

        return try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            guard try await transaction.getValue(
                for: versionKey,
                snapshot: false
            ) == nil else {
                return false
            }
            for store in stores {
                let rows = try await transaction.collectRange(
                    from: .firstGreaterOrEqual(store.range.begin),
                    to: .firstGreaterOrEqual(store.range.end),
                    limit: 1,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .small
                )
                guard rows.isEmpty else {
                    throw MigrationPlanError.unversionedStoreContainsEntities(
                        entity: store.entity
                    )
                }
                try await store.lifecycleStore.ensureReadable(
                    store.indexNames,
                    entityRange: store.range,
                    transaction: transaction
                )
            }
            try await registry.persistInitialSchema(
                self.schema,
                transaction: transaction
            )
            try Self.setCurrentSchemaSnapshot(
                self.schema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
            return true
        }
    }

    private func executeStage(_ stage: MigrationStage) async throws {
        logger.info("Executing \(stage.migrationDescription)")

        let sourceSchema = try stage.fromVersion.makeSchema()
        let targetSchema = try stage.toVersion.makeSchema()

        // Guard: a lightweight stage cannot move data across a `#Directory`
        // change. Compare the compiled directory contract directly so dynamic
        // partitions do not require a synthetic field value merely to validate
        // an otherwise metadata-only migration.
        if stage.isLightweight {
            let sourceEntities = Dictionary(
                uniqueKeysWithValues: sourceSchema.entities.map { ($0.name, $0) }
            )
            let mismatches = targetSchema.entities
                .compactMap { targetEntity -> String? in
                    guard let sourceEntity = sourceEntities[targetEntity.name] else {
                        return nil
                    }
                    return sourceEntity.directoryComponents
                            == targetEntity.directoryComponents
                        && sourceEntity.directoryLayer == targetEntity.directoryLayer
                        ? nil
                        : targetEntity.name
                }
                .sorted()
            if !mismatches.isEmpty {
                throw MigrationPlanError.lightweightDirectoryChange(
                    entityNames: mismatches,
                    from: stage.fromVersionIdentifier,
                    to: stage.toVersionIdentifier
                )
            }
        }

        let indexChanges = try stage.indexChanges
        let requiresStoreAccess = stage.willMigrate != nil
            || stage.didMigrate != nil
            || !indexChanges.added.isEmpty
            || !indexChanges.removed.isEmpty
        // Store registries are only needed by data and index work. Constructing
        // them for a metadata-only stage would incorrectly require one concrete
        // value for every dynamic partition.
        let sourceStoreRegistry = requiresStoreAccess
            ? try await buildStoreRegistry(for: sourceSchema)
            : [:]
        let targetStoreRegistry = requiresStoreAccess
            ? try await buildStoreRegistry(for: targetSchema)
            : [:]
        let metadataSubspace = try await getMetadataSubspace()
        let stageIndexConfigurations = Self.aggregateIndexConfigurations(
            configuration.indexConfigurations
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

        for indexName in indexChanges.added {
            logger.info("Adding index: \(indexName)")
            if let descriptor = targetSchema.indexDescriptor(named: indexName) {
                try await context.addIndex(descriptor)
            } else if targetSchema.polymorphicGroup(containingIndexNamed: indexName) != nil {
                try await context.addPolymorphicIndex(indexName: indexName)
            } else {
                throw DatabaseRuntimeError.indexNotFound(
                    "Index '\(indexName)' not found in target schema"
                )
            }
        }

        for indexName in indexChanges.removed {
            logger.info("Removing index: \(indexName)")
            try await context.removeIndex(indexName: indexName, addedVersion: stage.fromVersionIdentifier)
        }

        if let didMigrate = stage.didMigrate {
            try await didMigrate(context)
        }

        let registry = SchemaRegistry(
            database: engine,
            clock: monotonicClock
        )
        let persistMode: SchemaRegistryPersistMode = if stage.isLightweight {
            .strict
        } else {
            .allowBreakingChanges(
                entityNames: try stage.entitiesRequiringCustomMigration
            )
        }
        try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            try await registry.persist(
                targetSchema,
                mode: persistMode,
                transaction: transaction
            )
            try Self.setCurrentSchemaSnapshot(
                targetSchema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
        }
        logger.info("Updated schema version to \(stage.toVersionIdentifier)")
    }

    private func buildStoreRegistry(for schema: Schema) async throws -> [String: MigrationStoreInfo] {
        var registry: [String: MigrationStoreInfo] = [:]

        for entity in schema.entities {
            guard let persistableType = runtimeConfiguration.entityRuntimes.modelType(
                named: entity.name
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
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
    /// let plan = try await admin.explain(
    ///     Query<User>().where(User.fields.age > 18)
    /// )
    /// ```
    ///
    /// - Returns: New AdminContext instance
    public func newAdminContext() -> AdminContext {
        AdminContext(container: self)
    }
}

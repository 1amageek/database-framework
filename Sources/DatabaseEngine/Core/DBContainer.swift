import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit
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
/// - StorageEngine lifecycle (retains the injected engine)
/// - Directory resolution from Persistable type metadata
/// - DataStore factory
///
/// **Architecture**:
/// ```
/// DBContainer (Resource Manager)
///     ├── default: one injected StorageEngine and one data root
///     └── MultiBase trait:
///           control domain + Base roots + persisted Grants + Compositions
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
///     executionIdentity: DatabaseExecutionRuntimeIdentity(
///         identifier: "application",
///         revision: 1
///     ),
///     entityRuntimes: [User.self]
/// )
/// let container = try await DBContainer.open(
///     for: schema,
///     configuration: DBConfiguration(
///         storageEngine: engine,
///         monotonicClock: applicationMonotonicClock,
///         wallClock: applicationWallClock
///     ),
///     runtimeConfiguration: runtime
/// )
///
/// // 3. Create one context for the single database root
/// let context = container.newContext(authorization: authorization)
/// try context.insert(user)
/// try await context.save()
/// ```
/// With the `MultiBase` trait, use
/// `container.session(authorization:).base(baseID).newContext()` instead.
public final class DBContainer: Sendable {
    private struct PreparedStorage: Sendable {
        let engine: any StorageEngine
        let format: DatabaseFormatDescriptor
        #if DATABASE_MULTI_BASE
        let topology: DatabaseStorageRuntimeTopology
        let databaseDataRoot: DatabaseDataRootLease
        let baseCatalog: DatabaseBaseCatalog
        let compositionCatalog: DatabaseCompositionCatalog
        let baseGenerations: [DatabaseBaseGeneration]
        let databaseGrantStore: DatabaseGrantStore
        #else
        let defaultTenant: DatabaseTenantDirectories
        #endif
        let metadataSubspace: Subspace
        let schemaGeneration: UInt64
        let transactionCapabilities: TransactionCapabilities
    }

    // MARK: - Properties

    /// The underlying storage engine
    ///
    /// Thread-safe: storage engines handle thread safety internally.
    /// Used for system operations (DirectoryLayer, Migration).
    /// Application transactions should use DatabaseContext.withTransaction().
    #if DATABASE_MULTI_BASE
    private let controlEngine: any StorageEngine

    /// Storage selected by the current Base lease, or the control domain while
    /// executing a database-scoped MultiBase operation.
    package var engine: any StorageEngine {
        ActiveDatabaseDataRootContext.lease?.domain.engine
            ?? controlEngine
    }

    /// Logical identity of the domain currently hosting control metadata.
    public let controlDomainID: DatabaseStorageDomain.ID

    /// Prepared storage roots retained for the complete container lifetime.
    package let storageTopology: DatabaseStorageRuntimeTopology
    #else
    /// The single storage engine exclusively owned by this container.
    public let engine: any StorageEngine

    /// Reserved Directories of the Default Partition, resolved once for the
    /// container's one ordinary database.
    ///
    /// Framework metadata derives from `defaultTenant.systemRoot` and
    /// application binding from `defaultTenant.data`. The optional MultiBase
    /// runtime replaces this value with operation-bound leases instead of
    /// consulting it.
    package let defaultTenant: DatabaseTenantDirectories
    #endif

    /// Typed transaction execution over the dynamically selected storage engine.
    ///
    /// This concrete boundary avoids invoking generic protocol-extension methods
    /// on an existential engine in Embedded Swift.
    #if DATABASE_MULTI_BASE
    package let controlTransactionExecutor: StorageTransactionExecutor

    /// Transaction execution selected by the current Base lease.
    package var transactionExecutor: StorageTransactionExecutor {
        ActiveDatabaseDataRootContext.lease?.transactionExecutor
            ?? controlTransactionExecutor
    }

    /// Optional transaction semantics guaranteed by the selected backend.
    private let controlTransactionCapabilities: TransactionCapabilities

    package var transactionCapabilities: TransactionCapabilities {
        ActiveDatabaseDataRootContext.lease?
            .domain.transactionCapabilities
            ?? controlTransactionCapabilities
    }
    #else
    /// Typed transaction execution over the container's single engine.
    public let transactionExecutor: StorageTransactionExecutor

    /// Optional transaction semantics guaranteed by the single backend.
    public let transactionCapabilities: TransactionCapabilities
    #endif

    /// Schema (version, entities, indexes) from the request's retained
    /// generation, or the currently published generation outside a request.
    public var schema: Schema {
        activeSchemaLease.schema
    }

    /// Configuration
    public let configuration: DBConfiguration

    /// Container-scoped monotonic time source.
    public var monotonicClock: any StorageMonotonicClock {
        configuration.monotonicClock
    }

    public var wallClock: any WallClock {
        configuration.wallClock
    }

    /// Generation-scoped runtime extensions paired atomically with `schema`.
    public var runtimeConfiguration: DatabaseRuntimeConfiguration {
        activeSchemaLease.runtimeConfiguration
    }

    package var indexPhysicalLayouts: [String: IndexPhysicalLayout] {
        activeSchemaLease.indexPhysicalLayouts
    }

    /// Layer of every Directory node position the request's schema declares.
    package var directoryLayers: DirectoryLayerTagMap {
        activeSchemaLease.directoryLayers
    }

    /// Container-scoped factory for the database's canonical entity format.
    public let itemStorageFactory: ItemStorageFactory

    /// Persisted database-wide physical format source of truth.
    public let databaseFormat: DatabaseFormatDescriptor

    /// Security configuration
    public let securityConfiguration: SecurityConfiguration

    /// Security delegate used by the generation-bound read policy.
    ///
    /// The policy supplies the captured authorization context when invoking
    /// this legacy delegate adapter; ambient task state never grants or
    /// suppresses a read decision.
    package var securityDelegate: (any DataStoreSecurityDelegate)? {
        activeSchemaLease.securityDelegate
    }

    /// Container-scoped observer for data store metrics.
    internal let dataStoreDelegate: any DataStoreDelegate

    /// Database event logger selected by the container configuration.
    private let logger: DatabaseLogger

    #if DATABASE_MULTI_BASE
    /// Database-owned control root for explicitly database-scoped operations.
    private let databaseDataRoot: DatabaseDataRootLease

    /// Database-scoped Grants stored in the control transaction domain.
    package let databaseGrantStore: DatabaseGrantStore

    /// Durable Base definitions in the control domain.
    package let baseCatalog: DatabaseBaseCatalog

    /// Durable named Composition definitions in the control domain.
    package let compositionCatalog: DatabaseCompositionCatalog

    /// Immutable Base placement generations and operation admission leases.
    private let baseGenerationStore: DatabaseBaseGenerationStore
    #endif

    /// Stable metadata namespace used by schema lifecycle operations.
    package let metadataSubspace: Subspace

    /// Migration plan
    private let migrationPlanStorage: Mutex<(any SchemaMigrationPlan.Type)?>

    /// Admission boundary owned only by containers opened with a migration
    /// plan. Ordinary containers avoid migration locking on their hot path.
    private let migrationAdmissionGate: DatabaseMigrationAdmissionGate?

    /// Atomic owner of the currently published immutable execution generation.
    private let schemaGenerationStore: DatabaseSchemaGenerationStore

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
        initializeIndexes: Bool,
        migrationAdmissionGate: DatabaseMigrationAdmissionGate? = nil
    ) async throws -> DBContainer {
        #if DATABASE_MULTI_BASE
        let storageTopology = try configuration.claimStorageTopology()
        let storageEngine = storageTopology.controlDomain.engine
        #else
        let storageEngine = try configuration.claimStorageEngine()
        #endif
        do {
            try runtimeConfiguration.validate(schema: schema)
            let schemaFingerprint = try SchemaManifest(schema: schema)
                .fingerprint()
            let indexPhysicalLayouts = try IndexRuntimeConfigurationValidator.validate(
                schema: schema,
                runtimeConfiguration: runtimeConfiguration
            )
            let preparedGeneration = try DatabasePreparedSchemaGeneration(
                schemaFingerprint: schemaFingerprint,
                runtimeConfiguration: runtimeConfiguration,
                securityConfiguration: security,
                indexPhysicalLayouts: indexPhysicalLayouts
            )

            let transactionCapabilities = try await inspectTransactionCapabilities(
                storageEngine: storageEngine
            )
            try runtimeConfiguration.validateStorageRequirements(
                schema: schema,
                transactionCapabilities: transactionCapabilities
            )

            #if DATABASE_MULTI_BASE
            let preparedStorage = try await prepareStorage(
                storageTopology: storageTopology,
                storageEngine: storageEngine,
                configuration: configuration,
                transactionCapabilities: transactionCapabilities
            )
            #else
            let preparedStorage = try await prepareStorage(
                storageEngine: storageEngine,
                configuration: configuration,
                transactionCapabilities: transactionCapabilities
            )
            #endif
            let container = try DBContainer(
                schema: schema,
                schemaFingerprint: schemaFingerprint,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                schemaGeneration: nil,
                configuration: configuration,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                security: security,
                migrationAdmissionGate: migrationAdmissionGate,
                preparedStorage: preparedStorage
            )

            if persistSchemaCatalog {
                let generation =
                    try await container
                    .initializeSchemaCatalogIfNeeded(
                        schema,
                        fingerprint: schemaFingerprint,
                        indexPhysicalFingerprint:
                            preparedGeneration.indexPhysicalFingerprint,
                        executionRuntimeFingerprint:
                            preparedGeneration.executionRuntimeFingerprint,
                        indexPhysicalLayouts: indexPhysicalLayouts
                    )
                try container.publishSchemaGeneration(
                    schema,
                    fingerprint: schemaFingerprint,
                    indexPhysicalFingerprint:
                        preparedGeneration.indexPhysicalFingerprint,
                    executionRuntimeFingerprint:
                        preparedGeneration.executionRuntimeFingerprint,
                    runtimeConfiguration: runtimeConfiguration,
                    indexPhysicalLayouts: indexPhysicalLayouts,
                    generation: generation
                )
            }

            #if DATABASE_MULTI_BASE
            try await container.prepareTestingBaseIfConfigured()
            if initializeIndexes {
                try await container.ensureIndexesReadyForAllActiveBases()
            }
            #else
            _ = try await container.bootstrapInitialSchemaIfNeeded(
                targetVersion: schema.version
            )
            if initializeIndexes {
                try await container.withDatabaseDataRoot {
                    try await container.ensureIndexesReady()
                }
            }
            #endif

            #if DATABASE_MULTI_BASE
            try configuration.finishOpeningStorageTopology()
            #else
            try configuration.finishOpeningStorageEngine()
            #endif
            return container
        } catch {
            #if DATABASE_MULTI_BASE
            await configuration.shutdownStorageTopology()
            #else
            await configuration.shutdownStorageEngine()
            #endif
            throw error
        }
    }

    /// Opens the canonical schema persisted by `schemaExecute` and builds a
    /// type-independent runtime for it. A completely new store starts with an
    /// empty schema at version `0.0.0`; partial schema metadata is rejected.
    public static func openRestoringSchema(
        configuration: DBConfiguration,
        security: SecurityConfiguration = .enabled(),
        runtimeFactory: @escaping @Sendable (
            Schema
        ) async throws -> DatabaseRuntimeConfiguration
    ) async throws -> DBContainer {
        #if DATABASE_MULTI_BASE
        let storageTopology = try configuration.claimStorageTopology()
        let storageEngine = storageTopology.controlDomain.engine
        #else
        let storageEngine = try configuration.claimStorageEngine()
        #endif
        do {
            #if DATABASE_MULTI_BASE
            let preparedStorage = try await prepareStorage(
                storageTopology: storageTopology,
                storageEngine: storageEngine,
                configuration: configuration
            )
            #else
            let preparedStorage = try await prepareStorage(
                storageEngine: storageEngine,
                configuration: configuration
            )
            #endif
            let schemaRoot: Subspace
            let schemaMetadataSubspace: Subspace
            #if DATABASE_MULTI_BASE
            schemaRoot = preparedStorage.topology.controlDomain.systemRoot
            schemaMetadataSubspace = preparedStorage.metadataSubspace
            #else
            schemaRoot = preparedStorage.defaultTenant.systemRoot
            schemaMetadataSubspace = preparedStorage.metadataSubspace
            #endif
            let restored = try await restoreSchemaState(
                storageEngine: storageEngine,
                root: schemaRoot,
                metadataSubspace: schemaMetadataSubspace,
                clock: configuration.monotonicClock
            )
            let runtimeConfiguration = try await runtimeFactory(restored.schema)
            try runtimeConfiguration.validate(schema: restored.schema)
            let indexPhysicalLayouts = try IndexRuntimeConfigurationValidator.validate(
                schema: restored.schema,
                runtimeConfiguration: runtimeConfiguration
            )
            let preparedGeneration = try DatabasePreparedSchemaGeneration(
                schemaFingerprint: restored.fingerprint,
                runtimeConfiguration: runtimeConfiguration,
                securityConfiguration: security,
                indexPhysicalLayouts: indexPhysicalLayouts
            )
            if let persisted = restored.indexPhysicalFingerprint {
                guard persisted == preparedGeneration.indexPhysicalFingerprint
                else {
                    throw DatabaseSchemaRestorationError
                        .indexPhysicalFingerprintMismatch
                }
            } else if restored.schema.entities.isEmpty == false {
                throw DatabaseSchemaRestorationError
                    .missingIndexPhysicalFingerprint
            }
            let configuredLayoutFingerprints = indexPhysicalLayouts.mapValues({
                $0.fingerprint
            })
            guard
                restored.indexLayoutFingerprints
                    == configuredLayoutFingerprints
            else {
                throw DatabaseSchemaRestorationError
                    .indexPhysicalFingerprintMismatch
            }
            try runtimeConfiguration.validateStorageRequirements(
                schema: restored.schema,
                transactionCapabilities: preparedStorage.transactionCapabilities
            )
            let container = try DBContainer(
                schema: restored.schema,
                schemaFingerprint: restored.fingerprint,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                schemaGeneration: restored.generation,
                configuration: configuration,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                security: security,
                preparedStorage: preparedStorage
            )
            let generation = try await container.initializeSchemaCatalogIfNeeded(
                restored.schema,
                fingerprint: restored.fingerprint,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                indexPhysicalLayouts: indexPhysicalLayouts
            )
            try container.publishSchemaGeneration(
                restored.schema,
                fingerprint: restored.fingerprint,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                generation: generation
            )
            #if DATABASE_MULTI_BASE
            try await container.prepareTestingBaseIfConfigured()
            try await container.ensureIndexesReadyForAllActiveBases()
            #else
            _ = try await container.bootstrapInitialSchemaIfNeeded(
                targetVersion: restored.schema.version
            )
            try await container.withDatabaseDataRoot {
                try await container.ensureIndexesReady()
            }
            #endif
            #if DATABASE_MULTI_BASE
            try configuration.finishOpeningStorageTopology()
            #else
            try configuration.finishOpeningStorageEngine()
            #endif
            return container
        } catch {
            #if DATABASE_MULTI_BASE
            await configuration.shutdownStorageTopology()
            #else
            await configuration.shutdownStorageEngine()
            #endif
            throw error
        }
    }

    /// Opens DBContainer with schema and configuration.
    ///
    /// Prepares the injected storage topology, then initializes all Base-local
    /// indexes to `readable` state.
    ///
    /// **Example**:
    /// ```swift
    /// // Explicit storage topology
    /// let container = try await DBContainer.open(
    ///     for: schema,
    ///     configuration: .init(
    ///         storageTopology: topology,
    ///         monotonicClock: applicationMonotonicClock,
    ///         wallClock: applicationWallClock
    ///     ),
    ///     runtimeConfiguration: runtime
    /// )
    ///
    /// let session = container.session(authorization: authorization)
    /// let context = session.base(baseID).newContext()
    /// ```
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - configuration: Database configuration
    ///   - security: Security configuration (default: enabled)
    /// - Throws: Error if topology preparation, catalog restoration, or index
    ///   initialization fails
    ///
    /// - Note: Opening performs two side effects:
    ///   1. **Index validation** — initializes index metadata only for empty stores and rejects incomplete indexes
    ///   2. **Schema persistence** — writes `Schema.Entity` via `SchemaRegistry.persist()`,
    ///      enabling CLI and dynamic tools to discover schemas without compiled Swift types
    #if DATABASE_MULTI_BASE
    private static func prepareStorage(
        storageTopology: ClaimedDatabaseStorageTopology,
        storageEngine: any StorageEngine,
        configuration: DBConfiguration,
        transactionCapabilities: TransactionCapabilities? = nil
    ) async throws -> PreparedStorage {
        var preparedDomains: [
            DatabaseStorageDomain.ID: DatabaseStorageDomainRuntime
        ] = [:]
        preparedDomains.reserveCapacity(storageTopology.domains.count)
        for claimedDomain in storageTopology.domains.values {
            let capabilities = try await inspectTransactionCapabilities(
                storageEngine: claimedDomain.engine
            )
            let domainID = claimedDomain.id
            let rootPath = claimedDomain.rootPath
            let engine = claimedDomain.engine
            let access = engine.directoryAccess
            let executor = StorageTransactionExecutor(engine: engine)
            preparedDomains[domainID] = try await executor.withTransaction(
                configuration: .default,
                clock: configuration.monotonicClock
            ) { transaction in
                let databaseRoot = try await DatabaseDirectoryLayout
                    .openOrInitializeDatabaseRoot(
                        path: rootPath,
                        access: access,
                        transaction: transaction
                    )
                let defaultTenant = try await DatabaseDirectoryLayout
                    .openOrCreateDefaultTenant(
                        in: databaseRoot,
                        access: access,
                        transaction: transaction
                    )
                return DatabaseStorageDomainRuntime(
                    id: domainID,
                    rootPath: rootPath,
                    engine: engine,
                    transactionExecutor: executor,
                    databaseRoot: databaseRoot,
                    defaultTenant: defaultTenant,
                    transactionCapabilities: capabilities
                )
            }
        }
        let preparedTopology = DatabaseStorageRuntimeTopology(
            controlDomainID: storageTopology.controlDomainID,
            domains: preparedDomains,
            placements: storageTopology.placements,
            defaultPlacementID: storageTopology.defaultPlacementID
        )

        let resolvedTransactionCapabilities: TransactionCapabilities
        if let transactionCapabilities {
            resolvedTransactionCapabilities = transactionCapabilities
        } else {
            resolvedTransactionCapabilities = try await inspectTransactionCapabilities(
                storageEngine: storageEngine
            )
        }
        let expectedFormat = DatabaseFormatDescriptor.current(
            layoutKind: .multiBase,
            itemStorage: configuration.itemStorage
        )
        let databaseDataRoot = DatabaseDataRootLease(
            resource: .database,
            domain: preparedTopology.controlDomain,
            tenant: preparedTopology.controlDomain.defaultTenant,
            generation: 0
        )
        let databaseGrantStore = DatabaseGrantStore(
            resource: .database,
            root: preparedTopology.controlDomain.systemRoot
        )
        let bootstrapMarkerKey = preparedTopology.controlDomain.systemRoot
            .subspace("_metadata")
            .pack(Tuple("security-bootstrap-v1"))
        let persistedFormat = try await DatabaseFormatCatalog(
            database: storageEngine,
            root: preparedTopology.controlDomain.systemRoot,
            clock: configuration.monotonicClock
        ).installIfEmptyOrValidate(
            expectedFormat,
            initializeEmptyDatabase: { transaction in
                try await databaseGrantStore.installInitial(
                    [
                        Security.Grant(
                            subject: .principalRole("admin"),
                            resource: .database,
                            access: .all
                        )
                    ],
                    transaction: transaction
                )
                try transaction.setValue(
                    Tuple(UInt64(1)).pack(),
                    for: bootstrapMarkerKey
                )
            }
        )
        let baseCatalog = DatabaseBaseCatalog(
            controlDomain: preparedTopology.controlDomain,
            clock: configuration.monotonicClock
        )
        let compositionCatalog = DatabaseCompositionCatalog(
            controlDomain: preparedTopology.controlDomain,
            clock: configuration.monotonicClock
        )
        let baseRecords = try await baseCatalog.loadAll()
        var restorableRecords: [
            DatabaseStorageDomain.ID: [DatabaseBaseRecord]
        ] = [:]
        for record in baseRecords {
            switch record.lifecycle {
            case .provisioning, .tombstone:
                continue
            case .active, .retiring, .retired, .moving, .deleting:
                restorableRecords[record.domainID, default: []].append(record)
            }
        }
        var baseGenerations: [DatabaseBaseGeneration] = []
        baseGenerations.reserveCapacity(baseRecords.count)
        // One transaction per domain: a Base Partition is resolved against the
        // database root of the domain its record names, so records of different
        // domains cannot share a transaction.
        for (domainID, records) in restorableRecords {
            guard let domain = preparedTopology.domain(
                identifiedBy: domainID
            ) else {
                throw DatabaseBaseCatalogError.storageDomainNotFound(domainID)
            }
            let access = domain.directoryAccess
            let databaseRoot = domain.databaseRoot
            let tenants = try await domain.transactionExecutor.withTransaction(
                configuration: .default,
                clock: configuration.monotonicClock
            ) { transaction in
                var tenants: [DatabaseTenantDirectories] = []
                tenants.reserveCapacity(records.count)
                for record in records {
                    guard let tenant = try await DatabaseDirectoryLayout
                        .openBaseTenant(
                            record.id.value,
                            in: databaseRoot,
                            access: access,
                            transaction: transaction
                        ) else {
                        throw DatabaseBaseCatalogError.corruptedRecord(
                            record.id
                        )
                    }
                    tenants.append(tenant)
                }
                return tenants
            }
            for (record, tenant) in zip(records, tenants) {
                baseGenerations.append(
                    DatabaseBaseGeneration(
                        record: record,
                        domain: domain,
                        tenant: tenant
                    )
                )
            }
        }
        let metadataSubspace = preparedTopology.controlDomain.systemRoot
            .subspace("_metadata")
        let schemaGeneration = try await loadSchemaGeneration(
            storageEngine: storageEngine,
            metadataSubspace: metadataSubspace,
            clock: configuration.monotonicClock
        )
        return PreparedStorage(
            engine: storageEngine,
            format: persistedFormat,
            topology: preparedTopology,
            databaseDataRoot: databaseDataRoot,
            baseCatalog: baseCatalog,
            compositionCatalog: compositionCatalog,
            baseGenerations: baseGenerations,
            databaseGrantStore: databaseGrantStore,
            metadataSubspace: metadataSubspace,
            schemaGeneration: schemaGeneration,
            transactionCapabilities: resolvedTransactionCapabilities
        )
    }
    #else
    /// Prepares the one fixed database root used by the lightweight runtime.
    ///
    /// The injected engine and already-resolved root form the database
    /// boundary. Data operations derive tuple subspaces from the retained root
    /// and never resolve backend namespaces.
    private static func prepareStorage(
        storageEngine: any StorageEngine,
        configuration: DBConfiguration,
        transactionCapabilities: TransactionCapabilities? = nil
    ) async throws -> PreparedStorage {
        let resolvedTransactionCapabilities: TransactionCapabilities
        if let transactionCapabilities {
            resolvedTransactionCapabilities = transactionCapabilities
        } else {
            resolvedTransactionCapabilities = try await inspectTransactionCapabilities(
                storageEngine: storageEngine
            )
        }
        let expectedFormat = DatabaseFormatDescriptor.current(
            layoutKind: .singleDatabase,
            itemStorage: configuration.itemStorage
        )
        let directoryAccess = storageEngine.directoryAccess
        let rootPath = configuration.databaseRootPath
        let defaultTenant = try await StorageTransactionExecutor(
            engine: storageEngine
        ).withTransaction(
            configuration: .default,
            clock: configuration.monotonicClock
        ) { transaction in
            let databaseRoot = try await DatabaseDirectoryLayout
                .openOrInitializeDatabaseRoot(
                    path: rootPath,
                    access: directoryAccess,
                    transaction: transaction
                )
            return try await DatabaseDirectoryLayout.openOrCreateDefaultTenant(
                in: databaseRoot,
                access: directoryAccess,
                transaction: transaction
            )
        }
        let persistedFormat = try await DatabaseFormatCatalog(
            database: storageEngine,
            root: defaultTenant.systemRoot,
            clock: configuration.monotonicClock
        ).installIfEmptyOrValidate(expectedFormat)
        let metadataSubspace = defaultTenant.systemRoot.subspace("_metadata")
        let schemaGeneration = try await loadSchemaGeneration(
            storageEngine: storageEngine,
            metadataSubspace: metadataSubspace,
            clock: configuration.monotonicClock
        )
        return PreparedStorage(
            engine: storageEngine,
            format: persistedFormat,
            defaultTenant: defaultTenant,
            metadataSubspace: metadataSubspace,
            schemaGeneration: schemaGeneration,
            transactionCapabilities: resolvedTransactionCapabilities
        )
    }
    #endif

    private static func inspectTransactionCapabilities(
        storageEngine: any StorageEngine
    ) async throws -> TransactionCapabilities {
        let transaction = try storageEngine.createOwnedTransaction()
        let capabilities = transaction.capabilities
        try await transaction.cancel()
        return capabilities
    }

    private static func loadSchemaGeneration(
        storageEngine: any StorageEngine,
        metadataSubspace: Subspace,
        clock: any StorageMonotonicClock
    ) async throws -> UInt64 {
        try await StorageTransactionExecutor(engine: storageEngine)
            .withTransaction(configuration: .default, clock: clock) {
                transaction in
                guard let bytes = try await transaction.getValue(
                    for: schemaGenerationKey(metadataSubspace: metadataSubspace),
                    snapshot: true
                ) else {
                    return UInt64(0)
                }
                let tuple = try Tuple(packed: bytes)
                guard tuple.count == 1 else {
                    throw DatabaseRuntimeError.internalError(
                        "Invalid schema generation format"
                    )
                }
                switch try tuple.value(at: 0) {
                case .unsignedInteger(let generation):
                    return generation
                case .signedInteger(let generation) where generation >= 0:
                    return UInt64(generation)
                default:
                    throw DatabaseRuntimeError.internalError(
                        "Invalid schema generation format"
                    )
                }
            }
    }

    private init(
        schema: Schema,
        schemaFingerprint: SchemaFingerprint,
        indexPhysicalFingerprint: ByteString,
        executionRuntimeFingerprint: ByteString,
        schemaGeneration: UInt64?,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        security: SecurityConfiguration,
        migrationAdmissionGate: DatabaseMigrationAdmissionGate? = nil,
        preparedStorage: PreparedStorage
    ) throws(DirectoryLayerTagError) {
        #if DATABASE_MULTI_BASE
        self.controlEngine = preparedStorage.engine
        self.controlDomainID = preparedStorage.topology.controlDomainID
        self.storageTopology = preparedStorage.topology
        self.controlTransactionExecutor = StorageTransactionExecutor(
            engine: preparedStorage.engine
        )
        self.controlTransactionCapabilities =
            preparedStorage.transactionCapabilities
        #else
        self.engine = preparedStorage.engine
        self.defaultTenant = preparedStorage.defaultTenant
        self.transactionExecutor = StorageTransactionExecutor(
            engine: preparedStorage.engine
        )
        self.transactionCapabilities = preparedStorage.transactionCapabilities
        #endif
        self.configuration = configuration
        self.itemStorageFactory = ItemStorageFactory(
            configuration: preparedStorage.format.itemStorage
        )
        self.databaseFormat = preparedStorage.format
        self.securityConfiguration = security
        let securityDelegate: (any DataStoreSecurityDelegate)?
        switch security.policyEvaluation {
        case .enabled:
            securityDelegate = RequestSecurityPolicyDelegate(
                policies: runtimeConfiguration.authorizationPolicies,
                schema: schema
            )
        case .disabledForTesting:
            securityDelegate = nil
        }
        self.schemaGenerationStore = DatabaseSchemaGenerationStore(
            initial: DatabaseSchemaGeneration(
                identifier: schemaGeneration ?? preparedStorage.schemaGeneration,
                fingerprint: schemaFingerprint,
                indexPhysicalFingerprint: indexPhysicalFingerprint,
                executionRuntimeFingerprint: executionRuntimeFingerprint,
                schema: schema,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                directoryLayers: try DirectoryLayerTagMap(
                    entities: schema.entities,
                    polymorphicGroups: schema.polymorphicGroups
                ),
                securityDelegate: securityDelegate
            )
        )
        self.dataStoreDelegate = MetricsDataStoreDelegate(
            metrics: configuration.metrics
        )

        self.logger = configuration.logging.logger(
            label: "com.database.framework.container"
        )
        self.migrationPlanStorage = Mutex(nil)
        self.migrationAdmissionGate = migrationAdmissionGate
        #if DATABASE_MULTI_BASE
        self.databaseDataRoot = preparedStorage.databaseDataRoot
        self.databaseGrantStore = preparedStorage.databaseGrantStore
        self.baseCatalog = preparedStorage.baseCatalog
        self.compositionCatalog = preparedStorage.compositionCatalog
        self.baseGenerationStore = DatabaseBaseGenerationStore(
            generations: preparedStorage.baseGenerations
        )
        #endif
        self.metadataSubspace = preparedStorage.metadataSubspace
    }

    private var activeSchemaLease: DatabaseSchemaLease {
        if DatabaseSchemaExecutionScope.container === self,
            let lease = DatabaseSchemaExecutionScope.lease
        {
            return lease
        }
        return schemaGenerationStore.acquire()
    }

    #if DATABASE_MULTI_BASE
    private func prepareTestingBaseIfConfigured() async throws {
        guard let bootstrap = configuration.testingBootstrap else {
            return
        }
        try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            let current = try await self.databaseGrantStore.direct(
                subject: .principal(bootstrap.principal.identifier),
                transaction: transaction.storageAccess
            )
            let granted = current.grants.reduce(into: Security.Access()) {
                $0.formUnion($1.access)
            }
            let missing = Security.Access.all.subtracting(granted)
            if !missing.isEmpty {
                _ = try await self.databaseGrantStore.grant(
                    Security.Grant(
                        subject: .principal(bootstrap.principal.identifier),
                        resource: .database,
                        access: missing
                    ),
                    expectedRevision: current.revision,
                    transaction: transaction.storageAccess
                )
            }
        }
        _ = try await provisionBase(
            bootstrap.baseID,
            placementID: storageTopology.defaultPlacementID,
            initialGrants: [
                Security.Grant(
                    subject: .principal(bootstrap.principal.identifier),
                    resource: .base(bootstrap.baseID),
                    access: .all
                )
            ],
            expectedRevision: 0
        )
    }

    package func acquireBaseLease(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        try baseGenerationStore.acquire(id)
    }

    package func acquireBaseAdministrationLease(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        try baseGenerationStore.acquireAdministration(id)
    }

    package func acquireBaseSchemaMaintenanceLease(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        try baseGenerationStore.acquireSchemaMaintenance(id)
    }

    package func withBaseLease<Result: Sendable>(
        _ lease: DatabaseBaseLease,
        _ operation: @Sendable () async throws -> Result
    ) async rethrows -> Result {
        try await ActiveDatabaseDataRootContext.$lease.withValue(
            lease.dataRoot
        ) {
            try await ActiveDatabaseBaseContext.$lease.withValue(lease) {
                try await operation()
            }
        }
    }

    package func publishBaseGeneration(
        _ record: DatabaseBaseRecord,
        tenant: DatabaseTenantDirectories
    ) throws {
        guard let domain = storageTopology.domain(
            identifiedBy: record.domainID
        ) else {
            throw DatabaseBaseExecutionError.storageDomainUnavailable(
                record.domainID
            )
        }
        baseGenerationStore.publish(
            DatabaseBaseGeneration(
                record: record,
                domain: domain,
                tenant: tenant
            )
        )
    }

    package func stopBaseAdmissionAndDrain(_ id: Base.ID) async throws {
        try await baseGenerationStore.stopAdmissionAndDrain(id)
    }

    package func requireActiveBaseLease() throws -> DatabaseBaseLease {
        let lease = try requireBoundBaseLease()
        guard lease.permitsDataOperations,
              lease.generation.record.lifecycle == .active
                || lease.permitsInactiveMaintenance else {
            throw DatabaseBaseExecutionError.baseTargetRequired
        }
        return lease
    }

    package func requireBoundBaseLease() throws -> DatabaseBaseLease {
        guard let lease = ActiveDatabaseBaseContext.lease else {
            throw DatabaseBaseExecutionError.baseTargetRequired
        }
        return lease
    }
    #endif

    package func withDatabaseDataRoot<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async rethrows -> Result {
        #if DATABASE_MULTI_BASE
        try await ActiveDatabaseDataRootContext.$lease.withValue(
            databaseDataRoot
        ) {
            try await operation()
        }
        #else
        try await operation()
        #endif
    }

    @_spi(DatabaseExecution)
    public func withExecutionDataRoot<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async rethrows -> Result {
        try await withDatabaseDataRoot(operation)
    }

    #if DATABASE_MULTI_BASE
    package func requireActiveDataRoot() throws -> DatabaseDataRootLease {
        guard let lease = ActiveDatabaseDataRootContext.lease else {
            throw DatabaseRuntimeError.internalError(
                "A target-bound data root is required"
            )
        }
        return lease
    }

    #endif

    @_spi(DatabaseExecution)
    public func executionStorage() throws -> DatabaseExecutionStorage {
        #if DATABASE_MULTI_BASE
        let lease = try requireActiveDataRoot()
        return DatabaseExecutionStorage(
            engine: lease.domain.engine,
            transactionExecutor: lease.transactionExecutor,
            systemRoot: lease.systemRoot,
            dataRoot: lease.dataDirectory.root,
            resource: lease.resource,
            generation: lease.generation,
            domainIdentifier: lease.domain.id.value
        )
        #else
        return DatabaseExecutionStorage(
            engine: engine,
            transactionExecutor: transactionExecutor,
            systemRoot: defaultTenant.systemRoot,
            dataRoot: defaultTenant.data.root,
            generation: 0,
            domainIdentifier: "database"
        )
        #endif
    }

    @_spi(DatabaseExecution)
    public func controlStorage() -> DatabaseExecutionStorage {
        #if DATABASE_MULTI_BASE
        let domain = storageTopology.controlDomain
        return DatabaseExecutionStorage(
            engine: domain.engine,
            transactionExecutor: domain.transactionExecutor,
            systemRoot: domain.systemRoot,
            dataRoot: domain.dataDirectory.root,
            resource: .database,
            generation: 0,
            domainIdentifier: domain.id.value
        )
        #else
        return DatabaseExecutionStorage(
            engine: engine,
            transactionExecutor: transactionExecutor,
            systemRoot: defaultTenant.systemRoot,
            dataRoot: defaultTenant.data.root,
            generation: 0,
            domainIdentifier: "database"
        )
        #endif
    }

    @_spi(DatabaseExecution)
    public var hasActiveExecutionTransaction: Bool {
        ActiveDatabaseTransactionContext.binding != nil
    }

    /// Application data root of the Tenant Partition bound to the current
    /// operation. Content derived directly from this Subspace never collides
    /// with an application `#Directory`, because Directory children are
    /// allocated from the enclosing Partition rather than below this prefix.
    @_spi(DatabaseExecution)
    public func operationDataRoot() throws -> Subspace {
        #if DATABASE_MULTI_BASE
        return try requireActiveDataRoot().dataDirectory.root
        #else
        return defaultTenant.data.root
        #endif
    }

    /// Framework metadata root of the Tenant Partition bound to the current
    /// operation.
    @_spi(DatabaseExecution)
    public func operationSystemRoot() throws -> Subspace {
        #if DATABASE_MULTI_BASE
        return try requireActiveDataRoot().systemRoot
        #else
        return defaultTenant.systemRoot
        #endif
    }

    /// Directory an application `#Directory` path is bound below for the
    /// current operation.
    private func operationDataDirectory() throws -> Directory {
        #if DATABASE_MULTI_BASE
        return try requireActiveDataRoot().tenant.data
        #else
        return defaultTenant.data
        #endif
    }

    /// Directory catalog owning the Partition bound to the current operation.
    private func operationDirectoryAccess() throws -> any DirectoryAccess {
        #if DATABASE_MULTI_BASE
        return try requireActiveDataRoot().domain.directoryAccess
        #else
        return engine.directoryAccess
        #endif
    }

    /// Runs `body` in one transaction on the executor of the bound Partition.
    ///
    /// Only entry points that have no caller-owned transaction use this. A
    /// caller that owns one passes it, so directory metadata commits with the
    /// mutation that needed it.
    package func withOperationTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ body: @Sendable @escaping (any TransactionAccess) async throws -> Result
    ) async throws -> Result {
        #if DATABASE_MULTI_BASE
        let executor = try requireActiveDataRoot().transactionExecutor
        #else
        let executor = transactionExecutor
        #endif
        return try await executor.withTransaction(
            configuration: configuration,
            clock: monotonicClock
        ) { transaction in
            try await body(transaction)
        }
    }

    /// Opens the application Directory `relativePath` addresses, or `nil` when
    /// a component of it has never been created.
    ///
    /// This is the read direction of Section 12.1: absence is an absent
    /// keyspace, not a failure and not a reason to create anything. `layers`
    /// carries the declared layer of each component so a node stored under a
    /// different layer is rejected instead of being addressed at the wrong
    /// prefix.
    package func openDataDirectory(
        relativePath: [String],
        layers: [DirectoryLayer],
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        try await DatabaseDirectoryBinding.open(
            relativePath,
            layers: layers,
            below: operationDataDirectory(),
            access: operationDirectoryAccess(),
            transaction: transaction
        )?.root
    }

    /// Opens the application Directory `relativePath` addresses, creating the
    /// components that do not exist yet.
    ///
    /// The catalog metadata this records commits with `transaction`, so a
    /// mutation and the Directory it needed become visible together.
    package func resolveDataDirectory(
        relativePath: [String],
        layers: [DirectoryLayer],
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await DatabaseDirectoryBinding.openOrCreate(
            relativePath,
            layers: layers,
            below: operationDataDirectory(),
            access: operationDirectoryAccess(),
            transaction: transaction
        ).root
    }

    /// Opens the application Directory `relativePath` addresses without
    /// verifying a layer, or `nil` when a component of it does not exist.
    ///
    /// Only retirement of a path the active schema no longer declares uses
    /// this: there is no declared layer to verify against, and the stored tag
    /// remains authoritative for addressing.
    package func openUnverifiedDataDirectory(
        relativePath: [String],
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        try await DatabaseDirectoryBinding.openUnverified(
            relativePath,
            below: operationDataDirectory(),
            access: operationDirectoryAccess(),
            transaction: transaction
        )?.root
    }

    /// Declared layer of every component of an entity's directory path.
    ///
    /// A resolved dynamic component is not a static edge of the schema trie, so
    /// the entity's own declaration is the only source that types it. Falling
    /// back to a path walk would type that component, and everything below it,
    /// as a plain Directory.
    package func declaredDirectoryLayers(
        for entity: Schema.Entity
    ) throws -> [DirectoryLayer] {
        try Self.declaredLayers(for: entity, in: directoryLayers)
    }

    package static func declaredLayers(
        for entity: Schema.Entity,
        in layerMap: DirectoryLayerTagMap
    ) throws -> [DirectoryLayer] {
        guard let layers = layerMap.layers(forEntityNamed: entity.name) else {
            throw ContainerSchemaError.entityNotFound(entity.name)
        }
        return layers
    }

    /// Returns the latest atomically published generation even when the
    /// caller is executing under an older request lease. Schema coordination
    /// uses this boundary to serialize publications against current state;
    /// ordinary request execution must continue to use `activeSchemaLease`.
    @_spi(DatabaseExecution)
    public func acquirePublishedSchemaLease() -> DatabaseSchemaLease {
        schemaGenerationStore.acquire()
    }

    /// Waits until every request bound to a schema generation older than the
    /// supplied published generation has released its lease.
    @_spi(DatabaseExecution)
    public func waitForSchemaLeases(
        olderThan generation: UInt64
    ) async throws {
        try await schemaGenerationStore.waitUntilDrained(
            olderThan: generation
        )
    }

    package var pendingSchemaDrainWaiterCount: Int {
        schemaGenerationStore.pendingDrainWaiterCount
    }

    /// Retains the generation already bound to the current operation, or the
    /// published generation when no operation scope exists.
    package func acquireActiveSchemaLease() -> DatabaseSchemaLease {
        activeSchemaLease
    }

    /// Returns the provider layout paired with the schema generation bound to
    /// the current execution scope.
    @_spi(DatabaseExecution)
    public func executionIndexPhysicalLayout(
        named indexName: String
    ) throws -> IndexPhysicalLayout {
        let lease = activeSchemaLease
        guard let layout = lease.indexPhysicalLayouts[indexName] else {
            throw
                DatabaseIndexStorageIdentityError
                .physicalLayoutNotResolved(
                    indexName
                )
        }
        return layout
    }

    /// Acquires one immutable schema generation and binds it to all container
    /// reads performed by `operation` until that asynchronous operation ends.
    public func withSchemaLease<Result: Sendable>(
        _ operation: @Sendable (DatabaseSchemaLease) async throws -> Result
    ) async rethrows -> Result {
        let lease = schemaGenerationStore.acquire()
        return try await DatabaseSchemaExecutionScope.$container.withValue(self) {
            try await DatabaseSchemaExecutionScope.$lease.withValue(lease) {
                try await operation(lease)
            }
        }
    }

    /// Retains an enclosing request generation or acquires one for a local
    /// data operation that does not already have a request scope.
    package func withOperationSchemaLease<Result: Sendable>(
        _ operation: @Sendable (DatabaseSchemaLease) async throws -> Result
    ) async throws -> Result {
        let lease = activeSchemaLease
        guard let migrationAdmissionGate else {
            return try await withBoundSchemaLeaseIfNeeded(
                lease,
                operation: operation
            )
        }
        if DatabaseSchemaExecutionScope.migrationMaintenanceGate
            === migrationAdmissionGate
        {
            return try await withBoundSchemaLeaseIfNeeded(
                lease,
                operation: operation
            )
        }
        if let admissionLease = DatabaseSchemaExecutionScope.dataAdmissionLease,
            admissionLease.belongs(to: migrationAdmissionGate)
        {
            precondition(
                admissionLease.schemaGeneration == lease.generation,
                "A nested data operation must retain its admitted schema generation"
            )
            return try await operation(lease)
        }

        let admissionLease = try DatabaseDataOperationAdmissionLease(
            gate: migrationAdmissionGate,
            schemaGeneration: lease.generation
        )
        return try await DatabaseSchemaExecutionScope.$dataAdmissionLease.withValue(
            admissionLease
        ) {
            try await withBoundSchemaLeaseIfNeeded(
                lease,
                operation: operation
            )
        }
    }

    private func withBoundSchemaLeaseIfNeeded<Result: Sendable>(
        _ lease: DatabaseSchemaLease,
        operation: @Sendable (DatabaseSchemaLease) async throws -> Result
    ) async throws -> Result {
        if DatabaseSchemaExecutionScope.container === self,
            DatabaseSchemaExecutionScope.lease != nil
        {
            return try await operation(lease)
        }
        return try await DatabaseSchemaExecutionScope.$container.withValue(self) {
            try await DatabaseSchemaExecutionScope.$lease.withValue(lease) {
                try await operation(lease)
            }
        }
    }

    /// Runs framework-owned migration work through the same context APIs as
    /// ordinary operations without reopening admission to unrelated callers.
    @_spi(DatabaseExecution)
    public func withMigrationMaintenanceAccess<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async rethrows -> Result {
        guard let migrationAdmissionGate else {
            return try await operation()
        }
        return try await DatabaseSchemaExecutionScope.$migrationMaintenanceGate
            .withValue(migrationAdmissionGate) {
                try await operation()
            }
    }

    /// The generation bound to the current request, or the currently published
    /// generation when called outside a request.
    public var schemaGeneration: UInt64 {
        activeSchemaLease.generation
    }

    /// The canonical fingerprint paired with `schema` in the active lease.
    public var schemaFingerprint: SchemaFingerprint {
        activeSchemaLease.fingerprint
    }

    /// Publishes a fully built generation after its durable schema transaction
    /// has committed. Existing request leases continue to retain the old value.
    package func publishSchemaGeneration(
        _ schema: Schema,
        fingerprint: SchemaFingerprint,
        indexPhysicalFingerprint: ByteString,
        executionRuntimeFingerprint: ByteString,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        generation: UInt64
    ) throws {
        let securityDelegate: (any DataStoreSecurityDelegate)?
        switch securityConfiguration.policyEvaluation {
        case .enabled:
            securityDelegate = RequestSecurityPolicyDelegate(
                policies: runtimeConfiguration.authorizationPolicies,
                schema: schema
            )
        case .disabledForTesting:
            securityDelegate = nil
        }
        schemaGenerationStore.publish(
            DatabaseSchemaGeneration(
                identifier: generation,
                fingerprint: fingerprint,
                indexPhysicalFingerprint: indexPhysicalFingerprint,
                executionRuntimeFingerprint: executionRuntimeFingerprint,
                schema: schema,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                directoryLayers: try DirectoryLayerTagMap(
                    entities: schema.entities,
                    polymorphicGroups: schema.polymorphicGroups
                ),
                securityDelegate: securityDelegate
            )
        )
    }

    /// Releases the storage engine owned by this container.
    ///
    /// Shutdown is thread-safe and idempotent. It rejects new operations, waits
    /// for admitted operations to finish, and then releases the storage engine.
    public func shutdown() async {
        #if DATABASE_MULTI_BASE
        await configuration.shutdownStorageTopology()
        #else
        await configuration.shutdownStorageEngine()
        #endif
    }

    deinit {
        #if DATABASE_MULTI_BASE
        configuration.requestStorageTopologyShutdown()
        #else
        configuration.requestStorageEngineShutdown()
        #endif
    }

    // MARK: - Index Initialization

    /// Validate all indexes and initialize indexes only for empty stores.
    ///
    /// Existing incomplete states fail fast. A missing state is initialized
    /// only when its source entity range is empty in the same transaction.
    package func ensureIndexesReady() async throws {
        for entity in schema.entities {
            guard !entity.indexDescriptors.isEmpty else { continue }
            guard !entity.hasDynamicDirectory else { continue }
            guard runtimeConfiguration.entityRuntimes.registration(
                named: entity.name
            ) != nil else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            let indexNames = entity.indexDescriptors.map { $0.name }
            try await withOperationTransaction(
                configuration: .batch
            ) { transaction in
                let subspace = try await self.resolveDirectory(
                    for: entity,
                    transaction: transaction
                )
                let lifecycleStore = IndexLifecycleStore(
                    container: self,
                    subspace: subspace
                )
                let pending = try await self.pendingSchemaIndexBuilds(
                    entity: entity.name,
                    indexes: indexNames,
                    transaction: transaction
                )
                try await lifecycleStore.ensureReadable(
                    indexNames,
                    entityRange: subspace
                        .subspace(SubspaceKey.items)
                        .subspace(entity.name)
                        .range(),
                    pendingBuildIndexes: pending,
                    transaction: transaction
                )
            }
        }
        for group in schema.polymorphicGroups {
            guard !group.indexes.isEmpty else { continue }
            let indexNames = group.indexes.map { $0.name }
            try await withOperationTransaction(
                configuration: .batch
            ) { transaction in
                let subspace = try await self.resolvePolymorphicDirectory(
                    for: group.identifier,
                    transaction: transaction
                )
                let lifecycleStore = IndexLifecycleStore(
                    container: self,
                    subspace: subspace
                )
                let pending =
                    try await self
                    .pendingSchemaPolymorphicIndexBuilds(
                        group: group.identifier,
                        indexes: indexNames,
                        transaction: transaction
                    )
                try await lifecycleStore.ensureReadable(
                    indexNames,
                    entityRange: subspace
                        .subspace(SubspaceKey.items)
                        .range(),
                    pendingBuildIndexes: pending,
                    transaction: transaction
                )
            }
        }
    }

    #if DATABASE_MULTI_BASE
    private func ensureIndexesReadyForAllActiveBases() async throws {
        for generation in baseGenerationStore.snapshot()
        where generation.record.lifecycle == .active {
            let lease = try acquireBaseLease(generation.record.id)
            try await withBaseLease(lease) {
                try await self.ensureIndexesReady()
            }
        }
    }

    // MARK: - Session Management

    /// Creates an explicit local authorization session. Target selection is a
    /// separate synchronous operation on the returned session.
    public func session(
        authorization: AuthorizationContext
    ) -> DatabaseSession {
        DatabaseSession(container: self, authorization: authorization)
    }
    #else
    /// Creates a context bound to the database's single data root.
    public func newContext(
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        makeDatabaseContext(
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
    }

    /// Creates administrative APIs bound to the database's single data root.
    public func admin(
        authorization: AuthorizationContext
    ) -> AdminContext {
        AdminContext(context: newContext(authorization: authorization))
    }
    #endif

    package func makeDatabaseContext(
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        #if DATABASE_MULTI_BASE
        DatabaseContext(
            container: self,
            resource: .database,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
        #else
        DatabaseContext(
            container: self,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
        #endif
    }

    @_spi(DatabaseExecution)
    public func makeExecutionContext(
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        makeDatabaseContext(
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
    }

    @_spi(DatabaseExecution)
    public func makeActiveDataContext(
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) throws -> DatabaseContext {
        #if DATABASE_MULTI_BASE
        let resource = try requireActiveDataRoot().resource
        return DatabaseContext(
            container: self,
            resource: resource,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
        #else
        return DatabaseContext(
            container: self,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
        #endif
    }

    // MARK: - Directory Resolution

    /// Resolve directory for a Persistable type in a transaction of its own.
    ///
    /// This is the entry point for an application that holds no transaction.
    /// A Framework operation must instead use the `transaction:` form so the
    /// directory metadata it creates commits with the mutation it serves.
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
    #if DATABASE_MULTI_BASE
    package func resolveDirectory<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: schemaEntity(named: T.persistableType),
            path: try AnyDirectoryPath(path)
        )
    }

    package func resolveDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        let lease = try requireActiveDataRoot()
        let selectedTransactionExecutor = lease.transactionExecutor
        return try await selectedTransactionExecutor.withTransaction(
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
    #else
    public func resolveDirectory<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath()
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: schemaEntity(named: T.persistableType),
            path: try AnyDirectoryPath(path)
        )
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
    #endif

    package func resolveDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: entity,
            declaredIn: schema,
            directoryLayers: directoryLayers,
            path: path,
            transaction: transaction
        )
    }

    /// Opens the Directory an entity's `#Directory` declaration addresses,
    /// creating the components that do not exist yet.
    ///
    /// `authoritySchema` and `layerMap` are one pair: the layer of a node
    /// position is a property of the whole schema that declares it, so a map
    /// derived from a different generation could type a component the schema
    /// being applied types differently.
    package func resolveDirectory(
        for candidate: Schema.Entity,
        declaredIn authoritySchema: Schema,
        directoryLayers layerMap: DirectoryLayerTagMap,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let entity = try schemaEntity(
            matching: candidate,
            declaredIn: authoritySchema
        )
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        return try await resolveDataDirectory(
            relativePath: directoryPath.resolve(),
            layers: try Self.declaredLayers(for: entity, in: layerMap),
            transaction: transaction
        )
    }

    /// Opens the Directory an entity's `#Directory` declaration addresses, or
    /// `nil` when it has never been created.
    ///
    /// Creating nothing is what makes a read of a directory no write ever
    /// reached report no data rather than publishing that directory.
    package func openDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        let entity = try canonicalEntity(entity)
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        return try await openDataDirectory(
            relativePath: directoryPath.resolve(),
            layers: try declaredDirectoryLayers(for: entity),
            transaction: transaction
        )
    }

    /// Opens the Directory a `Persistable` type's `#Directory` declaration
    /// addresses in the caller's transaction, or `nil` when it has never been
    /// created.
    package func openDirectory<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath(),
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        try await openDirectory(
            for: schemaEntity(named: T.persistableType),
            path: try AnyDirectoryPath(path),
            transaction: transaction
        )
    }

    /// Opens the Directory a `Persistable` type's `#Directory` declaration
    /// addresses in the caller's transaction, creating the components that do
    /// not exist yet so the metadata commits with the mutation it serves.
    package func resolveDirectory<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath(),
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: schemaEntity(named: T.persistableType),
            path: try AnyDirectoryPath(path),
            transaction: transaction
        )
    }

    /// Resolves one declared index in the caller's read transaction.
    ///
    /// The read path never creates directory metadata, partition catalog
    /// entries, or index state. A directory that has never existed represents
    /// an empty logical partition and therefore returns `nil`.
    internal func readableIndexSubspace<Model: Persistable>(
        named indexName: String,
        for type: Model.Type,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        try await readableIndexSubspace(
            named: indexName,
            for: schemaEntity(named: Model.persistableType),
            path: path,
            transaction: transaction
        )
    }

    internal func readableIndexSubspace(
        named indexName: String,
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        let entity = try canonicalEntity(entity)
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        guard let subspace = try await openDataDirectory(
            relativePath: directoryPath.resolve(),
            layers: try declaredDirectoryLayers(for: entity),
            transaction: transaction
        ) else {
            return nil
        }
        let lifecycleStore = IndexLifecycleStore(
            container: self,
            subspace: subspace
        )
        try await lifecycleStore.validateReadableForRead(
            [indexName],
            transaction: transaction
        )
        return try lifecycleStore.indexSubspace(for: indexName)
    }

    /// One page of the directory partitions `entity` currently has, read in a
    /// transaction of its own.
    ///
    /// Paging is not one snapshot in either direction: a continuation
    /// re-descends the recorded path, so a partition created or removed between
    /// two pages is reported or skipped rather than making the page sequence
    /// fail.
    package func partitionCatalogPage(
        entity: String,
        continuation: ByteString? = nil,
        limit: Int
    ) async throws -> DatabasePartitionCatalogPage {
        try await withOperationTransaction { transaction in
            try await self.partitionCatalogPage(
                entity: entity,
                continuation: continuation,
                limit: limit,
                transaction: transaction
            )
        }
    }

    /// One page of the directory partitions `entity` currently has.
    ///
    /// Section 12.3 makes StorageKit's Directory catalog the only record of
    /// which partitions exist, so this walks that catalog instead of a second
    /// index the framework would have to keep consistent with it.
    package func partitionCatalogPage(
        entity: String,
        continuation: ByteString? = nil,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> DatabasePartitionCatalogPage {
        let entity = try schemaEntity(named: entity)
        return try await partitionCatalogPage(
            entity: entity,
            directoryLayers: directoryLayers,
            continuation: continuation,
            limit: limit,
            transaction: transaction
        )
    }

    /// One page of the directory partitions an entity of `layerMap`'s
    /// generation has.
    ///
    /// Schema application enumerates entities of the schema it is applying,
    /// which the container has not published yet, so the declaration and the
    /// layer map it is typed by are both supplied by the caller.
    package func partitionCatalogPage(
        entity: Schema.Entity,
        directoryLayers layerMap: DirectoryLayerTagMap,
        continuation: ByteString? = nil,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> DatabasePartitionCatalogPage {
        try await DatabaseDirectoryPartitionEnumerator.page(
            entity: entity,
            layers: try Self.declaredLayers(for: entity, in: layerMap),
            below: operationDataDirectory(),
            access: operationDirectoryAccess(),
            continuation: continuation,
            limit: limit,
            transaction: transaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionPartitionCatalogPage(
        entity: String,
        continuation: ByteString? = nil,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> DatabasePartitionCatalogPage {
        let page = try await partitionCatalogPage(
            entity: entity,
            continuation: continuation,
            limit: limit,
            transaction: transaction
        )
        return DatabasePartitionCatalogPage(
            entries: page.entries.map {
                DatabasePartitionCatalogItem(
                    entity: $0.entity,
                    partitions: $0.partitions
                )
            },
            continuation: page.continuation
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
        try await store(
            for: type,
            path: path,
            readPolicy: try readPolicyForCurrentOperation()
        )
    }

    package func store<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath(),
        readPolicy: DatabaseReadPolicy
    ) async throws -> DatabaseDataStore {
        guard let entity = readPolicy.schema.entity(named: T.persistableType)
        else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        return try await store(
            for: entity,
            path: try AnyDirectoryPath(path),
            readPolicy: readPolicy
        )
    }

    /// Builds a read-only store, or `nil` when the directory it would read has
    /// never been created.
    ///
    /// Nothing is created: no Directory node, no catalog metadata, and no index
    /// lifecycle state. The returned store performs all reads on the
    /// transaction supplied by its caller.
    package func readStore<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath(),
        readPolicy: DatabaseReadPolicy,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseDataStore? {
        guard let entity = readPolicy.schema.entity(named: T.persistableType)
        else {
            throw ContainerSchemaError.entityNotFound(T.persistableType)
        }
        let directoryPath = try AnyDirectoryPath(path)
        try directoryPath.validate()
        guard let subspace = try await openDataDirectory(
            relativePath: directoryPath.resolve(),
            layers: try Self.declaredLayers(
                for: entity,
                in: readPolicy.directoryLayers
            ),
            transaction: transaction
        ) else {
            return nil
        }
        return DatabaseDataStore(
            container: self,
            subspace: subspace,
            entity: entity,
            readPolicy: readPolicy,
            securityDelegate: securityDelegate,
            indexConfigurations: readPolicy.indexConfigurations
        )
    }

    /// Build a typed store whose directory and index state participate in the
    /// caller's transaction.
    package func store<T: Persistable>(
        for type: T.Type,
        path: DirectoryPath<T> = DirectoryPath(),
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        try await store(
            for: schemaEntity(named: T.persistableType),
            path: try AnyDirectoryPath(path),
            transaction: transaction
        )
    }

    /// Builds a store for an entry point that owns no transaction.
    ///
    /// The directory this creates and the index lifecycle state it initializes
    /// belong to one keyspace, so both commit together or neither does.
    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        readPolicy: DatabaseReadPolicy
    ) async throws -> DatabaseDataStore {
        try await withOperationTransaction { transaction in
            try await self.store(
                for: entity,
                path: path,
                readPolicy: readPolicy,
                transaction: transaction
            )
        }
    }

    /// Build a store whose directory and index state participate in the
    /// caller's transaction.
    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        try await store(
            for: entity,
            path: path,
            readPolicy: try readPolicyForCurrentOperation(),
            transaction: transaction
        )
    }

    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        readPolicy: DatabaseReadPolicy,
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        let entity = try schemaEntity(
            matching: entity,
            declaredIn: readPolicy.schema
        )
        let subspace = try await resolveDirectory(
            for: entity,
            declaredIn: readPolicy.schema,
            directoryLayers: readPolicy.directoryLayers,
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
            entity: entity,
            readPolicy: readPolicy,
            securityDelegate: securityDelegate,
            indexConfigurations: readPolicy.indexConfigurations
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
        let pending = try await pendingSchemaIndexBuilds(
            entity: entity.name,
            indexes: indexNames,
            transaction: transaction
        )
        try await lifecycleStore.initializeMissingStates(
            indexNames,
            entityRange: subspace
                .subspace(SubspaceKey.items)
                .subspace(entity.name)
                .range(),
            pendingBuildIndexes: pending,
            transaction: transaction
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

    private func canonicalEntity(
        _ candidate: Schema.Entity
    ) throws -> Schema.Entity {
        try schemaEntity(
            matching: candidate,
            declaredIn: schema
        )
    }

    private func schemaEntity(
        matching candidate: Schema.Entity,
        declaredIn authoritySchema: Schema
    ) throws -> Schema.Entity {
        guard let entity = authoritySchema.entity(named: candidate.name) else {
            throw ContainerSchemaError.entityNotFound(candidate.name)
        }
        guard entity == candidate else {
            throw ContainerSchemaError.entitySchemaMismatch(
                candidate.name
            )
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
    package func resolvePolymorphicDirectory<P: Polymorphable>(for protocolType: P.Type) async throws -> Subspace {
        let pathComponents = P.polymorphicDirectoryPathComponents
        var components: [String] = []

        for component in pathComponents {
            switch component {
            case .staticPath(let value):
                components.append(value)
            case .dynamicField:
                throw DatabaseRuntimeError.internalError(
                    "Polymorphic protocols cannot use Field path components. " +
                    "Use only static Path components (string literals) in #Directory."
                )
            }
        }

        // The path is fully resolved before the transaction begins, so it
        // crosses into the transaction as an immutable value.
        let path = components
        return try await withOperationTransaction { transaction in
            try await self.resolveDataDirectory(
                relativePath: path,
                layers: self.directoryLayers.layers(forPath: path),
                transaction: transaction
            )
        }
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

    /// Resolves the directory of a polymorphic group in a transaction of its
    /// own.
    ///
    /// This is the entry point for a caller that holds no transaction. A
    /// Framework operation must instead use the `transaction:` form so the
    /// projection directory commits with the writes it admits.
    package func resolvePolymorphicDirectory(for identifier: String) async throws -> Subspace {
        let path = try polymorphicGroupPath(identifier: identifier)
        return try await withOperationTransaction { transaction in
            try await self.resolveDataDirectory(
                relativePath: path,
                layers: self.directoryLayers.layers(forPath: path),
                transaction: transaction
            )
        }
    }

    /// Resolves a polymorphic projection directory in the caller-owned
    /// transaction so directory creation and projected writes commit
    /// atomically.
    package func resolvePolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let path = try polymorphicGroupPath(identifier: identifier)
        return try await resolveDataDirectory(
            relativePath: path,
            layers: directoryLayers.layers(forPath: path),
            transaction: transaction
        )
    }

    /// Opens an existing polymorphic projection without creating anything. An
    /// absent directory represents an empty projection.
    package func openPolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionReadAccess
    ) async throws -> Subspace? {
        let path = try polymorphicGroupPath(identifier: identifier)
        return try await openDataDirectory(
            relativePath: path,
            layers: directoryLayers.layers(forPath: path),
            transaction: transaction
        )
    }

    /// The literal path a polymorphic group declares.
    ///
    /// A group declares no entity, so its components are typed by walking the
    /// schema's derived positions rather than by an entity's declared layers.
    /// Every component of a group path is static, so that walk is exact.
    private func polymorphicGroupPath(identifier: String) throws -> [String] {
        try polymorphicGroup(identifier: identifier).resolvedDirectoryPath()
    }

    /// Opens and admits one exact polymorphic index without creating metadata.
    ///
    /// `nil` means the polymorphic namespace has never existed. Every existing
    /// namespace must have a readable lifecycle state for the selected index.
    package func readablePolymorphicIndex(
        _ descriptor: IndexDeclaration<String>,
        in group: PolymorphicGroup,
        transaction: any TransactionReadAccess
    ) async throws -> ReadablePolymorphicIndex? {
        guard group.indexes.contains(descriptor) else {
            throw IndexQueryContextError.polymorphicIndexNotFound(
                indexName: descriptor.name,
                groupIdentifier: group.identifier
            )
        }
        guard let subspace = try await openPolymorphicDirectory(
            for: group.identifier,
            transaction: transaction
        ) else {
            return nil
        }
        try await IndexLifecycleStore(
            container: self,
            subspace: subspace
        ).validateReadableForRead(
            [descriptor.name],
            transaction: transaction
        )
        return ReadablePolymorphicIndex(
            descriptor: descriptor,
            subspace: try IndexLifecycleStore(
                container: self,
                subspace: subspace
            ).indexSubspace(for: descriptor.name)
        )
    }

}

// MARK: - Migration Support

extension DBContainer {
    /// Returns schema-version metadata for the currently bound Base.
    /// Global schema catalog metadata remains in the control domain and is
    /// never used as a Base migration checkpoint.
    private func getMetadataSubspace() async throws -> Subspace {
        #if DATABASE_MULTI_BASE
        try requireActiveDataRoot().systemRoot.subspace("metadata")
        #else
        defaultTenant.systemRoot.subspace("metadata")
        #endif
    }

    /// Get the current schema version from storage
    package func getCurrentSchemaVersion() async throws -> Schema.Version? {
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
        #if DATABASE_MULTI_BASE
        let metadataSubspace = try requireActiveDataRoot().systemRoot
            .subspace("metadata")
        #else
        let metadataSubspace = defaultTenant.systemRoot.subspace("metadata")
        #endif
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
        let preparedGeneration = try prepareMigrationSchemaGeneration(
            installedSchema
        )
        let metadataSubspace = try await getMetadataSubspace()
        try await withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: .batch,
        ) { transaction in
            try Self.setCurrentSchemaSnapshot(
                installedSchema,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                indexPhysicalLayouts:
                    preparedGeneration.indexPhysicalLayouts,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
        }
    }

    package static func setCurrentSchemaSnapshot(
        _ schema: Schema,
        indexPhysicalFingerprint: ByteString,
        executionRuntimeFingerprint: ByteString,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) throws {
        guard
            indexPhysicalFingerprint.count
                == SHA256Accumulator.digestByteCount
        else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "index physical fingerprint has an invalid length"
            )
        }
        guard
            executionRuntimeFingerprint.count
                == SHA256Accumulator.digestByteCount
        else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "execution runtime fingerprint has an invalid length"
            )
        }
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
        try transaction.setValue(
            indexPhysicalFingerprint,
            for: activeIndexPhysicalFingerprintKey(
                metadataSubspace: metadataSubspace
            )
        )
        try transaction.setValue(
            executionRuntimeFingerprint,
            for: activeExecutionRuntimeFingerprintKey(
                metadataSubspace: metadataSubspace
            )
        )
        try setActiveIndexPhysicalLayouts(
            indexPhysicalLayouts,
            metadataSubspace: metadataSubspace,
            transaction: transaction
        )
    }

    package static func schemaGenerationKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("generation"))
    }

    /// Key holding the in-progress migration stage marker.
    ///
    /// The marker is written before a stage's data and index work begins and
    /// cleared in the same transaction as the version bump, so a crash between
    /// the two leaves observable evidence instead of a silent partial stage.
    private static func migrationStageMarkerKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("migrationStage"))
    }

    private static func packMigrationStageMarker(
        from: Schema.Version,
        to: Schema.Version
    ) -> ByteString {
        Tuple(
            Int(from.major),
            Int(from.minor),
            Int(from.patch),
            Int(to.major),
            Int(to.minor),
            Int(to.patch)
        ).pack()
    }

    private static func unpackMigrationStageMarker(
        _ bytes: ByteString
    ) throws -> (from: Schema.Version, to: Schema.Version) {
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 6 else {
            throw DatabaseRuntimeError.internalError(
                "Invalid migration stage marker format"
            )
        }
        var components: [UInt32] = []
        components.reserveCapacity(6)
        for index in 0..<6 {
            guard case .signedInteger(let value) = try tuple.value(at: index),
                let component = UInt32(exactly: value)
            else {
                throw DatabaseRuntimeError.internalError(
                    "Invalid migration stage marker format"
                )
            }
            components.append(component)
        }
        return (
            from: Schema.Version(components[0], components[1], components[2]),
            to: Schema.Version(components[3], components[4], components[5])
        )
    }
}

// MARK: - VersionedSchema Support

private enum DatabaseDataRootSchemaBootstrapAuthority: Sendable {
    case request
    #if DATABASE_MULTI_BASE
    case provisioning
    #endif
}

extension DBContainer {
    /// Opens an application-compiled schema and attaches its migration plan.
    public static func open<P: SchemaMigrationPlan>(
        for schema: Schema,
        migrationPlan: P.Type,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled()
    ) async throws -> DBContainer {
        do {
            try P.validate()
        } catch {
            #if DATABASE_MULTI_BASE
            await configuration.shutdownStorageTopologyIfUnclaimed()
            #else
            await configuration.shutdownStorageEngineIfUnclaimed()
            #endif
            throw error
        }
        let migrationAdmissionGate = DatabaseMigrationAdmissionGate()
        let container = try await open(
            for: schema,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: false,
            initializeIndexes: false,
            migrationAdmissionGate: migrationAdmissionGate
        )
        container.migrationPlanStorage.withLock { $0 = migrationPlan }
        migrationAdmissionGate.requireMigration()
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
        let schemaInstance: Schema
        do {
            try P.validate()
            schemaInstance = try S.makeSchema()
        } catch {
            #if DATABASE_MULTI_BASE
            await configuration.shutdownStorageTopologyIfUnclaimed()
            #else
            await configuration.shutdownStorageEngineIfUnclaimed()
            #endif
            throw error
        }
        let migrationAdmissionGate = DatabaseMigrationAdmissionGate()
        let container = try await open(
            for: schemaInstance,
            configuration: configuration,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            persistSchemaCatalog: false,
            initializeIndexes: false,
            migrationAdmissionGate: migrationAdmissionGate
        )
        container.migrationPlanStorage.withLock { $0 = migrationPlan }
        migrationAdmissionGate.requireMigration()
        return container
    }

    /// Return exact pending migration identifiers for the compiled schema.
    @_spi(DatabaseExecution)
    public func migrationStatus(
        targetVersion requestedTarget: Schema.Version? = nil
    ) async throws -> DatabaseMigrationStatus {
        let targetVersion = try migrationTarget(requestedTarget)
        return try await withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: .readOnly
        ) { transaction in
            try await self.migrationStatus(
                targetVersion: targetVersion,
                transaction: transaction
            )
        }
    }

    /// Resolves migration status in a caller-owned transaction.
    @_spi(DatabaseExecution)
    public func migrationStatus(
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
        guard
            let currentSchemaType = plan.schemas.first(where: {
                $0.versionIdentifier == currentVersion
            })
        else {
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
    @_spi(DatabaseExecution)
    public func runMigrations(
        targetVersion requestedTarget: Schema.Version? = nil,
        maximumStageCount: UInt64
    ) async throws -> DatabaseMigrationExecutionResult {
        guard migrationPlanStorage.withLock({ $0 }) != nil else {
            return try await executeMigrations(
                targetVersion: requestedTarget,
                maximumStageCount: maximumStageCount
            )
        }
        guard maximumStageCount > 0 else {
            return try await withMigrationMaintenanceAccess {
                try await self.executeMigrations(
                    targetVersion: requestedTarget,
                    maximumStageCount: maximumStageCount
                )
            }
        }

        guard let migrationAdmissionGate else {
            preconditionFailure(
                "A container with a migration plan must own an admission gate"
            )
        }
        try await migrationAdmissionGate.beginMigration()
        do {
            let result = try await withMigrationMaintenanceAccess {
                try await self.executeMigrations(
                    targetVersion: requestedTarget,
                    maximumStageCount: maximumStageCount
                )
            }
            let dataOperationAdmissionIsComplete: Bool
            if result.isComplete {
                let preparedGeneration = try prepareMigrationSchemaGeneration(
                    schema
                )
                dataOperationAdmissionIsComplete = try await
                    allActiveDataRootsMatch(
                        schema,
                        preparedGeneration: preparedGeneration
                    )
            } else {
                dataOperationAdmissionIsComplete = false
            }
            migrationAdmissionGate.finishMigration(
                isComplete: dataOperationAdmissionIsComplete,
                publishedSchemaGeneration:
                    acquirePublishedSchemaLease().generation
            )
            return result
        } catch {
            migrationAdmissionGate.failMigration()
            throw error
        }
    }

    private func executeMigrations(
        targetVersion requestedTarget: Schema.Version?,
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
        var completedStageCount: UInt64 = 0

        while completedStageCount < maximumStageCount {
            let status = try await migrationStatus(
                targetVersion: targetVersion
            )
            guard !status.pendingMigrationIdentifiers.isEmpty else {
                try await ensureIndexesReady()
                try await publishSchemaCatalogIfAllActiveBasesMatch(schema)
                return DatabaseMigrationExecutionResult(
                    completedStageCount: completedStageCount,
                    isComplete: true
                )
            }

            if status.currentVersion == nil {
                let bootstrapped = try await bootstrapInitialSchemaIfNeeded(
                    targetVersion: targetVersion
                )
                if bootstrapped {
                    completedStageCount += 1
                    logger.info("Set initial schema version: \(targetVersion)")
                }
                continue
            }

            guard let currentVersion = status.currentVersion else {
                throw DatabaseRuntimeError.internalError(
                    "Migration status lost its current schema version"
                )
            }
            // A data-domain commit can precede control-domain publication when
            // those domains are physically independent. Repair that durable
            // boundary before admitting the next migration stage.
            try await publishSchemaCatalogIfAllActiveBasesMatch(
                schemaDefinition(for: currentVersion)
            )

            guard let plan = migrationPlanStorage.withLock({ $0 }),
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
            try await publishSchemaCatalogIfAllActiveBasesMatch(schema)
            logger.info("Migration complete: now at version \(targetVersion)")
        }
        return DatabaseMigrationExecutionResult(
            completedStageCount: completedStageCount,
            isComplete: isComplete
        )
    }

    /// Migrate to the current compiled schema version if needed.
    package func migrateIfNeeded() async throws {
        _ = try await runMigrations(maximumStageCount: .max)
    }

    /// Publishes one committed migration boundary only after every active Base
    /// has reached the exact schema and physical index generation. Base
    /// migration checkpoints remain in their data domains; the discoverable
    /// schema remains database-wide.
    private func publishSchemaCatalogIfAllActiveBasesMatch(
        _ candidateSchema: Schema
    )
        async throws
    {
        let preparedGeneration = try prepareMigrationSchemaGeneration(
            candidateSchema
        )
        let targetExecutionRuntimeFingerprint =
            preparedGeneration.executionRuntimeFingerprint
        let targetIndexPhysicalFingerprint =
            preparedGeneration.indexPhysicalFingerprint
        let targetIndexPhysicalLayouts =
            preparedGeneration.indexPhysicalLayouts
        guard try await allActiveDataRootsMatch(
            candidateSchema,
            preparedGeneration: preparedGeneration
        ) else {
            return
        }
        #if DATABASE_MULTI_BASE
        let schemaEngine = controlEngine
        let schemaRoot = storageTopology.controlDomain.systemRoot
        let schemaTransactionExecutor = controlTransactionExecutor
        #else
        let schemaEngine = engine
        let schemaRoot = defaultTenant.systemRoot
        let schemaTransactionExecutor = transactionExecutor
        #endif
        let registry = SchemaRegistry(
            database: schemaEngine,
            root: schemaRoot,
            clock: monotonicClock
        )
        let targetFingerprint = try SchemaManifest(schema: candidateSchema)
            .fingerprint()
        let nextGeneration = try await schemaTransactionExecutor
            .withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                let persistedEntities = try await registry.loadAll(
                    transaction: transaction
                )
                let persistedVersion = try await Self.loadSchemaVersion(
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
                let persistedFingerprint = try await Self
                    .loadActiveSchemaFingerprint(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                let persistedGeneration = try await Self
                    .loadPersistedSchemaGeneration(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                let persistedExecutionRuntimeFingerprint =
                    try await Self
                    .loadActiveExecutionRuntimeFingerprint(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                let persistedIndexPhysicalFingerprint =
                    try await Self
                    .loadActiveIndexPhysicalFingerprint(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                let persistedIndexLayoutFingerprints =
                    try await Self
                    .loadActiveIndexLayoutFingerprints(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )

                if persistedEntities.isEmpty,
                    persistedFingerprint == nil,
                    persistedGeneration == nil
                {
                    guard
                        persistedVersion == nil
                            || persistedVersion == candidateSchema.version
                    else {
                        throw DatabaseSchemaPublicationError.corruptedState(
                            "bootstrap schema version does not match the migration boundary"
                        )
                    }
                    guard
                        persistedExecutionRuntimeFingerprint == nil
                            || persistedExecutionRuntimeFingerprint
                                == targetExecutionRuntimeFingerprint
                    else {
                        throw DatabaseSchemaRestorationError
                            .executionRuntimeFingerprintMismatch
                    }
                    guard
                        persistedIndexPhysicalFingerprint == nil
                            || persistedIndexPhysicalFingerprint
                                == targetIndexPhysicalFingerprint
                    else {
                        throw DatabaseSchemaRestorationError
                            .indexPhysicalFingerprintMismatch
                    }
                    guard
                        persistedIndexLayoutFingerprints.isEmpty
                            || persistedIndexLayoutFingerprints
                                == targetIndexPhysicalLayouts.mapValues({
                                    $0.fingerprint
                                })
                    else {
                        throw DatabaseSchemaRestorationError
                            .indexPhysicalFingerprintMismatch
                    }
                    try await registry.persistInitialSchema(
                        candidateSchema,
                        transaction: transaction
                    )
                    try Self.setCurrentSchemaSnapshot(
                        candidateSchema,
                        indexPhysicalFingerprint:
                            targetIndexPhysicalFingerprint,
                        executionRuntimeFingerprint:
                            targetExecutionRuntimeFingerprint,
                        indexPhysicalLayouts:
                            targetIndexPhysicalLayouts,
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                    try transaction.setValue(
                        targetFingerprint.bytes,
                        for: Self.activeSchemaFingerprintKey(
                            metadataSubspace: self.metadataSubspace
                        )
                    )
                    try transaction.setValue(
                        Tuple(UInt64(0)).pack(),
                        for: Self.schemaGenerationKey(
                            metadataSubspace: self.metadataSubspace
                        )
                    )
                    return UInt64(0)
                }

                guard let persistedVersion else {
                    throw DatabaseSchemaRestorationError.missingVersion
                }
                guard let persistedFingerprint else {
                    throw DatabaseSchemaRestorationError.missingFingerprint
                }
                guard let persistedGeneration else {
                    throw DatabaseSchemaRestorationError.invalidGeneration
                }
                guard let persistedExecutionRuntimeFingerprint else {
                    throw DatabaseSchemaRestorationError
                        .missingExecutionRuntimeFingerprint
                }
                guard let persistedIndexPhysicalFingerprint else {
                    throw DatabaseSchemaRestorationError
                        .missingIndexPhysicalFingerprint
                }
                let persistedSchema = try Schema(
                    entities: persistedEntities,
                    version: persistedVersion
                )
                guard
                    try SchemaManifest(schema: persistedSchema)
                        .fingerprint() == persistedFingerprint
                else {
                    throw DatabaseSchemaRestorationError.fingerprintMismatch
                }
                if persistedFingerprint == targetFingerprint {
                    guard
                        persistedIndexPhysicalFingerprint
                            == targetIndexPhysicalFingerprint
                    else {
                        throw DatabaseSchemaRestorationError
                            .indexPhysicalFingerprintMismatch
                    }
                    guard
                        persistedIndexLayoutFingerprints
                            == targetIndexPhysicalLayouts.mapValues({
                                $0.fingerprint
                            })
                    else {
                        throw DatabaseSchemaRestorationError
                            .indexPhysicalFingerprintMismatch
                    }
                    if persistedExecutionRuntimeFingerprint
                        == targetExecutionRuntimeFingerprint
                    {
                        return persistedGeneration
                    }
                }

                let incremented =
                    persistedGeneration
                    .addingReportingOverflow(1)
                guard !incremented.overflow else {
                    throw DatabaseSchemaPublicationError.generationOverflow
                }
                try await registry.persist(
                    candidateSchema,
                    mode: .allowBreakingChanges(
                        entityNames: Set(
                            candidateSchema.entities.map { $0.name }
                        )
                    ),
                    transaction: transaction
                )
                try Self.setCurrentSchemaSnapshot(
                    candidateSchema,
                    indexPhysicalFingerprint:
                        targetIndexPhysicalFingerprint,
                    executionRuntimeFingerprint:
                        targetExecutionRuntimeFingerprint,
                    indexPhysicalLayouts: targetIndexPhysicalLayouts,
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
                try transaction.setValue(
                    targetFingerprint.bytes,
                    for: Self.activeSchemaFingerprintKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )
                try transaction.setValue(
                    Tuple(incremented.partialValue).pack(),
                    for: Self.schemaGenerationKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )
                return incremented.partialValue
            }
        let compiledFingerprint = try SchemaManifest(schema: schema)
            .fingerprint()
        if targetFingerprint == compiledFingerprint {
            try publishSchemaGeneration(
                schema,
                fingerprint: targetFingerprint,
                indexPhysicalFingerprint:
                    preparedGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedGeneration.executionRuntimeFingerprint,
                runtimeConfiguration: runtimeConfiguration,
                indexPhysicalLayouts:
                    preparedGeneration.indexPhysicalLayouts,
                generation: nextGeneration
            )
        }
    }

    /// Returns whether every active data root has reached the candidate schema
    /// and physical index generation. The execution runtime is database-wide
    /// and is published through the control catalog after this condition holds.
    private func allActiveDataRootsMatch(
        _ candidateSchema: Schema,
        preparedGeneration: DatabasePreparedSchemaGeneration
    ) async throws -> Bool {
        #if DATABASE_MULTI_BASE
        let targetSchemaFingerprint = try DatabaseSchemaFingerprint.compute(
            candidateSchema
        )
        let targetLayoutFingerprints = preparedGeneration
            .indexPhysicalLayouts
            .mapValues({ $0.fingerprint })
        let activeBases = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            let records = try await self.baseCatalog.loadAll(
                transaction: transaction.storageAccess
            )
            return records.filter { $0.lifecycle == .active }
        }
        for record in activeBases {
            let lease = try acquireBaseLease(record.id)
            let matchesGeneration = try await withBaseLease(lease) {
                let metadataSubspace = try await self.getMetadataSubspace()
                return try await self.transactionExecutor.withTransaction(
                    configuration: .readOnly,
                    clock: self.monotonicClock
                ) { transaction in
                    let currentVersion = try await self
                        .getCurrentSchemaVersion(transaction: transaction)
                    let currentSchemaFingerprint = try await transaction
                        .getValue(
                            for: metadataSubspace
                                .subspace("schema")
                                .pack(Tuple("fingerprint")),
                            snapshot: false
                        )
                    let currentPhysicalFingerprint = try await Self
                        .loadActiveIndexPhysicalFingerprint(
                            metadataSubspace: metadataSubspace,
                            transaction: transaction
                        )
                    let currentLayoutFingerprints = try await Self
                        .loadActiveIndexLayoutFingerprints(
                            metadataSubspace: metadataSubspace,
                            transaction: transaction
                        )
                    return currentVersion == candidateSchema.version
                        && currentSchemaFingerprint
                            == targetSchemaFingerprint
                        && currentPhysicalFingerprint
                            == preparedGeneration.indexPhysicalFingerprint
                        && currentLayoutFingerprints
                            == targetLayoutFingerprints
                }
            }
            guard matchesGeneration else {
                return false
            }
        }
        #else
        _ = candidateSchema
        _ = preparedGeneration
        #endif
        return true
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

    package func schemaDefinition(
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
        #if DATABASE_MULTI_BASE
        let metadataSubspace = try requireActiveDataRoot().systemRoot
            .subspace("metadata")
        #else
        let metadataSubspace = defaultTenant.systemRoot.subspace("metadata")
        #endif
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
        authority: DatabaseDataRootSchemaBootstrapAuthority = .request
    ) async throws -> Bool {
        let metadataSubspace = try await getMetadataSubspace()
        let versionKey = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        var staticEntities: [Schema.Entity] = []
        for entity in schema.entities {
            guard !entity.hasDynamicDirectory else {
                continue
            }
            guard runtimeConfiguration.entityRuntimes.registration(
                named: entity.name
            ) != nil else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            staticEntities.append(entity)
        }
        let entities = staticEntities

        let bootstrap: @Sendable (any TransactionAccess) async throws -> Bool = {
            transaction in
            guard try await transaction.getValue(
                for: versionKey,
                snapshot: false
            ) == nil else {
                return false
            }
            // Each static store's Directory is resolved in the bootstrap
            // transaction, so the metadata it creates commits with the index
            // lifecycle state that same transaction writes for that store. An
            // entity that declares no index has no state to initialize, so its
            // Directory is opened rather than created and first appears when a
            // write reaches it.
            for entity in entities {
                let indexNames = entity.indexDescriptors.map { $0.name }
                let subspace: Subspace
                if indexNames.isEmpty {
                    guard let opened = try await self.openDirectory(
                        for: entity,
                        transaction: transaction
                    ) else { continue }
                    subspace = opened
                } else {
                    subspace = try await self.resolveDirectory(
                        for: entity,
                        transaction: transaction
                    )
                }
                let range = subspace
                    .subspace(SubspaceKey.items)
                    .subspace(entity.name)
                    .range()
                let rows = try await TransactionRangeCollection.collect(using: transaction,
                    from: .firstGreaterOrEqual(range.begin),
                    to: .firstGreaterOrEqual(range.end),
                    limit: 1,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .small
                )
                guard rows.isEmpty else {
                    throw MigrationPlanError.unversionedStoreContainsEntities(
                        entity: entity.name
                    )
                }
                guard !indexNames.isEmpty else { continue }
                let lifecycleStore = IndexLifecycleStore(
                    container: self,
                    subspace: subspace
                )
                try await lifecycleStore.ensureReadable(
                    indexNames,
                    entityRange: range,
                    transaction: transaction
                )
            }
            try Self.setCurrentSchemaSnapshot(
                self.schema,
                indexPhysicalFingerprint:
                    self.activeSchemaLease.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    self.activeSchemaLease.executionRuntimeFingerprint,
                indexPhysicalLayouts:
                    self.activeSchemaLease.indexPhysicalLayouts,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
            return true
        }
        switch authority {
        case .request:
            return try await withDatabaseTransaction(
                requiredAccess: .administer,
                configuration: .batch,
                bootstrap
            )
        #if DATABASE_MULTI_BASE
        case .provisioning:
            let lease = try requireActiveBaseLease()
            return try await lease.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock,
                bootstrap
            )
        #endif
        }
    }

    #if DATABASE_MULTI_BASE
    /// Initializes a newly allocated Base before its provisioning record is
    /// published as active. This is a narrowly scoped system transition: the
    /// caller has already persisted the Base's initial Grants, but no user
    /// request is allowed to enter the Base until this method completes.
    package func bootstrapProvisionedBase() async throws {
        _ = try await bootstrapInitialSchemaIfNeeded(
            targetVersion: schema.version,
            authority: .provisioning
        )
        let lease = try requireActiveBaseLease()
        let installedVersion = try await lease.transactionExecutor
            .withTransaction(
                configuration: .readOnly,
                clock: monotonicClock
            ) { transaction in
                try await self.getCurrentSchemaVersion(
                    transaction: transaction
                )
            }
        guard installedVersion == schema.version else {
            throw DatabaseRuntimeError.internalError(
                "Provisioned Base schema does not match the active generation"
            )
        }
        try await ensureIndexesReady()
    }
    #endif

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

        // Record the stage before any data or index work. Stage work spans
        // multiple transactions (index builds and data rewrites cannot fit in
        // one), so the version bump cannot be atomic with it. The marker makes
        // an interrupted stage observable on the next run — a matching marker
        // means the same stage re-runs from the start (stage work must be
        // idempotent for exactly this reason), a mismatched marker fails
        // instead of running a different stage over partial state. The marker
        // is cleared in the version-bump transaction below.
        let metadataSubspace = try await getMetadataSubspace()
        let sourceIndexLayoutFingerprints = try await withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: .readOnly
        ) { transaction in
            try await Self.loadActiveIndexLayoutFingerprints(
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
        }
        let preparedTargetGeneration = try prepareMigrationSchemaGeneration(
            targetSchema
        )
        let indexTransition = try DatabaseIndexTransitionPlan(
            currentSchema: sourceSchema,
            currentLayoutFingerprints: sourceIndexLayoutFingerprints,
            targetSchema: targetSchema,
            targetPhysicalLayouts:
                preparedTargetGeneration.indexPhysicalLayouts
        )
        let stageMarkerKey = Self.migrationStageMarkerKey(
            metadataSubspace: metadataSubspace
        )
        let expectedFrom = stage.fromVersionIdentifier
        let expectedTo = stage.toVersionIdentifier
        try await withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: .batch
        ) { transaction in
            if let markerBytes = try await transaction.getValue(
                for: stageMarkerKey,
                snapshot: false
            ) {
                let marker = try Self.unpackMigrationStageMarker(markerBytes)
                guard marker.from == expectedFrom, marker.to == expectedTo else {
                    throw MigrationPlanError.interruptedMigrationStage(
                        markedFrom: marker.from,
                        markedTo: marker.to,
                        expectedFrom: expectedFrom,
                        expectedTo: expectedTo
                    )
                }
                self.logger.warning(
                    "Re-running interrupted migration stage \(marker.from) -> \(marker.to); stage work re-applies from the start"
                )
            } else {
                try transaction.setValue(
                    Self.packMigrationStageMarker(
                        from: expectedFrom,
                        to: expectedTo
                    ),
                    for: stageMarkerKey
                )
            }
        }

        let targetIndexNames = Set(
            targetSchema.indexDescriptors.map { $0.name }
                + targetSchema.polymorphicGroups.flatMap {
                    $0.indexes.map { $0.name }
                }
        )
        let removedIndexNames = Set(
            sourceSchema.indexDescriptors.map { $0.name }
                + sourceSchema.polymorphicGroups.flatMap {
                    $0.indexes.map { $0.name }
                }
        ).subtracting(targetIndexNames).sorted()
        let requiresStoreAccess = stage.willMigrate != nil
            || stage.didMigrate != nil
            || !indexTransition.builds.isEmpty
            || !indexTransition.retirements.isEmpty
        // Store registries are only needed by data and index work. Constructing
        // them for a metadata-only stage would incorrectly require one concrete
        // value for every dynamic partition.
        let sourceStoreRegistry = requiresStoreAccess
            ? try await buildStoreRegistry(for: sourceSchema)
            : [:]
        let targetStoreRegistry = requiresStoreAccess
            ? try await buildStoreRegistry(for: targetSchema)
            : [:]
        let context = MigrationContext(
            container: self,
            schema: targetSchema,
            sourceSchema: sourceSchema,
            metadataSubspace: metadataSubspace,
            sourceStoreRegistry: sourceStoreRegistry,
            targetStoreRegistry: targetStoreRegistry,
            runtimeConfiguration: runtimeConfiguration,
            targetIndexPhysicalLayouts:
                preparedTargetGeneration.indexPhysicalLayouts
        )

        if let willMigrate = stage.willMigrate {
            try await willMigrate(context)
        }

        // Build every new physical generation before retiring the previous
        // generation. Replacements therefore never reuse bytes written for a
        // different definition and remain recoverable if a build is retried.
        for build in indexTransition.builds {
            switch build.scope {
            case .entity(let entityName, _):
                guard let entity = targetSchema.entity(named: entityName),
                    let descriptor = entity.indexDescriptors.first(
                        where: { $0.name == build.identity.name }
                    )
                else {
                    throw DatabaseSchemaPublicationError.corruptedState(
                        "index transition build target is absent from the target schema"
                    )
                }
                logger.info("Building index: \(descriptor.name)")
                try await context.addIndex(descriptor)
            case .polymorphicGroup:
                logger.info(
                    "Building polymorphic index: \(build.identity.name)"
                )
                try await context.addPolymorphicIndex(
                    indexName: build.identity.name
                )
            }
        }

        if let didMigrate = stage.didMigrate {
            try await didMigrate(context)
        }

        try await withDatabaseTransaction(
            requiredAccess: .administer,
            configuration: .batch
        ) { transaction in
            try Self.setCurrentSchemaSnapshot(
                targetSchema,
                indexPhysicalFingerprint:
                    preparedTargetGeneration.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    preparedTargetGeneration.executionRuntimeFingerprint,
                indexPhysicalLayouts:
                    preparedTargetGeneration.indexPhysicalLayouts,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
            for retirement in indexTransition.retirements {
                try await context.retireIndexStorage(
                    retirement,
                    transaction: transaction
                )
            }

            for indexName in removedIndexNames {
                try await context.removeIndex(
                    indexName: indexName,
                    addedVersion: stage.fromVersionIdentifier,
                    transaction: transaction
                )
            }
            try transaction.clear(key: stageMarkerKey)
        }
        try await publishSchemaCatalogIfAllActiveBasesMatch(targetSchema)
        logger.info("Updated schema version to \(stage.toVersionIdentifier)")
    }

    private func buildStoreRegistry(for schema: Schema) async throws -> [String: MigrationStoreInfo]
    {
        var registry: [String: MigrationStoreInfo] = [:]
        // A registry may be built for a schema the container has not published,
        // so the Directory layer tags come from that schema rather than from the
        // generation currently installed.
        let layers = try DirectoryLayerTagMap(
            entities: schema.entities,
            polymorphicGroups: schema.polymorphicGroups
        )

        for entity in schema.entities {
            guard runtimeConfiguration.entityRuntimes.registration(
                named: entity.name
            ) != nil else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            // Use resolveDirectory to respect #Directory definitions declared
            // by *this schema's* Swift type — V1 and V2 with the same entity
            // name may point to different directories.
            let subspace = try await withDatabaseTransaction(
                requiredAccess: .administer,
                configuration: .default
            ) { transaction in
                try await self.resolveDirectory(
                    for: entity,
                    declaredIn: schema,
                    directoryLayers: layers,
                    transaction: transaction
                )
            }
            let info = MigrationStoreInfo(
                subspace: subspace,
                blobsSubspace: subspace.subspace(SubspaceKey.blobs)
            )
            registry[entity.name] = info
        }

        return registry
    }
}

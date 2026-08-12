import DatabaseTypes
import StorageKit
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire
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
///     ├── single-root: schema, database Grants, and application data
///     └── MultipleBases trait:
///           control domain + Base roots + read-only Compositions
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
///     configuration: DBConfiguration(
///         storageTopology: topology,
///         monotonicClock: applicationMonotonicClock,
///         wallClock: applicationWallClock
///     ),
///     runtimeConfiguration: runtime
/// )
///
/// // 3. Bind authorization to the database data root
/// let context = container.newContext(authorization: authorization)
/// try context.insert(user)
/// try await context.save()
/// ```
/// With the `MultipleBases` trait, use
/// `container.session(authorization:).base(baseID).newContext()` instead.
public final class DBContainer: Sendable {
    private struct PreparedStorage: Sendable {
        let topology: DatabaseStorageRuntimeTopology
        let engine: any StorageEngine
        let format: DatabaseFormatDescriptor
        let databaseDataRoot: DatabaseDataRootLease
        #if DATABASE_MULTIPLE_BASES
        let baseCatalog: DatabaseBaseCatalog
        let compositionCatalog: DatabaseCompositionCatalog
        let layoutCatalog: DatabaseLayoutCatalog
        let layoutStatus: DatabaseLayoutStatus
        let baseGenerations: [DatabaseBaseGeneration]
        #endif
        let databaseGrantStore: DatabaseGrantStore
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
    private let controlEngine: any StorageEngine

    /// Storage selected by the current target lease, or the control domain
    /// while executing an explicitly database-scoped operation.
    package var engine: any StorageEngine {
        ActiveDatabaseDataRootContext.lease?.domain.engine
            ?? controlEngine
    }

    /// Logical identity of the domain currently hosting control metadata.
    public let controlDomainID: DatabaseStorageDomain.ID

    /// Prepared storage roots retained for the complete container lifetime.
    package let storageTopology: DatabaseStorageRuntimeTopology

    /// Typed transaction execution over the dynamically selected storage engine.
    ///
    /// This concrete boundary avoids invoking generic protocol-extension methods
    /// on an existential engine in Embedded Swift.
    package let controlTransactionExecutor: StorageTransactionExecutor

    /// Transaction execution selected by the current target lease.
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

    /// Container-scoped runtime extensions paired atomically with `schema`.
    public var runtimeConfiguration: DatabaseRuntimeConfiguration {
        activeSchemaLease.runtimeConfiguration
    }

    /// Container-scoped factory for the database's canonical entity format.
    public let itemStorageFactory: ItemStorageFactory

    /// Persisted database-wide physical format source of truth.
    public let databaseFormat: DatabaseFormatDescriptor

    /// Security configuration
    public let securityConfiguration: SecurityConfiguration

    /// Security delegate for DataStore operations
    ///
    /// Created from securityConfiguration and uses TaskLocal for auth context.
    public var securityDelegate: (any DataStoreSecurityDelegate)? {
        activeSchemaLease.securityDelegate
    }

    /// Container-scoped observer for data store metrics.
    internal let dataStoreDelegate: any DataStoreDelegate

    /// Index configurations grouped by indexName
    public let indexConfigurations: [String: [any IndexRuntimeConfiguration]]

    /// Database event logger selected by the container configuration.
    private let logger: DatabaseLogger

    /// Database-owned data root used when `MultipleBases` is not enabled and
    /// for explicitly database-scoped data operations.
    private let databaseDataRoot: DatabaseDataRootLease

    /// Database-scoped Grants stored in the control transaction domain.
    package let databaseGrantStore: DatabaseGrantStore

    #if DATABASE_MULTIPLE_BASES
    /// Durable Base definitions in the control domain.
    package let baseCatalog: DatabaseBaseCatalog

    /// Durable named Composition definitions in the control domain.
    package let compositionCatalog: DatabaseCompositionCatalog

    /// Durable v2 layout marker and atomically published admission state.
    package let layoutCatalog: DatabaseLayoutCatalog
    package let layoutStatusStorage: Mutex<DatabaseLayoutStatus>

    package var layoutStatus: DatabaseLayoutStatus {
        layoutStatusStorage.withLock { $0 }
    }

    /// Immutable Base placement generations and operation admission leases.
    private let baseGenerationStore: DatabaseBaseGenerationStore
    #endif

    /// Stable metadata namespace used by schema lifecycle operations.
    package let metadataSubspace: Subspace

    /// Migration plan
    private let migrationPlanStorage: Mutex<(any SchemaMigrationPlan.Type)?>

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
        initializeIndexes: Bool
    ) async throws -> DBContainer {
        let storageTopology = try configuration.claimStorageTopology()
        let storageEngine = storageTopology.controlDomain.engine
        do {
            try runtimeConfiguration.validate(schema: schema)
            try IndexRuntimeConfigurationValidator.validate(
                configuration.indexConfigurations,
                schema: schema,
                entityRuntimes: runtimeConfiguration.entityRuntimes
            )

            let transactionCapabilities = try await inspectTransactionCapabilities(
                storageEngine: storageEngine
            )
            try runtimeConfiguration.validateStorageRequirements(
                schema: schema,
                transactionCapabilities: transactionCapabilities
            )

            let preparedStorage = try await prepareStorage(
                storageTopology: storageTopology,
                storageEngine: storageEngine,
                configuration: configuration,
                transactionCapabilities: transactionCapabilities
            )
            let schemaFingerprint = try SchemaManifest(schema: schema)
                .fingerprint()
            let container = DBContainer(
                schema: schema,
                schemaFingerprint: schemaFingerprint,
                schemaGeneration: nil,
                configuration: configuration,
                runtimeConfiguration: runtimeConfiguration,
                security: security,
                preparedStorage: preparedStorage
            )

            #if DATABASE_MULTIPLE_BASES
            try await container.prepareTestingBaseIfConfigured()
            if initializeIndexes, container.layoutStatus == .current {
                try await container.ensureIndexesReadyForAllActiveBases()
            }
            #else
            if initializeIndexes {
                try await container.withDatabaseDataRoot {
                    try await container.ensureIndexesReady()
                }
            }
            #endif

            if persistSchemaCatalog {
                try await container.initializeSchemaCatalogIfNeeded(
                    schema,
                    fingerprint: schemaFingerprint
                )
            }
            try configuration.finishOpeningStorageTopology()
            return container
        } catch {
            await configuration.shutdownStorageTopology()
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
        let storageTopology = try configuration.claimStorageTopology()
        let storageEngine = storageTopology.controlDomain.engine
        do {
            let preparedStorage = try await prepareStorage(
                storageTopology: storageTopology,
                storageEngine: storageEngine,
                configuration: configuration
            )
            let schemaRoot: Subspace
            let schemaMetadataSubspace: Subspace
            #if DATABASE_MULTIPLE_BASES
            if preparedStorage.layoutStatus == .migrationRequired {
                schemaRoot = Subspace()
                schemaMetadataSubspace = try await storageEngine
                    .resolveExistingNamespace(path: ["_metadata"])
            } else {
                schemaRoot = preparedStorage.topology.controlDomain.root
                schemaMetadataSubspace = preparedStorage.metadataSubspace
            }
            #else
            schemaRoot = preparedStorage.topology.controlDomain.root
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
            try IndexRuntimeConfigurationValidator.validate(
                configuration.indexConfigurations,
                schema: restored.schema,
                entityRuntimes: runtimeConfiguration.entityRuntimes
            )
            try runtimeConfiguration.validateStorageRequirements(
                schema: restored.schema,
                transactionCapabilities: preparedStorage.transactionCapabilities
            )
            let container = DBContainer(
                schema: restored.schema,
                schemaFingerprint: restored.fingerprint,
                schemaGeneration: restored.generation,
                configuration: configuration,
                runtimeConfiguration: runtimeConfiguration,
                security: security,
                preparedStorage: preparedStorage
            )
            #if DATABASE_MULTIPLE_BASES
            if container.layoutStatus == .migrationRequired {
                try await SchemaRegistry(
                    database: container.engine,
                    root: container.storageTopology.controlDomain.root,
                    clock: container.monotonicClock
                ).persist(restored.schema)
            }
            try await container.prepareTestingBaseIfConfigured()
            if container.layoutStatus == .current {
                try await container.ensureIndexesReadyForAllActiveBases()
            }
            #else
            try await container.withDatabaseDataRoot {
                try await container.ensureIndexesReady()
            }
            #endif
            try configuration.finishOpeningStorageTopology()
            return container
        } catch {
            await configuration.shutdownStorageTopology()
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
            let root = try await claimedDomain.engine.resolveOrCreateNamespace(
                path: claimedDomain.namespacePath
            )
            preparedDomains[claimedDomain.id] = DatabaseStorageDomainRuntime(
                id: claimedDomain.id,
                namespacePath: claimedDomain.namespacePath,
                engine: claimedDomain.engine,
                transactionExecutor: StorageTransactionExecutor(
                    engine: claimedDomain.engine
                ),
                root: root,
                transactionCapabilities: capabilities
            )
        }
        #if DATABASE_MULTIPLE_BASES
        let preparedTopology = DatabaseStorageRuntimeTopology(
            controlDomainID: storageTopology.controlDomainID,
            domains: preparedDomains,
            placements: storageTopology.placements,
            defaultPlacementID: storageTopology.defaultPlacementID
        )
        #else
        let preparedTopology = DatabaseStorageRuntimeTopology(
            controlDomainID: storageTopology.controlDomainID,
            domains: preparedDomains
        )
        #endif

        let resolvedTransactionCapabilities: TransactionCapabilities
        if let transactionCapabilities {
            resolvedTransactionCapabilities = transactionCapabilities
        } else {
            resolvedTransactionCapabilities = try await inspectTransactionCapabilities(
                storageEngine: storageEngine
            )
        }
        let expectedFormat = DatabaseFormatDescriptor.v1(
            itemStorage: configuration.itemStorage
        )
        let databaseDataRoot = DatabaseDataRootLease(
            resource: .database,
            domain: preparedTopology.controlDomain,
            root: preparedTopology.controlDomain.root,
            generation: 0
        )
        #if DATABASE_MULTIPLE_BASES
        let layoutCatalog = DatabaseLayoutCatalog(
            engine: storageEngine,
            controlRoot: preparedTopology.controlDomain.root,
            clock: configuration.monotonicClock
        )
        let existingLayoutStatus = try await layoutCatalog.load()
        let initialLayoutStatus: DatabaseLayoutStatus
        if let existingLayoutStatus {
            initialLayoutStatus = existingLayoutStatus
        } else if let legacyFormat = try await layoutCatalog.legacyFormat() {
            guard legacyFormat == expectedFormat else {
                throw DatabaseFormatCatalogError.descriptorMismatch(
                    stored: legacyFormat,
                    expected: expectedFormat
                )
            }
            guard !preparedTopology.controlDomain.root.prefix.isEmpty else {
                throw DatabaseRuntimeError.internalError(
                    "Legacy migration requires a distinct non-empty control namespace"
                )
            }
            initialLayoutStatus = .migrationRequired
        } else {
            initialLayoutStatus = .current
        }
        #endif
        let databaseGrantStore = DatabaseGrantStore(
            resource: .database,
            root: preparedTopology.controlDomain.root
        )
        let bootstrapMarkerKey = preparedTopology.controlDomain.root
            .subspace("_metadata")
            .pack(Tuple("security-bootstrap-v1"))
        let persistedFormat = try await DatabaseFormatCatalog(
            database: storageEngine,
            root: preparedTopology.controlDomain.root,
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
                        ),
                    ],
                    transaction: transaction
                )
                try transaction.setValue(
                    Tuple(UInt64(1)).pack(),
                    for: bootstrapMarkerKey
                )
                #if DATABASE_MULTIPLE_BASES
                try await layoutCatalog.storeInitial(
                    initialLayoutStatus,
                    transaction: transaction
                )
                #endif
            }
        )
        #if DATABASE_MULTIPLE_BASES
        guard let persistedLayoutStatus = try await layoutCatalog.load() else {
            throw DatabaseRuntimeError.internalError(
                "Base layout marker is missing"
            )
        }
        guard persistedLayoutStatus == initialLayoutStatus else {
            throw DatabaseRuntimeError.internalError(
                "Base layout marker changed during storage preparation"
            )
        }
        let baseCatalog = DatabaseBaseCatalog(
            controlDomain: preparedTopology.controlDomain,
            clock: configuration.monotonicClock
        )
        let compositionCatalog = DatabaseCompositionCatalog(
            controlDomain: preparedTopology.controlDomain,
            clock: configuration.monotonicClock
        )
        let baseRecords = try await baseCatalog.loadAll()
        var baseGenerations: [DatabaseBaseGeneration] = []
        baseGenerations.reserveCapacity(baseRecords.count)
        for record in baseRecords {
            switch record.lifecycle {
            case .provisioning, .tombstone:
                continue
            case .active, .retiring, .retired, .moving, .deleting:
                guard let domain = preparedTopology.domain(
                    identifiedBy: record.domainID
                ) else {
                    throw DatabaseBaseCatalogError.storageDomainNotFound(
                        record.domainID
                    )
                }
                let root: Subspace
                do {
                    root = try await domain.engine.resolveExistingNamespace(
                        path: record.namespacePath
                    )
                } catch {
                    throw DatabaseBaseCatalogError.corruptedRecord(record.id)
                }
                baseGenerations.append(
                    DatabaseBaseGeneration(
                        record: record,
                        domain: domain,
                        root: root
                    )
                )
            }
        }
        #endif
        let metadataSubspace = preparedTopology.controlDomain.root
            .subspace("_metadata")
        let schemaGeneration = try await loadSchemaGeneration(
            storageEngine: storageEngine,
            metadataSubspace: metadataSubspace,
            clock: configuration.monotonicClock
        )
        #if DATABASE_MULTIPLE_BASES
        return PreparedStorage(
            topology: preparedTopology,
            engine: storageEngine,
            format: persistedFormat,
            databaseDataRoot: databaseDataRoot,
            baseCatalog: baseCatalog,
            compositionCatalog: compositionCatalog,
            layoutCatalog: layoutCatalog,
            layoutStatus: persistedLayoutStatus,
            baseGenerations: baseGenerations,
            databaseGrantStore: databaseGrantStore,
            metadataSubspace: metadataSubspace,
            schemaGeneration: schemaGeneration,
            transactionCapabilities: resolvedTransactionCapabilities
        )
        #else
        return PreparedStorage(
            topology: preparedTopology,
            engine: storageEngine,
            format: persistedFormat,
            databaseDataRoot: databaseDataRoot,
            databaseGrantStore: databaseGrantStore,
            metadataSubspace: metadataSubspace,
            schemaGeneration: schemaGeneration,
            transactionCapabilities: resolvedTransactionCapabilities
        )
        #endif
    }

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
        schemaGeneration: UInt64?,
        configuration: DBConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration,
        preparedStorage: PreparedStorage
    ) {
        self.controlEngine = preparedStorage.engine
        self.controlDomainID = preparedStorage.topology.controlDomainID
        self.storageTopology = preparedStorage.topology
        self.controlTransactionExecutor = StorageTransactionExecutor(
            engine: preparedStorage.engine
        )
        self.controlTransactionCapabilities =
            preparedStorage.transactionCapabilities
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
                schema: schema,
                runtimeConfiguration: runtimeConfiguration,
                securityDelegate: securityDelegate
            )
        )
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
        self.databaseDataRoot = preparedStorage.databaseDataRoot
        self.databaseGrantStore = preparedStorage.databaseGrantStore
        #if DATABASE_MULTIPLE_BASES
        self.baseCatalog = preparedStorage.baseCatalog
        self.compositionCatalog = preparedStorage.compositionCatalog
        self.layoutCatalog = preparedStorage.layoutCatalog
        self.layoutStatusStorage = Mutex(preparedStorage.layoutStatus)
        self.baseGenerationStore = DatabaseBaseGenerationStore(
            generations: preparedStorage.baseGenerations
        )
        #endif
        self.metadataSubspace = preparedStorage.metadataSubspace
    }

    private var activeSchemaLease: DatabaseSchemaLease {
        DatabaseSchemaExecutionScope.lease ?? schemaGenerationStore.acquire()
    }

    #if DATABASE_MULTIPLE_BASES
    private func prepareTestingBaseIfConfigured() async throws {
        guard layoutStatus == .current else { return }
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
                ),
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
        root: Subspace
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
                root: root
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
        try await ActiveDatabaseDataRootContext.$lease.withValue(
            databaseDataRoot
        ) {
            try await operation()
        }
    }

    package func requireActiveDataRoot() throws -> DatabaseDataRootLease {
        guard let lease = ActiveDatabaseDataRootContext.lease else {
            throw DatabaseRuntimeError.internalError(
                "A target-bound data root is required"
            )
        }
        return lease
    }

    /// Storage root for operation metadata that must commit atomically with
    /// the selected target. Base operation state lives beside Base data;
    /// database operation state remains in the control domain.
    package func operationStateRoot(
        for target: DatabaseOperationTarget
    ) throws -> Subspace {
        switch target {
        case .database:
            return metadataSubspace.subspace("operation-state")
        case .base(let baseID):
            #if DATABASE_MULTIPLE_BASES
            let lease = try requireBoundBaseLease()
            guard lease.baseID == baseID else {
                throw DatabaseBaseExecutionError.baseTargetRequired
            }
            return lease.root.subspace("operation-state")
            #else
            _ = baseID
            throw DatabaseRuntimeError.internalError(
                "MultipleBases is not enabled"
            )
            #endif
        case .composition:
            throw DatabaseRuntimeError.internalError(
                "A Composition cannot own operation state"
            )
        }
    }

    package func activeDataSubspace(
        relativePath: [String]
    ) throws -> Subspace {
        var subspace = try requireActiveDataRoot().root
            .subspace("data")
        for component in relativePath {
            subspace = subspace.subspace(component)
        }
        return subspace
    }

    private func activePartitionCatalog() throws -> DatabasePartitionCatalog {
        let lease = try requireActiveDataRoot()
        return DatabasePartitionCatalog(
            engine: lease.domain.engine,
            root: lease.root.subspace("data"),
            clock: monotonicClock
        )
    }

    /// Returns the latest atomically published generation even when the
    /// caller is executing under an older request lease. Schema coordination
    /// uses this boundary to serialize publications against current state;
    /// ordinary request execution must continue to use `activeSchemaLease`.
    package func acquirePublishedSchemaLease() -> DatabaseSchemaLease {
        schemaGenerationStore.acquire()
    }

    /// Acquires one immutable schema generation and binds it to all container
    /// reads performed by `operation` until that asynchronous operation ends.
    public func withSchemaLease<Result: Sendable>(
        _ operation: @Sendable (DatabaseSchemaLease) async throws -> Result
    ) async rethrows -> Result {
        let lease = schemaGenerationStore.acquire()
        return try await DatabaseSchemaExecutionScope.$lease.withValue(lease) {
            try await operation(lease)
        }
    }

    /// Retains an enclosing request generation or acquires one for a local
    /// data operation that does not already have a request scope.
    package func withOperationSchemaLease<Result: Sendable>(
        _ operation: @Sendable (DatabaseSchemaLease) async throws -> Result
    ) async rethrows -> Result {
        if let lease = DatabaseSchemaExecutionScope.lease {
            return try await operation(lease)
        }
        return try await withSchemaLease(operation)
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
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        generation: UInt64
    ) {
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
                schema: schema,
                runtimeConfiguration: runtimeConfiguration,
                securityDelegate: securityDelegate
            )
        )
    }

    /// Releases the storage engine owned by this container.
    ///
    /// Shutdown is thread-safe and idempotent. It rejects new operations, waits
    /// for admitted operations to finish, and then releases the storage engine.
    public func shutdown() async {
        await configuration.shutdownStorageTopology()
    }

    deinit {
        configuration.requestStorageTopologyShutdown()
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
            let subspace = try await resolveDirectory(for: entity)
            let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
            let indexNames = entity.indexDescriptors.map { $0.name }
            try await transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
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
            let subspace = try await resolvePolymorphicDirectory(for: group.identifier)
            let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
            let indexNames = group.indexes.map { $0.name }
            try await lifecycleStore.ensureReadable(
                indexNames,
                entityRange: subspace.subspace(SubspaceKey.items).range()
            )
        }
    }

    #if DATABASE_MULTIPLE_BASES
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
        DatabaseContext(
            container: self,
            resource: .database,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
    }

    package func makeActiveDataContext(
        authorization: AuthorizationContext,
        autosaveEnabled: Bool = false
    ) throws -> DatabaseContext {
        let resource = try requireActiveDataRoot().resource
        return DatabaseContext(
            container: self,
            resource: resource,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
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
        return try await lease.transactionExecutor.withTransaction(
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

    package func resolveDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        try await resolveDirectory(
            for: entity,
            declaredIn: schema,
            path: path,
            transaction: transaction
        )
    }

    package func resolveDirectory(
        for candidate: Schema.Entity,
        declaredIn authoritySchema: Schema,
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

        let subspace = try activeDataSubspace(
            relativePath: directoryPath.resolve()
        )

        let partitions = directoryPath.canonicalPartitions()
        if !partitions.isEmpty {
            try await activePartitionCatalog().register(
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
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let entity = try canonicalEntity(entity)
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        return try activeDataSubspace(relativePath: directoryPath.resolve())
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
        transaction: any TransactionAccess
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
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        let entity = try canonicalEntity(entity)
        let directoryPath: AnyDirectoryPath
        if let path {
            directoryPath = path
        } else {
            directoryPath = try AnyDirectoryPath(for: entity)
        }
        try directoryPath.validate()
        let components = directoryPath.resolve()
        let partitions = directoryPath.canonicalPartitions()
        if entity.hasDynamicDirectory {
            guard try await activePartitionCatalog().contains(
                entity: entity.name,
                partitions: partitions,
                transaction: transaction
            ) else {
                return nil
            }
        }
        let subspace = try activeDataSubspace(relativePath: components)
        let lifecycleStore = IndexLifecycleStore(
            container: self,
            subspace: subspace
        )
        try await lifecycleStore.validateReadableForRead(
            [indexName],
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
        return try await activePartitionCatalog().page(
            entity: entity,
            continuation: continuation,
            limit: limit
        )
    }

    package func partitionCatalogPage(
        entity: String,
        continuation: ByteString? = nil,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> DatabasePartitionCatalogPage {
        try await activePartitionCatalog().page(
            entity: entity,
            continuation: continuation,
            limit: limit,
            transaction: transaction
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
        let entity = try schemaEntity(named: T.persistableType)
        let subspace = try await resolveDirectory(for: type, path: path)
        try await initializeIndexStates(for: entity, subspace: subspace)
        return DatabaseDataStore(
            container: self,
            subspace: subspace,
            entity: entity,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
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

    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> DatabaseDataStore {
        let entity = try canonicalEntity(entity)
        let subspace = try await resolveDirectory(for: entity, path: path)
        try await initializeIndexStates(for: entity, subspace: subspace)
        return DatabaseDataStore(
            container: self,
            subspace: subspace,
            entity: entity,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
    }

    /// Build a store whose directory and index state participate in the
    /// caller's transaction.
    internal func store(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseDataStore {
        let entity = try canonicalEntity(entity)
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
            entity: entity,
            securityDelegate: securityDelegate,
            indexConfigurations: indexConfigurations.values.flatMap { $0 }
        )
    }

    private func initializeIndexStates(
        for entity: Schema.Entity,
        subspace: Subspace
    ) async throws {
        let indexNames = entity.indexDescriptors.map { $0.name }
        guard !indexNames.isEmpty else { return }

        let lifecycleStore = IndexLifecycleStore(container: self, subspace: subspace)
        try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let pending = try await self.pendingSchemaIndexBuilds(
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

        return try activeDataSubspace(relativePath: path)
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
    package func resolvePolymorphicDirectory(for identifier: String) async throws -> Subspace {
        let group = try polymorphicGroup(identifier: identifier)
        let path = try group.resolvedDirectoryPath()
        return try activeDataSubspace(relativePath: path)
    }

    /// Resolves a polymorphic projection directory in the caller-owned
    /// transaction so namespace creation and projected writes commit atomically.
    package func resolvePolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let group = try polymorphicGroup(identifier: identifier)
        return try activeDataSubspace(
            relativePath: group.resolvedDirectoryPath()
        )
    }

    /// Opens an existing polymorphic projection without mutating namespace
    /// metadata. An absent directory represents an empty projection.
    package func openPolymorphicDirectory(
        for identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Subspace? {
        let group = try polymorphicGroup(identifier: identifier)
        return try activeDataSubspace(
            relativePath: group.resolvedDirectoryPath()
        )
    }

    /// Opens and admits one exact polymorphic index without creating metadata.
    ///
    /// `nil` means the polymorphic namespace has never existed. Every existing
    /// namespace must have a readable lifecycle state for the selected index.
    package func readablePolymorphicIndex(
        _ descriptor: PolymorphicIndexMetadata,
        in group: PolymorphicGroup,
        transaction: any TransactionAccess
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
            subspace: subspace
                .subspace(SubspaceKey.indexes)
                .subspace(descriptor.name)
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
    /// Returns schema-version metadata for the currently bound Base.
    /// Global schema catalog metadata remains in the control domain and is
    /// never used as a Base migration checkpoint.
    private func getMetadataSubspace() async throws -> Subspace {
        try requireActiveDataRoot().root.subspace("metadata")
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
        let metadataSubspace = try requireActiveDataRoot().root
            .subspace("metadata")
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
        try await withActiveDataRootTransaction(
            requiredAccess: .administer,
            configuration: .batch,
        ) { transaction in
            try Self.setCurrentSchemaSnapshot(
                installedSchema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
        }
    }

    package static func setCurrentSchemaSnapshot(
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
                  let component = UInt32(exactly: value) else {
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
    #if DATABASE_MULTIPLE_BASES
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
            await configuration.shutdownStorageTopologyIfUnclaimed()
            throw error
        }
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
        let schemaInstance: Schema
        do {
            try P.validate()
            schemaInstance = try S.makeSchema()
        } catch {
            await configuration.shutdownStorageTopologyIfUnclaimed()
            throw error
        }
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
    package func migrationStatus(
        targetVersion requestedTarget: Schema.Version? = nil
    ) async throws -> DatabaseMigrationStatus {
        let targetVersion = try migrationTarget(requestedTarget)
        return try await withActiveDataRootTransaction(
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
    package func runMigrations(
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
    /// has reached that exact version. Base migration checkpoints remain in
    /// their data domains; the discoverable schema remains database-wide.
    private func publishSchemaCatalogIfAllActiveBasesMatch(
        _ candidateSchema: Schema
    )
        async throws
    {
        #if DATABASE_MULTIPLE_BASES
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
            let currentVersion = try await withBaseLease(lease) {
                try await self.getCurrentSchemaVersion()
            }
            guard currentVersion == candidateSchema.version else {
                return
            }
        }
        #endif
        let registry = SchemaRegistry(
            database: controlEngine,
            root: storageTopology.controlDomain.root,
            clock: monotonicClock
        )
        let targetFingerprint = try SchemaManifest(schema: candidateSchema)
            .fingerprint()
        let nextGeneration = try await controlTransactionExecutor
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

                if persistedEntities.isEmpty,
                   persistedVersion == nil,
                   persistedFingerprint == nil,
                   persistedGeneration == nil {
                    try await registry.persistInitialSchema(
                        candidateSchema,
                        transaction: transaction
                    )
                    try Self.setCurrentSchemaSnapshot(
                        candidateSchema,
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

                guard let persistedVersion,
                      let persistedFingerprint,
                      let persistedGeneration else {
                    throw DatabaseSchemaRestorationError.missingFingerprint
                }
                let persistedSchema = try Schema(
                    entities: persistedEntities,
                    version: persistedVersion
                )
                guard try SchemaManifest(schema: persistedSchema)
                    .fingerprint() == persistedFingerprint else {
                    throw DatabaseSchemaRestorationError.fingerprintMismatch
                }
                if persistedFingerprint == targetFingerprint {
                    return persistedGeneration
                }

                let incremented = persistedGeneration
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
            publishSchemaGeneration(
                schema,
                fingerprint: targetFingerprint,
                runtimeConfiguration: runtimeConfiguration,
                generation: nextGeneration
            )
        }
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
        let metadataSubspace = try requireActiveDataRoot().root
            .subspace("metadata")
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
            guard runtimeConfiguration.entityRuntimes.registration(
                named: entity.name
            ) != nil else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledEntityType(entityName: entity.name)
            }
            let subspace = try await resolveDirectory(for: entity)
            staticStores.append((
                entity: entity.name,
                range: subspace
                    .subspace(SubspaceKey.items)
                    .subspace(entity.name)
                    .range(),
                lifecycleStore: IndexLifecycleStore(
                    container: self,
                    subspace: subspace
                ),
                indexNames: entity.indexDescriptors.map { $0.name }
            ))
        }
        let stores = staticStores

        let bootstrap: @Sendable (any TransactionAccess) async throws -> Bool = {
            transaction in
            guard try await transaction.getValue(
                for: versionKey,
                snapshot: false
            ) == nil else {
                return false
            }
            for store in stores {
                let rows = try await TransactionRangeCollection.collect(using: transaction,
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
            try Self.setCurrentSchemaSnapshot(
                self.schema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
            return true
        }
        switch authority {
        case .request:
            return try await withActiveDataRootTransaction(
                requiredAccess: .administer,
                configuration: .batch,
                bootstrap
            )
        #if DATABASE_MULTIPLE_BASES
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

    #if DATABASE_MULTIPLE_BASES
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
        let stageMarkerKey = Self.migrationStageMarkerKey(
            metadataSubspace: metadataSubspace
        )
        let expectedFrom = stage.fromVersionIdentifier
        let expectedTo = stage.toVersionIdentifier
        try await withActiveDataRootTransaction(
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

        try await withActiveDataRootTransaction(
            requiredAccess: .administer,
            configuration: .batch
        ) { transaction in
            try Self.setCurrentSchemaSnapshot(
                targetSchema,
                metadataSubspace: metadataSubspace,
                transaction: transaction
            )
            try transaction.clear(key: stageMarkerKey)
        }
        try await publishSchemaCatalogIfAllActiveBasesMatch(targetSchema)
        logger.info("Updated schema version to \(stage.toVersionIdentifier)")
    }

    private func buildStoreRegistry(for schema: Schema) async throws -> [String: MigrationStoreInfo] {
        var registry: [String: MigrationStoreInfo] = [:]

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
            let subspace = try await withActiveDataRootTransaction(
                requiredAccess: .administer,
                configuration: .default
            ) { transaction in
                try await self.resolveDirectory(
                    for: entity,
                    declaredIn: schema,
                    transaction: transaction
                )
            }
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

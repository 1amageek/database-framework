import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseKit
import StorageKit

/// Immutable application composition consumed by a storage-owning host.
public struct DatabaseContainerDefinition: Sendable {
    /// A statically compiled schema, or `nil` when storage owns the schema.
    public let declaredSchema: Schema?
    public let security: SecurityConfiguration
    public let databaseName: String?
    public let monotonicClock: any StorageMonotonicClock
    public let wallClock: any WallClock
    public let indexConfigurations: [any IndexRuntimeConfiguration]
    public let itemStorage: ItemStorageConfiguration
    public let logging: DatabaseLoggingConfiguration
    public let metrics: DatabaseMetricsConfiguration

    private let openContainer: @Sendable (
        DatabaseStorageTopology
    ) async throws -> DBContainer

    /// Defines a container backed by a statically compiled schema.
    public init(
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        databaseName: String? = nil,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.declaredSchema = schema
        self.security = security
        self.databaseName = databaseName
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.indexConfigurations = indexConfigurations
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.openContainer = { storageTopology in
            try await DBContainer.open(
                for: schema,
                configuration: DBConfiguration(
                    name: databaseName,
                    storageTopology: storageTopology,
                    monotonicClock: monotonicClock,
                    wallClock: wallClock,
                    indexConfigurations: indexConfigurations,
                    itemStorage: itemStorage,
                    logging: logging,
                    metrics: metrics
                ),
                runtimeConfiguration: runtimeConfiguration,
                security: security
            )
        }
    }

    /// Defines a compiled schema with an application-owned migration plan.
    public init<MigrationPlan: SchemaMigrationPlan>(
        schema: Schema,
        migrationPlan: MigrationPlan.Type,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        databaseName: String? = nil,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.declaredSchema = schema
        self.security = security
        self.databaseName = databaseName
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.indexConfigurations = indexConfigurations
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.openContainer = { storageTopology in
            try await DBContainer.open(
                for: schema,
                migrationPlan: migrationPlan,
                configuration: DBConfiguration(
                    name: databaseName,
                    storageTopology: storageTopology,
                    monotonicClock: monotonicClock,
                    wallClock: wallClock,
                    indexConfigurations: indexConfigurations,
                    itemStorage: itemStorage,
                    logging: logging,
                    metrics: metrics
                ),
                runtimeConfiguration: runtimeConfiguration,
                security: security
            )
        }
    }

    /// Defines a schema-driven container restored from its durable catalog.
    public init(
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory,
        security: SecurityConfiguration = .enabled(),
        databaseName: String? = nil,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.declaredSchema = nil
        self.security = security
        self.databaseName = databaseName
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.indexConfigurations = indexConfigurations
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.openContainer = { storageTopology in
            try await DBContainer.openRestoringSchema(
                configuration: DBConfiguration(
                    name: databaseName,
                    storageTopology: storageTopology,
                    monotonicClock: monotonicClock,
                    wallClock: wallClock,
                    indexConfigurations: indexConfigurations,
                    itemStorage: itemStorage,
                    logging: logging,
                    metrics: metrics
                ),
                security: security
            ) { schema in
                try await schemaRuntimeFactory.makeOperationConfiguration(
                    for: schema
                )
            }
        }
    }

    public var isSchemaDriven: Bool {
        declaredSchema == nil
    }

    /// Opens the single-use definition with the host-selected storage topology.
    public func open(
        storageTopology: DatabaseStorageTopology
    ) async throws -> DBContainer {
        try await openContainer(storageTopology)
    }
}

import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseOperationServiceContext: Sendable {
    public let container: DBContainer
    public let stateStore: DatabaseMutationStateStore
    public let coordinator: DatabaseTransactionalOperationCoordinator
    public let runtimeLimits: DatabaseOperationLimits
    public let wireLimits: DatabaseWireLimits
    public let clock: AnyDatabaseWallClock
    public let hostServices: DatabaseOperationHostServices
    public let schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory?
    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif

    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseOperationLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        hostServices: DatabaseOperationHostServices = .none
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
        self.schemaRuntimeFactory = schemaRuntimeFactory
        self.hostServices = hostServices
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseOperationLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        graphOperationLimits: GraphOperationLimits,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        hostServices: DatabaseOperationHostServices = .none
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
        self.schemaRuntimeFactory = schemaRuntimeFactory
        self.graphOperationLimits = graphOperationLimits
        self.hostServices = hostServices
    }
    #endif

    public func withHostServices(
        _ hostServices: DatabaseOperationHostServices
    ) -> DatabaseOperationServiceContext {
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        DatabaseOperationServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: runtimeLimits,
            wireLimits: wireLimits,
            clock: clock,
            graphOperationLimits: graphOperationLimits,
            schemaRuntimeFactory: schemaRuntimeFactory,
            hostServices: hostServices
        )
        #else
        DatabaseOperationServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: runtimeLimits,
            wireLimits: wireLimits,
            clock: clock,
            schemaRuntimeFactory: schemaRuntimeFactory,
            hostServices: hostServices
        )
        #endif
    }
}

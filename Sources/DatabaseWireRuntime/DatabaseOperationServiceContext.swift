import DatabaseEngine
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseOperationServiceContext: Sendable {
    public let container: DBContainer
    public let stateStore: DatabaseMutationStateStore
    public let coordinator: DatabaseTransactionalOperationCoordinator
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let clock: AnyDatabaseWallClock
    public let hostServices: DatabaseHostServices
    public let schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory?
    #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif

    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        hostServices: DatabaseHostServices = .none
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
        self.schemaRuntimeFactory = schemaRuntimeFactory
        self.hostServices = hostServices
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        graphOperationLimits: GraphOperationLimits,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        hostServices: DatabaseHostServices = .none
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
        _ hostServices: DatabaseHostServices
    ) -> DatabaseOperationServiceContext {
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
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

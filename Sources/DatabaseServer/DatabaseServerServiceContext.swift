import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseServerServiceContext: Sendable {
    public let container: DBContainer
    public let stateStore: DatabaseMutationStateStore
    public let coordinator: DatabaseTransactionalOperationCoordinator
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let clock: AnyDatabaseWallClock
    public let hostServices: DatabaseServerHostServices
    #if DATABASE_SERVER_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif

    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        hostServices: DatabaseServerHostServices = .none
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
        self.hostServices = hostServices
        #if DATABASE_SERVER_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_SERVER_GRAPH_INDEXES
    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock,
        graphOperationLimits: GraphOperationLimits,
        hostServices: DatabaseServerHostServices = .none
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
        self.graphOperationLimits = graphOperationLimits
        self.hostServices = hostServices
    }
    #endif

    public func withHostServices(
        _ hostServices: DatabaseServerHostServices
    ) -> DatabaseServerServiceContext {
        #if DATABASE_SERVER_GRAPH_INDEXES
        DatabaseServerServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: runtimeLimits,
            wireLimits: wireLimits,
            clock: clock,
            graphOperationLimits: graphOperationLimits,
            hostServices: hostServices
        )
        #else
        DatabaseServerServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: runtimeLimits,
            wireLimits: wireLimits,
            clock: clock,
            hostServices: hostServices
        )
        #endif
    }
}

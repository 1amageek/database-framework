import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseServerServiceContext: Sendable {
    public let container: DBContainer
    public let stateStore: DatabaseMutationStateStore
    public let coordinator: DatabaseTransactionalOperationCoordinator
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let clock: AnyDatabaseWallClock

    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits,
        wireLimits: DatabaseWireLimits,
        clock: AnyDatabaseWallClock
    ) {
        self.container = container
        self.stateStore = stateStore
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.clock = clock
    }
}

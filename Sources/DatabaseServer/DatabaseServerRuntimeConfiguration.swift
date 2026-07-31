import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

public final class DatabaseServerRuntimeConfiguration: Sendable {
    public let identity: DatabaseRuntimeIdentity
    public let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    public let middlewares: [AnyDatabaseRequestMiddleware]
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let errorMapper: AnyDatabaseErrorMapper
    public let clock: AnyDatabaseWallClock
    #if DATABASE_SERVER_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif
    private let serviceFactory: AnyDatabaseServerServiceFactory

    public init<Clock: WallClock>(
        identity: DatabaseRuntimeIdentity,
        serviceFactory: AnyDatabaseServerServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        clock: Clock,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default,
        errorMapper: AnyDatabaseErrorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    ) throws(DatabaseRuntimeConfigurationError) {
        try runtimeLimits.validateConfiguration()
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.errorMapper = errorMapper
        self.clock = AnyDatabaseWallClock(clock)
        #if DATABASE_SERVER_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_SERVER_GRAPH_INDEXES
    public init<Clock: WallClock>(
        identity: DatabaseRuntimeIdentity,
        serviceFactory: AnyDatabaseServerServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        clock: Clock,
        graphOperationLimits: GraphOperationLimits,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default,
        errorMapper: AnyDatabaseErrorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    ) throws(DatabaseRuntimeConfigurationError) {
        try runtimeLimits.validateConfiguration()
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.errorMapper = errorMapper
        self.clock = AnyDatabaseWallClock(clock)
        self.graphOperationLimits = graphOperationLimits
    }
    #endif

    public func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
        try await serviceFactory.makeServices(context: context)
    }
}

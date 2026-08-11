import DatabaseEngine
@_spi(DatabaseWireRuntime) import DatabaseWire

public final class DatabaseOperationRuntimeConfiguration: Sendable {
    public let identity: DatabaseRuntimeIdentity
    public let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    public let middlewares: [AnyDatabaseRequestMiddleware]
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let errorMapper: AnyDatabaseErrorMapper
    public let clock: AnyDatabaseWallClock
    public let schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory?
    #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif
    private let serviceFactory: AnyDatabaseOperationServiceFactory

    public init<Clock: WallClock>(
        identity: DatabaseRuntimeIdentity,
        serviceFactory: AnyDatabaseOperationServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        clock: Clock,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
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
        self.schemaRuntimeFactory = schemaRuntimeFactory
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
    public init<Clock: WallClock>(
        identity: DatabaseRuntimeIdentity,
        serviceFactory: AnyDatabaseOperationServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        clock: Clock,
        graphOperationLimits: GraphOperationLimits,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
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
        self.schemaRuntimeFactory = schemaRuntimeFactory
        self.graphOperationLimits = graphOperationLimits
    }
    #endif

    public func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        try await serviceFactory.makeServices(context: context)
    }
}

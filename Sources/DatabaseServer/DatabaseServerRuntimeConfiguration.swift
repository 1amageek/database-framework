import DatabaseWire

public final class DatabaseServerRuntimeConfiguration: Sendable {
    public let identity: DatabaseRuntimeIdentity
    public let authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy
    public let middlewares: [AnyDatabaseRequestMiddleware]
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let errorMapper: AnyDatabaseErrorMapper
    private let serviceFactory: AnyDatabaseServerServiceFactory

    public init(
        identity: DatabaseRuntimeIdentity,
        serviceFactory: AnyDatabaseServerServiceFactory,
        authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default,
        errorMapper: AnyDatabaseErrorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    ) {
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.authorizationPolicy = authorizationPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.errorMapper = errorMapper
    }

    public func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
        try await serviceFactory.makeServices(context: context)
    }
}

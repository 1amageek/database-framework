@_spi(DatabaseServer) import DatabaseWire

public final class DatabaseServerRuntimeConfiguration: Sendable {
    public let identity: DatabaseRuntimeIdentity
    public let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    public let middlewares: [AnyDatabaseRequestMiddleware]
    public let runtimeLimits: DatabaseRuntimeLimits
    public let wireLimits: DatabaseWireLimits
    public let errorMapper: AnyDatabaseErrorMapper
    public let clock: AnyDatabaseWallClock
    private let serviceFactory: AnyDatabaseServerServiceFactory

    public init<Clock: DatabaseWallClock>(
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
    ) {
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
        self.errorMapper = errorMapper
        self.clock = AnyDatabaseWallClock(clock)
    }

    public func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
        try await serviceFactory.makeServices(context: context)
    }
}

/// Type-erased maintenance service factory for runtime composition.
public final class AnyDatabaseMaintenanceServiceFactory: Sendable {
    private let createMaintenanceService: @Sendable (
        DatabaseServerServiceContext
    ) async throws -> AnyDatabaseMaintenanceService

    public init<Factory: DatabaseMaintenanceServiceFactory>(_ factory: Factory) {
        self.createMaintenanceService = { context in
            try await factory.makeMaintenanceService(context: context)
        }
    }

    public func makeMaintenanceService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseMaintenanceService {
        try await createMaintenanceService(context)
    }
}

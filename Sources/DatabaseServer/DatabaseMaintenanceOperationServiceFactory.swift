public final class DatabaseMaintenanceOperationServiceFactory:
    DatabaseMaintenanceServiceFactory,
    Sendable {
    private let identifierGenerator: AnyDatabaseUUIDGenerator

    public init<IdentifierGenerator: DatabaseUUIDGenerator>(
        identifierGenerator: IdentifierGenerator
    ) {
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
    }

    public func makeMaintenanceService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseMaintenanceService {
        AnyDatabaseMaintenanceService(
            DatabaseMaintenanceOperationService(
                context: context,
                identifierGenerator: identifierGenerator
            )
        )
    }
}

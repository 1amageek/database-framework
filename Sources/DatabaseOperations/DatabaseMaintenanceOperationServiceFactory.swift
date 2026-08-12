import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine

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
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseMaintenanceService {
        AnyDatabaseMaintenanceService(
            DatabaseMaintenanceOperationService(
                context: context,
                identifierGenerator: identifierGenerator
            )
        )
    }
}

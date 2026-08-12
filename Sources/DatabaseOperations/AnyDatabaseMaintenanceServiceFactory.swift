import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
/// Type-erased maintenance service factory for runtime composition.
public final class AnyDatabaseMaintenanceServiceFactory: Sendable {
    private let createMaintenanceService: @Sendable (
        DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseMaintenanceService

    public init<Factory: DatabaseMaintenanceServiceFactory>(_ factory: Factory) {
        self.createMaintenanceService = { context in
            try await factory.makeMaintenanceService(context: context)
        }
    }

    public func makeMaintenanceService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseMaintenanceService {
        try await createMaintenanceService(context)
    }
}

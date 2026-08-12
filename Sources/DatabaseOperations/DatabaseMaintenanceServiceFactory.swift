import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
public protocol DatabaseMaintenanceServiceFactory: Sendable {
    func makeMaintenanceService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseMaintenanceService
}

import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
public protocol DatabaseJobServiceFactory: Sendable {
    func makeJobService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseJobService
}

import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
public protocol DatabaseOperationServiceFactory: AnyObject, Sendable {
    func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices
}

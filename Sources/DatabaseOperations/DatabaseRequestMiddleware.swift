import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public typealias DatabaseRequestHandler = @Sendable (
    DatabaseWireRequestEnvelope,
    DatabaseOperationContext
) async throws -> DatabaseOperationResult

public protocol DatabaseRequestMiddleware: Sendable {
    func handle(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext,
        next: DatabaseRequestHandler
    ) async throws -> DatabaseOperationResult
}

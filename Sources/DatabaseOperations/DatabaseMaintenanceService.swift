import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public protocol DatabaseMaintenanceService: Sendable {
    func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult
}

import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

/// Type-erased maintenance service for runtime composition.
public final class AnyDatabaseMaintenanceService:
    DatabaseMaintenanceService,
    Sendable {
    private let executeMaintenance: @Sendable (
        MaintenanceExecuteOperation.Request,
        DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult

    public init<Service: DatabaseMaintenanceService>(_ service: Service) {
        self.executeMaintenance = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult {
        try await executeMaintenance(request, context)
    }
}

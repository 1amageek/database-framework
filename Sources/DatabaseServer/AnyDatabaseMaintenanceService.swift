@_spi(DatabaseServer) import DatabaseWire

/// Type-erased maintenance service for runtime composition.
public final class AnyDatabaseMaintenanceService:
    DatabaseMaintenanceService,
    Sendable {
    private let executeMaintenance: @Sendable (
        MaintenanceExecuteOperation.Request,
        DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<
        MaintenanceExecuteOperation
    >

    public init<Service: DatabaseMaintenanceService>(_ service: Service) {
        self.executeMaintenance = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<
        MaintenanceExecuteOperation
    > {
        try await executeMaintenance(request, context)
    }
}

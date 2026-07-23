import DatabaseWire

public protocol DatabaseMaintenanceService: Sendable {
    func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<
        MaintenanceExecuteOperation
    >
}

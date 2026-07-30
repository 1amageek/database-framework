import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct MaintenanceExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = MaintenanceExecuteOperation

    private let service: AnyDatabaseMaintenanceService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseMaintenanceService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func invoke(
        request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        try runtimeLimits.validate(request.budget)
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.monotonicClock
        ) {
            try await service.execute(request, context: context)
                .operationResult
        }
    }
}

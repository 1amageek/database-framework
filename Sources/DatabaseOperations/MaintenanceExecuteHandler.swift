import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct MaintenanceExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = MaintenanceExecuteOperation

    private let service: AnyDatabaseMaintenanceService
    private let runtimeLimits: DatabaseOperationLimits

    public init(
        service: AnyDatabaseMaintenanceService,
        runtimeLimits: DatabaseOperationLimits = .default
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
            clock: context.executor.monotonicClock
        ) {
            try await service.execute(request, context: context)
                .operationResult
        }
    }
}

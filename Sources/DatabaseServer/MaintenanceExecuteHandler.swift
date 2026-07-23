import DatabaseValue
import DatabaseWire

public struct MaintenanceExecuteHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.maintenanceExecute

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
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            MaintenanceExecuteOperation.Request.self,
            from: payload,
            limits: limits
        )
        try runtimeLimits.validate(request.budget)
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.engine.monotonicClock
        ) {
            try await service.execute(request, context: context)
                .operationResult
        }
    }
}

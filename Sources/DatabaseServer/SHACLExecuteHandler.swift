#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct SHACLExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = SHACLExecuteOperation

    private let service: AnyDatabaseSHACLService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseSHACLService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func invoke(
        request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        try runtimeLimits.validate(request.budget)
        try validatePageLimit(request.page.limit, budget: request.budget)
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.monotonicClock
        ) {
            try await service.execute(request, context: context)
                .operationResult
        }
    }

    private func validatePageLimit(
        _ limit: UInt32,
        budget: ExecutionBudget
    ) throws {
        guard limit > 0, limit <= budget.maximumRows else {
            throw DatabaseRuntimeLimitError.invalidMaximumRows(
                requested: limit,
                maximum: budget.maximumRows
            )
        }
    }
}

#endif

import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct OntologyExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = OntologyExecuteOperation

    private let service: AnyDatabaseOntologyService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseOntologyService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func invoke(
        request: OntologyExecuteOperation.Request,
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

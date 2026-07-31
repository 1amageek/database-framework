#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

public struct GraphAlgorithmHandler: DatabaseOperationHandler {
    public typealias Operation = GraphAlgorithmOperation

    private let service: AnyDatabaseGraphAlgorithmService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseGraphAlgorithmService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func handle(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response {
        try runtimeLimits.validate(request.budget)
        try validatePageLimit(request.page.limit, budget: request.budget)
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.monotonicClock
        ) {
            try await service.execute(request, context: context)
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

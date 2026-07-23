import DatabaseValue
import DatabaseWire

public struct OntologyExecuteHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.ontologyExecute

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
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            OntologyExecuteOperation.Request.self,
            from: payload,
            limits: limits
        )
        try runtimeLimits.validate(request.budget)
        try validatePageLimit(request.page.limit, budget: request.budget)
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.engine.monotonicClock
        ) {
            try await service.execute(request, context: context)
                .operationResult
        }
    }

    private func validatePageLimit(
        _ limit: UInt32,
        budget: DatabaseExecutionBudget
    ) throws {
        guard limit > 0, limit <= budget.maximumRows else {
            throw DatabaseRuntimeLimitError.invalidMaximumRows(
                requested: limit,
                maximum: budget.maximumRows
            )
        }
    }
}

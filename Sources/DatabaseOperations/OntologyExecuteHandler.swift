import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct OntologyExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = OntologyExecuteOperation

    private let service: AnyDatabaseOntologyService
    private let runtimeLimits: DatabaseOperationLimits

    public init(
        service: AnyDatabaseOntologyService,
        runtimeLimits: DatabaseOperationLimits = .default
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
            clock: context.executor.monotonicClock
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
            throw DatabaseOperationLimitError.invalidMaximumRows(
                requested: limit,
                maximum: budget.maximumRows
            )
        }
    }
}

#endif

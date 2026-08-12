import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
@_spi(DatabaseOperations) import DatabaseWire

/// Type-erased graph algorithm service for runtime composition.
public final class AnyDatabaseGraphAlgorithmService:
    DatabaseGraphAlgorithmService,
    Sendable {
    private let executeAlgorithm: @Sendable (
        GraphAlgorithmOperation.Request,
        DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response

    public init<Service: DatabaseGraphAlgorithmService>(_ service: Service) {
        self.executeAlgorithm = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response {
        try await executeAlgorithm(request, context)
    }
}

#endif

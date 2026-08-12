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

/// Type-erased ontology service for runtime composition.
public final class AnyDatabaseOntologyService:
    DatabaseOntologyService,
    Sendable {
    private let executeOntology: @Sendable (
        OntologyExecuteOperation.Request,
        DatabaseOperationContext
    ) async throws -> OntologyExecutionResult

    public init<Service: DatabaseOntologyService>(_ service: Service) {
        self.executeOntology = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        try await executeOntology(request, context)
    }
}

#endif

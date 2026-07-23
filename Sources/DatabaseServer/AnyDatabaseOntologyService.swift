import DatabaseWire

/// Type-erased ontology service for runtime composition.
public final class AnyDatabaseOntologyService:
    DatabaseOntologyService,
    Sendable {
    private let executeOntology: @Sendable (
        OntologyExecuteOperation.Request,
        DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<OntologyExecuteOperation>

    public init<Service: DatabaseOntologyService>(_ service: Service) {
        self.executeOntology = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<OntologyExecuteOperation> {
        try await executeOntology(request, context)
    }
}

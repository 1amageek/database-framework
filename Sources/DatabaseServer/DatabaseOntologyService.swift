import DatabaseWire

public protocol DatabaseOntologyService: Sendable {
    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<OntologyExecuteOperation>
}

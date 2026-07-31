#if DATABASE_SERVER_GRAPH_INDEXES
@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseOntologyService: Sendable {
    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult
}

#endif

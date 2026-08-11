#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseOntologyService: Sendable {
    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult
}

#endif

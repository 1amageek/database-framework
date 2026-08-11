#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseGraphAlgorithmService: Sendable {
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response
}

#endif

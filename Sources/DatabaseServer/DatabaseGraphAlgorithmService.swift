#if DATABASE_SERVER_GRAPH_INDEXES
@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseGraphAlgorithmService: Sendable {
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response
}

#endif

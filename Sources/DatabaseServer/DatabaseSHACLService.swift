#if DATABASE_SERVER_GRAPH_INDEXES
@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseSHACLService: Sendable {
    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecutionResult
}

#endif

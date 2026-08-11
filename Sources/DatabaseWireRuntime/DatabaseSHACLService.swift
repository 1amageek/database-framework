#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseSHACLService: Sendable {
    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecutionResult
}

#endif

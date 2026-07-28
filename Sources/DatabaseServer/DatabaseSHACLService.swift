@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseSHACLService: Sendable {
    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecutionResult
}

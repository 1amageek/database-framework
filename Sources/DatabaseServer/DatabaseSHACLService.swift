import DatabaseWire

public protocol DatabaseSHACLService: Sendable {
    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation>
}

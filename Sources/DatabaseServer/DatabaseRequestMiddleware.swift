import DatabaseWire

public typealias DatabaseRequestHandler = @Sendable (
    DatabaseWireRequestEnvelope,
    DatabaseOperationContext
) async throws -> DatabaseOperationResult

public protocol DatabaseRequestMiddleware: Sendable {
    func handle(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext,
        next: DatabaseRequestHandler
    ) async throws -> DatabaseOperationResult
}

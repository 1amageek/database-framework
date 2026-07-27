@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseOperationEndpointHandler: Sendable {
    associatedtype Operation: ServerOperationDeclaration

    func invoke(
        request: Operation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult
}

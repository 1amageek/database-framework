@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseOperationEndpointHandler: Sendable {
    associatedtype Operation: ServerOperationDeclaration

    func invoke(
        request: Operation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult

    func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement
}

extension DatabaseOperationEndpointHandler {
    public func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement {
        _ = request
        return .canonical(for: Operation.operation.identifier)
    }
}

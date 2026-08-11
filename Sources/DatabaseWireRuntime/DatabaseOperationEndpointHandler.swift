@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseOperationEndpointHandler: Sendable {
    associatedtype Operation: DatabaseOperationDeclaration

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

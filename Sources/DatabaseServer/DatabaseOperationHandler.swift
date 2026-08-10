@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseOperationHandler: Sendable {
    associatedtype Operation: ServerOperationDeclaration

    func handle(
        _ request: Operation.Request,
        context: DatabaseOperationContext
    ) async throws -> Operation.Response

    func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement
}

extension DatabaseOperationHandler {
    public func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement {
        _ = request
        return .canonical(for: Operation.operation.identifier)
    }
}

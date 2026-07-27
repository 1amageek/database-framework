@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseOperationHandler: Sendable {
    associatedtype Operation: ServerOperationDeclaration

    func handle(
        _ request: Operation.Request,
        context: DatabaseOperationContext
    ) async throws -> Operation.Response
}

import DatabaseWire

public protocol DatabaseOperationHandler: Sendable {
    associatedtype Operation: DatabaseOperation

    func handle(
        _ request: Operation.Request,
        context: DatabaseOperationContext
    ) async throws -> Operation.Response
}

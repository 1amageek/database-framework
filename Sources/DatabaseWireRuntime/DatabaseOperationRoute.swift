@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseOperationRoute<Operation: DatabaseOperationDeclaration>:
    DatabaseOperationHandler,
    Sendable
{
    private let handleOperation: @Sendable (
        Operation.Request,
        DatabaseOperationContext
    ) async throws -> Operation.Response

    public init(
        _ handleOperation: @escaping @Sendable (
            Operation.Request,
            DatabaseOperationContext
        ) async throws -> Operation.Response
    ) {
        self.handleOperation = handleOperation
    }

    public func handle(
        _ request: Operation.Request,
        context: DatabaseOperationContext
    ) async throws -> Operation.Response {
        try await handleOperation(request, context)
    }
}

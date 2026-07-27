@_spi(DatabaseServer) import DatabaseWire

public struct AnyDatabaseOperationHandler: Sendable {
    public let identifier: DatabaseOperationIdentifier

    private let invokeOperation: @Sendable (
        DatabaseWireRequestEnvelope,
        DatabaseOperationContext,
        DatabaseWireLimits
    ) async throws -> DatabaseOperationResult

    public init<Handler: DatabaseOperationHandler>(_ handler: Handler) {
        self.identifier = Handler.Operation.operation.identifier
        self.invokeOperation = { envelope, context, limits in
            let request = try DatabaseWireDecoder(limits: limits).decodeRequest(
                Handler.Operation.operation,
                from: envelope
            )
            let response = try await handler.handle(request, context: context)
            return DatabaseOperationResult(
                Handler.Operation.self,
                response: response
            )
        }
    }

    public init<Handler: DatabaseOperationEndpointHandler>(_ handler: Handler) {
        self.identifier = Handler.Operation.operation.identifier
        self.invokeOperation = { envelope, context, limits in
            let request = try DatabaseWireDecoder(limits: limits).decodeRequest(
                Handler.Operation.operation,
                from: envelope
            )
            return try await handler.invoke(
                request: request,
                context: context,
                limits: limits
            )
        }
    }

    func invoke(
        envelope: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        try await invokeOperation(envelope, context, limits)
    }
}

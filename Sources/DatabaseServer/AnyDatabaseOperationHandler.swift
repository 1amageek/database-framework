import DatabaseValue
import DatabaseWire

public struct AnyDatabaseOperationHandler: Sendable {
    public let identifier: DatabaseOperationIdentifier

    private let invokeOperation: @Sendable (
        DatabaseBytes,
        DatabaseOperationContext,
        DatabaseWireLimits
    ) async throws -> DatabaseOperationResult

    public init<Handler: DatabaseOperationHandler>(_ handler: Handler) {
        self.identifier = Handler.Operation.identifier
        self.invokeOperation = { payload, context, limits in
            let request = try DatabaseEnvelopeCodec.decode(
                Handler.Operation.Request.self,
                from: payload,
                limits: limits
            )
            let response = try await handler.handle(request, context: context)
            return DatabaseOperationResult(
                operation: Handler.Operation.identifier,
                encoder: DatabaseOperationResponseEncoder(response)
            )
        }
    }

    public init<Handler: DatabaseOperationEndpointHandler>(_ handler: Handler) {
        self.identifier = handler.identifier
        self.invokeOperation = handler.invoke
    }

    func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        try await invokeOperation(payload, context, limits)
    }
}

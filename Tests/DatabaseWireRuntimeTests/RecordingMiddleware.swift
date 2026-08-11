import DatabaseWireRuntime
import DatabaseWire

actor RecordingMiddleware: DatabaseRequestMiddleware {
    private(set) var invocationCount = 0
    private(set) var traceID: String?
    private(set) var requestID: UInt64?

    func handle(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext,
        next: DatabaseRequestHandler
    ) async throws -> DatabaseOperationResult {
        invocationCount += 1
        traceID = context.metadata.traceID
        requestID = context.requestID
        return try await next(request, context)
    }
}

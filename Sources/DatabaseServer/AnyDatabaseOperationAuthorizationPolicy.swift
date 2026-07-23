import DatabaseWire

/// Type-erased database operation authorization policy.
public final class AnyDatabaseOperationAuthorizationPolicy:
    DatabaseOperationAuthorizationPolicy,
    Sendable {
    private let authorizeRequest: @Sendable (
        DatabaseWireRequestEnvelope,
        DatabaseOperationContext
    ) async throws -> Void

    public init<Policy: DatabaseOperationAuthorizationPolicy>(
        _ policy: Policy
    ) {
        self.authorizeRequest = { request, context in
            try await policy.authorize(
                request: request,
                context: context
            )
        }
    }

    public func authorize(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext
    ) async throws {
        try await authorizeRequest(request, context)
    }
}

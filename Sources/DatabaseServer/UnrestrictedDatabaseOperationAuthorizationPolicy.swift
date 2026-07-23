import DatabaseWire

/// Permits every valid database operation envelope.
public struct UnrestrictedDatabaseOperationAuthorizationPolicy:
    DatabaseOperationAuthorizationPolicy {
    public init() {}

    public func authorize(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext
    ) async throws {
        _ = request
        _ = context
    }
}

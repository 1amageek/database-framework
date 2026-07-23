import DatabaseWire

/// Authorizes a decoded database operation before request middleware or
/// operation handlers can observe it.
public protocol DatabaseOperationAuthorizationPolicy: Sendable {
    func authorize(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext
    ) async throws
}

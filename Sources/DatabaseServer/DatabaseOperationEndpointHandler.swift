import DatabaseValue
import DatabaseWire

public protocol DatabaseOperationEndpointHandler: Sendable {
    var identifier: DatabaseOperationIdentifier { get }

    func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult
}

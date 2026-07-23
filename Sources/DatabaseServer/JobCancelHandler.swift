import DatabaseValue
import DatabaseWire

public struct JobCancelHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.jobCancel

    private let service: AnyDatabaseJobService

    public init(service: AnyDatabaseJobService) {
        self.service = service
    }

    public func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            JobCancelOperation.Request.self,
            from: payload,
            limits: limits
        )
        return try await service.cancel(request, context: context)
            .operationResult
    }
}

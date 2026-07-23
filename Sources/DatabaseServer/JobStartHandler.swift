import DatabaseValue
import DatabaseWire

public struct JobStartHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.jobStart

    private let service: AnyDatabaseJobService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseJobService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            JobStartOperation.Request.self,
            from: payload,
            limits: limits
        )
        guard request.maximumSliceWorkUnits > 0,
              request.maximumSliceWorkUnits <= runtimeLimits.maximumWorkUnits else {
            throw DatabaseRuntimeLimitError.invalidMaximumWorkUnits(
                requested: request.maximumSliceWorkUnits,
                maximum: runtimeLimits.maximumWorkUnits
            )
        }
        return try await service.start(request, context: context)
            .operationResult
    }
}

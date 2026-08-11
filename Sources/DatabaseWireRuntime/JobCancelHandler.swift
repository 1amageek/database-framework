import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct JobCancelHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = JobCancelOperation

    private let service: AnyDatabaseJobService

    public init(service: AnyDatabaseJobService) {
        self.service = service
    }

    public func invoke(
        request: JobCancelOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        return try await service.cancel(request, context: context)
            .operationResult
    }
}

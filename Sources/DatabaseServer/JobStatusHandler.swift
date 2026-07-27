@_spi(DatabaseServer) import DatabaseWire

public struct JobStatusHandler: DatabaseOperationHandler {
    public typealias Operation = JobStatusOperation

    private let service: AnyDatabaseJobService

    public init(service: AnyDatabaseJobService) {
        self.service = service
    }

    public func handle(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        try await service.status(request, context: context)
    }
}

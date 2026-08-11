@_spi(DatabaseWireRuntime) import DatabaseWire

public struct JobResultHandler: DatabaseOperationHandler {
    public typealias Operation = JobResultOperation

    private let service: AnyDatabaseJobService

    public init(service: AnyDatabaseJobService) {
        self.service = service
    }

    public func handle(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response {
        try await service.result(request, context: context)
    }
}

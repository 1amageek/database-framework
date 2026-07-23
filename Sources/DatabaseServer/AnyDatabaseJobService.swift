import DatabaseWire

/// Type-erased persistent job service for runtime composition.
public final class AnyDatabaseJobService: DatabaseJobService, Sendable {
    public let jobOperations: [DatabaseJobOperationIdentifier]

    private let startJob: @Sendable (
        JobStartOperation.Request,
        DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<JobStartOperation>
    private let readJobStatus: @Sendable (
        JobStatusOperation.Request,
        DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response
    private let readJobResult: @Sendable (
        JobResultOperation.Request,
        DatabaseOperationContext
    ) async throws -> JobResultOperation.Response
    private let cancelJob: @Sendable (
        JobCancelOperation.Request,
        DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<JobCancelOperation>
    private let performScheduledWork: @Sendable () async throws -> Void

    public init<Service: DatabaseJobService>(_ service: Service) {
        self.jobOperations = service.jobOperations
        self.startJob = { request, context in
            try await service.start(request, context: context)
        }
        self.readJobStatus = { request, context in
            try await service.status(request, context: context)
        }
        self.readJobResult = { request, context in
            try await service.result(request, context: context)
        }
        self.cancelJob = { request, context in
            try await service.cancel(request, context: context)
        }
        self.performScheduledWork = {
            try await service.runScheduledWork()
        }
    }

    public func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<JobStartOperation> {
        try await startJob(request, context)
    }

    public func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        try await readJobStatus(request, context)
    }

    public func result(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response {
        try await readJobResult(request, context)
    }

    public func cancel(
        _ request: JobCancelOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<JobCancelOperation> {
        try await cancelJob(request, context)
    }

    public func runScheduledWork() async throws {
        try await performScheduledWork()
    }
}

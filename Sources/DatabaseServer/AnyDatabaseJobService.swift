import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

/// Type-erased persistent job service for runtime composition.
public final class AnyDatabaseJobService: DatabaseJobService, Sendable {
    public let jobOperations: [JobOperationIdentifier]

    private let startJob: @Sendable (
        JobStartOperation.Request,
        DatabaseOperationContext
    ) async throws -> JobStartExecutionResult
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
    ) async throws -> JobCancellationExecutionResult
    private let performScheduledWork: @Sendable () async throws -> Void
    private let createJobInTransaction: (@Sendable (
        JobStartOperation.Request,
        DatabaseOperationContext,
        DatabaseTransaction
    ) async throws -> JobIdentity)?
    private let recoverJobSchedule: (@Sendable () async throws -> Void)?

    public convenience init<Service: DatabaseJobService>(_ service: Service) {
        self.init(
            service: service,
            createJobInTransaction: nil,
            recoverJobSchedule: nil
        )
    }

    package convenience init<Service>(persistent service: Service)
    where Service: DatabaseJobService & DatabasePersistentJobCreating {
        self.init(
            service: service,
            createJobInTransaction: { request, context, transaction in
                try await service.createPersistentJob(
                    request,
                    context: context,
                    transaction: transaction
                )
            },
            recoverJobSchedule: {
                try await service.recoverPersistentJobSchedule()
            }
        )
    }

    private init<Service: DatabaseJobService>(
        service: Service,
        createJobInTransaction: (@Sendable (
            JobStartOperation.Request,
            DatabaseOperationContext,
            DatabaseTransaction
        ) async throws -> JobIdentity)?,
        recoverJobSchedule: (@Sendable () async throws -> Void)?
    ) {
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
        self.createJobInTransaction = createJobInTransaction
        self.recoverJobSchedule = recoverJobSchedule
    }

    public func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStartExecutionResult {
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
    ) async throws -> JobCancellationExecutionResult {
        try await cancelJob(request, context)
    }

    public func runScheduledWork() async throws {
        try await performScheduledWork()
    }

    package func createPersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity {
        guard let createJobInTransaction else {
            throw DatabaseSchemaExecutionError
                .persistentJobServiceUnavailable
        }
        return try await createJobInTransaction(request, context, transaction)
    }

    package func recoverPersistentJobSchedule() async throws {
        guard let recoverJobSchedule else {
            throw DatabaseSchemaExecutionError
                .persistentJobServiceUnavailable
        }
        try await recoverJobSchedule()
    }
}

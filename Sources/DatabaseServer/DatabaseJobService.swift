@_spi(DatabaseServer) import DatabaseWire

public protocol DatabaseJobService: Sendable {
    var jobOperations: [JobOperationIdentifier] { get }

    func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStartExecutionResult

    func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response

    func result(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response

    func cancel(
        _ request: JobCancelOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobCancellationExecutionResult

    /// Processes due work and persists the next required scheduler wake-up.
    ///
    /// Phase failures are reported as `PersistentJobScheduledWorkError`.
    /// Task cancellation is propagated directly as `CancellationError`.
    func runScheduledWork() async throws
}

import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Context for operations whose durable checkpoint is independent of job state.
public struct DatabaseCheckpointedResumableOperationContext: Sendable {
    public let jobID: DatabaseTypes.UUID
    public let completedWorkUnitsBeforeSlice: UInt64
    public let operationContext: DatabaseOperationContext
    private let permitsOperationOwnedDataAccess: Bool

    public init(
        jobID: DatabaseTypes.UUID,
        completedWorkUnitsBeforeSlice: UInt64,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.completedWorkUnitsBeforeSlice = completedWorkUnitsBeforeSlice
        self.operationContext = operationContext
        self.permitsOperationOwnedDataAccess = false
    }

    package init(
        jobID: DatabaseTypes.UUID,
        completedWorkUnitsBeforeSlice: UInt64,
        operationContext: DatabaseOperationContext,
        permitsOperationOwnedDataAccess: Bool
    ) {
        self.jobID = jobID
        self.completedWorkUnitsBeforeSlice = completedWorkUnitsBeforeSlice
        self.operationContext = operationContext
        self.permitsOperationOwnedDataAccess = permitsOperationOwnedDataAccess
    }

    /// Runs idempotent cleanup owned by the exact leased persistent job.
    ///
    /// The runner grants this capability only after validating the durable job
    /// lease and pending unsuccessful outcome. It intentionally does not reuse
    /// stale user roles or require a Grant that may have been revoked after the
    /// job began.
    package func withOperationOwnedStorageTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard permitsOperationOwnedDataAccess else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try await operationContext.requireDataExecutor()
            .withOperationOwnedStorageTransaction(
                configuration: configuration,
                operation
            )
    }
}

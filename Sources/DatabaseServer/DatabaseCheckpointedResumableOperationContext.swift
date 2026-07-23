import DatabaseValue

/// Context for operations whose durable checkpoint is independent of job state.
public struct DatabaseCheckpointedResumableOperationContext: Sendable {
    public let jobID: DatabaseUUID
    public let completedWorkUnitsBeforeSlice: UInt64
    public let operationContext: DatabaseOperationContext

    public init(
        jobID: DatabaseUUID,
        completedWorkUnitsBeforeSlice: UInt64,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.completedWorkUnitsBeforeSlice = completedWorkUnitsBeforeSlice
        self.operationContext = operationContext
    }
}

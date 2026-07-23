import DatabaseEngine
import DatabaseValue

/// Transaction-scoped context used to validate and provision a resumable job.
public struct DatabaseResumableOperationStartContext: Sendable {
    public let jobID: DatabaseUUID
    public let maximumSliceWorkUnits: UInt64
    public let transaction: TransactionContext
    public let operationContext: DatabaseOperationContext

    public init(
        jobID: DatabaseUUID,
        maximumSliceWorkUnits: UInt64,
        transaction: TransactionContext,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.maximumSliceWorkUnits = maximumSliceWorkUnits
        self.transaction = transaction
        self.operationContext = operationContext
    }
}

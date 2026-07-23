import DatabaseEngine
import DatabaseValue

/// Transaction-scoped context used to validate and provision a resumable job.
public struct DatabaseResumableOperationStartContext: Sendable {
    public let jobID: DatabaseUUID
    public let maximumSliceWorkUnits: UInt64
    public let request: DatabaseCommandRequestContext
    public let transaction: any DatabaseTransactionWriting

    package let databaseTransaction: DatabaseTransaction
    package let operationContext: DatabaseOperationContext

    package init(
        jobID: DatabaseUUID,
        maximumSliceWorkUnits: UInt64,
        transaction: DatabaseTransaction,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.maximumSliceWorkUnits = maximumSliceWorkUnits
        self.request = DatabaseCommandRequestContext(
            requestID: operationContext.requestID,
            metadata: operationContext.metadata
        )
        self.transaction = transaction
        self.databaseTransaction = transaction
        self.operationContext = operationContext
    }
}

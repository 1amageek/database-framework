import DatabaseEngine
import DatabaseValue

public struct DatabaseResumableOperationContext: Sendable {
    public let jobID: DatabaseUUID
    public let completedWorkUnitsBeforeSlice: UInt64
    public let request: DatabaseCommandRequestContext
    public let transaction: any DatabaseTransactionWriting

    package let databaseTransaction: DatabaseTransaction
    package let operationContext: DatabaseOperationContext

    package init(
        jobID: DatabaseUUID,
        completedWorkUnitsBeforeSlice: UInt64,
        transaction: DatabaseTransaction,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.completedWorkUnitsBeforeSlice = completedWorkUnitsBeforeSlice
        self.request = DatabaseCommandRequestContext(
            requestID: operationContext.requestID,
            metadata: operationContext.metadata
        )
        self.transaction = transaction
        self.databaseTransaction = transaction
        self.operationContext = operationContext
    }
}

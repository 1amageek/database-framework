import DatabaseEngine
import DatabaseValue

public struct DatabaseResumableOperationContext: Sendable {
    public let jobID: DatabaseUUID
    public let completedWorkUnitsBeforeSlice: UInt64
    public let transaction: TransactionContext
    public let operationContext: DatabaseOperationContext

    public init(
        jobID: DatabaseUUID,
        completedWorkUnitsBeforeSlice: UInt64,
        transaction: TransactionContext,
        operationContext: DatabaseOperationContext
    ) {
        self.jobID = jobID
        self.completedWorkUnitsBeforeSlice = completedWorkUnitsBeforeSlice
        self.transaction = transaction
        self.operationContext = operationContext
    }
}

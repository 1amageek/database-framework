import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes

public struct DatabaseResumableOperationContext: Sendable {
    public let jobID: DatabaseTypes.UUID
    public let completedWorkUnitsBeforeSlice: UInt64
    public let request: DatabaseCommandRequestContext
    public let transaction: DatabaseTransaction

    package let databaseTransaction: DatabaseTransaction
    package let operationContext: DatabaseOperationContext

    package init(
        jobID: DatabaseTypes.UUID,
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

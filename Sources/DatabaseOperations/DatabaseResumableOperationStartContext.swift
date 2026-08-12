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

/// Transaction-scoped context used to validate and provision a resumable job.
public struct DatabaseResumableOperationStartContext: Sendable {
    public let jobID: DatabaseTypes.UUID
    public let maximumSliceWorkUnits: UInt64
    public let request: DatabaseCommandRequestContext
    public let transaction: DatabaseTransaction

    package let databaseTransaction: DatabaseTransaction
    package let operationContext: DatabaseOperationContext

    package init(
        jobID: DatabaseTypes.UUID,
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

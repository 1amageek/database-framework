import DatabaseEngine
import DatabaseWire
import StorageKit

public struct DatabaseWriteCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: any Transaction
    public let records: DatabaseTransactionRecords
    public let budget: DatabaseExecutionBudget

    init(
        operation: DatabaseOperationContext,
        transaction: any Transaction,
        budget: DatabaseExecutionBudget
    ) {
        self.request = DatabaseCommandRequestContext(
            requestID: operation.requestID,
            metadata: operation.metadata
        )
        self.transaction = transaction
        self.records = DatabaseTransactionRecords(
            container: operation.container,
            transaction: transaction
        )
        self.budget = budget
    }
}

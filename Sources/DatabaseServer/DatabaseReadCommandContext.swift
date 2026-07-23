import DatabaseEngine
import DatabaseWire
import StorageKit

public struct DatabaseReadCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: DatabaseReadTransaction
    public let records: DatabaseReadTransactionRecords
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
        self.transaction = DatabaseReadTransaction(transaction: transaction)
        self.records = DatabaseReadTransactionRecords(
            container: operation.container,
            transaction: transaction
        )
        self.budget = budget
    }
}

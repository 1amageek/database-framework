import DatabaseEngine
import DatabaseWire

public struct DatabaseReadCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: any DatabaseTransactionReading
    public let budget: DatabaseExecutionBudget

    init(
        operation: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        budget: DatabaseExecutionBudget
    ) {
        self.request = DatabaseCommandRequestContext(
            requestID: operation.requestID,
            metadata: operation.metadata
        )
        self.transaction = transaction
        self.budget = budget
    }
}

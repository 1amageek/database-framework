import DatabaseEngine
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseReadCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: DatabaseTransaction
    public let budget: ExecutionBudget

    init(
        operation: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        budget: ExecutionBudget
    ) {
        self.request = DatabaseCommandRequestContext(
            requestID: operation.requestID,
            metadata: operation.metadata
        )
        self.transaction = transaction
        self.budget = budget
    }
}

import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseWriteCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: any DatabaseTransactionWriting
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

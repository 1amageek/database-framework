import DatabaseOperationCore
import DatabaseEngine
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseWriteCommandContext: Sendable {
    public let request: DatabaseCommandRequestContext
    public let transaction: DatabaseTransaction
    public let budget: ExecutionBudget

    package init(
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        transaction: DatabaseTransaction,
        budget: ExecutionBudget
    ) {
        self.request = DatabaseCommandRequestContext(
            requestID: requestID,
            metadata: metadata
        )
        self.transaction = transaction
        self.budget = budget
    }
}

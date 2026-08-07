import DatabaseKit
import DatabaseTypes

/// Read-only access to the final state of one logical database transaction.
///
/// The context is issued by `DatabaseTransaction` and remains valid only for
/// the operation that received it.
public struct PersistableValidationContext: ~Copyable, Sendable {
    public let schema: Schema

    private let transaction: DatabaseTransaction
    private let operationID: UInt64
    private let operationGate: TransactionOperationGate

    package init(
        schema: Schema,
        transaction: DatabaseTransaction,
        operationID: UInt64,
        operationGate: TransactionOperationGate
    ) {
        self.schema = schema
        self.transaction = transaction
        self.operationID = operationID
        self.operationGate = operationGate
    }

    public func fetch(
        _ identity: EntityReference
    ) async throws -> PersistedModel? {
        try operationGate.enter()
        do {
            try ensureDatabaseTaskIsActive()
            let model = try await transaction.fetchPersistedModel(
                identifiedBy: identity,
                within: operationID
            )
            operationGate.leave()
            return model
        } catch {
            operationGate.leave()
            throw error
        }
    }

    package func closeAndWait() async {
        await operationGate.closeAndWait()
    }
}

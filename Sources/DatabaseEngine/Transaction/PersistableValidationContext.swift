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
    private let scope: DatabaseTransactionScope

    package init(
        schema: Schema,
        transaction: DatabaseTransaction,
        operationID: UInt64,
        scope: DatabaseTransactionScope
    ) {
        self.schema = schema
        self.transaction = transaction
        self.operationID = operationID
        self.scope = scope
    }

    public func fetch(
        _ identity: EntityReference
    ) async throws -> (any Persistable)? {
        try await scope.enter()
        do {
            try Task.checkCancellation()
            let model = try await transaction.fetchPersistedModel(
                identifiedBy: identity,
                within: operationID
            )
            await scope.leave()
            return model
        } catch {
            await scope.leave()
            throw error
        }
    }

    package func closeAndWait() async {
        await scope.closeAndWait()
    }
}

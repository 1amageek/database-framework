import DatabaseKit

/// Dynamic read projection for dependent persistence work. The wrapped actor
/// remains the lifecycle owner, while callers cannot recover mutation APIs by
/// downcasting the value supplied at the package boundary.
package struct DatabaseTransactionPersistenceReadProjection:
    DatabaseTransactionPersistenceReading,
    Sendable
{
    private let transaction: DatabaseTransaction

    package init(transaction: DatabaseTransaction) {
        self.transaction = transaction
    }

    package func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        consistency: TransactionReadConsistency
    ) async throws -> Model? {
        try await transaction.fetch(
            type,
            identifiedBy: id,
            consistency: consistency
        )
    }

    package func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        in partition: DirectoryPath<Model>,
        consistency: TransactionReadConsistency
    ) async throws -> Model? {
        try await transaction.fetch(
            type,
            identifiedBy: id,
            in: partition,
            consistency: consistency
        )
    }

    package func scan<Model: Persistable>(
        _ type: Model.Type,
        in partition: DirectoryPath<Model>,
        after continuation: DatabaseScanContinuation?,
        limit: Int,
        consistency: TransactionReadConsistency
    ) async throws -> sending DatabaseScanPage<Model> {
        try await transaction.scan(
            type,
            in: partition,
            after: continuation,
            limit: limit,
            consistency: consistency
        )
    }

    package func fetchPersistedModel(
        identifiedBy identity: EntityReference
    ) async throws -> PersistedModel? {
        try await transaction.fetchPersistedModel(identifiedBy: identity)
    }
}

import DatabaseKit
import StorageKit

/// Engine-owned implementation that keeps the raw root transaction hidden.
package struct ScopedPersistenceQueryReadAccess:
    PersistenceQueryReadAccess {
    private let context: DatabaseContext
    private let transaction: any TransactionReadAccess

    package init(
        context: DatabaseContext,
        transaction: any TransactionReadAccess
    ) {
        self.context = context
        self.transaction = transaction
    }

    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        try await context.fetchPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: primaryKeys,
            partitions: partitions,
            transaction: transaction,
            workMeter: workMeter
        )
    }
}

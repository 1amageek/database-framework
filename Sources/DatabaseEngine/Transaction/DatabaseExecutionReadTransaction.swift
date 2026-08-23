import DatabaseKit
import StorageKit

/// Non-forgeable read capability for server-owned work executed inside one
/// canonical DatabaseEngine transaction.
///
/// DatabaseEngine remains the owner of identity, root, schema generation,
/// storage domain, retry, and lifetime. Cross-package consumers receive only
/// read-only storage access plus the persistence projection needed to resolve
/// schema-qualified entities.
@_spi(DatabaseExecution)
public struct DatabaseExecutionReadTransaction: Sendable {
    private let transaction: DatabaseTransaction
    private let readStorageAccess: any TransactionReadAccess

    package init(transaction: DatabaseTransaction) {
        self.transaction = transaction
        self.readStorageAccess = transaction.storageReadProjection
    }

    public var storageAccess: any TransactionReadAccess {
        readStorageAccess
    }

    public func loadPersistedModel(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?
    ) async throws -> PersistedModel? {
        try await transaction.loadPersistedModel(
            entity: entity,
            id: id,
            partition: partition
        )
    }
}

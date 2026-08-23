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
    private let context: DatabaseContext?

    package init(
        transaction: DatabaseTransaction,
        context: DatabaseContext? = nil
    ) {
        self.transaction = transaction
        self.readStorageAccess = transaction.storageReadProjection
        self.context = context
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

    /// Resolves one schema-admitted index and lends read access confined to
    /// that index for the lifetime of `operation`.
    ///
    /// The originating `DatabaseContext` is captured by DatabaseEngine when
    /// the snapshot is opened. Callers cannot combine a transaction from one
    /// database root with schema or index metadata from another root.
    public func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let context else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        let queryContext = IndexQueryContext(context: context)
        let readableIndex = try await queryContext.readableIndex(
            named: indexName,
            indexType: indexType,
            forEntityName: entityName,
            partitions: partitions,
            authorization: authorization,
            transaction: readStorageAccess
        )
        return try await operation(
            readableIndex,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: readStorageAccess,
                index: readableIndex
            )
        )
    }
}

import DatabaseKit
import StorageKit

/// Resolves schema-admitted indexes on one hidden Engine-owned snapshot.
/// Physical root storage never appears in this capability's public surface.
package struct ScopedIndexQuerySnapshotAccess: IndexQuerySnapshotAccess {
    private let queryContext: IndexQueryContext
    private let transaction: any TransactionReadAccess

    package init(
        queryContext: IndexQueryContext,
        transaction: any TransactionReadAccess
    ) {
        self.queryContext = queryContext
        self.transaction = transaction
    }

    package func withAuxiliaryReadStorage<Result: Sendable>(
        namespace: ByteString,
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard !namespace.isEmpty else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        let subspace = try queryContext.context.operationDataRoot()
            .subspace("data")
            .subspace(namespace)
        return try await operation(
            subspace,
            ScopedIndexReadAccess(
                transaction: transaction,
                subspace: subspace
            )
        )
    }

    package func withAuxiliaryReadStorage<Result: Sendable>(
        path: [String],
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard !path.isEmpty, path.allSatisfy({ !$0.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        var subspace = try queryContext.context.operationDataRoot()
            .subspace("data")
        for component in path {
            subspace = subspace.subspace(component)
        }
        return try await operation(
            subspace,
            ScopedIndexReadAccess(
                transaction: transaction,
                subspace: subspace
            )
        )
    }

    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        try await queryContext.context.fetchPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: primaryKeys,
            partitions: partitions,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    package func withReadableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        let index = try await queryContext.readableIndex(
            named: indexName,
            indexType: indexType,
            for: type,
            authorization: authorization,
            transaction: transaction
        )
        return try await operation(
            index,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                index: index
            )
        )
    }

    package func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        let index = try await queryContext.readableIndex(
            named: indexName,
            indexType: indexType,
            forEntityName: entityName,
            partitions: partitions,
            authorization: authorization,
            transaction: transaction
        )
        return try await operation(
            index,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                index: index
            )
        )
    }

    package func withReadableIndexes<Result: Sendable>(
        _ requests: [IndexReadRequest],
        _ operation: @Sendable @escaping (
            [ReadableIndex?],
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        var indexes: [ReadableIndex?] = []
        indexes.reserveCapacity(requests.count)
        for request in requests {
            indexes.append(
                try await queryContext.readableIndex(
                    named: request.indexName,
                    indexType: request.indexType,
                    forEntityName: request.entityName,
                    partitions: request.partitions,
                    authorization: request.authorization,
                    transaction: transaction
                )
            )
        }
        return try await operation(
            indexes,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                indexes: indexes.compactMap { $0 }
            )
        )
    }
}

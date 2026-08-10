import DatabaseKit
import DatabaseTypes
import StorageKit

extension DatabaseContext {
    package func scanPersistedModels(
        entity: Schema.Entity,
        partitions: FieldObject,
        limit: Int,
        configuration: TransactionConfiguration,
        transaction: (any TransactionAccess)?
    ) async throws -> [PersistedModel] {
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        if let transaction {
            _ = try requireOperationBaseLease()
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: container
            )
            return try await databaseTransaction.scanPersistedModels(
                entity: entity.name,
                partition: partition,
                limit: limit
            )
        }
        return try await withTransaction(
            requiredAccess: .read,
            configuration: configuration
        ) {
            transaction in
            try await transaction.scanPersistedModels(
                entity: entity.name,
                partition: partition,
                limit: limit
            )
        }
    }

    /// Fetch canonical entities in index order on the caller-owned transaction.
    /// A missing primary-key target is retained as `nil` so an index reader can
    /// report physical index corruption instead of silently dropping a row.
    package func fetchPersistedModelsPreservingOrder(
        entity: Schema.Entity,
        primaryKeys: [Tuple],
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> [PersistedModel?] {
        _ = try requireOperationBaseLease()
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        let databaseTransaction = DatabaseTransaction(
            storageAccess: transaction,
            container: container
        )
        var models: [PersistedModel?] = []
        models.reserveCapacity(primaryKeys.count)
        for primaryKey in primaryKeys {
            models.append(
                try await databaseTransaction.loadPersistedModel(
                    entity: entity.name,
                    id: primaryKey,
                    partition: partition
                )
            )
        }
        return models
    }

    package func authorizeCanonicalListAccess(
        entity: Schema.Entity,
        selectQuery: SelectQuery
    ) throws {
        let limit = try canonicalWindowValue(
            selectQuery.limit,
            parameter: "limit"
        )
        let offset = try canonicalWindowValue(
            selectQuery.offset,
            parameter: "offset"
        )
        try container.securityDelegate?.evaluateList(
            entity: entity.name,
            limit: limit,
            offset: offset,
            orderBy: try selectQuery.requiredOrderByColumnNames()
        )
    }

    private func canonicalWindowValue(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let result = Int(exactly: value) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: parameter,
                value: value
            )
        }
        return result
    }
}

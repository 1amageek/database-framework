import DatabaseKit
import DatabaseTypes
import StorageKit

extension DatabaseContext {
    package func scanPersistedModels(
        entity: Schema.Entity,
        partitions: FieldObject,
        limit: Int,
        offset: Int = 0,
        startingAfterIdentifier: ByteString? = nil,
        workMeter: DatabaseWorkMeter? = nil,
        configuration: TransactionConfiguration,
        transaction: (any TransactionAccess)?
    ) async throws -> [PersistedModel] {
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        if let transaction {
            #if DATABASE_MULTI_BASE
            _ = try requireOperationDataRoot()
            #endif
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: container
            )
            return try await databaseTransaction.scanPersistedModels(
                entity: entity.name,
                partition: partition,
                limit: limit,
                offset: offset,
                startingAfterIdentifier: startingAfterIdentifier,
                workMeter: workMeter
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
                limit: limit,
                offset: offset,
                startingAfterIdentifier: startingAfterIdentifier,
                workMeter: workMeter
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
        transaction: any TransactionAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedPersistedModels {
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        let databaseTransaction = DatabaseTransaction(
            storageAccess: transaction,
            container: container
        )
        let arrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: primaryKeys.count,
                element: DatabaseRetainedPersistedModels.Entry?.self
            )
        let reservation = try workMeter.reserveIntermediate(
            rows: UInt64(primaryKeys.count),
            bytes: arrayFootprint.bytes,
            at: .storageRow
        )
        var models = [DatabaseRetainedPersistedModels.Entry?](
            repeating: nil,
            count: primaryKeys.count
        )
        for index in primaryKeys.indices {
            // DatabaseTransaction deliberately rejects overlapping operations.
            // Preserve one serial transaction snapshot until StorageKit owns a
            // backend-neutral batch-read contract.
            try workMeter.consume(at: .storageRow)
            models[index] = try await databaseTransaction
                .loadRetainedPersistedModel(
                entity: entity.name,
                id: primaryKeys[index],
                partition: partition,
                snapshot: snapshot,
                workMeter: workMeter
                )
        }
        return DatabaseRetainedPersistedModels(
            entries: models,
            arrayReservation: reservation
        )
    }

    package func authorizeCanonicalListAccess(
        entity: Schema.Entity,
        selectQuery: SelectQuery
    ) throws {
        try authorizeCanonicalListAccess(
            entityName: entity.name,
            selectQuery: selectQuery
        )
    }

    /// Performs logical list authorization from the requested name before
    /// schema resolution can reveal whether that name exists.
    package func authorizeCanonicalListAccess(
        entityName: String,
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
            entity: entityName,
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

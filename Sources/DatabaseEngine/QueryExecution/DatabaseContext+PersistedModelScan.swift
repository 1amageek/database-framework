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
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseReadTransaction,
        authorizationRequirement: DatabaseListReadAuthorizationRequirement
    ) async throws -> DatabaseRetainedPersistedModels {
        guard limit >= 0 else {
            throw DatabaseTransactionError.invalidLimit(limit)
        }
        guard offset >= 0 else {
            throw DatabaseTransactionError.invalidLimit(offset)
        }
        guard let authorization = transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        let policy = try readPolicy()
        try policy.validate(authorization)
        guard authorizationRequirement.entityName == entity.name,
              authorization.covers(
                  listRequirement: authorizationRequirement
              ),
              authorization.fields.fieldsByEntity[entity.name] != nil else {
            throw DatabaseReadSessionError.authorizationMismatch
        }

        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif
        if limit == 0 {
            let empty = try DatabaseRetainedArrayBuilder<
                DatabaseRetainedPersistedModels.Entry?
            >(
                workMeter: workMeter,
                stage: .storageRow,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    DatabaseRetainedPersistedModels.Entry?.self
                )
            )
            return try DatabaseRetainedPersistedModels(buffer: empty.finish())
        }
        let databaseTransaction = DatabaseTransaction(
            storageAccess: transaction.storageAccess,
            container: container,
            readPolicy: policy,
            readAuthorization: authorization
        )
        return try await databaseTransaction.scanPersistedModels(
            entity: entity.name,
            partition: partition,
            limit: limit,
            offset: offset,
            startingAfterIdentifier: startingAfterIdentifier,
            workMeter: workMeter,
            authorizationRequirement: authorizationRequirement
        )
    }

    /// Fetch canonical entities in index order on the caller-owned transaction.
    /// A missing primary-key target is retained as `nil` so an index reader can
    /// report physical index corruption instead of silently dropping a row.
    func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        transaction: DatabaseReadTransaction,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        admittedFieldNames: Set<String>
    ) async throws -> DatabaseRetainedPersistedModels
    where PrimaryKeys: Collection & Sendable,
        PrimaryKeys.Element == Tuple {
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        let databaseTransaction = DatabaseTransaction(
            storageAccess: transaction.storageAccess,
            container: container,
            readPolicy: try readPolicy(),
            readAuthorization: transaction.authorization
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
        for (offset, index) in primaryKeys.indices.enumerated() {
            // DatabaseTransaction deliberately rejects overlapping operations.
            // Preserve one serial transaction snapshot until StorageKit owns a
            // backend-neutral batch-read contract.
            try workMeter.consume(at: .storageRow)
            models[offset] = try await databaseTransaction
                .loadRetainedPersistedModel(
                entity: entity.name,
                id: primaryKeys[index],
                partition: partition,
                snapshot: snapshot,
                workMeter: workMeter,
                fields: admittedFieldNames
            )
        }
        return try DatabaseRetainedPersistedModels(
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
        try readPolicy().authorizeCanonicalListAccess(
            entityName: entityName,
            selectQuery: selectQuery
        )
    }
}

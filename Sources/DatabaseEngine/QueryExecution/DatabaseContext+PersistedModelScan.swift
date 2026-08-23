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
        configuration: TransactionConfiguration,
        transaction: (any TransactionReadAccess)?
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel> {
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        if let transaction {
            return try await scanPersistedModels(
                entity: entity.name,
                partition: partition,
                limit: limit,
                offset: offset,
                startingAfterIdentifier: startingAfterIdentifier,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            try await self.scanPersistedModels(
                entity: entity.name,
                partition: partition,
                limit: limit,
                offset: offset,
                startingAfterIdentifier: startingAfterIdentifier,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    private func scanPersistedModels(
        entity entityName: String,
        partition: AnyDirectoryPath?,
        limit: Int,
        offset: Int,
        startingAfterIdentifier: ByteString?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel> {
        guard limit > 0, offset >= 0 else {
            throw DatabaseTransactionError.invalidLimit(
                limit > 0 ? offset : limit
            )
        }
        guard let runtime = container.runtimeConfiguration.entityRuntimes
            .registration(named: entityName) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: entityName
            )
        }
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entityName,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        guard let store = try await container.openStore(
            for: runtime.entity,
            path: partition,
            transaction: transaction
        ) else {
            return try DatabaseSharedRetainedArray.empty(
                workMeter: workMeter,
                stage: .storageRow
            )
        }
        let entitySubspace = store.itemSubspace.subspace(entityName)
        let (begin, end) = entitySubspace.range()
        let startingAfter = try startingAfterIdentifier.map {
            entitySubspace.pack(try Tuple(packed: $0))
        }
        let (requestedScanLimit, overflow) = limit.addingReportingOverflow(
            offset
        )
        let storage = container.itemStorageFactory.makeReader(
            transaction: transaction,
            blobsSubspace: store.blobsSubspace
        )
        var builder = try DatabaseRetainedArrayBuilder<PersistedModel>(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: PersistedModel.self
            ),
            expectedCount: 0
        )
        var iterator = storage.scanRetained(
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: true,
            limit: overflow ? Int.max : requestedScanLimit,
            workMeter: workMeter,
            stage: .storageRow
        ).makeAsyncIterator()
        var skipped = 0
        while let (_, retainedValue) = try await iterator.next() {
            try workMeter.consume(at: .storageRow)
            if skipped < offset {
                skipped += 1
                continue
            }
            let fieldOverhead = try DatabaseIntermediateFootprint(
                bytes: 96
            ).multiplied(by: UInt64(runtime.entity.fields.count))
            let detachmentReservation = try workMeter.reserveIntermediate(
                bytes: try DatabaseIntermediateFootprint(
                    bytes: UInt64(retainedValue.count) + 256
                ).adding(fieldOverhead).bytes,
                at: .storageRow
            )
            defer { detachmentReservation.release() }
            let persisted = try retainedValue.withValue { data in
                try DataAccess.deserializePersistedModel(
                    data,
                    expectedEntity: entityName
                )
            }
            let model = try runtime.canonicalized(persisted).detached()
            let admission = try builder.prepareAppend(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: model,
                    workMeter: workMeter
                ),
                at: .storageRow
            )
            try container.securityDelegate?.evaluateGet(model, fields: nil)
            builder.append(model, using: admission)
        }
        return try builder.finish().moveToSharedOwnership(at: .storageRow)
    }

    /// Fetch canonical entities in index order on the caller-owned transaction.
    /// A missing primary-key target is retained as `nil` so an index reader can
    /// report physical index corruption instead of silently dropping a row.
    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        try await withReadStorageAccess { transaction in
            try await self.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: partitions,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    /// Fetch canonical entities in index order on an explicit trusted
    /// transaction capability.
    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        #if DATABASE_MULTI_BASE
        _ = try requireOperationDataRoot()
        #endif
        let partition = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        guard let runtime = container.runtimeConfiguration.entityRuntimes
            .registration(named: entity.name) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: entity.name
            )
        }
        let store = try await container.openStore(
            for: entity,
            path: partition,
            transaction: transaction
        )
        let storage = store.map {
            container.itemStorageFactory.makeReader(
                transaction: transaction,
                blobsSubspace: $0.blobsSubspace
            )
        }
        let entitySubspace = store?.itemSubspace.subspace(entity.name)
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: primaryKeys.count,
                element: PersistedModel?.self
            ).bytes,
            at: .storageRow
        )
        var transferredReservation = false
        defer {
            if !transferredReservation { reservation.release() }
        }
        var models = [PersistedModel?](
            repeating: nil,
            count: primaryKeys.count
        )
        for (index, primaryKey) in primaryKeys.enumerated() {
            // DatabaseTransaction deliberately rejects overlapping operations.
            // Preserve one serial transaction snapshot until StorageKit owns a
            // backend-neutral batch-read contract.
            try workMeter.consume(at: .storageRow)
            guard let storage, let entitySubspace else {
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: 16,
                    at: .storageRow
                )
                continue
            }
            let key = entitySubspace.pack(primaryKey)
            guard let retainedValue = try await storage.readRetained(
                for: key,
                workMeter: workMeter,
                stage: .storageRow
            ) else {
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: 16,
                    at: .storageRow
                )
                continue
            }
            let fieldOverhead = try DatabaseIntermediateFootprint(
                bytes: 96
            ).multiplied(by: UInt64(entity.fields.count))
            let provisionalFootprint = try DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(retainedValue.count) + 256
            ).adding(fieldOverhead)
            try reservation.reserveAdditional(
                rows: provisionalFootprint.rows,
                bytes: provisionalFootprint.bytes,
                at: .storageRow
            )
            let persisted = try retainedValue.withValue { data in
                try DataAccess.deserializePersistedModel(
                    data,
                    expectedEntity: entity.name
                )
            }
            let model = try runtime.canonicalized(persisted).detached()
            let actualFootprint = try CanonicalRelationalFootprintMeter
                .footprint(of: model, workMeter: workMeter)
            if actualFootprint.bytes > provisionalFootprint.bytes {
                try reservation.reserveAdditional(
                    bytes: actualFootprint.bytes - provisionalFootprint.bytes,
                    at: .storageRow
                )
            } else if actualFootprint.bytes < provisionalFootprint.bytes {
                try reservation.releasePartial(
                    bytes: provisionalFootprint.bytes - actualFootprint.bytes
                )
            }
            try container.securityDelegate?.evaluateGet(
                model,
                fields: nil
            )
            models[index] = model
        }
        let retainedModels = try DatabaseSharedRetainedArray.adopting(
            models,
            reservation: reservation,
            workMeter: workMeter,
            stage: .storageRow
        )
        transferredReservation = true
        return retainedModels
    }

    package func authorizeCanonicalListAccess(
        entity: Schema.Entity,
        selectQuery: SelectQuery
    ) throws {
        try indexQueryContext.authorizeListAccess(
            entityName: entity.name,
            authorization: IndexReadAuthorization(selectQuery: selectQuery)
        )
    }
}

#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes
import StorageKit

extension DBContainer {
    package func retireBase(
        _ id: Base.ID,
        expectedRevision: UInt64
    ) async throws -> DatabaseBaseRecord {
        let retiring = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await self.requireNoActiveSchemaTransition(
                transaction: transaction.storageAccess
            )
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            switch current.lifecycle {
            case .active:
                guard current.revision == expectedRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: expectedRevision,
                        actual: current.revision
                    )
                }
                guard !(try await self.compositionCatalog.contains(
                    baseID: id,
                    transaction: transaction.storageAccess
                )) else {
                    throw DatabaseBaseCatalogError
                        .baseReferencedByComposition(id)
                }
                return try await self.replaceLifecycle(
                    current,
                    with: .retiring,
                    transaction: transaction.storageAccess
                )
            case .retiring, .retired:
                guard current.revision >= expectedRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: expectedRevision,
                        actual: current.revision
                    )
                }
                return current
            case .provisioning, .moving, .deleting, .tombstone:
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.retiring.rawValue
                )
            }
        }
        let root = try await root(for: retiring)
        try publishBaseGeneration(retiring, root: root)
        try await stopBaseAdmissionAndDrain(id)

        let retired = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            if current.lifecycle == .retired {
                return current
            }
            guard current.lifecycle == .retiring else {
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.retired.rawValue
                )
            }
            return try await self.replaceLifecycle(
                current,
                with: .retired,
                transaction: transaction.storageAccess
            )
        }
        try publishBaseGeneration(retired, root: root)
        return retired
    }

    package func activateBase(
        _ id: Base.ID,
        expectedRevision: UInt64,
        authorization: AuthorizationContext
    ) async throws -> DatabaseBaseRecord {
        let retired = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try await self.requireNoActiveSchemaTransition(
                transaction: transaction.storageAccess
            )
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            if current.lifecycle == .active {
                guard current.revision >= expectedRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: expectedRevision,
                        actual: current.revision
                    )
                }
                return current
            }
            guard current.lifecycle == .retired else {
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.active.rawValue
                )
            }
            guard current.revision == expectedRevision else {
                throw DatabaseBaseCatalogError.revisionConflict(
                    expected: expectedRevision,
                    actual: current.revision
                )
            }
            return current
        }
        if retired.lifecycle == .active {
            return retired
        }

        guard let domain = storageTopology.domain(
            identifiedBy: retired.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(
                retired.domainID
            )
        }
        let root = try await root(for: retired)
        let readinessRecord = DatabaseBaseRecord(
            id: retired.id,
            placementID: retired.placementID,
            domainID: retired.domainID,
            namespacePath: retired.namespacePath,
            placementGeneration: retired.placementGeneration,
            revision: retired.revision,
            lifecycle: .active
        )
        let readinessLease = DatabaseBaseLease(
            generation: DatabaseBaseGeneration(
                record: readinessRecord,
                domain: domain,
                root: root
            ),
            token: DatabaseBaseLeaseToken(finishOperation: {})
        )
        try await withBaseLease(readinessLease) {
            try await RequestAuthorization.$context.withValue(authorization) {
                try await self.migrateIfNeeded()
            }
        }

        let active = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            if current.lifecycle == .active { return current }
            guard current.lifecycle == .retired else {
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.active.rawValue
                )
            }
            guard current.revision == expectedRevision else {
                throw DatabaseBaseCatalogError.revisionConflict(
                    expected: expectedRevision,
                    actual: current.revision
                )
            }
            return try await self.replaceLifecycle(
                current,
                with: .active,
                transaction: transaction.storageAccess
            )
        }
        try publishBaseGeneration(active, root: root)
        return active
    }

    package func prepareBaseDeletion(
        _ id: Base.ID,
        expectedRevision: UInt64,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidDeletionOwner
        }
        let deletionStore = DatabaseBaseDeletionStore(
            root: storageTopology.controlDomain.root,
            collection: "intents"
        )
        let deleting = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await self.requireNoActiveSchemaTransition(
                transaction: transaction.storageAccess
            )
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            if current.lifecycle == .deleting || current.lifecycle == .tombstone {
                guard let intent = try await deletionStore.load(
                    id,
                    transaction: transaction.storageAccess
                ), intent.owner == owner,
                intent.deletingRevision <= current.revision else {
                    throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
                }
                return current
            }
            guard current.lifecycle == .retired else {
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.deleting.rawValue
                )
            }
            guard current.revision == expectedRevision else {
                throw DatabaseBaseCatalogError.revisionConflict(
                    expected: expectedRevision,
                    actual: current.revision
                )
            }
            guard !(try await self.compositionCatalog.contains(
                baseID: id,
                transaction: transaction.storageAccess
            )) else {
                throw DatabaseBaseCatalogError.baseReferencedByComposition(id)
            }
            let record = try await self.replaceLifecycle(
                current,
                with: .deleting,
                transaction: transaction.storageAccess
            )
            try await deletionStore.insert(
                DatabaseBaseDeletionRecord(
                    baseID: id,
                    owner: owner,
                    deletingRevision: record.revision
                ),
                transaction: transaction.storageAccess
            )
            return record
        }
        guard deleting.lifecycle != .tombstone else { return deleting }
        let root = try await root(for: deleting)
        try publishBaseGeneration(deleting, root: root)
        try await stopBaseAdmissionAndDrain(id)
        return deleting
    }

    /// Clears Base-local data only after the current Grant is read in the same
    /// transaction. The marker is intentionally outside the Base root so the
    /// exact lifecycle job can recover after the Grant records are removed.
    package func clearBaseForDeletion(
        _ id: Base.ID,
        owner: ByteString,
        authorization: AuthorizationContext
    ) async throws -> DatabaseBaseRecord {
        let prepared = try await baseDeletionPreparation(id, owner: owner)
        let record = prepared.record
        let intent = prepared.intent
        guard record.lifecycle == .deleting || record.lifecycle == .tombstone,
              intent.deletingRevision <= record.revision else {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
        let domain = try domain(for: record)
        let markerStore = DatabaseBaseDeletionStore(
            root: domain.root,
            collection: "markers"
        )
        if record.lifecycle == .tombstone {
            try await requireBaseDeletionMarker(
                intent,
                store: markerStore,
                domain: domain
            )
            return record
        }
        let baseRoot = try await root(for: record)
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            if let marker = try await markerStore.load(
                id,
                transaction: transaction
            ) {
                guard marker == intent else {
                    throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
                }
                return
            }
            try await DatabaseGrantStore(
                resource: .base(id),
                root: baseRoot
            ).require(
                .administer,
                authorization: authorization,
                transaction: transaction
            )
            let range = baseRoot.range()
            try transaction.clearRange(
                beginKey: range.begin,
                endKey: range.end
            )
            try await markerStore.insert(intent, transaction: transaction)
        }
        return record
    }

    package func finishBaseDeletion(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        let prepared = try await baseDeletionPreparation(id, owner: owner)
        let domain = try domain(for: prepared.record)
        let markerStore = DatabaseBaseDeletionStore(
            root: domain.root,
            collection: "markers"
        )
        try await requireBaseDeletionMarker(
            prepared.intent,
            store: markerStore,
            domain: domain
        )

        let tombstone = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            guard let intent = try await DatabaseBaseDeletionStore(
                root: self.storageTopology.controlDomain.root,
                collection: "intents"
            ).load(id, transaction: transaction.storageAccess),
            intent == prepared.intent else {
                throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
            }
            if current.lifecycle == .tombstone {
                return current
            }
            guard current.lifecycle == .deleting,
                  current.revision == intent.deletingRevision else {
                throw DatabaseBaseCatalogError.corruptedRecord(id)
            }
            return try await self.replaceLifecycle(
                current,
                with: .tombstone,
                transaction: transaction.storageAccess
            )
        }

        if try await domain.engine.namespaceExists(path: tombstone.namespacePath) {
            let tombstoneRoot = try await domain.engine.resolveExistingNamespace(
                path: tombstone.namespacePath
            )
            try publishBaseGeneration(tombstone, root: tombstoneRoot)
            if domain.engine.namespaceCatalog != nil {
                try await domain.engine.removeNamespace(
                    path: tombstone.namespacePath
                )
            }
        }
        return tombstone
    }

    package func permitsBaseDeletionFinalization(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> Bool {
        guard owner.count == 16 else { return false }
        do {
            let catalogState = try await withControlMetadataTransaction(
                configuration: .readOnly
            ) { transaction in
                let record = try await self.baseCatalog.load(
                    id,
                    transaction: transaction.storageAccess
                )
                let intent = try await DatabaseBaseDeletionStore(
                    root: self.storageTopology.controlDomain.root,
                    collection: "intents"
                ).load(id, transaction: transaction.storageAccess)
                return (record, intent)
            }
            guard let record = catalogState.0,
                  record.lifecycle == .deleting
                    || record.lifecycle == .tombstone else {
                return false
            }
            if record.lifecycle == .deleting {
                guard catalogState.1?.owner == owner else { return false }
            }
            let domain = try domain(for: record)
            let markerStore = DatabaseBaseDeletionStore(
                root: domain.root,
                collection: "markers"
            )
            return try await domain.transactionExecutor.withTransaction(
                configuration: .readOnly,
                clock: monotonicClock
            ) { transaction in
                try await markerStore.load(
                    id,
                    transaction: transaction
                )?.owner == owner
            }
        } catch let error as DatabaseBaseCatalogError {
            switch error {
            case .baseNotFound, .baseDeletionClaimed:
                return false
            default:
                throw error
            }
        }
    }

    /// Reconciles Base-local deletion effects before the persistent job
    /// publishes its terminal unsuccessful outcome. The owner intent remains
    /// durable until `finalizeUnsuccessfulBaseDeletion` commits with job state.
    package func prepareUnsuccessfulBaseDeletionRecovery(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        let prepared = try await baseDeletionPreparation(id, owner: owner)
        let current = prepared.record
        let intent = prepared.intent
        let domain = try domain(for: current)
        let markerStore = DatabaseBaseDeletionStore(
            root: domain.root,
            collection: "markers"
        )
        let marker = try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            try await markerStore.load(id, transaction: transaction)
        }
        if marker != nil {
            guard marker == intent else {
                throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
            }
            let tombstone = try await withControlMetadataTransaction(
                configuration: .batch
            ) { transaction in
                guard let latest = try await self.baseCatalog.load(
                    id,
                    transaction: transaction.storageAccess
                ), let latestIntent = try await DatabaseBaseDeletionStore(
                    root: self.storageTopology.controlDomain.root,
                    collection: "intents"
                ).load(id, transaction: transaction.storageAccess),
                latestIntent == intent else {
                    throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
                }
                if latest.lifecycle == .tombstone { return latest }
                guard latest.lifecycle == .deleting,
                      latest.revision == intent.deletingRevision else {
                    throw DatabaseBaseCatalogError.corruptedRecord(id)
                }
                return try await self.replaceLifecycle(
                    latest,
                    with: .tombstone,
                    transaction: transaction.storageAccess
                )
            }
            if domain.engine.namespaceCatalog != nil,
               try await domain.engine.namespaceExists(
                   path: tombstone.namespacePath
               ) {
                let tombstoneRoot = try await domain.engine
                    .resolveExistingNamespace(path: tombstone.namespacePath)
                try publishBaseGeneration(tombstone, root: tombstoneRoot)
                try await domain.engine.removeNamespace(
                    path: tombstone.namespacePath
                )
            }
            return tombstone
        }

        let restored = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let latest = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ), let latestIntent = try await DatabaseBaseDeletionStore(
                root: self.storageTopology.controlDomain.root,
                collection: "intents"
            ).load(id, transaction: transaction.storageAccess),
            latestIntent == intent else {
                throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
            }
            if latest.lifecycle == .retired { return latest }
            guard latest.lifecycle == .deleting,
                  latest.revision == intent.deletingRevision else {
                throw DatabaseBaseCatalogError.corruptedRecord(id)
            }
            return try await self.replaceLifecycle(
                latest,
                with: .retired,
                transaction: transaction.storageAccess
            )
        }
        try publishBaseGeneration(restored, root: try await root(for: restored))
        return restored
    }

    /// Validates operation-owned recovery and removes its transient intent in
    /// the same control-domain transaction as the terminal persistent-job state.
    package func finalizeUnsuccessfulBaseDeletion(
        _ id: Base.ID,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidDeletionOwner
        }
        guard let current = try await baseCatalog.load(
            id,
            transaction: controlTransaction
        ) else {
            throw DatabaseBaseCatalogError.baseNotFound(id)
        }
        let intentStore = DatabaseBaseDeletionStore(
            root: storageTopology.controlDomain.root,
            collection: "intents"
        )
        guard let intent = try await intentStore.load(
            id,
            transaction: controlTransaction
        ), intent.owner == owner else {
            throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
        }
        guard current.lifecycle == .retired
                || current.lifecycle == .tombstone else {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
        try await intentStore.remove(
            id,
            owner: owner,
            transaction: controlTransaction
        )
    }

    /// Removes the deletion intent only in the transaction that also commits
    /// the owning persistent job's successful result. The data-domain marker
    /// remains as immutable evidence that Base-local data was cleared.
    package func finalizeSuccessfulBaseDeletion(
        _ id: Base.ID,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidDeletionOwner
        }
        guard let current = try await baseCatalog.load(
            id,
            transaction: controlTransaction
        ), current.lifecycle == .tombstone else {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
        let intentStore = DatabaseBaseDeletionStore(
            root: storageTopology.controlDomain.root,
            collection: "intents"
        )
        guard try await intentStore.load(
            id,
            transaction: controlTransaction
        )?.owner == owner else {
            throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
        }
        try await intentStore.remove(
            id,
            owner: owner,
            transaction: controlTransaction
        )
    }

    private func baseDeletionPreparation(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> (
        record: DatabaseBaseRecord,
        intent: DatabaseBaseDeletionRecord
    ) {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidDeletionOwner
        }
        return try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let record = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            guard let intent = try await DatabaseBaseDeletionStore(
                root: self.storageTopology.controlDomain.root,
                collection: "intents"
            ).load(id, transaction: transaction.storageAccess),
            intent.owner == owner else {
                throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
            }
            return (record, intent)
        }
    }

    private func requireBaseDeletionMarker(
        _ expected: DatabaseBaseDeletionRecord,
        store: DatabaseBaseDeletionStore,
        domain: DatabaseStorageDomainRuntime
    ) async throws {
        try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            guard try await store.load(
                expected.baseID,
                transaction: transaction
            ) == expected else {
                throw DatabaseBaseCatalogError.baseDeletionMarkerMissing(
                    expected.baseID
                )
            }
        }
    }

    private func replaceLifecycle(
        _ record: DatabaseBaseRecord,
        with lifecycle: DatabaseBaseLifecycleState,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseRecord {
        let (revision, overflow) = record.revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseBaseCatalogError.corruptedRecord(record.id)
        }
        return try await baseCatalog.replace(
            DatabaseBaseRecord(
                id: record.id,
                placementID: record.placementID,
                domainID: record.domainID,
                namespacePath: record.namespacePath,
                placementGeneration: record.placementGeneration,
                revision: revision,
                lifecycle: lifecycle
            ),
            expectedRecordRevision: record.revision,
            transaction: transaction
        )
    }

    private func domain(
        for record: DatabaseBaseRecord
    ) throws -> DatabaseStorageDomainRuntime {
        guard let domain = storageTopology.domain(identifiedBy: record.domainID)
        else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(record.domainID)
        }
        return domain
    }

    private func root(
        for record: DatabaseBaseRecord
    ) async throws -> Subspace {
        let domain = try domain(for: record)
        do {
            return try await domain.engine.resolveExistingNamespace(
                path: record.namespacePath
            )
        } catch {
            throw DatabaseBaseExecutionError.placementRootMissing(record.id)
        }
    }
}

#endif

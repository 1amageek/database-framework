#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

extension DBContainer {
    private static let placementTransferBatchSize = 16

    package func prepareBasePlacementMove(
        _ id: Base.ID,
        destinationPlacementID: Base.Placement.ID,
        expectedRevision: UInt64,
        owner: ByteString
    ) async throws -> DatabaseBasePlacementMoveDescriptor {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidPlacementMoveOwner
        }
        let moveStore = DatabaseBasePlacementMoveStore(
            controlDomain: storageTopology.controlDomain
        )
        let prepared = try await withControlMetadataTransaction(
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
            case .retired:
                guard let destinationPlacement = self.storageTopology.placement(
                    identifiedBy: destinationPlacementID
                ) else {
                    throw DatabaseBaseCatalogError.placementNotFound(
                        destinationPlacementID
                    )
                }
                guard let destinationDomain = self.storageTopology.domain(
                    identifiedBy: destinationPlacement.domainID
                ) else {
                    throw DatabaseBaseCatalogError.storageDomainNotFound(
                        destinationPlacement.domainID
                    )
                }
                guard current.placementID != destinationPlacementID else {
                    throw DatabaseBaseCatalogError
                        .placementAlreadySelected(destinationPlacementID)
                }
                // Section 14 fixes a Base Partition at `bases/<Base.ID>` below
                // the database root of its domain, so two placements naming one
                // domain address one Partition. Only the domain distinguishes a
                // destination from the source.
                guard current.domainID != destinationDomain.id else {
                    throw DatabaseBaseCatalogError
                        .placementDestinationMatchesSource(id)
                }
                guard current.revision == expectedRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: expectedRevision,
                        actual: current.revision
                    )
                }
                let revision = try Self.incrementPlacementValue(
                    current.revision,
                    baseID: id
                )
                let destinationGeneration = try Self.incrementPlacementValue(
                    current.placementGeneration,
                    baseID: id
                )
                let descriptor = DatabaseBasePlacementMoveDescriptor(
                    baseID: id,
                    sourcePlacementID: current.placementID,
                    sourceDomainID: current.domainID,
                    sourcePlacementGeneration: current.placementGeneration,
                    movingRevision: revision,
                    destinationPlacementID: destinationPlacementID,
                    destinationDomainID: destinationDomain.id,
                    destinationPlacementGeneration: destinationGeneration
                )
                try await moveStore.insert(
                    DatabaseBasePlacementMoveRecord(
                        descriptor: descriptor,
                        owner: owner
                    ),
                    transaction: transaction.storageAccess
                )
                let record = try await self.baseCatalog.replace(
                    DatabaseBaseRecord(
                        id: current.id,
                        placementID: current.placementID,
                        domainID: current.domainID,
                        placementGeneration: current.placementGeneration,
                        revision: revision,
                        lifecycle: .moving
                    ),
                    expectedRecordRevision: current.revision,
                    transaction: transaction.storageAccess
                )
                return (record, descriptor)
            case .moving:
                let requiredRevision = try Self.incrementPlacementValue(
                    expectedRevision,
                    baseID: id
                )
                guard current.revision == requiredRevision else {
                    throw DatabaseBaseCatalogError.revisionConflict(
                        expected: requiredRevision,
                        actual: current.revision
                    )
                }
                guard let stored = try await moveStore.load(
                    id,
                    transaction: transaction.storageAccess
                ), stored.owner == owner,
                stored.descriptor.destinationPlacementID
                    == destinationPlacementID,
                stored.descriptor.sourcePlacementID == current.placementID,
                stored.descriptor.sourceDomainID == current.domainID,
                stored.descriptor.sourcePlacementGeneration
                    == current.placementGeneration,
                stored.descriptor.movingRevision == current.revision else {
                    throw DatabaseBaseCatalogError.corruptedRecord(id)
                }
                return (current, stored.descriptor)
            case .provisioning, .active, .retiring, .deleting, .tombstone:
                throw DatabaseBaseCatalogError.invalidLifecycleTransition(
                    baseID: id,
                    from: current.lifecycle.rawValue,
                    to: DatabaseBaseLifecycleState.moving.rawValue
                )
            }
        }
        let moving = prepared.0
        let descriptorIntent = prepared.1

        let sourceDomain = try placementDomain(descriptorIntent.sourceDomainID)
        let sourceTenant = try await placementTenant(
            descriptorIntent.baseID,
            in: sourceDomain,
            expecting: nil
        )
        try publishBaseGeneration(moving, tenant: sourceTenant)
        try await stopBaseAdmissionAndDrain(id)

        let destinationTenant = try await claimDestinationTenant(
            descriptorIntent.baseID,
            domainID: descriptorIntent.destinationDomainID,
            owner: owner
        )
        let descriptor = DatabaseBasePlacementMoveDescriptor(
            baseID: descriptorIntent.baseID,
            sourcePlacementID: descriptorIntent.sourcePlacementID,
            sourceDomainID: descriptorIntent.sourceDomainID,
            sourcePlacementGeneration:
                descriptorIntent.sourcePlacementGeneration,
            movingRevision: descriptorIntent.movingRevision,
            destinationPlacementID: descriptorIntent.destinationPlacementID,
            destinationDomainID: descriptorIntent.destinationDomainID,
            destinationPlacementGeneration:
                descriptorIntent.destinationPlacementGeneration,
            sourceRootPrefix: sourceTenant.partition.root.generation,
            destinationRootPrefix: destinationTenant.partition.root.generation
        )
        try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await moveStore.replacePrepared(
                DatabaseBasePlacementMoveRecord(
                    descriptor: descriptor,
                    owner: owner
                ),
                transaction: transaction.storageAccess
            )
        }
        return descriptor
    }

    package func copyBasePlacementBatch(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        continuation: ByteString?,
        digest: ByteString?,
        keyCount: UInt64,
        byteCount: UInt64
    ) async throws -> DatabaseBasePlacementTransferProgress {
        let sourceDomain = try placementDomain(descriptor.sourceDomainID)
        let destinationDomain = try placementDomain(
            descriptor.destinationDomainID
        )
        let sourceTenant = try await placementTenant(
            descriptor.baseID,
            in: sourceDomain,
            expecting: descriptor.sourceRootPrefix
        )
        let destinationTenant = try await placementTenant(
            descriptor.baseID,
            in: destinationDomain,
            expecting: descriptor.destinationRootPrefix
        )
        let page = try await placementPage(
            tenant: sourceTenant,
            domain: sourceDomain,
            continuation: continuation
        )
        // The destination structure is created through the catalog rather than
        // rebased from source keys: Directory Layout V1 stores a parent prefix
        // inside each child edge key and the child prefix inside its value, so
        // only the catalog can produce edges that address the destination.
        let access = destinationDomain.directoryAccess
        let baseID = descriptor.baseID
        try await destinationDomain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            for node in page.nodes {
                let directory = try await DatabaseBaseTenantTransfer
                    .resolveOrCreate(
                        node.path,
                        layers: node.layers,
                        in: destinationTenant,
                        access: access,
                        transaction: transaction
                    )
                for row in node.rows {
                    let key = directory.root.prefix.appending(
                        contentsOf: row.suffix
                    )
                    if let existing = try await transaction.getValue(
                        for: key,
                        snapshot: false
                    ) {
                        guard existing == row.value else {
                            throw DatabaseBaseCatalogError
                                .placementDigestMismatch(baseID)
                        }
                    } else {
                        try transaction.setValue(row.value, for: key)
                    }
                }
            }
        }
        return try Self.progress(
            page: page,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount
        )
    }

    package func verifyBasePlacementBatch(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        destination: Bool,
        continuation: ByteString?,
        digest: ByteString?,
        keyCount: UInt64,
        byteCount: UInt64
    ) async throws -> DatabaseBasePlacementTransferProgress {
        let domain = try placementDomain(
            destination
                ? descriptor.destinationDomainID
                : descriptor.sourceDomainID
        )
        // A side whose Partition is absent holds no keys, so verification
        // reports the empty keyspace instead of failing. Cleanup removes the
        // source Partition, and the caller proves the removal by verifying
        // that side again. A destination that was never created also verifies
        // as empty and diverges from a non-empty source in the digest.
        guard
            let tenant = try await placementTenantIfPresent(
                descriptor.baseID,
                in: domain,
                expecting: destination
                    ? descriptor.destinationRootPrefix
                    : descriptor.sourceRootPrefix
            )
        else {
            return try Self.progress(
                page: DatabaseBaseTenantTransfer.Page(
                    nodes: [],
                    continuation: nil
                ),
                digest: digest,
                keyCount: keyCount,
                byteCount: byteCount
            )
        }
        let page = try await placementPage(
            tenant: tenant,
            domain: domain,
            continuation: continuation
        )
        return try Self.progress(
            page: page,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount
        )
    }

    package func cutOverBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor
    ) async throws -> DatabaseBaseRecord {
        let destinationDomain = try placementDomain(
            descriptor.destinationDomainID
        )
        let destinationTenant = try await placementTenant(
            descriptor.baseID,
            in: destinationDomain,
            expecting: descriptor.destinationRootPrefix
        )
        let record = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                descriptor.baseID,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(descriptor.baseID)
            }
            if current.lifecycle == .retired,
               current.placementID == descriptor.destinationPlacementID,
               current.placementGeneration
                == descriptor.destinationPlacementGeneration {
                return current
            }
            guard current.lifecycle == .moving,
                  current.placementID == descriptor.sourcePlacementID,
                  current.domainID == descriptor.sourceDomainID,
                  current.placementGeneration
                    == descriptor.sourcePlacementGeneration,
                  current.revision == descriptor.movingRevision else {
                throw DatabaseBaseCatalogError.corruptedRecord(
                    descriptor.baseID
                )
            }
            let revision = try Self.incrementPlacementValue(
                current.revision,
                baseID: descriptor.baseID
            )
            return try await self.baseCatalog.replace(
                DatabaseBaseRecord(
                    id: current.id,
                    placementID: descriptor.destinationPlacementID,
                    domainID: descriptor.destinationDomainID,
                    placementGeneration:
                        descriptor.destinationPlacementGeneration,
                    revision: revision,
                    lifecycle: .retired
                ),
                expectedRecordRevision: current.revision,
                transaction: transaction.storageAccess
            )
        }
        try publishBaseGeneration(record, tenant: destinationTenant)
        return record
    }

    package func finishBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidPlacementMoveOwner
        }
        let moveStore = DatabaseBasePlacementMoveStore(
            controlDomain: storageTopology.controlDomain
        )
        let current = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                descriptor.baseID,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(descriptor.baseID)
            }
            guard current.lifecycle == .retired,
                  current.placementID == descriptor.destinationPlacementID,
                  current.domainID == descriptor.destinationDomainID,
                  current.placementGeneration
                    == descriptor.destinationPlacementGeneration else {
                throw DatabaseBaseCatalogError.corruptedRecord(
                    descriptor.baseID
                )
            }
            guard let move = try await moveStore.load(
                descriptor.baseID,
                transaction: transaction.storageAccess
            ), move.owner == owner, move.descriptor == descriptor else {
                throw DatabaseBaseCatalogError.placementDestinationClaimed(
                    descriptor.baseID
                )
            }
            return current
        }
        try await clearPlacementClaim(descriptor, owner: owner)
        // Both `remove` implementations of `DirectoryAccess` delete the whole
        // subtree in one transaction, so no separate content clear is needed
        // and an absent Partition is the state a retried finish expects.
        try await removePlacementTenant(
            descriptor.baseID,
            domainID: descriptor.sourceDomainID
        )
        return current
    }

    package func finalizeSuccessfulBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidPlacementMoveOwner
        }
        guard let current = try await baseCatalog.load(
            descriptor.baseID,
            transaction: controlTransaction
        ), current.lifecycle == .retired,
        current.placementID == descriptor.destinationPlacementID,
        current.domainID == descriptor.destinationDomainID,
        current.placementGeneration
            == descriptor.destinationPlacementGeneration else {
            throw DatabaseBaseCatalogError.corruptedRecord(descriptor.baseID)
        }
        try await DatabaseBasePlacementMoveStore(
            controlDomain: storageTopology.controlDomain
        ).remove(
            descriptor.baseID,
            owner: owner,
            transaction: controlTransaction
        )
    }

    /// Converges a cancelled or permanently failed move to a stable retired
    /// authority. Before cutover the source remains authoritative; after
    /// cutover cleanup completes against the destination authority.
    package func prepareUnsuccessfulBasePlacementMoveRecovery(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidPlacementMoveOwner
        }
        let moveStore = DatabaseBasePlacementMoveStore(
            controlDomain: storageTopology.controlDomain
        )
        let current = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                descriptor.baseID,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(descriptor.baseID)
            }
            guard let move = try await moveStore.load(
                descriptor.baseID,
                transaction: transaction.storageAccess
            ), move.owner == owner, move.descriptor == descriptor else {
                throw DatabaseBaseCatalogError.placementDestinationClaimed(
                    descriptor.baseID
                )
            }
            return current
        }
        if current.lifecycle == .retired,
           current.placementID == descriptor.destinationPlacementID,
           current.placementGeneration
            == descriptor.destinationPlacementGeneration {
            let destinationDomain = try placementDomain(
                descriptor.destinationDomainID
            )
            let destinationTenant = try await placementTenant(
                descriptor.baseID,
                in: destinationDomain,
                expecting: descriptor.destinationRootPrefix
            )
            try await removePlacementTenant(
                descriptor.baseID,
                domainID: descriptor.sourceDomainID
            )
            try await clearPlacementClaim(descriptor, owner: owner)
            try publishBaseGeneration(current, tenant: destinationTenant)
            return current
        }

        let sourceRecord: DatabaseBaseRecord
        if current.lifecycle == .moving,
           current.placementID == descriptor.sourcePlacementID,
           current.placementGeneration
            == descriptor.sourcePlacementGeneration,
           current.revision == descriptor.movingRevision {
            try await discardPreparedDestination(descriptor, owner: owner)
            sourceRecord = try await withControlMetadataTransaction(
                configuration: .batch
            ) { transaction in
                guard let latest = try await self.baseCatalog.load(
                    descriptor.baseID,
                    transaction: transaction.storageAccess
                ), latest.lifecycle == .moving,
                latest.placementID == descriptor.sourcePlacementID,
                latest.placementGeneration
                    == descriptor.sourcePlacementGeneration,
                latest.revision == descriptor.movingRevision,
                let move = try await moveStore.load(
                    descriptor.baseID,
                    transaction: transaction.storageAccess
                ), move.owner == owner,
                move.descriptor == descriptor else {
                    throw DatabaseBaseCatalogError.corruptedRecord(
                        descriptor.baseID
                    )
                }
                let revision = try Self.incrementPlacementValue(
                    latest.revision,
                    baseID: latest.id
                )
                return try await self.baseCatalog.replace(
                    DatabaseBaseRecord(
                        id: latest.id,
                        placementID: descriptor.sourcePlacementID,
                        domainID: descriptor.sourceDomainID,
                        placementGeneration:
                            descriptor.sourcePlacementGeneration,
                        revision: revision,
                        lifecycle: .retired
                    ),
                    expectedRecordRevision: latest.revision,
                    transaction: transaction.storageAccess
                )
            }
        } else if current.lifecycle == .retired,
                  current.placementID == descriptor.sourcePlacementID,
                  current.placementGeneration
                    == descriptor.sourcePlacementGeneration {
            sourceRecord = current
            try await discardPreparedDestination(descriptor, owner: owner)
        } else {
            throw DatabaseBaseCatalogError.corruptedRecord(descriptor.baseID)
        }
        let sourceDomain = try placementDomain(descriptor.sourceDomainID)
        let sourceTenant = try await placementTenant(
            descriptor.baseID,
            in: sourceDomain,
            expecting: descriptor.sourceRootPrefix
        )
        try publishBaseGeneration(sourceRecord, tenant: sourceTenant)
        return sourceRecord
    }

    package func finalizeUnsuccessfulBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        guard owner.count == 16 else {
            throw DatabaseBaseCatalogError.invalidPlacementMoveOwner
        }
        guard let current = try await baseCatalog.load(
            descriptor.baseID,
            transaction: controlTransaction
        ) else {
            throw DatabaseBaseCatalogError.baseNotFound(descriptor.baseID)
        }
        let moveStore = DatabaseBasePlacementMoveStore(
            controlDomain: storageTopology.controlDomain
        )
        guard let move = try await moveStore.load(
            descriptor.baseID,
            transaction: controlTransaction
        ), move.owner == owner, move.descriptor == descriptor else {
            throw DatabaseBaseCatalogError.placementDestinationClaimed(
                descriptor.baseID
            )
        }
        let isStableSource = current.lifecycle == .retired
            && current.placementID == descriptor.sourcePlacementID
            && current.placementGeneration
                == descriptor.sourcePlacementGeneration
        let isStableDestination = current.lifecycle == .retired
            && current.placementID == descriptor.destinationPlacementID
            && current.placementGeneration
                == descriptor.destinationPlacementGeneration
        guard isStableSource || isStableDestination else {
            throw DatabaseBaseCatalogError.corruptedRecord(descriptor.baseID)
        }
        try await moveStore.remove(
            descriptor.baseID,
            owner: owner,
            transaction: controlTransaction
        )
    }

    /// Key of the destination claim of one Base move.
    ///
    /// The claim lives in the Default Partition system Directory of the
    /// destination domain, never inside the Base Partition it protects. A
    /// recovery slice removes that Partition, and a marker stored inside it
    /// would disappear with it; a marker stored at the Partition root would
    /// also collide with the content copied into that root.
    private static func placementClaimKey(
        _ id: Base.ID,
        systemRoot: Subspace
    ) -> ByteString {
        systemRoot.subspace("base-placement-claims").pack(Tuple(id.value))
    }

    /// Opens or creates the destination Base Partition and claims it in one
    /// transaction.
    ///
    /// A Partition that already exists without a claim belongs to a live Base,
    /// so the move refuses it rather than merging two Bases at one address.
    private func claimDestinationTenant(
        _ id: Base.ID,
        domainID: DatabaseStorageDomain.ID,
        owner: ByteString
    ) async throws -> DatabaseTenantDirectories {
        let domain = try placementDomain(domainID)
        let access = domain.directoryAccess
        let databaseRoot = domain.databaseRoot
        let claimKey = Self.placementClaimKey(id, systemRoot: domain.systemRoot)
        let name = id.value
        return try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let claim = try await transaction.getValue(
                for: claimKey,
                snapshot: false
            )
            if let claim {
                guard claim == owner else {
                    throw DatabaseBaseCatalogError
                        .placementDestinationClaimed(id)
                }
            } else {
                let existing = try await DatabaseDirectoryLayout.openBaseTenant(
                    name,
                    in: databaseRoot,
                    access: access,
                    transaction: transaction
                )
                guard existing == nil else {
                    throw DatabaseBaseCatalogError
                        .placementDestinationNotEmpty(id)
                }
                try transaction.setValue(owner, for: claimKey)
            }
            return try await DatabaseDirectoryLayout.openOrCreateBaseTenant(
                name,
                in: databaseRoot,
                access: access,
                transaction: transaction
            )
        }
    }

    /// Opens the Base Partition of one side of a move.
    ///
    /// `generation` is the ``Directory/generation`` recorded at prepare. A
    /// Partition removed and recreated at the same address receives a different
    /// generation, so comparing it stops a resumed slice from copying into a
    /// keyspace that is no longer the one the move prepared.
    private func placementTenant(
        _ id: Base.ID,
        in domain: DatabaseStorageDomainRuntime,
        expecting generation: ByteString?
    ) async throws -> DatabaseTenantDirectories {
        guard
            let tenant = try await placementTenantIfPresent(
                id,
                in: domain,
                expecting: generation
            )
        else {
            throw DatabaseBaseExecutionError.placementRootMissing(id)
        }
        return tenant
    }

    /// Opens the Base Partition of one side of a move when that side exists.
    ///
    /// A step that must move or take authority over data requires the
    /// Partition and uses ``placementTenant(_:in:expecting:)``. A step that
    /// only observes the keyspace accepts the absent Partition as the empty
    /// keyspace, because a completed cleanup removes the Partition itself
    /// rather than emptying it.
    private func placementTenantIfPresent(
        _ id: Base.ID,
        in domain: DatabaseStorageDomainRuntime,
        expecting generation: ByteString?
    ) async throws -> DatabaseTenantDirectories? {
        let access = domain.directoryAccess
        let databaseRoot = domain.databaseRoot
        let name = id.value
        let tenant = try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            try await DatabaseDirectoryLayout.openBaseTenant(
                name,
                in: databaseRoot,
                access: access,
                transaction: transaction
            )
        }
        guard let tenant else { return nil }
        if let generation, tenant.partition.root.generation != generation {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
        return tenant
    }

    /// Removes a Base Partition and its whole subtree from one domain.
    ///
    /// An absent Partition is success: a retried cleanup slice observes the
    /// state its predecessor already reached.
    private func removePlacementTenant(
        _ id: Base.ID,
        domainID: DatabaseStorageDomain.ID
    ) async throws {
        let domain = try placementDomain(domainID)
        let access = domain.directoryAccess
        let databaseRoot = domain.databaseRoot
        let name = id.value
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            try await DatabaseDirectoryLayout.removeBaseTenant(
                name,
                in: databaseRoot,
                access: access,
                transaction: transaction
            )
        }
    }

    /// Releases the destination claim of a move that reached its destination.
    private func clearPlacementClaim(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws {
        let domain = try placementDomain(descriptor.destinationDomainID)
        let claimKey = Self.placementClaimKey(
            descriptor.baseID,
            systemRoot: domain.systemRoot
        )
        let baseID = descriptor.baseID
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let recorded = try await transaction.getValue(
                for: claimKey,
                snapshot: false
            )
            guard recorded == nil || recorded == owner else {
                throw DatabaseBaseCatalogError
                    .placementDestinationClaimed(baseID)
            }
            if recorded != nil { try transaction.clear(key: claimKey) }
        }
    }

    /// Discards a destination that never took authority.
    ///
    /// The Partition is removed before the claim so that a slice interrupted
    /// between the two leaves a claimed empty address rather than an unclaimed
    /// half-filled one, which a later prepare would refuse.
    private func discardPreparedDestination(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws {
        try await removePlacementTenant(
            descriptor.baseID,
            domainID: descriptor.destinationDomainID
        )
        try await clearPlacementClaim(descriptor, owner: owner)
    }

    private func placementPage(
        tenant: DatabaseTenantDirectories,
        domain: DatabaseStorageDomainRuntime,
        continuation: ByteString?
    ) async throws -> DatabaseBaseTenantTransfer.Page {
        let access = domain.directoryAccess
        return try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            try await DatabaseBaseTenantTransfer.page(
                tenant: tenant,
                access: access,
                continuation: continuation,
                limit: Self.placementTransferBatchSize,
                transaction: transaction
            )
        }
    }

    private func placementDomain(
        _ id: DatabaseStorageDomain.ID
    ) throws -> DatabaseStorageDomainRuntime {
        guard let domain = storageTopology.domain(identifiedBy: id) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(id)
        }
        return domain
    }

    /// Folds one page into the running transfer digest.
    ///
    /// The digest covers the path of every visited node as well as its rows,
    /// because two sides can hold identical bytes under different structures
    /// once content is recreated through the catalog instead of rebased.
    private static func progress(
        page: DatabaseBaseTenantTransfer.Page,
        digest: ByteString?,
        keyCount: UInt64,
        byteCount: UInt64
    ) throws -> DatabaseBasePlacementTransferProgress {
        if let digest, digest.count != 32 {
            throw DatabaseBaseCatalogError.placementDigestMismatch(nil)
        }
        var nextDigest = digest ?? ByteString(repeating: 0, count: 32)
        var nextKeyCount = keyCount
        var nextByteCount = byteCount
        for node in page.nodes {
            let path = Tuple(node.path.map { $0 as any TupleElement }).pack()
            // A node contributes its path once even when it holds no rows, so
            // a destination missing an empty Directory diverges here instead
            // of verifying as equal. `resumed` keeps a node that spans several
            // pages contributing that path exactly once.
            if !node.resumed {
                var header = SHA256Accumulator()
                nextDigest.withUnsafeBytes { header.update($0) }
                update(UInt64(path.count), accumulator: &header)
                path.withUnsafeBytes { header.update($0) }
                nextDigest = header.finalize()
            }
            for row in node.rows {
                var accumulator = SHA256Accumulator()
                nextDigest.withUnsafeBytes { accumulator.update($0) }
                update(UInt64(path.count), accumulator: &accumulator)
                path.withUnsafeBytes { accumulator.update($0) }
                update(UInt64(row.suffix.count), accumulator: &accumulator)
                row.suffix.withUnsafeBytes { accumulator.update($0) }
                update(UInt64(row.value.count), accumulator: &accumulator)
                row.value.withUnsafeBytes { accumulator.update($0) }
                nextDigest = accumulator.finalize()
                nextKeyCount = try adding(nextKeyCount, 1)
                nextByteCount = try adding(
                    nextByteCount,
                    try adding(
                        UInt64(row.suffix.count),
                        UInt64(row.value.count)
                    )
                )
            }
        }
        return DatabaseBasePlacementTransferProgress(
            continuation: page.continuation,
            digest: nextDigest,
            keyCount: nextKeyCount,
            byteCount: nextByteCount
        )
    }

    private static func update(
        _ value: UInt64,
        accumulator: inout SHA256Accumulator
    ) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) { accumulator.update($0) }
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw DatabaseBaseCatalogError.placementTransferOverflow
        }
        return result.partialValue
    }

    private static func incrementPlacementValue(
        _ value: UInt64,
        baseID: Base.ID
    ) throws -> UInt64 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw DatabaseBaseCatalogError.corruptedRecord(baseID)
        }
        return result.partialValue
    }
}

#endif

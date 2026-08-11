#if DATABASE_MULTIPLE_BASES
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
                let destinationPath = destinationDomain.namespacePath
                    + destinationPlacement.path
                    + [id.value]
                guard current.placementID != destinationPlacementID else {
                    throw DatabaseBaseCatalogError
                        .placementAlreadySelected(destinationPlacementID)
                }
                guard current.domainID != destinationDomain.id
                        || current.namespacePath != destinationPath else {
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
                    sourceNamespacePath: current.namespacePath,
                    sourcePlacementGeneration: current.placementGeneration,
                    movingRevision: revision,
                    destinationPlacementID: destinationPlacementID,
                    destinationDomainID: destinationDomain.id,
                    destinationNamespacePath: destinationPath,
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
                        namespacePath: current.namespacePath,
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
                stored.descriptor.sourceNamespacePath == current.namespacePath,
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

        let sourceRoot = try await placementRoot(
            domainID: descriptorIntent.sourceDomainID,
            namespacePath: descriptorIntent.sourceNamespacePath,
            create: false
        )
        try publishBaseGeneration(moving, root: sourceRoot)
        try await stopBaseAdmissionAndDrain(id)

        let destinationRoot = try await placementRoot(
            domainID: descriptorIntent.destinationDomainID,
            namespacePath: descriptorIntent.destinationNamespacePath,
            create: true
        )
        let destinationExecutor = try placementDomain(
            descriptorIntent.destinationDomainID
        ).transactionExecutor
        try await destinationExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            if let existingOwner = try await transaction.getValue(
                for: destinationRoot.prefix,
                snapshot: false
            ) {
                guard existingOwner == owner else {
                    throw DatabaseBaseCatalogError
                        .placementDestinationClaimed(id)
                }
                return
            }
            let range = destinationRoot.range()
            let existing = try await TransactionRangeCollection.collect(
                using: transaction,
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: 1,
                reverse: false,
                snapshot: false,
                streamingMode: .small
            )
            guard existing.isEmpty else {
                throw DatabaseBaseCatalogError
                    .placementDestinationNotEmpty(id)
            }
            try transaction.setValue(owner, for: destinationRoot.prefix)
        }
        let descriptor = DatabaseBasePlacementMoveDescriptor(
            baseID: descriptorIntent.baseID,
            sourcePlacementID: descriptorIntent.sourcePlacementID,
            sourceDomainID: descriptorIntent.sourceDomainID,
            sourceNamespacePath: descriptorIntent.sourceNamespacePath,
            sourcePlacementGeneration:
                descriptorIntent.sourcePlacementGeneration,
            movingRevision: descriptorIntent.movingRevision,
            destinationPlacementID: descriptorIntent.destinationPlacementID,
            destinationDomainID: descriptorIntent.destinationDomainID,
            destinationNamespacePath: descriptorIntent.destinationNamespacePath,
            destinationPlacementGeneration:
                descriptorIntent.destinationPlacementGeneration,
            sourceRootPrefix: sourceRoot.prefix,
            destinationRootPrefix: destinationRoot.prefix
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
        let sourceRoot = try Self.preparedRoot(
            descriptor.sourceRootPrefix,
            baseID: descriptor.baseID
        )
        let destinationRoot = try Self.preparedRoot(
            descriptor.destinationRootPrefix,
            baseID: descriptor.baseID
        )
        let page = try await placementPage(
            root: sourceRoot,
            domainID: descriptor.sourceDomainID,
            continuation: continuation
        )
        let destinationExecutor = try placementDomain(
            descriptor.destinationDomainID
        ).transactionExecutor
        try await destinationExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            for (key, value) in page.rows {
                let destinationKey = try Self.rebase(
                    key,
                    from: sourceRoot,
                    to: destinationRoot
                )
                if let existing = try await transaction.getValue(
                    for: destinationKey,
                    snapshot: false
                ) {
                    guard existing == value else {
                        throw DatabaseBaseCatalogError
                            .placementDigestMismatch(descriptor.baseID)
                    }
                } else {
                    try transaction.setValue(value, for: destinationKey)
                }
            }
        }
        return try Self.progress(
            rows: page.rows,
            root: sourceRoot,
            continuation: page.continuation,
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
        let domainID = destination
            ? descriptor.destinationDomainID
            : descriptor.sourceDomainID
        let root = try Self.preparedRoot(
            destination
                ? descriptor.destinationRootPrefix
                : descriptor.sourceRootPrefix,
            baseID: descriptor.baseID
        )
        let page = try await placementPage(
            root: root,
            domainID: domainID,
            continuation: continuation
        )
        return try Self.progress(
            rows: page.rows,
            root: root,
            continuation: page.continuation,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount
        )
    }

    package func cutOverBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor
    ) async throws -> DatabaseBaseRecord {
        let destinationRoot = try Self.preparedRoot(
            descriptor.destinationRootPrefix,
            baseID: descriptor.baseID
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
                  current.namespacePath == descriptor.sourceNamespacePath,
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
                    namespacePath: descriptor.destinationNamespacePath,
                    placementGeneration:
                        descriptor.destinationPlacementGeneration,
                    revision: revision,
                    lifecycle: .retired
                ),
                expectedRecordRevision: current.revision,
                transaction: transaction.storageAccess
            )
        }
        try publishBaseGeneration(record, root: destinationRoot)
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
                  current.namespacePath == descriptor.destinationNamespacePath,
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
        let destinationRoot = try Self.preparedRoot(
            descriptor.destinationRootPrefix,
            baseID: descriptor.baseID
        )
        let destinationDomain = try placementDomain(
            descriptor.destinationDomainID
        )
        try await destinationDomain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let recordedOwner = try await transaction.getValue(
                for: destinationRoot.prefix,
                snapshot: false
            )
            guard recordedOwner == nil || recordedOwner == owner else {
                throw DatabaseBaseCatalogError.placementDestinationClaimed(
                    descriptor.baseID
                )
            }
            if recordedOwner != nil {
                try transaction.clear(key: destinationRoot.prefix)
            }
        }

        let sourceDomain = try placementDomain(descriptor.sourceDomainID)
        if try await sourceDomain.engine.namespaceExists(
            path: descriptor.sourceNamespacePath
        ) {
            let sourceRoot = try await sourceDomain.engine
                .resolveExistingNamespace(path: descriptor.sourceNamespacePath)
            try await sourceDomain.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                let range = sourceRoot.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
            if sourceDomain.engine.namespaceCatalog != nil {
                try await sourceDomain.engine.removeNamespace(
                    path: descriptor.sourceNamespacePath
                )
            }
        }
        return current
    }

    /// Removes the move record only in the transaction that also commits the
    /// owning persistent job's successful result. Until then a repeated
    /// cleanup slice can prove ownership and converge after a crash.
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
        current.namespacePath == descriptor.destinationNamespacePath,
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
            try await clearPreparedPlacementRoot(
                prefix: descriptor.sourceRootPrefix,
                domainID: descriptor.sourceDomainID,
                clearExactRootKey: false
            )
            try await clearPlacementOwner(
                descriptor,
                owner: owner
            )
            try publishBaseGeneration(
                current,
                root: try Self.preparedRoot(
                    descriptor.destinationRootPrefix,
                    baseID: descriptor.baseID
                )
            )
            return current
        }

        let sourceRecord: DatabaseBaseRecord
        if current.lifecycle == .moving,
           current.placementID == descriptor.sourcePlacementID,
           current.placementGeneration
            == descriptor.sourcePlacementGeneration,
           current.revision == descriptor.movingRevision {
            try await clearPreparedPlacementRoot(
                prefix: descriptor.destinationRootPrefix,
                domainID: descriptor.destinationDomainID,
                clearExactRootKey: true
            )
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
                        namespacePath: descriptor.sourceNamespacePath,
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
            try await clearPreparedPlacementRoot(
                prefix: descriptor.destinationRootPrefix,
                domainID: descriptor.destinationDomainID,
                clearExactRootKey: true
            )
        } else {
            throw DatabaseBaseCatalogError.corruptedRecord(descriptor.baseID)
        }
        try publishBaseGeneration(
            sourceRecord,
            root: try Self.preparedRoot(
                descriptor.sourceRootPrefix,
                baseID: descriptor.baseID
            )
        )
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

    private func clearPlacementOwner(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws {
        let root = try Self.preparedRoot(
            descriptor.destinationRootPrefix,
            baseID: descriptor.baseID
        )
        let domain = try placementDomain(descriptor.destinationDomainID)
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let recorded = try await transaction.getValue(
                for: root.prefix,
                snapshot: false
            )
            guard recorded == nil || recorded == owner else {
                throw DatabaseBaseCatalogError.placementDestinationClaimed(
                    descriptor.baseID
                )
            }
            if recorded != nil { try transaction.clear(key: root.prefix) }
        }
    }

    private func clearPreparedPlacementRoot(
        prefix: ByteString?,
        domainID: DatabaseStorageDomain.ID,
        clearExactRootKey: Bool
    ) async throws {
        let root = try Self.preparedRoot(prefix, baseID: nil)
        let clear: @Sendable (any TransactionAccess) async throws -> Void = {
            transaction in
            let range = root.range()
            try transaction.clearRange(
                beginKey: range.begin,
                endKey: range.end
            )
            if clearExactRootKey {
                try transaction.clear(key: root.prefix)
            }
        }
        let domain = try placementDomain(domainID)
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock,
            clear
        )
    }

    private func placementPage(
        root: Subspace,
        domainID: DatabaseStorageDomain.ID,
        continuation: ByteString?
    ) async throws -> (
        rows: [(ByteString, ByteString)],
        continuation: ByteString?
    ) {
        let range = root.range()
        let begin: KeySelector
        if let continuation {
            guard root.contains(continuation) else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            begin = .firstGreaterThan(continuation)
        } else {
            begin = .firstGreaterOrEqual(range.begin)
        }
        let domain = try placementDomain(domainID)
        return try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            let rows = try await TransactionRangeCollection.collect(
                using: transaction,
                from: begin,
                to: .firstGreaterOrEqual(range.end),
                limit: Self.placementTransferBatchSize + 1,
                reverse: false,
                snapshot: false,
                streamingMode: .iterator
            )
            let visible = Array(rows.prefix(Self.placementTransferBatchSize))
            return (
                visible,
                rows.count > Self.placementTransferBatchSize
                    ? visible.last?.0
                    : nil
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

    private func placementRoot(
        domainID: DatabaseStorageDomain.ID,
        namespacePath: [String],
        create: Bool
    ) async throws -> Subspace {
        let domain = try placementDomain(domainID)
        if create {
            return try await domain.engine.resolveOrCreateNamespace(
                path: namespacePath
            )
        }
        return try await domain.engine.resolveExistingNamespace(
            path: namespacePath
        )
    }

    private static func progress(
        rows: [(ByteString, ByteString)],
        root: Subspace,
        continuation: ByteString?,
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
        for (key, value) in rows {
            guard root.contains(key) else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            let suffix = key[
                (key.startIndex + root.prefix.count)..<key.endIndex
            ]
            var accumulator = SHA256Accumulator()
            nextDigest.withUnsafeBytes { accumulator.update($0) }
            update(UInt64(suffix.count), accumulator: &accumulator)
            suffix.withUnsafeBytes { accumulator.update($0) }
            update(UInt64(value.count), accumulator: &accumulator)
            value.withUnsafeBytes { accumulator.update($0) }
            nextDigest = accumulator.finalize()
            nextKeyCount = try adding(nextKeyCount, 1)
            nextByteCount = try adding(
                nextByteCount,
                try adding(UInt64(suffix.count), UInt64(value.count))
            )
        }
        return DatabaseBasePlacementTransferProgress(
            continuation: continuation,
            digest: nextDigest,
            keyCount: nextKeyCount,
            byteCount: nextByteCount
        )
    }

    private static func rebase(
        _ key: ByteString,
        from source: Subspace,
        to destination: Subspace
    ) throws -> ByteString {
        guard source.contains(key) else {
            throw DatabaseBaseCatalogError.corruptedRecord(nil)
        }
        return destination.prefix.appending(
            contentsOf: key[
                (key.startIndex + source.prefix.count)..<key.endIndex
            ]
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

    private static func preparedRoot(
        _ prefix: ByteString?,
        baseID: Base.ID?
    ) throws -> Subspace {
        guard let prefix, !prefix.isEmpty else {
            throw DatabaseBaseCatalogError.corruptedRecord(baseID)
        }
        return Subspace(prefix: prefix)
    }
}

#endif

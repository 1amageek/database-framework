import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

extension DBContainer {
    private static let legacyTransferBatchSize = 16
    private static let legacyDigestByteCount = 32

    /// Discovers every authoritative layout-v1 range without creating legacy
    /// namespace metadata. The resulting order is canonical and therefore also
    /// defines the layout fingerprint and resumable copy order.
    package func legacyLayoutInventory(
        allowCurrentLayout: Bool = false
    )
        async throws -> DatabaseLegacyLayoutInventory {
        guard layoutStatus == .migrationRequired || allowCurrentLayout else {
            throw DatabaseLegacyLayoutMigrationError.layoutIsCurrent
        }
        let sourceEngine = storageTopology.controlDomain.engine
        var entries: [DatabaseLegacyLayoutInventory.Entry] = []
        var cleanupPaths: [[String]] = []

        func destinationSuffix(_ components: [String]) -> ByteString {
            var root = Subspace().subspace("data")
            for component in components {
                root = root.subspace(component)
            }
            return root.prefix
        }

        func appendNamespaceRange(
            identifier: String,
            sourcePath: [String],
            sourceChild: String? = nil,
            destinationSuffix explicitDestinationSuffix: ByteString? = nil,
            destinationComponents: [String] = []
        ) async throws {
            guard try await sourceEngine.namespaceExists(path: sourcePath)
            else { return }
            var source = try await sourceEngine.resolveExistingNamespace(
                path: sourcePath
            )
            if let sourceChild {
                source = source.subspace(sourceChild)
            }
            entries.append(
                DatabaseLegacyLayoutInventory.Entry(
                    identifier: identifier,
                    sourceRoot: source,
                    destinationSuffix: explicitDestinationSuffix
                        ?? destinationSuffix(destinationComponents)
                )
            )
            cleanupPaths.append(sourcePath)
        }

        let legacyJobPath = ["database-framework", "persistent-jobs"]
        if try await sourceEngine.namespaceExists(path: legacyJobPath) {
            let legacyJobs = try await sourceEngine.resolveExistingNamespace(
                path: legacyJobPath
            )
            if try await containsAnyValue(
                root: legacyJobs,
                domain: storageTopology.controlDomain
            ) {
                throw DatabaseLegacyLayoutMigrationError.legacyJobsPresent
            }
            cleanupPaths.append(legacyJobPath)
        }

        let legacyPartitionPath = ["database-framework", "partition-catalog"]
        var dynamicDirectories: [(entity: Schema.Entity, partitions: FieldObject)] = []
        if try await sourceEngine.namespaceExists(path: legacyPartitionPath) {
            let catalogRoot = try await sourceEngine.resolveExistingNamespace(
                path: legacyPartitionPath
            )
            let catalog = DatabasePartitionCatalog(
                legacyEngine: sourceEngine,
                resolvedCatalogRoot: catalogRoot,
                clock: monotonicClock
            )
            var continuation: ByteString?
            repeat {
                let page = try await catalog.page(
                    continuation: continuation,
                    limit: 256
                )
                for entry in page.entries {
                    guard let entity = schema.entities.first(where: {
                        $0.name == entry.entity && $0.hasDynamicDirectory
                    }) else {
                        throw DatabaseLegacyLayoutMigrationError
                            .unknownPartitionEntity(entry.entity)
                    }
                    dynamicDirectories.append((entity, entry.partitions))
                }
                continuation = page.continuation
            } while continuation != nil

            entries.append(
                DatabaseLegacyLayoutInventory.Entry(
                    identifier: "partition-catalog",
                    sourceRoot: catalogRoot,
                    destinationSuffix: destinationSuffix([
                        "database-framework", "partition-catalog",
                    ])
                )
            )
            cleanupPaths.append(legacyPartitionPath)
        }

        var entityDirectories: [(identifier: String, path: [String])] = []
        for entity in schema.entities.sorted(by: { $0.name < $1.name }) {
            if entity.hasDynamicDirectory {
                for dynamic in dynamicDirectories
                where dynamic.entity.name == entity.name {
                    let path = try AnyDirectoryPath(
                        entity: entity,
                        partitions: dynamic.partitions
                    ).resolve()
                    entityDirectories.append((
                        identifier: "entity.\(entity.name).\(path.joined(separator: "."))",
                        path: path
                    ))
                }
            } else {
                let path = try AnyDirectoryPath(for: entity).resolve()
                entityDirectories.append((
                    identifier: "entity.\(entity.name)",
                    path: path
                ))
            }
        }
        for group in schema.polymorphicGroups.sorted(by: {
            $0.identifier < $1.identifier
        }) {
            entityDirectories.append((
                identifier: "polymorphic.\(group.identifier)",
                path: try group.resolvedDirectoryPath()
            ))
        }

        for directory in entityDirectories {
            try await appendNamespaceRange(
                identifier: "\(directory.identifier).items",
                sourcePath: directory.path,
                sourceChild: SubspaceKey.items,
                destinationComponents: directory.path + [SubspaceKey.items]
            )
            try await appendNamespaceRange(
                identifier: "\(directory.identifier).blobs",
                sourcePath: directory.path,
                sourceChild: SubspaceKey.blobs,
                destinationComponents: directory.path + [SubspaceKey.blobs]
            )
        }

        try await appendNamespaceRange(
            identifier: "operation-state",
            sourcePath: ["database-framework", "wire-runtime"],
            destinationSuffix: Subspace().subspace("operation-state").prefix
        )
        let legacyOntologyPath = ["database-framework", "ontology-index"]
        var hasNamespacedOntology = false
        if try await sourceEngine.namespaceExists(path: legacyOntologyPath) {
            let root = try await sourceEngine.resolveExistingNamespace(
                path: legacyOntologyPath
            )
            hasNamespacedOntology = try await containsAnyValue(
                root: root,
                domain: storageTopology.controlDomain
            )
            if hasNamespacedOntology {
                try await appendNamespaceRange(
                    identifier: "ontology-index",
                    sourcePath: legacyOntologyPath,
                    destinationComponents: [
                        "database-framework", "ontology-index",
                    ]
                )
            } else {
                cleanupPaths.append(legacyOntologyPath)
            }
        }
        for namespace in ["ontology", "shacl"] {
            try await appendNamespaceRange(
                identifier: "rdf-document.\(namespace)",
                sourcePath: [
                    "database-framework", "rdf-documents", namespace,
                ],
                destinationComponents: [
                    "database-framework", "rdf-documents", namespace,
                ]
            )
        }

        let dataRoot = Subspace().subspace("data")
        let rawOntologyRoot = Subspace(prefix: ByteString(utf8: "O"))
        let hasRawOntology = try await containsAnyValue(
            root: rawOntologyRoot,
            domain: storageTopology.controlDomain
        )
        guard !(hasNamespacedOntology && hasRawOntology) else {
            throw DatabaseLegacyLayoutMigrationError
                .conflictingLegacyOntologyStores
        }
        let rawMappings: [(
            identifier: String,
            source: Subspace,
            destinationSuffix: ByteString
        )] = [
            (
                "rdf-graph-store",
                Subspace(prefix: Tuple([
                    "_database-framework", "rdf-graph-store", Int64(1),
                ]).pack()),
                dataRoot
                    .subspace("_database-framework")
                    .subspace("rdf-graph-store")
                    .subspace(Int64(1)).prefix
            ),
            (
                "property-graph-definitions",
                Subspace(prefix: Tuple([
                    "_database-framework",
                    "property-graph-definition-catalog",
                    Int64(1),
                ]).pack()),
                dataRoot
                    .subspace("_database-framework")
                    .subspace("property-graph-definition-catalog")
                    .subspace(Int64(1)).prefix
            ),
            (
                "shacl-context",
                Subspace(prefix: ByteString(utf8: "S")),
                dataRoot.subspace(ByteString(utf8: "S")).prefix
            ),
        ] + (hasRawOntology ? [(
            "ontology-context",
            rawOntologyRoot,
            dataRoot
                .subspace("database-framework")
                .subspace("ontology-index").prefix
        )] : [])
        for mapping in rawMappings {
            entries.append(
                DatabaseLegacyLayoutInventory.Entry(
                    identifier: mapping.identifier,
                    sourceRoot: mapping.source,
                    destinationSuffix: mapping.destinationSuffix
                )
            )
        }

        let inventory = try DatabaseLegacyLayoutInventory(
            entries: entries,
            cleanupNamespacePaths: cleanupPaths + [
                ["_metadata"],
                ["_metadata", "statistics"],
            ],
            cleanupRawRoots: [
                Subspace().subspace("_schema"),
                Subspace().subspace("_database-framework"),
                Subspace(prefix: ByteString(utf8: "O")),
                Subspace(prefix: ByteString(utf8: "S")),
            ]
        )
        let controlRoot = storageTopology.controlDomain.root
        for entry in inventory.entries where Self.rootsOverlap(
            entry.sourceRoot,
            controlRoot
        ) {
            throw DatabaseLegacyLayoutMigrationError
                .controlDomainOverlapsLegacyData
        }
        for root in inventory.cleanupRawRoots where Self.rootsOverlap(
            root,
            controlRoot
        ) {
            throw DatabaseLegacyLayoutMigrationError
                .controlDomainOverlapsLegacyData
        }
        return inventory
    }

    package func legacyLayoutFingerprint(
        inventory: DatabaseLegacyLayoutInventory
    ) async throws -> ByteString {
        var progress = DatabaseLegacyLayoutTransferProgress(
            entryIndex: 0,
            continuation: nil,
            digest: ByteString(repeating: 0, count: Self.legacyDigestByteCount),
            keyCount: 0,
            byteCount: 0,
            isComplete: inventory.entries.isEmpty
        )
        while !progress.isComplete {
            progress = try await scanLegacyLayoutBatch(
                inventory: inventory,
                destinationBaseRoot: nil,
                destinationDomainID: nil,
                mode: .source,
                progress: progress
            )
        }
        return progress.digest
    }

    package func prepareLegacyMigrationBase(
        _ id: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedRevision: UInt64
    ) async throws -> (record: DatabaseBaseRecord, root: Subspace) {
        guard layoutStatus == .migrationRequired else {
            throw DatabaseLegacyLayoutMigrationError.layoutIsCurrent
        }
        guard !initialGrants.isEmpty,
              initialGrants.allSatisfy({ $0.resource == .base(id) }),
              initialGrants.contains(where: {
                  $0.access.contains(.administer)
              }) else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: .base(id),
                required: .administer
            )
        }
        guard let placement = storageTopology.placement(
            identifiedBy: placementID
        ) else {
            throw DatabaseBaseCatalogError.placementNotFound(placementID)
        }
        guard let domain = storageTopology.domain(
            identifiedBy: placement.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(
                placement.domainID
            )
        }
        let record = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            if let existing = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) {
                guard existing.lifecycle == .provisioning,
                      existing.placementID == placementID,
                      existing.revision == 1 else {
                    throw DatabaseLegacyLayoutMigrationError
                        .destinationBaseExists(id)
                }
                return existing
            }
            return try await self.baseCatalog.insertProvisioning(
                id: id,
                placement: placement,
                domain: domain,
                expectedRevision: expectedRevision,
                transaction: transaction.storageAccess
            )
        }
        let root = try await domain.engine.resolveOrCreateNamespace(
            path: record.namespacePath
        )
        let grantStore = DatabaseGrantStore(
            resource: .base(id),
            root: root
        )
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let existing = try await grantStore.direct(
                transaction: transaction
            )
            if existing.revision == 0 {
                try await grantStore.installInitial(
                    initialGrants,
                    transaction: transaction
                )
            }
        }
        return (record, root)
    }

    package enum LegacyLayoutTransferMode: Sendable {
        case source
        case copy
        case destination
    }

    package func scanLegacyLayoutBatch(
        inventory: DatabaseLegacyLayoutInventory,
        destinationBaseRoot: Subspace?,
        destinationDomainID: DatabaseStorageDomain.ID?,
        mode: LegacyLayoutTransferMode,
        progress: DatabaseLegacyLayoutTransferProgress
    ) async throws -> DatabaseLegacyLayoutTransferProgress {
        guard progress.digest.count == Self.legacyDigestByteCount,
              progress.entryIndex >= 0,
              progress.entryIndex <= inventory.entries.count else {
            throw DatabaseLegacyLayoutMigrationError.invalidTransferState
        }
        guard progress.entryIndex < inventory.entries.count else {
            return DatabaseLegacyLayoutTransferProgress(
                entryIndex: progress.entryIndex,
                continuation: nil,
                digest: progress.digest,
                keyCount: progress.keyCount,
                byteCount: progress.byteCount,
                isComplete: true
            )
        }
        let entry = inventory.entries[progress.entryIndex]
        let scanRoot: Subspace
        let scanDomain: DatabaseStorageDomainRuntime
        switch mode {
        case .source, .copy:
            scanRoot = entry.sourceRoot
            scanDomain = storageTopology.controlDomain
        case .destination:
            guard let destinationBaseRoot,
                  let destinationDomainID,
                  let domain = storageTopology.domain(
                      identifiedBy: destinationDomainID
                  ) else {
                throw DatabaseLegacyLayoutMigrationError.invalidTransferState
            }
            scanRoot = entry.destinationRoot(in: destinationBaseRoot)
            scanDomain = domain
        }

        let page = try await legacyPage(
            root: scanRoot,
            domain: scanDomain,
            continuation: progress.continuation
        )
        if case .copy = mode {
            guard let destinationBaseRoot else {
                throw DatabaseLegacyLayoutMigrationError.invalidTransferState
            }
            let destinationRoot = entry.destinationRoot(in: destinationBaseRoot)
            guard let destinationDomainID,
                  let destinationDomain = storageTopology.domain(
                      identifiedBy: destinationDomainID
                  ) else {
                throw DatabaseLegacyLayoutMigrationError.invalidTransferState
            }
            try await destinationDomain.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                for (key, value) in page.rows {
                    let destinationKey = try Self.rebaseLegacyKey(
                        key,
                        from: scanRoot,
                        to: destinationRoot
                    )
                    if let existing = try await transaction.getValue(
                        for: destinationKey,
                        snapshot: false
                    ) {
                        guard existing == value else {
                            throw DatabaseLegacyLayoutMigrationError
                                .destinationDigestMismatch
                        }
                    } else {
                        try transaction.setValue(value, for: destinationKey)
                    }
                }
            }
        }
        let updated = try Self.updateLegacyProgress(
            entry: entry,
            scanRoot: scanRoot,
            rows: page.rows,
            continuation: page.continuation,
            previous: progress
        )
        if page.continuation != nil { return updated }
        let nextIndex = progress.entryIndex + 1
        return DatabaseLegacyLayoutTransferProgress(
            entryIndex: nextIndex,
            continuation: nil,
            digest: updated.digest,
            keyCount: updated.keyCount,
            byteCount: updated.byteCount,
            isComplete: nextIndex == inventory.entries.count
        )
    }

    package func rebuildAndCutOverLegacyMigration(
        record: DatabaseBaseRecord,
        root: Subspace
    ) async throws -> DatabaseBaseRecord {
        if record.lifecycle == .active, layoutStatus == .current {
            return record
        }
        guard record.lifecycle == .provisioning else {
            throw DatabaseLegacyLayoutMigrationError.invalidTransferState
        }
        guard let domain = storageTopology.domain(
            identifiedBy: record.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(record.domainID)
        }
        let provisional = DatabaseBaseRecord(
            id: record.id,
            placementID: record.placementID,
            domainID: record.domainID,
            namespacePath: record.namespacePath,
            placementGeneration: record.placementGeneration,
            revision: record.revision,
            lifecycle: .active
        )
        let lease = DatabaseBaseLease(
            generation: DatabaseBaseGeneration(
                record: provisional,
                domain: domain,
                root: root
            ),
            token: DatabaseBaseLeaseToken(finishOperation: {})
        )
        try await withBaseLease(lease) {
            try await self.ensureIndexesReady()
        }
        let active = try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            guard let current = try await self.baseCatalog.load(
                record.id,
                transaction: transaction.storageAccess
            ), current.lifecycle == .provisioning,
               current.revision == record.revision else {
                throw DatabaseLegacyLayoutMigrationError.invalidTransferState
            }
            let nextRevision = current.revision + 1
            let published = try await self.baseCatalog.replace(
                DatabaseBaseRecord(
                    id: current.id,
                    placementID: current.placementID,
                    domainID: current.domainID,
                    namespacePath: current.namespacePath,
                    placementGeneration: current.placementGeneration,
                    revision: nextRevision,
                    lifecycle: .active
                ),
                expectedRecordRevision: current.revision,
                transaction: transaction.storageAccess
            )
            try await self.layoutCatalog.markCurrent(
                transaction: transaction.storageAccess
            )
            return published
        }
        layoutStatusStorage.withLock { $0 = .current }
        try publishBaseGeneration(active, root: root)
        return active
    }

    package func legacyMigrationBase(
        _ id: Base.ID
    ) async throws -> (record: DatabaseBaseRecord, root: Subspace) {
        let record = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let record = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            return record
        }
        guard let domain = storageTopology.domain(
            identifiedBy: record.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(record.domainID)
        }
        let root = try await domain.engine.resolveExistingNamespace(
            path: record.namespacePath
        )
        return (record, root)
    }

    package func validateLegacyMigrationDestination(
        _ destinationRoot: Subspace,
        inventory: DatabaseLegacyLayoutInventory
    ) async throws {
        for entry in inventory.entries where Self.rootsOverlap(
            entry.sourceRoot,
            destinationRoot
        ) {
            throw DatabaseLegacyLayoutMigrationError
                .destinationOverlapsLegacyData
        }
        for root in inventory.cleanupRawRoots where Self.rootsOverlap(
            root,
            destinationRoot
        ) {
            throw DatabaseLegacyLayoutMigrationError
                .destinationOverlapsLegacyData
        }
        let engine = storageTopology.controlDomain.engine
        for path in inventory.cleanupNamespacePaths {
            guard try await engine.namespaceExists(path: path) else { continue }
            let root = try await engine.resolveExistingNamespace(path: path)
            if Self.rootsOverlap(root, destinationRoot) {
                throw DatabaseLegacyLayoutMigrationError
                    .destinationOverlapsLegacyData
            }
        }
    }

    /// Restores the pre-cutover authority after cancellation or a permanent
    /// failure. Only an unpublished provisioning Base may be removed.
    package func abortLegacyMigrationBase(
        _ id: Base.ID
    ) async throws {
        let existing = try await withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            )
        }
        guard let existing else { return }
        guard existing.lifecycle == .provisioning else {
            if existing.lifecycle == .active, layoutStatus == .current {
                return
            }
            throw DatabaseLegacyLayoutMigrationError.invalidTransferState
        }
        guard let domain = storageTopology.domain(
            identifiedBy: existing.domainID
        ) else {
            throw DatabaseBaseCatalogError.storageDomainNotFound(
                existing.domainID
            )
        }
        if try await domain.engine.namespaceExists(path: existing.namespacePath) {
            let root = try await domain.engine.resolveExistingNamespace(
                path: existing.namespacePath
            )
            try await domain.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: monotonicClock
            ) { transaction in
                let range = root.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
            if domain.engine.namespaceCatalog != nil {
                try await domain.engine.removeNamespace(
                    path: existing.namespacePath
                )
            }
        }
        try await withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await self.baseCatalog.removeProvisioning(
                id,
                expectedRevision: existing.revision,
                transaction: transaction.storageAccess
            )
        }
    }

    package func cleanupLegacyLayout(
        inventory: DatabaseLegacyLayoutInventory
    ) async throws {
        let source = storageTopology.controlDomain
        var namespaceRoots: [Subspace] = []
        namespaceRoots.reserveCapacity(inventory.cleanupNamespacePaths.count)
        for path in inventory.cleanupNamespacePaths {
            guard try await source.engine.namespaceExists(path: path) else {
                continue
            }
            namespaceRoots.append(
                try await source.engine.resolveExistingNamespace(path: path)
            )
        }
        let resolvedNamespaceRoots = namespaceRoots
        try await source.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            for root in inventory.cleanupRawRoots {
                let range = root.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
            for entry in inventory.entries {
                let range = entry.sourceRoot.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
            for root in resolvedNamespaceRoots {
                let range = root.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
        }
        if source.engine.namespaceCatalog != nil {
            for path in inventory.cleanupNamespacePaths.reversed() {
                guard try await source.engine.namespaceExists(path: path) else {
                    continue
                }
                do {
                    try await source.engine.removeNamespace(path: path)
                } catch {
                    throw DatabaseLegacyLayoutMigrationError.cleanupFailed(
                        path.joined(separator: "/")
                    )
                }
            }
        }
    }

    private func containsAnyValue(
        root: Subspace,
        domain: DatabaseStorageDomainRuntime
    ) async throws -> Bool {
        try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            let range = root.range()
            return !(try await TransactionRangeCollection.collect(
                using: transaction,
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: 1,
                reverse: false,
                snapshot: true,
                streamingMode: .small
            )).isEmpty
        }
    }

    private func legacyPage(
        root: Subspace,
        domain: DatabaseStorageDomainRuntime,
        continuation: ByteString?
    ) async throws -> (
        rows: [(ByteString, ByteString)],
        continuation: ByteString?
    ) {
        try await domain.transactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock
        ) { transaction in
            let range = root.range()
            let begin: KeySelector
            if let continuation {
                guard root.contains(continuation) else {
                    throw DatabaseLegacyLayoutMigrationError
                        .invalidTransferState
                }
                begin = .firstGreaterThan(continuation)
            } else {
                begin = .firstGreaterOrEqual(range.begin)
            }
            // A durable migration checkpoint owns this bounded batch. The
            // materialization is the intentional ownership boundary between
            // the source read transaction and the destination write transaction.
            let rows = try await TransactionRangeCollection.collect(
                using: transaction,
                from: begin,
                to: .firstGreaterOrEqual(range.end),
                limit: Self.legacyTransferBatchSize + 1,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            let visible = Array(rows.prefix(Self.legacyTransferBatchSize))
            return (
                visible,
                rows.count > Self.legacyTransferBatchSize
                    ? visible.last?.0
                    : nil
            )
        }
    }

    private static func updateLegacyProgress(
        entry: DatabaseLegacyLayoutInventory.Entry,
        scanRoot: Subspace,
        rows: [(ByteString, ByteString)],
        continuation: ByteString?,
        previous: DatabaseLegacyLayoutTransferProgress
    ) throws -> DatabaseLegacyLayoutTransferProgress {
        var digest = previous.digest
        if previous.continuation == nil {
            var accumulator = SHA256Accumulator()
            digest.withUnsafeBytes { accumulator.update($0) }
            let identifier = ByteString(utf8: entry.identifier)
            updateLegacyLength(UInt64(identifier.count), into: &accumulator)
            identifier.withUnsafeBytes { accumulator.update($0) }
            digest = accumulator.finalize()
        }
        var keyCount = previous.keyCount
        var byteCount = previous.byteCount
        for (key, value) in rows {
            guard scanRoot.contains(key) else {
                throw DatabaseLegacyLayoutMigrationError.invalidInventory
            }
            let suffix = key[
                (key.startIndex + scanRoot.prefix.count)..<key.endIndex
            ]
            var accumulator = SHA256Accumulator()
            digest.withUnsafeBytes { accumulator.update($0) }
            updateLegacyLength(UInt64(suffix.count), into: &accumulator)
            suffix.withUnsafeBytes { accumulator.update($0) }
            updateLegacyLength(UInt64(value.count), into: &accumulator)
            value.withUnsafeBytes { accumulator.update($0) }
            digest = accumulator.finalize()
            keyCount = try addLegacyCount(keyCount, 1)
            byteCount = try addLegacyCount(
                byteCount,
                try addLegacyCount(
                    UInt64(suffix.count),
                    UInt64(value.count)
                )
            )
        }
        return DatabaseLegacyLayoutTransferProgress(
            entryIndex: previous.entryIndex,
            continuation: continuation,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount,
            isComplete: false
        )
    }

    private static func rebaseLegacyKey(
        _ key: ByteString,
        from source: Subspace,
        to destination: Subspace
    ) throws -> ByteString {
        guard source.contains(key) else {
            throw DatabaseLegacyLayoutMigrationError.invalidInventory
        }
        return destination.prefix.appending(
            contentsOf: key[
                (key.startIndex + source.prefix.count)..<key.endIndex
            ]
        )
    }

    private static func rootsOverlap(
        _ lhs: Subspace,
        _ rhs: Subspace
    ) -> Bool {
        lhs.contains(rhs.prefix) || rhs.contains(lhs.prefix)
    }

    private static func updateLegacyLength(
        _ value: UInt64,
        into accumulator: inout SHA256Accumulator
    ) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) { accumulator.update($0) }
    }

    private static func addLegacyCount(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw DatabaseLegacyLayoutMigrationError.transferOverflow
        }
        return result.partialValue
    }
}

#if MultipleBases
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseEngine
import DatabaseFoundation
import StorageKit
import TestSupport
import Testing

@Suite("Legacy global layout migration", .serialized)
struct DatabaseLegacyLayoutMigrationTests {
    @Test("Authoritative legacy bytes move to one Base and old keys are removed")
    func authoritativeBytesMoveAndCutOver() async throws {
        let engine = InMemoryEngine()
        let clock = TestProcessMonotonicClock()
        _ = try await DatabaseFormatCatalog(
            database: engine,
            root: Subspace(),
            clock: clock
        ).installIfEmptyOrValidate(
            .v1(itemStorage: .v1)
        )

        let legacyRDFRoot = Subspace(prefix: Tuple([
            "_database-framework", "rdf-graph-store", Int64(1),
        ]).pack())
        let legacyKey = legacyRDFRoot.pack(Tuple("fixture"))
        let legacyValue = ByteString(utf8: "authoritative-rdf")
        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch,
            clock: clock
        ) { transaction in
            try transaction.setValue(legacyValue, for: legacyKey)
        }

        let domainID = try DatabaseStorageDomain.ID("primary")
        let placementID = try Base.Placement.ID("default")
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["database", "main"],
                    storageEngine: engine
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: placementID
        )
        let schema = try Schema(
            entities: [],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(
                storageTopology: topology,
                monotonicClock: clock,
                wallClock: RealtimeDatabaseWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        #expect(container.layoutStatus == .migrationRequired)
        let inventory = try await container.legacyLayoutInventory()
        let sourceFingerprint = try await container.legacyLayoutFingerprint(
            inventory: inventory
        )
        let baseID = try Base.ID("migrated")
        let prepared = try await container.prepareLegacyMigrationBase(
            baseID,
            placementID: placementID,
            initialGrants: [
                Security.Grant(
                    subject: .principal("migration-admin"),
                    resource: .base(baseID),
                    access: .all
                ),
            ],
            expectedRevision: 0
        )

        let copied = try await scanAll(
            container: container,
            inventory: inventory,
            destination: prepared,
            mode: .copy
        )
        let sourceAfterCopy = try await scanAll(
            container: container,
            inventory: inventory,
            destination: nil,
            mode: .source
        )
        let destination = try await scanAll(
            container: container,
            inventory: inventory,
            destination: prepared,
            mode: .destination
        )

        #expect(copied.digest == sourceFingerprint)
        #expect(sourceAfterCopy.digest == sourceFingerprint)
        #expect(destination.digest == sourceFingerprint)
        #expect(destination.keyCount == sourceAfterCopy.keyCount)
        #expect(destination.byteCount == sourceAfterCopy.byteCount)

        let rdfEntry = try #require(inventory.entries.first(where: {
            $0.identifier == "rdf-graph-store"
        }))
        let migratedKey = rdfEntry.destinationRoot(in: prepared.root)
            .pack(Tuple("fixture"))
        let migratedValue = try await preparedDomainValue(
            key: migratedKey,
            container: container,
            domainID: prepared.record.domainID
        )
        #expect(migratedValue == legacyValue)

        let active = try await container.rebuildAndCutOverLegacyMigration(
            record: prepared.record,
            root: prepared.root
        )
        #expect(active.lifecycle == .active)
        #expect(active.revision == 2)
        #expect(container.layoutStatus == .current)

        try await container.cleanupLegacyLayout(inventory: inventory)
        let legacyValueAfterCleanup = try await StorageTransactionExecutor(
            engine: engine
        ).withTransaction(
            configuration: .readOnly,
            clock: clock
        ) { transaction in
            try await transaction.getValue(
                for: legacyKey,
                snapshot: true
            )
        }
        #expect(legacyValueAfterCleanup == nil)
        #expect(
            try await preparedDomainValue(
                key: migratedKey,
                container: container,
                domainID: prepared.record.domainID
            ) == legacyValue
        )
    }

    private func scanAll(
        container: DBContainer,
        inventory: DatabaseLegacyLayoutInventory,
        destination: (record: DatabaseBaseRecord, root: Subspace)?,
        mode: DBContainer.LegacyLayoutTransferMode
    ) async throws -> DatabaseLegacyLayoutTransferProgress {
        var progress = DatabaseLegacyLayoutTransferProgress(
            entryIndex: 0,
            continuation: nil,
            digest: ByteString(repeating: 0, count: 32),
            keyCount: 0,
            byteCount: 0,
            isComplete: inventory.entries.isEmpty
        )
        while !progress.isComplete {
            progress = try await container.scanLegacyLayoutBatch(
                inventory: inventory,
                destinationBaseRoot: destination?.root,
                destinationDomainID: destination?.record.domainID,
                mode: mode,
                progress: progress
            )
        }
        return progress
    }

    private func preparedDomainValue(
        key: ByteString,
        container: DBContainer,
        domainID: DatabaseStorageDomain.ID
    ) async throws -> ByteString? {
        let domain = try #require(
            container.storageTopology.domain(identifiedBy: domainID)
        )
        return try await domain.transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: container.monotonicClock
        ) { transaction in
            try await transaction.getValue(for: key, snapshot: true)
        }
    }
}
#endif

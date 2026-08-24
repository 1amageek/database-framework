import DatabaseKit
import DatabaseRuntime
import Foundation
import ScalarIndex
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine

@Suite("OnlineIndexer Batch Dispatch Tests", .heartbeat)
struct OnlineIndexerBatchDispatchTests {
    @Test("Unique build records existing duplicates before readable transition")
    func uniqueBuildRejectsExistingDuplicates() async throws {
        let database = InMemoryEngine()
        let storeSubspace = Subspace(
            prefix: Tuple("test", "onlineindexer", "unique", UUID().uuidString).pack()
        )
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        let index = try ResolvedIndex(
            for: Player.self,
            name: "unique_player_name_idx",
            definition: .ordered(
                keys: [.ascending(Player.fields.name.identity)],
                includedFields: [],
                unique: true
            ),
            rootExpression: FieldKeyExpression(fieldName: "name")
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: Player.self,
                    including: [index.descriptor]
                )
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        Player.self,
                        including: [index.descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: storeSubspace
        )
        let maintainer = ScalarIndexMaintainer<Player>(
            index: index,
            subspace: try lifecycleStore.indexSubspace(for: index.name),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        try await lifecycleStore.enable(index.name)

        var first = Player.fixture(name: "duplicate", score: 1, level: 1)
        first.id = "first"
        var second = Player.fixture(name: "duplicate", score: 2, level: 1)
        second.id = "second"
        let players = [first, second]
        try await database.withTransaction { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace,
                configuration: .v1
            )
            for player in players {
                let key = itemSubspace
                    .subspace(Player.persistableType)
                    .pack(Tuple(player.id))
                try await storage.write(
                    try DataAccess.serialize(player),
                    for: key
                )
            }
        }

        let indexer = try OnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            uniquenessMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: 10
        )
        await #expect(
            throws: OnlineIndexBuildError.uniquenessViolationsDetected(
                indexName: index.name,
                violationCount: 1,
                totalConflictingEntities: 2
            )
        ) {
            try await indexer.buildIndex(clearFirst: true)
        }
        #expect(try await lifecycleStore.state(of: index.name) == .writeOnly)
        let tracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
        )
        let summary = try await tracker.violationSummary(indexName: index.name)
        #expect(summary.violationCount == 1)
        #expect(summary.totalConflictingEntities == 2)
    }

    @Test("Invalid build configuration fails before index state changes")
    func invalidBuildConfigurationFailsBeforeStateChanges() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(
            prefix: Tuple("test", "onlineindexer", "configuration", testId).pack()
        )
        let index = try PlayerIdentifierIndexDefinition.make(
            name: "invalid_configuration_idx"
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: Player.self,
                    including: [index.descriptor]
                )
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        Player.self,
                        including: [index.descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: storeSubspace
        )
        let maintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: try lifecycleStore.indexSubspace(for: index.name)
        )

        #expect(throws: OnlineIndexBuildError.invalidBatchSize(0)) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 0
            )
        }
        #expect(
            throws: OnlineIndexBuildError.invalidThrottleDelayMilliseconds(-1)
        ) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                throttleDelayMs: -1
            )
        }
        let unsupportedUniqueIndex = try ResolvedIndex(
            for: Player.self,
            name: "unsupported_unique_idx",
            definition: .ordered(
                keys: [.ascending(Player.fields.id.identity)],
                includedFields: [],
                unique: true
            ),
            rootExpression: index.rootExpression,
            itemTypes: index.itemTypes
        )
        #expect(
            throws: OnlineIndexBuildError.unsupportedUniquenessConstraint(
                indexName: unsupportedUniqueIndex.name
            )
        ) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: unsupportedUniqueIndex,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore
            )
        }

        let indexer = try OnlineIndexer<Player>(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore
        )
        await #expect(
            throws: OnlineIndexBuildError.invalidMaximumConcurrency(0)
        ) {
            try await indexer.buildIndexInParallel(maxConcurrency: 0)
        }
        await #expect(
            throws: OnlineIndexBuildError.invalidChunkSizeBytes(0)
        ) {
            try await indexer.buildIndexInParallel(chunkSizeBytes: 0)
        }

        #expect(try await lifecycleStore.state(of: index.name) == .disabled)
    }

    @Test("Build index dispatches complete batches to scanItems")
    func buildIndexDispatchesBatchesToScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "batch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        let index = try PlayerIdentifierIndexDefinition.make(
            name: "batch_hook_idx"
        )

        let schema = try Schema(entities: [
                try Schema.Entity(
                    from: Player.self,
                    including: [index.descriptor]
                )
            ],
            version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        Player.self,
                        including: [index.descriptor]
                    )
                ]),
            security: .testingDisabled
        )

        let batchSize = 5
        let players = PlayerDatasetGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: storeSubspace
        )
        let maintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: try lifecycleStore.indexSubspace(for: index.name)
        )

        try await lifecycleStore.enable(index.name)

        let indexer = try OnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: batchSize
        )

        try await indexer.buildIndex(clearFirst: true)

        #expect(maintainer.getBatchSizes() == [5, 5, 2])
        #expect(maintainer.getScanItemCallCount() == 0)
        #expect(maintainer.getUniqueProcessedCount() == players.count)
    }

}

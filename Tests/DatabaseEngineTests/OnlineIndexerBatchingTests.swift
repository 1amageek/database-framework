#if !os(WASI)
#if FOUNDATION_DB
// OnlineIndexerBatchingTests.swift
// Tests for OnlineIndexer transaction batching semantics.
//
// These tests verify that:
// The benchmark package owns dataset scaling and throughput measurement.

import Testing
import Foundation
@testable import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseKit
import StorageKit
import FDBStorage
import TestSupport

@Suite("OnlineIndexer batching tests", .tags(.requiresFDB), .foundationDBScenario, .serialized, .heartbeat)
struct OnlineIndexerBatchingTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Test Context

    struct BatchingIndexingContext: Sendable {
        let database: any StorageEngine
        let container: DBContainer
        let testSubspace: Subspace
        let itemSubspace: Subspace
        let blobsSubspace: Subspace

        init(index: ResolvedIndex) async throws {
            self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
            let testId = UUID().uuidString.prefix(8)
            self.testSubspace = Subspace(prefix: Tuple("test", "largedata", String(testId)).pack())
            self.itemSubspace = testSubspace.subspace(SubspaceKey.items)
            self.blobsSubspace = testSubspace.subspace(SubspaceKey.blobs)

            // Create container with Player schema
            let schema = try Schema(
                entities: [
                    try Schema.Entity(
                        from: Player.self,
                        including: [index.descriptor]
                    )
                ],
                version: Schema.Version(1, 0, 0)
            )
            self.container = try await DBContainer.open(
                for: schema, configuration: .testing(storageEngine: database),
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
                    ]), security: .testingDisabled)
        }

        func cleanup() async throws {
            try await database.withTransaction { tx in
                let range = testSubspace.range()
                try tx.clearRange(beginKey: range.begin, endKey: range.end)
            }
        }

        func insertPlayers(_ players: [Player]) async throws {
            // Batch inserts to avoid too many transactions
            let batchSize = 50
            for batchStart in stride(from: 0, to: players.count, by: batchSize) {
                let batchEnd = min(batchStart + batchSize, players.count)
                let batch = Array(players[batchStart..<batchEnd])
                try await database.withTransaction { tx in
                    let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace, configuration: .v1)
                    for player in batch {
                        let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                        let value = try DataAccess.serialize(player)
                        try await storage.write(value, for: key)
                    }
                }
            }
        }

    }

    @Test("Build index respects batch boundaries")
    func testBatchBoundaryProcessing() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "batch_test_idx"
            )
            let ctx = try await BatchingIndexingContext(index: index)

            let batchSize = 3
            // Generate exactly 3 batches + 1 remainder.
            let players = PlayerDatasetGenerator.generateForBatchTesting(
                batchSize: batchSize,
                batches: 3,
                remainder: 1
            )
            try await ctx.insertPlayers(players)

            let lifecycleStore = IndexLifecycleStore(
                container: ctx.container,
                subspace: ctx.testSubspace
            )
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: try lifecycleStore.indexSubspace(for: index.name)
            )

            try await lifecycleStore.enable(index.name)

        let indexer = try OnlineIndexer(
            container: ctx.container,
            storeSubspace: ctx.testSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: batchSize
        )

            try await indexer.buildIndex(clearFirst: true)

            // Verify all items were indexed exactly once
            #expect(maintainer.getUniqueProcessedCount() == players.count)
            #expect(maintainer.getTotalProcessCount() == players.count)
            #expect(maintainer.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    // MARK: - Edge Cases

    @Test("Build index with empty dataset")
    func testBuildIndexWithEmptyDataset() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "empty_idx"
            )
            let ctx = try await BatchingIndexingContext(index: index)

            let lifecycleStore = IndexLifecycleStore(
                container: ctx.container,
                subspace: ctx.testSubspace
            )
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: try lifecycleStore.indexSubspace(for: index.name)
            )

            try await lifecycleStore.enable(index.name)

            let indexer = try OnlineIndexer(
                container: ctx.container,
                storeSubspace: ctx.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 100
            )

            // Should complete without error
            try await indexer.buildIndex(clearFirst: true)

            #expect(maintainer.getUniqueProcessedCount() == 0)
            try await ctx.cleanup()
        }

    }

    @Test("Build index with single item")
    func testBuildIndexWithSingleItem() async throws {
        let index = try PlayerIdentifierIndexDefinition.make(name: "single_idx")
        let ctx = try await BatchingIndexingContext(index: index)

        var player = Player(name: "Only One", score: 100, level: 1)
        player.id = "single"
        try await ctx.insertPlayers([player])

        let lifecycleStore = IndexLifecycleStore(
            container: ctx.container,
            subspace: ctx.testSubspace
        )
        let maintainer = CountingIndexMaintainer<Player>(
            indexSubspace: try lifecycleStore.indexSubspace(for: index.name)
        )

        try await lifecycleStore.enable(index.name)

        let indexer = try OnlineIndexer(
            container: ctx.container,
            storeSubspace: ctx.testSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: 100
        )

        try await indexer.buildIndex(clearFirst: true)

        #expect(maintainer.getUniqueProcessedCount() == 1)
        #expect(maintainer.getTotalProcessCount() == 1)

        try await ctx.cleanup()
    }
}
#endif

#endif

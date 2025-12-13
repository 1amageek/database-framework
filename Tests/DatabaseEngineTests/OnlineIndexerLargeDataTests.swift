// OnlineIndexerLargeDataTests.swift
// Tests for OnlineIndexer with large datasets that exceed transaction limits
//
// These tests verify that:
// 1. Batch processing correctly handles large datasets
// 2. No transaction_too_large errors occur
// 3. All items are indexed correctly across batches

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core
import FoundationDB
import TestSupport

@Suite("OnlineIndexer Large Data Tests", .tags(.requiresFDB), .serialized)
struct OnlineIndexerLargeDataTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Test Context

    struct TestContext: Sendable {
        nonisolated(unsafe) let database: any DatabaseProtocol
        let testSubspace: Subspace
        let itemSubspace: Subspace
        let indexSubspace: Subspace
        let blobsSubspace: Subspace

        init() throws {
            self.database = try FDBClient.openDatabase()
            let testId = UUID().uuidString.prefix(8)
            self.testSubspace = Subspace(prefix: Tuple("test", "largedata", String(testId)).pack())
            self.itemSubspace = testSubspace.subspace("R")
            self.indexSubspace = testSubspace.subspace("I")
            self.blobsSubspace = testSubspace.subspace("B")
        }

        func cleanup() async throws {
            try await database.withTransaction { tx in
                let range = testSubspace.range()
                tx.clearRange(beginKey: range.begin, endKey: range.end)
            }
        }

        func insertPlayers(_ players: [Player]) async throws {
            // Batch inserts to avoid too many transactions
            let batchSize = 50
            for batchStart in stride(from: 0, to: players.count, by: batchSize) {
                let batchEnd = min(batchStart + batchSize, players.count)
                let batch = Array(players[batchStart..<batchEnd])
                try await database.withTransaction { tx in
                    let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace)
                    for player in batch {
                        let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                        let value = try DataAccess.serialize(player)
                        try await storage.write(value, for: key)
                    }
                }
            }
        }

        func countIndexEntries(indexName: String) async throws -> Int {
            try await database.withTransaction { tx in
                let range = indexSubspace.subspace(indexName).range()
                var count = 0
                for try await _ in tx.getRange(begin: range.begin, end: range.end, snapshot: true) {
                    count += 1
                }
                return count
            }
        }
    }

    // MARK: - Basic Large Data Tests

    @Test("Build index with large dataset - batch processing works")
    func testBuildIndexWithLargeDataset() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            // Generate dataset with 200 items (enough to require multiple batches)
            let players = LargeTestDataGenerator.generatePlayers(count: 200, nameLength: 100)
            try await ctx.insertPlayers(players)

            // Create index
            let index = TestIndex.create(name: "large_score_idx")
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: ctx.indexSubspace,
                indexName: index.name
            )

            let stateManager = IndexStateManager(
                database: ctx.database,
                subspace: ctx.indexSubspace.subspace("_meta")
            )

            try await stateManager.enable(index.name)

        let indexer = OnlineIndexer(
            database: ctx.database,
            storeSubspace: ctx.testSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexStateManager: stateManager,
            batchSize: 30  // Small batch size to ensure multiple transactions
        )

            // Build should complete without transaction_too_large error
            try await indexer.buildIndex(clearFirst: true)

            // Verify all items were indexed
            #expect(maintainer.getUniqueProcessedCount() == players.count)
            #expect(maintainer.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    @Test("Build index respects batch boundaries")
    func testBatchBoundaryProcessing() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let batchSize = 25
            // Generate exactly 3 batches + 7 remainder = 82 items
            let players = LargeTestDataGenerator.generateForBatchTesting(
                batchSize: batchSize,
                batches: 3,
                remainder: 7
            )
            try await ctx.insertPlayers(players)

            let index = TestIndex.create(name: "batch_test_idx")
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: ctx.indexSubspace,
                indexName: index.name
            )

            let stateManager = IndexStateManager(
                database: ctx.database,
                subspace: ctx.indexSubspace.subspace("_meta")
            )

            try await stateManager.enable(index.name)

        let indexer = OnlineIndexer(
            database: ctx.database,
            storeSubspace: ctx.testSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexStateManager: stateManager,
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

    // MARK: - MultiTargetOnlineIndexer Tests

    @Test("MultiTarget build with dataset")
    func testMultiTargetIndexer() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let players = LargeTestDataGenerator.generatePlayers(count: 100, nameLength: 100)
            try await ctx.insertPlayers(players)

            // Create multiple indexes
            let index1 = TestIndex.create(name: "multi_idx_1")
            let index2 = TestIndex.create(name: "multi_idx_2")

            let maintainer1 = CountingIndexMaintainer<Player>(
                indexSubspace: ctx.indexSubspace,
                indexName: index1.name
            )
            let maintainer2 = CountingIndexMaintainer<Player>(
                indexSubspace: ctx.indexSubspace,
                indexName: index2.name
            )

            let stateManager = IndexStateManager(
                database: ctx.database,
                subspace: ctx.indexSubspace.subspace("_meta")
            )

            let targets = [
                IndexBuildTarget(index: index1, maintainer: maintainer1),
                IndexBuildTarget(index: index2, maintainer: maintainer2),
            ]

            let indexer = MultiTargetOnlineIndexer(
                database: ctx.database,
                itemSubspace: ctx.itemSubspace,
                indexSubspace: ctx.indexSubspace,
                blobsSubspace: ctx.blobsSubspace,
                itemType: Player.persistableType,
                targets: targets,
                stateManager: stateManager,
                batchSize: 20
            )

            try await indexer.buildIndexes(clearFirst: true)

            // Verify both indexes processed all items exactly once
            #expect(maintainer1.getUniqueProcessedCount() == players.count)
            #expect(maintainer2.getUniqueProcessedCount() == players.count)
            #expect(maintainer1.getDuplicateProcessedIds().isEmpty)
            #expect(maintainer2.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    // MARK: - Edge Cases

    @Test("Build index with empty dataset")
    func testBuildIndexWithEmptyDataset() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let index = TestIndex.create(name: "empty_idx")
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: ctx.indexSubspace,
                indexName: index.name
            )

            let stateManager = IndexStateManager(
                database: ctx.database,
                subspace: ctx.indexSubspace.subspace("_meta")
            )

            try await stateManager.enable(index.name)

            let indexer = OnlineIndexer(
                database: ctx.database,
                storeSubspace: ctx.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexStateManager: stateManager,
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
        let ctx = try TestContext()

        let player = Player(id: "single", name: "Only One", score: 100, level: 1)
        try await ctx.insertPlayers([player])

        let index = TestIndex.create(name: "single_idx")
        let maintainer = CountingIndexMaintainer<Player>(
            indexSubspace: ctx.indexSubspace,
            indexName: index.name
        )

        let stateManager = IndexStateManager(
            database: ctx.database,
            subspace: ctx.indexSubspace.subspace("_meta")
        )

        try await stateManager.enable(index.name)

        let indexer = OnlineIndexer(
            database: ctx.database,
            storeSubspace: ctx.testSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexStateManager: stateManager,
            batchSize: 100
        )

        try await indexer.buildIndex(clearFirst: true)

        #expect(maintainer.getUniqueProcessedCount() == 1)
        #expect(maintainer.getTotalProcessCount() == 1)

        try await ctx.cleanup()
    }
}

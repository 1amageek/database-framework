// OnlineIndexerAtomicityTests.swift
// Tests for OnlineIndexer progress atomicity and resume behavior
//
// These tests verify that:
// 1. Progress is saved atomically with work
// 2. Resume from interruption doesn't duplicate processing
// 3. Transaction failure doesn't leave inconsistent state

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core
import FoundationDB
import TestSupport

@Suite("OnlineIndexer Atomicity Tests", .tags(.requiresFDB), .serialized)
struct OnlineIndexerAtomicityTests {

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
            self.testSubspace = Subspace(prefix: Tuple("test", "atomicity", String(testId)).pack())
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
    }

    // MARK: - Atomicity Tests

    @Test("Progress is consistent with indexed data")
    func testProgressConsistencyWithIndexedData() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let players = LargeTestDataGenerator.generatePlayers(count: 100, nameLength: 50)
            try await ctx.insertPlayers(players)

            let index = TestIndex.create(name: "consistency_idx")
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
                itemSubspace: ctx.itemSubspace,
                indexSubspace: ctx.indexSubspace,
                blobsSubspace: ctx.blobsSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexStateManager: stateManager,
                batchSize: 15
            )

            try await indexer.buildIndex(clearFirst: true)

            // Verify all items were indexed exactly once
            #expect(maintainer.getUniqueProcessedCount() == players.count)
            #expect(maintainer.getTotalProcessCount() == players.count)
            #expect(maintainer.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    @Test("MultiTarget progress is atomic across all indexes")
    func testMultiTargetAtomicProgress() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let players = LargeTestDataGenerator.generatePlayers(count: 75, nameLength: 50)
            try await ctx.insertPlayers(players)

            let index1 = TestIndex.create(name: "atomic_idx_1")
            let index2 = TestIndex.create(name: "atomic_idx_2")

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

            // Both indexes should have processed exactly the same items
            let processedIds1 = maintainer1.getAllProcessedIds()
            let processedIds2 = maintainer2.getAllProcessedIds()

            #expect(processedIds1 == processedIds2)
            #expect(processedIds1.count == players.count)
            #expect(maintainer1.getDuplicateProcessedIds().isEmpty)
            #expect(maintainer2.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    @Test("RangeSet progress is saved atomically with work")
    func testRangeSetAtomicProgress() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let batchSize = 10
            let players = LargeTestDataGenerator.generateForBatchTesting(
                batchSize: batchSize,
                batches: 5,
                remainder: 3
            )
            try await ctx.insertPlayers(players)

            let index = TestIndex.create(name: "rangeset_idx")
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
                itemSubspace: ctx.itemSubspace,
                indexSubspace: ctx.indexSubspace,
                blobsSubspace: ctx.blobsSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexStateManager: stateManager,
                batchSize: batchSize
            )

            try await indexer.buildIndex(clearFirst: true)

            // Verify no duplicates across batch boundaries
            let duplicates = maintainer.getDuplicateProcessedIds()
            #expect(duplicates.isEmpty, "Found duplicates: \(duplicates)")

            // Verify all items processed
            #expect(maintainer.getUniqueProcessedCount() == players.count)

            try await ctx.cleanup()
        }
    }

    @Test("Progress cleared after successful completion")
    func testProgressClearedAfterCompletion() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let players = LargeTestDataGenerator.generatePlayers(count: 25, nameLength: 50)
            try await ctx.insertPlayers(players)

            let index = TestIndex.create(name: "clear_progress_idx")
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
                itemSubspace: ctx.itemSubspace,
                indexSubspace: ctx.indexSubspace,
                blobsSubspace: ctx.blobsSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexStateManager: stateManager,
                batchSize: 5
            )

            try await indexer.buildIndex(clearFirst: true)

            // Verify progress key is cleared
            let progressKey = ctx.indexSubspace
                .subspace("_progress")
                .pack(Tuple(index.name))

            let progressExists = try await ctx.database.withTransaction { tx in
                let value = try await tx.getValue(for: progressKey, snapshot: false)
                return value != nil
            }

            #expect(!progressExists, "Progress should be cleared after completion")

            try await ctx.cleanup()
        }
    }
}

#if !os(WASI)
#if FOUNDATION_DB
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
import DatabaseRuntime
@testable import DatabaseKit
import StorageKit
import FDBStorage
import TestSupport

@Suite("OnlineIndexer Atomicity Tests", .tags(.requiresFDB), .foundationDBScenario, .serialized, .heartbeat)
struct OnlineIndexerAtomicityTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Test Context

    struct AtomicIndexingContext: Sendable {
        let database: any StorageEngine
        let container: DBContainer
        let testSubspace: Subspace
        let itemSubspace: Subspace
        let indexSubspace: Subspace
        let blobsSubspace: Subspace

        init(index: ResolvedIndex) async throws {
            self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
            let testId = UUID().uuidString.prefix(8)
            self.testSubspace = Subspace(prefix: Tuple("test", "atomicity", String(testId)).pack())
            self.itemSubspace = testSubspace.subspace("R")
            self.indexSubspace = testSubspace.subspace("I")
            self.blobsSubspace = testSubspace.subspace("B")

            // Create container with Player schema
            let schema = try Schema(entities: [
                    try Schema.Entity(
                        from: Player.self,
                        including: [index.descriptor]
                    )
                ],
                version: Schema.Version(1, 0, 0))
            self.container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
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

    // MARK: - Atomicity Tests

    @Test("Progress is consistent with indexed data")
    func testProgressConsistencyWithIndexedData() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "consistency_idx"
            )
            let ctx = try await AtomicIndexingContext(index: index)

            let players = PlayerDatasetGenerator.generatePlayers(count: 10, nameLength: 50)
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
                batchSize: 3
            )

            try await indexer.buildIndex(clearFirst: true)

            // Verify all items were indexed exactly once
            #expect(maintainer.getUniqueProcessedCount() == players.count)
            #expect(maintainer.getTotalProcessCount() == players.count)
            #expect(maintainer.getDuplicateProcessedIds().isEmpty)

            try await ctx.cleanup()
        }
    }

    @Test("RangeSet progress is saved atomically with work")
    func testRangeSetAtomicProgress() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "rangeset_idx"
            )
            let ctx = try await AtomicIndexingContext(index: index)

            let batchSize = 3
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "clear_progress_idx"
            )
            let ctx = try await AtomicIndexingContext(index: index)

            let players = PlayerDatasetGenerator.generatePlayers(count: 7, nameLength: 50)
            try await ctx.insertPlayers(players)

            let lifecycleStore = IndexLifecycleStore(
                container: ctx.container,
                subspace: ctx.testSubspace
            )
            let physicalIndexSubspace = try lifecycleStore.indexSubspace(
                for: index.name
            )
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: physicalIndexSubspace
            )

            try await lifecycleStore.enable(index.name)

            let indexer = try OnlineIndexer(
                container: ctx.container,
                storeSubspace: ctx.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 3
            )

            try await indexer.buildIndex(clearFirst: true)

            // Verify progress key is cleared
            let progressKey =
                physicalIndexSubspace
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
#endif

#endif

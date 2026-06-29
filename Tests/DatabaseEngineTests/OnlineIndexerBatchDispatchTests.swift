import Testing
import Foundation
import StorageKit
import Core
import TestSupport
@testable import DatabaseEngine

@Suite("OnlineIndexer Batch Dispatch Tests", .heartbeat)
struct OnlineIndexerBatchDispatchTests {
    @Test("Build index dispatches complete batches to scanItems")
    func buildIndexDispatchesBatchesToScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "batch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = Schema([Player.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )

        let batchSize = 5
        let players = LargeTestDataGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let index = TestIndex.create(name: "batch_hook_idx")
        let maintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: index.name
        )

        let stateManager = IndexStateManager(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        try await stateManager.enable(index.name)

        let indexer = OnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexStateManager: stateManager,
            batchSize: batchSize
        )

        try await indexer.buildIndex(clearFirst: true)

        #expect(maintainer.getBatchSizes() == [5, 5, 2])
        #expect(maintainer.getScanItemCallCount() == 0)
        #expect(maintainer.getUniqueProcessedCount() == players.count)
    }

    @Test("Multi-target build dispatches complete batches to each scanItems")
    func multiTargetBuildDispatchesBatchesToEachScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "multiBatch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = Schema([Player.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )

        let batchSize = 5
        let players = LargeTestDataGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let firstIndex = TestIndex.create(name: "multi_batch_hook_1")
        let secondIndex = TestIndex.create(name: "multi_batch_hook_2")
        let firstMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: firstIndex.name
        )
        let secondMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: secondIndex.name
        )

        let stateManager = IndexStateManager(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        let indexer = MultiTargetOnlineIndexer(
            container: container,
            itemSubspace: itemSubspace,
            indexSubspace: indexSubspace,
            blobsSubspace: blobsSubspace,
            itemType: Player.persistableType,
            targets: [
                IndexBuildTarget(index: firstIndex, maintainer: firstMaintainer),
                IndexBuildTarget(index: secondIndex, maintainer: secondMaintainer)
            ],
            stateManager: stateManager,
            batchSize: batchSize
        )

        try await indexer.buildIndexes(clearFirst: true)

        #expect(firstMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(secondMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(firstMaintainer.getScanItemCallCount() == 0)
        #expect(secondMaintainer.getScanItemCallCount() == 0)
        #expect(firstMaintainer.getUniqueProcessedCount() == players.count)
        #expect(secondMaintainer.getUniqueProcessedCount() == players.count)
    }

    @Test("Mutual build dispatches complete batches to both scanItems")
    func mutualBuildDispatchesBatchesToBothScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "mutualBatch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = Schema([Player.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )

        let batchSize = 5
        let players = LargeTestDataGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let forwardIndex = TestIndex.create(name: "mutual_batch_forward")
        let reverseIndex = TestIndex.create(name: "mutual_batch_reverse")
        let forwardMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: forwardIndex.name
        )
        let reverseMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: reverseIndex.name
        )

        let stateManager = IndexStateManager(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        let indexer = MutualOnlineIndexer(
            container: container,
            itemSubspace: itemSubspace,
            indexSubspace: indexSubspace,
            blobsSubspace: blobsSubspace,
            itemType: Player.persistableType,
            forwardIndex: forwardIndex,
            reverseIndex: reverseIndex,
            forwardMaintainer: forwardMaintainer,
            reverseMaintainer: reverseMaintainer,
            stateManager: stateManager,
            batchSize: batchSize
        )

        try await indexer.buildIndexes(clearFirst: true)

        #expect(forwardMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(reverseMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(forwardMaintainer.getScanItemCallCount() == 0)
        #expect(reverseMaintainer.getScanItemCallCount() == 0)
        #expect(forwardMaintainer.getUniqueProcessedCount() == players.count)
        #expect(reverseMaintainer.getUniqueProcessedCount() == players.count)
    }
}
